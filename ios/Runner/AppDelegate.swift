import UIKit
import Flutter
import Firebase
import FirebaseMessaging
import AVFoundation
import PushKit
import CallKit
import PushToTalk
import Foundation
import CryptoKit

// ─────────────────────────────────────────────────────────────────────────────
// MARK: - NativePTTPlayer
// Pure-Swift background WebSocket + audio player.
// Runs entirely without Flutter — works when phone is locked/app suspended.
// ─────────────────────────────────────────────────────────────────────────────
class NativePTTPlayer: NSObject, URLSessionWebSocketDelegate {

    static let shared = NativePTTPlayer()
    private override init() { super.init() }

    private var webSocketTask: URLSessionWebSocketTask?
    private var currentDisconnectToken: UUID?
    private var endTransactionTimer: Timer?
    
    // ✅ For Native Sending (Lock Screen PTT)
    var currentGroupId: String?
    private var audioRecorder: AVAudioRecorder?
    private var chunkTimer: Timer?
    private var currentRecordFileUrl: URL?
    private var isTransmitting = false

    // ✅ Must wait for PushToTalk to fully activate before playing!
    var isAudioSessionActive = false 

    private lazy var urlSession: URLSession = {
        let config = URLSessionConfiguration.default
        config.timeoutIntervalForRequest = 30
        return URLSession(configuration: config, delegate: self, delegateQueue: nil)
    }()

    private static let defaultServerUrl = "wss://ptt.visionvivante.in"

    private var audioPlayer: AVAudioPlayer?
    private var isReceiving = false
    private var disconnectTimer: Timer?
    private var connectedGroupId: String?
    private var currentPlaybackFileUrl: URL?

    // ✅ Queue for audio chunks so they don't overlap
    private var audioQueue: [Data] = []
    private var isPlaying = false

    // ✅ Called when a VoIP push arrives — connect and play in background
    func startBackgroundReceive(groupId: String) {
        // Read current userId that Flutter saved in SharedPreferences (UserDefaults key: flutter.ptt_user_id)
        let userId = UserDefaults.standard.string(forKey: "flutter.ptt_user_id") ?? ""
        guard !userId.isEmpty else {
            print("⚠️ NativePTTPlayer: No userId in UserDefaults — cannot connect")
            return
        }
        
        // Skip only if ALREADY connected to the exact same group.
        // A different groupId means a new conversation channel — always reconnect.
        // This is critical: applicationDidEnterBackground pre-emptively connects on the
        // last known group; when a VoIP push arrives for a DIFFERENT group, we must
        // NOT return early or that push's audio is silently dropped.
        if isReceiving && webSocketTask != nil && connectedGroupId == groupId {
            print("✅ NativePTTPlayer: Already receiving on group \(groupId) — skipping duplicate push")
            return
        }

        // Disconnect previous connection WITHOUT resetting isAudioSessionActive.
        // iOS may NOT re-fire sessionDidActivate if the PTT session is already active!
        softDisconnect()
        isReceiving = true
        connectedGroupId = groupId

        print("🔊 NativePTTPlayer: Connecting as \(userId) to receive group \(groupId)")

        let serverUrl = UserDefaults.standard.string(forKey: "flutter.ptt_server_url") ?? NativePTTPlayer.defaultServerUrl
        print("🔗 Using PTT server: \(serverUrl)")

        guard let url = URL(string: serverUrl) else {
            print("❌ Invalid PTT server URL: \(serverUrl)")
            return
        }
        webSocketTask = urlSession.webSocketTask(with: url)
        webSocketTask?.resume()

        // Register — include stored VoIP token so server can wake this device
        var registerMsg = ["type": "register", "userId": userId]
        if let token = UserDefaults.standard.string(forKey: "voip_token"), !token.isEmpty {
            registerMsg["voipToken"] = token
            registerMsg["tokenType"] = UserDefaults.standard.string(forKey: "voip_token_type") ?? "ptt"
        }
        sendMessage(registerMsg)
        // Join the target group (the groupId from the push payload = sender's channel)
        sendMessage(["type": "switch", "newGroupId": groupId])

        // Start receiving audio chunks
        receiveNextMessage()

        // Auto-disconnect after 45s (saves battery, PTT messages are short)
        DispatchQueue.main.asyncAfter(deadline: .now() + 45) { [weak self] in
            self?.disconnect()
        }
    }

    private func sendMessage(_ dict: [String: String]) {
        guard let data = try? JSONSerialization.data(withJSONObject: dict),
              let str = String(data: data, encoding: .utf8) else { return }
        webSocketTask?.send(.string(str)) { _ in }
    }

    private func receiveNextMessage() {
        guard isReceiving else { return }
        webSocketTask?.receive { [weak self] result in
            guard let self = self, self.isReceiving else { return }
            switch result {
            case .success(let message):
                DispatchQueue.main.async {
                    self.endTransactionTimer?.invalidate() // ✅ Cancel the shutdown, more chunks are arriving!
                }
                if case .string(let text) = message {
                    self.handleMessage(text)
                }
                self.receiveNextMessage() // ✅ Keep listening for more chunks
            case .failure(let error):
                print("⚠️ NativePTTPlayer receive error: \(error.localizedDescription)")
                self.isReceiving = false
            }
        }
    }

    private func handleMessage(_ text: String) {
        guard let data = text.data(using: .utf8),
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let type = json["type"] as? String,
              type == "audio",
              let chunk = json["chunk"] as? String,
              let audioData = Data(base64Encoded: chunk) else { return }

        // ✅ FIX: Ignore our own audio chunks to prevent local echo
        let senderId = json["sender"] as? String ?? ""
        let myUserId = UserDefaults.standard.string(forKey: "flutter.ptt_user_id") ?? ""
        if !senderId.isEmpty && senderId == myUserId {
            print("🔇 NativePTTPlayer: Ignoring our own audio chunk")
            return
        }

        print("🔊 NativePTTPlayer: Received \(audioData.count) bytes of audio")
        
        DispatchQueue.main.async {
            self.audioQueue.append(audioData)
            print("📦 Queue size: \(self.audioQueue.count), isPlaying: \(self.isPlaying), sessionActive: \(self.isAudioSessionActive)")
            // ⚡ If sessionDidActivate already fired (consecutive push), play immediately.
            // If session isn't active yet, processQueue() will be triggered by sessionDidActivate.
            self.forceStartIfSessionActive()
        }
    }

    // Called from AppDelegate when the system is absolutely ready
    func sessionDidActivate(audioSession: AVAudioSession? = nil) {
        isAudioSessionActive = true
        
        // 🚨 CRITICAL APPLE RULE: If the system provided the audioSession (via PushToTalk),
        // it is ALREADY active and fully configured for Walkie-Talkie (including Speaker routing).
        // Modifying the category or overriding the port will crash Apple's internal routing!
        if audioSession == nil {
            let session = AVAudioSession.sharedInstance()
            // Interrupt other audio (Spotify, Instagram, etc.) so PTT is clearly heard.
            // .notifyOthersOnDeactivation tells Spotify to resume when we release the session.
            // No .mixWithOthers — PTT must take over the audio focus exclusively.
            // Use .playAndRecord which reliably supports Bluetooth A2DP + HFP.
            // .playback + .allowBluetoothA2DP fails (-50) when PTChannelManager resets
            // the session between our setActive and the retry. .playAndRecord is what
            // PTChannelManager itself uses and is not rejected by the system.
            // .defaultToSpeaker routes to loud speaker when no BT device is connected.
            // .allowBluetooth = HFP (car / older BT), .allowBluetoothA2DP = A2DP (AirPods / modern BT).
            do {
                try session.setCategory(.playAndRecord, mode: .voiceChat, options: [.defaultToSpeaker, .allowBluetooth, .allowBluetoothA2DP])
                print("🔊 NativePTTPlayer: Audio category set — BT + speaker ready")
            } catch {
                try? session.setCategory(.playback, mode: .default, options: [])
                print("⚠️ NativePTTPlayer: setCategory failed (\(error)) — using plain playback")
            }
            do {
                try session.setActive(true, options: .notifyOthersOnDeactivation)
                print("🔊 NativePTTPlayer: Session activated — Spotify/other audio paused")
            } catch {
                print("⚠️ NativePTTPlayer: setActive failed (\(error)) — will attempt playback anyway")
            }
        } else {
            print("🔊 NativePTTPlayer: PushToTalk audio session active")
            do {
                try AVAudioSession.sharedInstance().setCategory(.playAndRecord, mode: .voiceChat, options: [.defaultToSpeaker, .allowBluetooth, .allowBluetoothA2DP, .allowAirPlay])
                print("🔊 Unified LiveKit PushToTalk Session configured")
            } catch {
                print("⚠️ Failed to configure PushToTalk session: \(error)")
            }
        }

        processQueue() // Start playing any queued chunks!
        
        // If the user pressed Talk and we were waiting for the session to activate:
        if isTransmitting && audioRecorder == nil {
            print("🎙️ Starting to record audio chunks (Session is now active)...")
            startRecordingChunk()

            chunkTimer?.invalidate()
            chunkTimer = Timer.scheduledTimer(withTimeInterval: 1.5, repeats: true) { [weak self] _ in
                self?.flushAndContinueRecording()
            }
            print("⏱️ Chunk timer started (1.5s interval)")
        }
    }

    // Called when a NEW push arrives and session might ALREADY be active.
    // Force-start the queue immediately without waiting for sessionDidActivate.
    func forceStartIfSessionActive() {
        if isAudioSessionActive {
            print("⚡ NativePTTPlayer: Session already active — force-starting queue")
            processQueue()
        }
    }

    private func processQueue() {
        print("🎬 processQueue called: sessionActive=\(isAudioSessionActive), isPlaying=\(isPlaying), queueSize=\(audioQueue.count)")
        guard isAudioSessionActive, !isPlaying, !audioQueue.isEmpty else { 
            if !isAudioSessionActive {
                print("⚠️ Cannot process queue: audio session not active yet")
            }
            return 
        }
        isPlaying = true
        
        let data = audioQueue.removeFirst()
        
        // ✅ FIX: Hardware Amplifier Warmup
        // If the audio player was completely stopped, the iOS hardware amplifier takes ~500ms to physically turn on.
        // If we play immediately, the first half-second of audio is swallowed (which ruins short 1-second messages).
        // We delay the very FIRST chunk by 500ms. Subsequent chunks will play instantly because the amp is already warm.
        if audioPlayer == nil {
            print("⏳ Waiting 500ms for hardware amplifier to warm up before playing first chunk...")
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) { [weak self] in
                self?.playAudio(data: data)
            }
        } else {
            playAudio(data: data)
        }
    }

    private func playAudio(data: Data) {
        do {
            let tempDir = FileManager.default.temporaryDirectory
            let tempFileUrl = tempDir.appendingPathComponent(UUID().uuidString + ".m4a")
            try data.write(to: tempFileUrl)
            currentPlaybackFileUrl = tempFileUrl

            audioPlayer = try AVAudioPlayer(contentsOf: tempFileUrl)
            audioPlayer?.delegate = self
            audioPlayer?.volume = 1.0
            audioPlayer?.prepareToPlay()
            audioPlayer?.play()
            print("✅ NativePTTPlayer: Audio chunk playing at full volume on speaker")
        } catch {
            print("❌ NativePTTPlayer: Audio playback error: \(error)")
            self.isPlaying = false
            self.processQueue() // skip to next
        }
    }

    // 🔌 Soft disconnect: closes WebSocket but PRESERVES isAudioSessionActive.
    // Use this between consecutive pushes when the PTT session may still be active.
    // iOS will NOT re-fire sessionDidActivate if already active!
    func softDisconnect() {
        isReceiving = false
        isPlaying = false
        audioQueue.removeAll()
        audioPlayer?.stop()
        audioPlayer = nil
        webSocketTask?.cancel(with: .normalClosure, reason: nil)
        webSocketTask = nil
        print("🔌 NativePTTPlayer: Soft-disconnected (audio session state preserved)")
    }

    // Sends a lightweight register message to the live WebSocket (if connected) or
    // opens a short-lived connection just to deliver the token update.
    func registerTokenWithServer(userId: String, token: String, tokenType: String) {
        let serverUrl = UserDefaults.standard.string(forKey: "flutter.ptt_server_url") ?? NativePTTPlayer.defaultServerUrl
        guard let url = URL(string: serverUrl) else { return }
        // If already connected reuse the existing socket
        if let task = webSocketTask {
            let msg: [String: String] = ["type": "register", "userId": userId, "voipToken": token, "tokenType": tokenType]
            if let data = try? JSONSerialization.data(withJSONObject: msg),
               let str = String(data: data, encoding: .utf8) {
                task.send(.string(str)) { _ in }
            }
            return
        }
        // No existing socket — open a one-shot connection just to push the token
        let tmpSession = URLSession(configuration: .default)
        let tmpTask = tmpSession.webSocketTask(with: url)
        tmpTask.resume()
        let msg: [String: String] = ["type": "register", "userId": userId, "voipToken": token, "tokenType": tokenType]
        if let data = try? JSONSerialization.data(withJSONObject: msg),
           let str = String(data: data, encoding: .utf8) {
            tmpTask.send(.string(str)) { _ in
                tmpTask.cancel(with: .normalClosure, reason: nil)
            }
        }
    }

    // 🔴 Full disconnect: resets ALL state including audio session flag.
    // Only call this when the PTT session has officially ended (didDeactivate fires).
    func disconnect() {
        isReceiving = false
        isTransmitting = false
        isAudioSessionActive = false // ✅ ONLY reset here (when PTT session truly ends)
        isPlaying = false
        connectedGroupId = nil
        currentPlaybackFileUrl = nil
        audioQueue.removeAll()
        audioPlayer?.stop()
        audioPlayer = nil
        chunkTimer?.invalidate()
        chunkTimer = nil
        audioRecorder?.stop()
        audioRecorder = nil
        webSocketTask?.cancel(with: .normalClosure, reason: nil)
        webSocketTask = nil
        // Release audio session so Spotify / other interrupted apps resume automatically
        try? AVAudioSession.sharedInstance().setActive(false, options: .notifyOthersOnDeactivation)
        print("🔌 NativePTTPlayer: Disconnected — audio session released, other apps can resume")
    }

    // ────────────────────────────────────────────────────────────
    // MARK: - Native Transmitting (Lock Screen PTT)
    // ────────────────────────────────────────────────────────────
    func startTransmitting(groupId: String) {
        print("🎤 startTransmitting called for group: \(groupId)")
        
        let userId = UserDefaults.standard.string(forKey: "flutter.ptt_user_id") ?? ""
        print("👤 User ID: \(userId)")
        
        guard !userId.isEmpty else {
            print("❌ Cannot transmit: No userId in UserDefaults")
            return
        }

        isTransmitting = true
        self.currentGroupId = groupId
        print("✅ isTransmitting = true, currentGroupId = \(groupId)")

        // Ensure WebSocket is connected
        if webSocketTask == nil {
            print("🔌 WebSocket is nil, creating new connection...")
            let serverUrl = UserDefaults.standard.string(forKey: "flutter.ptt_server_url") ?? NativePTTPlayer.defaultServerUrl
            print("🔗 Using PTT server for transmit: \(serverUrl)")
            
            guard let url = URL(string: serverUrl) else { 
                print("❌ Invalid PTT server URL: \(serverUrl)")
                return 
            }
            webSocketTask = urlSession.webSocketTask(with: url)
            webSocketTask?.resume()
            var registerMsg = ["type": "register", "userId": userId]
            if let token = UserDefaults.standard.string(forKey: "voip_token"), !token.isEmpty {
                registerMsg["voipToken"] = token
                registerMsg["tokenType"] = UserDefaults.standard.string(forKey: "voip_token_type") ?? "ptt"
            }
            print("📡 Sent register message for userId: \(userId)")
            sendMessage(registerMsg)
            print("📡 Sent switch message to group: \(groupId)")
            sendMessage(["type": "switch", "newGroupId": groupId])
            receiveNextMessage()
        } else {
            print("♻️ Reusing existing WebSocket connection")
            print("📡 Sent switch message to group: \(groupId)")
            sendMessage(["type": "switch", "newGroupId": groupId])
        }

        if isAudioSessionActive {
            print("🎙️ Session already active. Starting to record audio chunks...")
            startRecordingChunk()

            chunkTimer?.invalidate()
            chunkTimer = Timer.scheduledTimer(withTimeInterval: 1.5, repeats: true) { [weak self] _ in
                self?.flushAndContinueRecording()
            }
            print("⏱️ Chunk timer started (1.5s interval)")
        } else {
            print("⏳ Waiting for Audio Session to activate before recording...")
        }
    }

    private func startRecordingChunk() {
        let tempDir = FileManager.default.temporaryDirectory
        let fileUrl = tempDir.appendingPathComponent("tx_\(Date().timeIntervalSince1970).m4a")
        currentRecordFileUrl = fileUrl

        let settings: [String: Any] = [
            AVFormatIDKey: Int(kAudioFormatMPEG4AAC),
            AVSampleRateKey: 44100.0,
            AVNumberOfChannelsKey: 1,
            AVEncoderAudioQualityKey: AVAudioQuality.high.rawValue
        ]

        do {
            audioRecorder = try AVAudioRecorder(url: fileUrl, settings: settings)
            audioRecorder?.prepareToRecord()
            audioRecorder?.record()
            print("🎙️ NativePTTPlayer: Started recording chunk to \(fileUrl.lastPathComponent)")
        } catch {
            print("❌ NativePTTPlayer: Failed to start recording - \(error)")
        }
    }

    private func flushAndContinueRecording() {
        guard isTransmitting else { return }
        audioRecorder?.stop()
        if let fileUrl = currentRecordFileUrl {
            sendAudioChunk(fileUrl: fileUrl)
        }
        startRecordingChunk()
    }

    func stopTransmitting() {
        isTransmitting = false
        chunkTimer?.invalidate()
        chunkTimer = nil
        audioRecorder?.stop()
        if let fileUrl = currentRecordFileUrl {
            sendAudioChunk(fileUrl: fileUrl)
        }
        audioRecorder = nil
        currentRecordFileUrl = nil
        print("🎙️ NativePTTPlayer: Stopped transmitting")
    }

    private func sendAudioChunk(fileUrl: URL) {
        guard let groupId = currentGroupId else { return }
        let userId = UserDefaults.standard.string(forKey: "flutter.ptt_user_id") ?? ""
        
        do {
            let data = try Data(contentsOf: fileUrl)
            if data.isEmpty { return }
            
            let base64String = data.base64EncodedString()
            let msg: [String: Any] = [
                "type": "audio",
                "groupId": groupId,
                "sender": userId,
                "chunk": base64String
            ]
            
            if let jsonData = try? JSONSerialization.data(withJSONObject: msg),
               let str = String(data: jsonData, encoding: .utf8) {
                webSocketTask?.send(.string(str)) { error in
                    if let error = error {
                        print("❌ NativePTTPlayer: Failed to send audio chunk - \(error)")
                    } else {
                        print("📤 NativePTTPlayer: Sent audio chunk (\(data.count) bytes)")
                    }
                }
            }
            
            // Clean up file
            try FileManager.default.removeItem(at: fileUrl)
        } catch {
            print("❌ NativePTTPlayer: Failed to read/send chunk - \(error)")
        }
    }

    // URLSessionWebSocketDelegate
    func urlSession(_ session: URLSession, webSocketTask: URLSessionWebSocketTask,
                    didOpenWithProtocol protocol: String?) {
        print("✅ NativePTTPlayer: WebSocket connected")
        // ⚡ If the PTT session was already active (consecutive push), kick off the queue now.
        DispatchQueue.main.async {
            self.forceStartIfSessionActive()
        }
    }

    func urlSession(_ session: URLSession, webSocketTask: URLSessionWebSocketTask,
                    didCloseWith closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
        print("🔌 NativePTTPlayer: WebSocket closed")
        isReceiving = false
    }
}

extension NativePTTPlayer: AVAudioPlayerDelegate {
    func audioPlayerDidFinishPlaying(_ player: AVAudioPlayer, successfully flag: Bool) {
        isPlaying = false
        if let fileUrl = currentPlaybackFileUrl {
            try? FileManager.default.removeItem(at: fileUrl)
            currentPlaybackFileUrl = nil
        }
        if audioQueue.isEmpty {
            // ✅ FIX: Increased from 3.5s to 8.0s
            // Because Android streams in 1.5s chunks, network jitter could delay the next chunk by 3 or 4 seconds.
            // If we close the session too early, the message gets cut mid-sentence!
            print("⏱️ Audio queue empty, waiting 8.0s before ending session...")
            DispatchQueue.main.async {
                self.endTransactionTimer?.invalidate()
                self.endTransactionTimer = Timer.scheduledTimer(withTimeInterval: 8.0, repeats: false) { _ in
                    print("⏰ Timer expired, posting PTTAudioFinished notification")
                    NotificationCenter.default.post(name: NSNotification.Name("PTTAudioFinished"), object: nil)
                }
            }
        } else {
            processQueue() // Play next chunk if any
        }
    }
}

@main
@objc class AppDelegate: FlutterAppDelegate, PKPushRegistryDelegate {

  // CallKit provider for required iOS 13+ VoIP push compliance
  var callProvider: CXProvider?
  var callController = CXCallController()
  var activeCallUUID: UUID?
  var channelManager: Any? // PTChannelManager on iOS 16+
  var pendingJoinChannelUUID: UUID? // Used to deduplicate rapid joinChannel calls

  // 🔑 Tracks if the app has ever been in the foreground during this process lifetime.
  // false = app was just woken from killed state by a VoIP/PTT push
  // true  = app was backgrounded by user (Home button) — PTT works silently
  var hasBeenInForeground = false

  // 📻 Once user accepts/declines the first CallKit screen, this stays TRUE
  // so all subsequent PTT pushes in the same session skip the CallKit UI
  // and just play audio silently. Resets when the full audio session ends.
  var isPTTKilledSessionActive = false
  
  // ✅ Helper function to generate UUID from groupId
  func channelUUIDFromGroupId(_ groupId: String) -> UUID {
    let md5 = Insecure.MD5.hash(data: Data(groupId.utf8))
    let hex = md5.compactMap { String(format: "%02x", $0) }.joined()
    let formatted = "\(hex.prefix(8))-\(hex.dropFirst(8).prefix(4))-\(hex.dropFirst(12).prefix(4))-\(hex.dropFirst(16).prefix(4))-\(hex.dropFirst(20).prefix(12))".uppercased()
    let finalUUID = UUID(uuidString: formatted) ?? UUID(uuidString: "00000000-0000-0000-0000-000000000000")!
    print("📱 Channel UUID: \(groupId) -> \(finalUUID)")
    return finalUUID
  }

  override func applicationDidBecomeActive(_ application: UIApplication) {
    hasBeenInForeground = true
    super.applicationDidBecomeActive(application)
  }

  // ── When app backgrounds, hand off to native Swift layer ─────────────────
  // Flutter engine will suspend soon; NativePTTPlayer keeps the WebSocket alive.
  override func applicationDidEnterBackground(_ application: UIApplication) {
    super.applicationDidEnterBackground(application)

    let groupId = NativePTTPlayer.shared.currentGroupId ?? ""
    guard !groupId.isEmpty else {
      print("📱 Backgrounded — no active group, NativePTTPlayer idle")
      return
    }

    print("📱 Backgrounded — handing off group '\(groupId)' to NativePTTPlayer")

    // Start native background WebSocket so audio plays while Flutter is suspended
    NativePTTPlayer.shared.startBackgroundReceive(groupId: groupId)

    // ✅ ALWAYS fire sessionDidActivate as a guaranteed fallback.
    // - On iOS 16+ with working PTChannelManager: PTChannelManager fires didActivate
    //   first (within ~0.5s), which sets isAudioSessionActive = true.
    //   Our delayed call checks the flag and skips to avoid double-activation.
    // - On iOS 16+ with BROKEN PTChannelManager (Code=2 error): PTChannelManager
    //   never fires, so our fallback is the ONLY activation path. Without this,
    //   isAudioSessionActive stays false and audio is silently dropped.
    // - On iOS < 15: No PTT framework at all, this is the only path.
    DispatchQueue.main.asyncAfter(deadline: .now() + 1.5) {
      if !NativePTTPlayer.shared.isAudioSessionActive {
        print("🔊 PTChannelManager not ready — manually activating audio session for background playback")
        NativePTTPlayer.shared.sessionDidActivate()
      } else {
        print("✅ PTChannelManager already activated session — skipping manual activation")
      }
    }
  }

  override func application(
    _ application: UIApplication,
    didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?
  ) -> Bool {

    // ✅ Configure Firebase
    FirebaseApp.configure()

    // ✅ Set up push notification delegate
    UNUserNotificationCenter.current().delegate = self

    // ✅ Register for remote notifications
    application.registerForRemoteNotifications()

    // ✅ Register Flutter plugins
    GeneratedPluginRegistrant.register(with: self)

    // VoIP token is sent to Flutter when nativeConnect is called (Flutter signals ready)

    // ✅ Set up CallKit provider once — reused for all PTT calls (Apple recommends a single CXProvider lifetime)
    let config = CXProviderConfiguration(localizedName: "Walkie-Talkie")
    config.supportsVideo = false
    config.maximumCallsPerCallGroup = 1
    config.supportedHandleTypes = [.generic]
    callProvider = CXProvider(configuration: config)
    callProvider?.setDelegate(self, queue: nil)
    // ✅ Listen for when the background audio finishes playing (Globally)
    NotificationCenter.default.addObserver(forName: NSNotification.Name("PTTAudioFinished"), object: nil, queue: .main) { [weak self] _ in
        guard let self = self else { return }
        
        if #available(iOS 16.0, *) {
            // End PushToTalk remote participant
            if let manager = self.channelManager as? PTChannelManager, let activeUUID = manager.activeChannelUUID {
                print("🛑 Ending PTT Active Remote Participant")
                manager.setActiveRemoteParticipant(nil, channelUUID: activeUUID, completionHandler: nil)
            }
        }
        
        // ✅ End CallKit call IMMEDIATELY when audio finishes, no more arbitrary delays
        if let uuid = self.activeCallUUID {
            self.endPTTCallKitCall(uuid: uuid)
        }
        
        // 🔄 Reset the killed session flag so next fresh kill shows the call screen again
        self.isPTTKilledSessionActive = false
        print("🔄 PTT killed session ended — next kill will show call screen again")
        
        // ✅ FIX: Do NOT call AVAudioSession.setActive(false) here! 
        // LiveKit owns the session globally. Releasing it here instantly terminates WebRTC.
        print("🎧 Audio session natively preserved for LiveKit WebRTC")
    }

    // ✅ Initialize Push To Talk Framework (iOS 16+)
    if #available(iOS 16.0, *) {

        PTChannelManager.channelManager(delegate: self, restorationDelegate: self) { manager, error in
            if let error = error {
                print("❌ Failed to initialize PTChannelManager: \(error)")
            } else if let manager = manager {
                self.channelManager = manager
                
                if let activeUUID = manager.activeChannelUUID {
                    print("✅ Already joined PTT Channel: \(activeUUID)")
                } else {
                    print("📻 Ready to join PTT channels dynamically.")
                }
            }
        }
    } else {
        // ✅ Register for VoIP push notifications (PushKit - for iOS < 16 ONLY)
        let voipRegistry = PKPushRegistry(queue: DispatchQueue.main)
        voipRegistry.delegate = self
        voipRegistry.desiredPushTypes = [.voIP]
    }

    // ✅ Set up custom audio method channel
    if let controller = window?.rootViewController as? FlutterViewController {

      let audioChannel = FlutterMethodChannel(name: "custom.audio", binaryMessenger: controller.binaryMessenger)
      audioChannel.setMethodCallHandler { (call: FlutterMethodCall, result: @escaping FlutterResult) in

        if call.method == "forceSpeaker" {
          self.forceSpeakerAfterDisable()
          result(nil)
        } else if call.method == "forceMic" {
          self.configureAudioSession()
          result(nil)
        } else if call.method == "forceVideoChat" {
          self.configureVideoChatAudioSession()
          result(nil)

        // ── Native background WebSocket (survives app backgrounding) ──────
        } else if call.method == "nativeConnect" {
          // Called from Dart when PTT connects — starts the native Swift
          // WebSocket player that keeps running even when Flutter engine is suspended.
          if let args = call.arguments as? [String: Any],
             let userId = args["userId"] as? String {
            // Store userId so NativePTTPlayer can read it after wakeup
            UserDefaults.standard.set(userId, forKey: "flutter.ptt_user_id")
            UserDefaults.standard.synchronize()
            print("📱 nativeConnect: userId stored = \(userId)")
          }
          // Flutter method channel is now ready — safe to send stored VoIP token
          if let storedToken = UserDefaults.standard.string(forKey: "voip_token"), !storedToken.isEmpty {
            print("📲 Flutter ready — sending VoIP token: \(storedToken.prefix(8))...")
            self.sendVoIPTokenToFlutter(storedToken)
          }
          result(nil)

        } else if call.method == "nativeJoinGroup" {
          // Called from Dart on every joinGroup() — tells the native player
          // which group to listen to in the background.
          if let args = call.arguments as? [String: Any],
             let groupId = args["groupId"] as? String {
            NativePTTPlayer.shared.currentGroupId = groupId
            print("📱 nativeJoinGroup: currentGroupId = \(groupId)")
          }
          result(nil)

        } else {
          result(FlutterMethodNotImplemented)
        }
      }

      // ✅ Set up VoIP method channel (Flutter → Native registration)
      let voipChannel = FlutterMethodChannel(name: "ptt/voip", binaryMessenger: controller.binaryMessenger)
      voipChannel.setMethodCallHandler { (call: FlutterMethodCall, result: @escaping FlutterResult) in
        if call.method == "getVoIPToken" {
          result(UserDefaults.standard.string(forKey: "voip_token"))
        } else if call.method == "getPendingVoIPPayload" {
          // ✅ Flutter reads this on resume to catch push received while locked
          let payload = UserDefaults.standard.dictionary(forKey: "pending_voip_payload")
          result(payload)
        } else if call.method == "clearPendingVoIPPayload" {
          // ✅ Clear after Flutter has processed it
          UserDefaults.standard.removeObject(forKey: "pending_voip_payload")
          result(nil)
        } else if call.method == "isAppInBackground" {
          // ✅ Tell Flutter if the app is truly in the background or not
          let state = UIApplication.shared.applicationState
          result(state == .background || state == .inactive)
        } else if call.method == "joinChannel" {
          // ✅ Tell the iOS PushToTalk framework which channel we are active in
          if let args = call.arguments as? [String: Any], let groupId = args["groupId"] as? String {
              let newUUID = self.channelUUIDFromGroupId(groupId)
              
              if #available(iOS 16.0, *) {
                  let descriptor = PTChannelDescriptor(name: "Walkie-Talkie", image: nil)
                  
                  if let manager = self.channelManager as? PTChannelManager {
                      // Deduplicate rapid rapid calls from Flutter that happen before the framework updates
                      if self.pendingJoinChannelUUID == newUUID {
                          print("✅ Already in the process of joining: \(newUUID)")
                          result(nil)
                          return
                      }
                      
                      self.pendingJoinChannelUUID = newUUID

                      // Apple PushToTalk daemon has a known bug where it keeps a "zombie" lock on a channel
                      // from a previous app session, even if `activeChannelUUID` is locally nil!
                      // To fix Code=2 (channelLimitReached), we must FORCE the daemon to drop any channel it holds.
                      
                      if let oldUUID = manager.activeChannelUUID {
                          manager.leaveChannel(channelUUID: oldUUID)
                      }
                      // ALWAYS forcefully leave the newUUID as well just in case the daemon holds a zombie lock on it
                      manager.leaveChannel(channelUUID: newUUID)

                      // Now wait 1 full second for the iOS daemon to process the leaves, then join safely.
                      DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
                          // Ensure the user hasn't switched to a third group during the delay
                          if self.pendingJoinChannelUUID == newUUID {
                              manager.requestJoinChannel(channelUUID: newUUID, descriptor: descriptor)
                              print("📻 Native PTT Framework Joined Channel: \(newUUID)")
                          }
                      }
                  }
              }
          }
          result(nil)
        } else {
          result(FlutterMethodNotImplemented)
        }
      }
    }

    return super.application(application, didFinishLaunchingWithOptions: launchOptions)
  }

  // ─────────────────────────────────────────────────────
  // MARK: - PushKit VoIP Delegate
  // ─────────────────────────────────────────────────────

  // ✅ Called when a new VoIP push token is available
  func pushRegistry(_ registry: PKPushRegistry,
                    didUpdate pushCredentials: PKPushCredentials,
                    for type: PKPushType) {
    let token = pushCredentials.token.map { String(format: "%02x", $0) }.joined()
    print("📲 VoIP Push Token: \(token)")
    UserDefaults.standard.set(token, forKey: "voip_token")
    // Tag as PushKit VoIP token so the server uses the <bundle-id>.voip APNs topic
    UserDefaults.standard.set("voip", forKey: "voip_token_type")
    sendVoIPTokenToFlutter(token)
  }

  // ✅ Called when a VoIP push arrives (wakes the app even when locked/killed)
  func pushRegistry(_ registry: PKPushRegistry,
                    didReceiveIncomingPushWith payload: PKPushPayload,
                    for type: PKPushType,
                    completion: @escaping () -> Void) {

    print("📨 VoIP Push Received: \(payload.dictionaryPayload)")

    // ✅ MANDATORY on iOS 13+: Report a call to CallKit immediately
    // If we skip this, Apple will kill the app
    let uuid = UUID()
    activeCallUUID = uuid
    let update = CXCallUpdate()
    update.remoteHandle = CXHandle(type: .generic, value: "PTT Message")
    update.hasVideo = false
    update.localizedCallerName = payload.dictionaryPayload["senderName"] as? String ?? "PTT Message"

    callProvider?.reportNewIncomingCall(with: uuid, update: update) { error in
      if let error = error {
        print("❌ CallKit report error: \(error)")
      } else {
        print("✅ CallKit call reported")
      }
      completion()
    }

    // ✅ Activate audio session to play received PTT audio
    activateAudioSessionForPTT()

    // ✅ Start native background WebSocket + audio player (works with phone locked)
    // This plays audio WITHOUT involving Flutter at all
    let payloadData = payload.dictionaryPayload
    let groupId = payloadData["groupId"] as? String ?? ""
    
    // ✅ CRITICAL: Store the groupId so the talk button knows where to send replies
    if !groupId.isEmpty {
        NativePTTPlayer.shared.currentGroupId = groupId
        NativePTTPlayer.shared.startBackgroundReceive(groupId: groupId)
        // ✅ FIX: On iOS < 16 (PushKit path) there is no PTT framework didActivate callback.
        // We must manually signal the audio session is ready after a short delay
        // so processQueue() can start playing the received chunks.
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
            NativePTTPlayer.shared.sessionDidActivate()
        }
    }

    // ✅ Also notify Flutter that VoIP push was received (for when it resumes)
    sendVoIPPushToFlutter(payloadData)
  }

  // ─────────────────────────────────────────────────────
  // MARK: - CallKit for PTT (when app is killed)
  // ─────────────────────────────────────────────────────

  // Shows a CallKit incoming-call screen — the ONLY reliable way to wake a force-killed app.
  // Audio plays through NativePTTPlayer immediately; CallKit is auto-ended when audio finishes.
  private func reportPTTCallKitCall(senderName: String, groupId: String) {

    // ✅ CRITICAL: Store the groupId so the talk button knows where to send replies
    if !groupId.isEmpty {
        NativePTTPlayer.shared.currentGroupId = groupId
    }

    // 📻 If already in a killed-session (user saw & dismissed the first call screen),
    // ALL subsequent pushes just play audio silently — no new call screen!
    if isPTTKilledSessionActive {
        print("📻 PTT session already active — playing audio silently (no new call screen)")
        if !groupId.isEmpty {
            NativePTTPlayer.shared.startBackgroundReceive(groupId: groupId)
        }
        return
    }

    // 🔒 If a CallKit call is still showing (user hasn't acted yet), don't create a second one.
    if activeCallUUID != nil {
        print("⚠️ CallKit call already showing — starting audio for new group silently")
        if !groupId.isEmpty {
            NativePTTPlayer.shared.startBackgroundReceive(groupId: groupId)
        }
        return
    }

    // ✅ First push in this killed session — show the call screen
    isPTTKilledSessionActive = true
    let uuid = UUID()
    activeCallUUID = uuid

    let update = CXCallUpdate()
    update.remoteHandle = CXHandle(type: .generic, value: senderName)
    update.hasVideo = false
    update.localizedCallerName = "📻 \(senderName)"
    update.supportsHolding = false
    update.supportsDTMF = false
    update.supportsGrouping = false
    update.supportsUngrouping = false

    callProvider?.reportNewIncomingCall(with: uuid, update: update) { [weak self] error in
      guard let self = self else { return }
      if let error = error {
        print("❌ PTT CallKit failed: \(error.localizedDescription)")
        // Fallback: try NativePTTPlayer anyway
        if !groupId.isEmpty {
          NativePTTPlayer.shared.startBackgroundReceive(groupId: groupId)
          self.activateAudioSessionForPTT()
          NativePTTPlayer.shared.sessionDidActivate()
        }
      } else {
        print("✅ PTT CallKit call reported — waking killed app for audio!")
        // Activate audio and start NativePTTPlayer
        self.activateAudioSessionForPTT()
        if !groupId.isEmpty {
          NativePTTPlayer.shared.startBackgroundReceive(groupId: groupId)
        }
        // Give NativePTTPlayer 1 second to connect, then force-start if needed
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) {
          NativePTTPlayer.shared.sessionDidActivate()
        }
        // ✅ Extend auto-end timeout to 60s to give user time to reply
        DispatchQueue.main.asyncAfter(deadline: .now() + 60.0) { [weak self] in
          guard let self = self, let uuid = self.activeCallUUID else { return }
          self.endPTTCallKitCall(uuid: uuid)
        }
      }
    }
  }

  private func endPTTCallKitCall(uuid: UUID) {
    guard activeCallUUID == uuid else { return } // already ended
    activeCallUUID = nil
    let endAction = CXEndCallAction(call: uuid)
    let transaction = CXTransaction(action: endAction)
    callController.request(transaction) { error in
      if let e = error { print("⚠️ PTT CallKit end error: \(e)") }
      else { print("✅ PTT CallKit call auto-ended after audio finished") }
    }
  }

  // ─────────────────────────────────────────────────────
  // MARK: - Audio Session Helpers
  // ─────────────────────────────────────────────────────

  private func activateAudioSessionForPTT() {
    do {
      let session = AVAudioSession.sharedInstance()
      try session.setCategory(.playAndRecord, mode: .voiceChat, options: [.defaultToSpeaker, .allowBluetooth, .allowBluetoothA2DP, .allowAirPlay])
      try session.setActive(true)
      print("✅ AVAudioSession activated for PTT delivery — LiveKit unified config")
    } catch {
      print("⚠️ Failed to activate audio session for PTT: \(error)")
    }
  }

  private func configureAudioSession() {
    do {
      let session = AVAudioSession.sharedInstance()
      try session.setCategory(.playAndRecord, mode: .voiceChat, options: [.defaultToSpeaker, .allowBluetooth, .allowBluetoothA2DP, .allowAirPlay])
      try session.setActive(true)
      print("✅ AVAudioSession configured (LiveKit unified config)")
    } catch {
      print("⚠️ Failed to configure AVAudioSession: \(error)")
    }
  }

  private func configureVideoChatAudioSession() {
    do {
      let session = AVAudioSession.sharedInstance()
      try session.setCategory(.playAndRecord, mode: .voiceChat, options: [.defaultToSpeaker, .allowBluetooth, .allowBluetoothA2DP, .allowAirPlay])
      try session.setActive(true)
      print("✅ AVAudioSession switched to videoChat (LiveKit unified config)")
    } catch {
      print("❌ Failed to set videoChat session: \(error)")
    }
  }

  private func forceSpeakerAfterDisable() {
    let session = AVAudioSession.sharedInstance()
    do {
      // ✅ FIX: Do NOT call setCategory here — that resets the whole AVAudioSession
      // and can interfere with the Flutter-managed session on iPhone 12 Pro.
      // Instead, just override the output port. This is a lightweight, non-destructive call.
      // Only .playAndRecord category supports this override. If it fails, we silently ignore.
      if session.category == .playAndRecord {
        try session.setActive(true)
        try session.overrideOutputAudioPort(.speaker)
        print("🔊 Speaker output overridden (lightweight, session category preserved)")
      }
    } catch {
      // Silently ignore harmless errors like -50 (kAudioSessionBadParam) 
      // which happen if category is .playback, since .playback already uses the speaker.
    }
  }

  // ─────────────────────────────────────────────────────
  // MARK: - Flutter Channel Helpers
  // ─────────────────────────────────────────────────────

  private func sendVoIPTokenToFlutter(_ token: String) {
    // Mirror token type with the flutter. prefix so SharedPreferences.getString('voip_token_type') works in Dart
    let tokenType = UserDefaults.standard.string(forKey: "voip_token_type") ?? "ptt"
    UserDefaults.standard.set(tokenType, forKey: "flutter.voip_token_type")
    DispatchQueue.main.async {
      guard let controller = self.window?.rootViewController as? FlutterViewController else { return }
      let channel = FlutterMethodChannel(name: "ptt/voip", binaryMessenger: controller.binaryMessenger)
      channel.invokeMethod("onVoIPToken", arguments: token)
    }
  }

  private func sendVoIPPushToFlutter(_ payload: [AnyHashable: Any]) {
    // ✅ ALWAYS persist payload to UserDefaults FIRST
    // This ensures Flutter can read it even if the engine isn't ready yet
    var stringPayload: [String: String] = [:]
    for (key, value) in payload {
      if let k = key as? String {
        stringPayload[k] = "\(value)"
      }
    }
    UserDefaults.standard.set(stringPayload, forKey: "pending_voip_payload")
    UserDefaults.standard.synchronize()
    print("📦 VoIP payload persisted to UserDefaults: \(stringPayload)")

    // ✅ Try to deliver to Flutter immediately (works when app is foreground/active)
    _deliverVoIPPayloadToFlutter(stringPayload, retries: 5)
  }

  private func _deliverVoIPPayloadToFlutter(_ payload: [String: String], retries: Int) {
    guard let controller = window?.rootViewController as? FlutterViewController else {
      // Flutter engine not ready yet — retry after a short delay
      if retries > 0 {
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.8) {
          self._deliverVoIPPayloadToFlutter(payload, retries: retries - 1)
        }
      } else {
        print("⚠️ Flutter not ready after retries — payload stored in UserDefaults for resume")
      }
      return
    }
    let channel = FlutterMethodChannel(name: "ptt/voip", binaryMessenger: controller.binaryMessenger)
    DispatchQueue.main.async {
      channel.invokeMethod("onVoIPPush", arguments: payload)
      print("✅ VoIP payload delivered to Flutter")
      // Clear persisted payload once successfully delivered
      UserDefaults.standard.removeObject(forKey: "pending_voip_payload")
    }
  }

  // ─────────────────────────────────────────────────────
  // MARK: - APNs Token Registration
  // ─────────────────────────────────────────────────────

  override func application(
    _ application: UIApplication,
    didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data
  ) {
    #if DEBUG
    Auth.auth().setAPNSToken(deviceToken, type: .sandbox)
    #else
    Auth.auth().setAPNSToken(deviceToken, type: .prod)
    #endif

    Messaging.messaging().apnsToken = deviceToken
    super.application(application, didRegisterForRemoteNotificationsWithDeviceToken: deviceToken)
  }

  override func application(
    _ application: UIApplication,
    didFailToRegisterForRemoteNotificationsWithError error: Error
  ) {
    print("❌ Failed to register for remote notifications: \(error.localizedDescription)")
  }
}

// ─────────────────────────────────────────────────────
// MARK: - CallKit Provider Delegate (Required for PushKit)
// ─────────────────────────────────────────────────────
extension AppDelegate: CXProviderDelegate {
  func providerDidReset(_ provider: CXProvider) {}

  // User tapped ANSWER on the Walkie-Talkie call screen
  func provider(_ provider: CXProvider, perform action: CXAnswerCallAction) {
    print("📞 User answered PTT CallKit call — opening app")
    // The audio is already playing via NativePTTPlayer.
    // Notify Flutter so it can open/foreground the app if needed.
    if let controller = window?.rootViewController as? FlutterViewController {
      let channel = FlutterMethodChannel(name: "ptt/voip", binaryMessenger: controller.binaryMessenger)
      channel.invokeMethod("onCallAnswered", arguments: nil)
    }
    action.fulfill()
  }

  // User tapped DECLINE or call timed out.
  // 💡 IMPORTANT: We do NOT stop audio here! The voice message always plays to completion.
  // Declining just dismisses the UI — isPTTKilledSessionActive stays true so
  // future pushes also play silently without showing another call screen.
  func provider(_ provider: CXProvider, perform action: CXEndCallAction) {
    print("🛑 PTT CallKit call UI dismissed — audio keeps playing silently")
    activeCallUUID = nil // Clear UUID so auto-end timer won't fire again
    // ❌ Do NOT call NativePTTPlayer.shared.disconnect() here!
    // ❌ Do NOT reset isPTTKilledSessionActive here!
    // Audio will auto-stop when PTTAudioFinished fires, which THEN resets everything.
    if let controller = window?.rootViewController as? FlutterViewController {
      let channel = FlutterMethodChannel(name: "ptt/voip", binaryMessenger: controller.binaryMessenger)
      channel.invokeMethod("onCallEnded", arguments: nil)
    }
    action.fulfill()
  }
}

// ─────────────────────────────────────────────────────
// MARK: - Push To Talk Framework Delegates (iOS 16+)
// ─────────────────────────────────────────────────────
@available(iOS 16.0, *)
extension AppDelegate: PTChannelManagerDelegate, PTChannelRestorationDelegate {
    
    func channelManager(_ channelManager: PTChannelManager, receivedEphemeralPushToken pushToken: Data) {
        let token = pushToken.map { String(format: "%02x", $0) }.joined()
        print("📲 PTT Framework VoIP Token: \(token)")
        UserDefaults.standard.set(token, forKey: "voip_token")
        // Tag this as a PushToTalk token so the server uses the correct APNs topic (voip-ptt)
        UserDefaults.standard.set("ptt", forKey: "voip_token_type")
        sendVoIPTokenToFlutter(token)
        // Re-register with the server immediately so the stored type is up to date
        if let userId = UserDefaults.standard.string(forKey: "flutter.ptt_user_id"), !userId.isEmpty {
            NativePTTPlayer.shared.registerTokenWithServer(userId: userId, token: token, tokenType: "ptt")
        }
        print("✅ VoIP token refreshed and stored successfully")
    }
    
    func incomingPushResult(channelManager: PTChannelManager, channelUUID: UUID, pushPayload: [String : Any]) -> PTPushResult {
        print("📨 PTT Push Received: \(pushPayload)")
        print("📱 Channel UUID: \(channelUUID)")  // ✅ Log the channelUUID

        let groupId = pushPayload["groupId"] as? String ?? ""
        let senderName = pushPayload["senderName"] as? String ?? "Walkie-Talkie"
        NativePTTPlayer.shared.currentGroupId = groupId // ✅ Cache for replying
        
        // ✅ Generate expected channelUUID from groupId
        let expectedChannelUUID = channelUUIDFromGroupId(groupId)
        print("🔑 Expected channelUUID from groupId: \(expectedChannelUUID)")
        print("🔑 Actual channelUUID from push: \(channelUUID)")
        
        if channelUUID != expectedChannelUUID {
            print("⚠️  channelUUID mismatch! This might cause issues.")
        }

        // ✅ Start background audio playback
        // The PTT framework (iOS 16+) wakes the app on its own. CallKit is NOT needed.
        // Whether the app was killed or backgrounded, just connect and play audio.
        print("🔊 PTT push — will play audio and show system UI")
        
        DispatchQueue.main.async {
            if UIApplication.shared.applicationState == .active {
                print("🛑 App is in FOREGROUND — skipping NativePTTPlayer to avoid double audio!")
            } else {
                if !groupId.isEmpty {
                    NativePTTPlayer.shared.startBackgroundReceive(groupId: groupId)
                    // Give the WebSocket 0.5s to connect before force-starting the queue.
                    // The real activation also comes from channelManager(_:didActivate:) below.
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
                        // ✅ FIX: Only trigger fallback if the system didn't already activate the session!
                        // Overriding the session while Apple's Walkie-Talkie is playing causes the audio to mute.
                        if !NativePTTPlayer.shared.isAudioSessionActive {
                            NativePTTPlayer.shared.sessionDidActivate()
                        }
                    }
                }
            }
        }

        // Notify Flutter (with UserDefaults fallback for when Flutter isn't ready)
        sendVoIPPushToFlutter(pushPayload)

        // ✅ Return activeRemoteParticipant to keep PTT session alive and show system UI
        let participant = PTParticipant(name: senderName, image: nil)
        return .activeRemoteParticipant(participant)
    }
    
    func channelDescriptor(restoredChannelUUID channelUUID: UUID) -> PTChannelDescriptor {
        return PTChannelDescriptor(name: "Walkie-Talkie", image: nil)
    }
    
        
    func channelManager(_ channelManager: PTChannelManager, didActivate audioSession: AVAudioSession) {
        print("🎙️ PTT Audio Session Activated")
        NativePTTPlayer.shared.sessionDidActivate(audioSession: audioSession)
    }
    
    func channelManager(_ channelManager: PTChannelManager, didDeactivate audioSession: AVAudioSession) {
        print("🎙️ PTT Audio Session Deactivated")
        NativePTTPlayer.shared.disconnect()
    }

    func channelManager(_ channelManager: PTChannelManager, didJoinChannel channelUUID: UUID, reason: PTChannelJoinReason) {
        print("🎙️ Joined PTT Channel")
    }
    
    func channelManager(_ channelManager: PTChannelManager, didLeaveChannel channelUUID: UUID, reason: PTChannelLeaveReason) {
        print("🎙️ Left PTT Channel: \(channelUUID)")
    }
    
    func channelManager(_ channelManager: PTChannelManager, channelUUID: UUID, didBeginTransmittingFrom source: PTChannelTransmitRequestSource) {
        print("🎙️ Began Transmitting (source: \(source.rawValue))")
        
        // ✅ Get the groupId from either cached value or try to retrieve from pending push
        var groupId = NativePTTPlayer.shared.currentGroupId
        print("📍 Current groupId in memory: \(groupId ?? "nil")")
        
        if groupId == nil || groupId!.isEmpty {
            // ⚠️ Fallback: Try to get groupId from the pending VoIP payload
            if let payload = UserDefaults.standard.dictionary(forKey: "pending_voip_payload"),
               let gId = payload["groupId"] as? String {
                groupId = gId
                NativePTTPlayer.shared.currentGroupId = gId
                print("🔄 Retrieved groupId from pending payload: \(gId)")
            } else {
                print("⚠️ No pending payload found in UserDefaults")
            }
        }
        
        if let groupId = groupId, !groupId.isEmpty {
            print("✅ Starting transmission to group: \(groupId)")
            NativePTTPlayer.shared.startTransmitting(groupId: groupId)
        } else {
            print("❌ Cannot transmit: No groupId available!")
            print("💡 Tip: Make sure a PTT message was received before trying to reply")
        }
    }
    
    func channelManager(_ channelManager: PTChannelManager, channelUUID: UUID, didEndTransmittingFrom source: PTChannelTransmitRequestSource) {
        print("🎙️ Ended Transmitting (source: \(source.rawValue))")
        NativePTTPlayer.shared.stopTransmitting()
        
        // Hide Walkie-Talkie UI immediately after user releases Talk button
        // print("🛑 Hiding Walkie-Talkie UI by leaving channel...")
        // channelManager.leaveChannel(channelUUID: channelUUID)
    }
    
    // Duplicate didLeaveChannel removed.
    
    func channelManager(_ channelManager: PTChannelManager, failedToJoinChannel channelUUID: UUID, error: Error) {
        print("❌ Failed to join PTT Channel: \(error)")
    }
}
