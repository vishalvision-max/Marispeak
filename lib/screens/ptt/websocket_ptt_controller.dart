import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:flutter/widgets.dart';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:just_audio/just_audio.dart';
import 'package:marispeaks/config/app_config.dart';
import 'package:marispeaks/screens/home/CustomBottomSection.dart';
import 'package:record/record.dart';
import 'package:path_provider/path_provider.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:audio_session/audio_session.dart';

class WebSocketPTTController with WidgetsBindingObserver {
  static final WebSocketPTTController _instance =
      WebSocketPTTController._internal();
  factory WebSocketPTTController() => _instance;
  WebSocketPTTController._internal();

  static const platform = MethodChannel('custom.audio');

  WebSocketChannel? _channel;
  StreamSubscription? _wsSubscription;
  Timer? _pingTimer;
  Timer? _netRetryTimer;

  final AudioRecorder _recorder = AudioRecorder();
  final AudioPlayer _player = AudioPlayer();

  String? senderId;
  String? groupId;
  String? voipToken;
  String? _filePath;
  bool isRecording = false;
  bool isConnected = false;

  // ------------------------------------------------------------
  // INITIALIZE
  // ------------------------------------------------------------
  Future<void> initialize() async {
    WidgetsBinding.instance.addObserver(this);

    await Permission.microphone.request();

    final session = await AudioSession.instance;
    await session.configure(const AudioSessionConfiguration.music());
    await session.setActive(true);

    if (Platform.isIOS) {
      forceSpeakerOnIOS();
      platform.setMethodCallHandler((call) async {
        if (call.method == 'onVoipToken') {
          voipToken = call.arguments as String;
          debugPrint("📱 Received VoIP Token: $voipToken");
        }
      });
    }

    startNetworkMonitor();

    _player.playerStateStream.listen((state) async {
      if (state.processingState == ProcessingState.completed) {
        await _player.stop(); // fully reset
      }
    });

    debugPrint("🎧 PTT Controller Ready");
  }

  Future<void> forceSpeakerOnIOS() async {
    try {
      await platform.invokeMethod("forceSpeaker");
    } catch (_) {}
  }

  // ------------------------------------------------------------
  // NETWORK WATCHER
  // ------------------------------------------------------------
  void startNetworkMonitor() {
    Connectivity().onConnectivityChanged.listen((status) {
      if (status != ConnectivityResult.none) {
        debugPrint("🌐 Network back");
        if (!isConnected && senderId != null) connect(senderId!);
      } else {
        debugPrint("⚠ No network, retrying...");
        _startRetry();
      }
    });
  }

  void _startRetry() {
    _netRetryTimer?.cancel();
    _netRetryTimer = Timer.periodic(const Duration(seconds: 2), (_) async {
      final result = await Connectivity().checkConnectivity();
      if (result != ConnectivityResult.none) {
        _netRetryTimer?.cancel();
        if (!isConnected && senderId != null) connect(senderId!);
      }
    });
  }

  // ------------------------------------------------------------
  // CONNECT
  // ------------------------------------------------------------
  Future<void> connect(String uid) async {
    if (isConnected) return;

    senderId = uid.trim();
    groupId = uid.trim();

    try {
      _channel = WebSocketChannel.connect(Uri.parse(AppConfig.pttServerUrl));

      _channel!.sink.add(jsonEncode({
        "type": "register",
        "userId": senderId,
        "voipToken": voipToken,
      }));

      _wsSubscription = _channel!.stream.listen(
        (event) => _onWSMessage(event),
        onError: (_) => _onDisconnect(),
        onDone: () => _onDisconnect(),
        cancelOnError: true,
      );

      isConnected = true;
      _startPing();

      debugPrint("✅ Connected as $senderId");

      // ✅ Also start the native Swift background WebSocket (survives background)
      if (Platform.isIOS) {
        try {
          await platform.invokeMethod('nativeConnect', {'userId': senderId});
        } catch (_) {}
      }

      joinGroup(groupId!);
    } catch (e) {
      debugPrint("❌ Failed connect: $e");
      _onDisconnect();
    }
  }

  void _startPing() {
    _pingTimer?.cancel();
    _pingTimer = Timer.periodic(const Duration(seconds: 55), (_) async {
      // Check network connectivity
      var connectivityResult = await Connectivity().checkConnectivity();
      if (connectivityResult == ConnectivityResult.none) return;

      // Make sure the WebSocket is not null and still open
      if (_channel != null) {
        try {
          _channel!.sink.add(jsonEncode({"type": "ping"}));
        } catch (e) {
          debugPrint("⚠️ PTT ping failed: $e");
        }
      }
    });
  }

  // ------------------------------------------------------------
  // RECEIVE MESSAGES
  // ------------------------------------------------------------
  Future<void> _onWSMessage(dynamic event) async {
    try {
      final data = jsonDecode(event as String) as Map<String, dynamic>;
      if (data["type"] == "audio") {
        final bytes = base64Decode(data["chunk"] as String);
        final dir = await getApplicationDocumentsDirectory();
        final path = "${dir.path}/rx_${DateTime.now().millisecondsSinceEpoch}.aac";
        await File(path).writeAsBytes(bytes, flush: true);
        playReceived(path);
      }
    } catch (e) {
      debugPrint("❌ PTT message error: $e");
    }
  }

  // ------------------------------------------------------------
  // PLAYBACK (Latest always wins)
  // ------------------------------------------------------------
  Future<void> playReceived(String path) async {
    try {
      if (Platform.isIOS) forceSpeakerOnIOS();
      await _player.setAudioSource(AudioSource.uri(Uri.file(path)));
      await _player.play();
      // Delete temp file after playback completes
      _player.playerStateStream
          .firstWhere((s) => s.processingState == ProcessingState.completed)
          .then((_) => File(path).delete().catchError((_) {}));
    } catch (e) {
      debugPrint("❌ PTT playback error: $e");
      File(path).delete().catchError((_) {});
    }
  }

  // ------------------------------------------------------------
  // RECORDING
  // ------------------------------------------------------------
  Future<void> startRecording() async {
    if (isRecording) return;
    await customBottomSection.currentState?.playBeep();
    if (!await _recorder.hasPermission()) {
      await Permission.microphone.request();
      return;
    }

    final dir = await getApplicationDocumentsDirectory();
    _filePath = "${dir.path}/tx_${DateTime.now().millisecondsSinceEpoch}.aac";

    await _recorder.start(
      const RecordConfig(
        encoder: AudioEncoder.aacLc,
        sampleRate: 44100,
        bitRate: 128000,
        numChannels: 1,
      ),
      path: _filePath!,
    );

    isRecording = true;
  }

  Future<void> stopRecording() async {
    if (!isRecording) return;
    await _recorder.stop();
    isRecording = false;
  }

  Future<void> sendAudio() async {
    if (_filePath == null || groupId == null || !isConnected) return;

    final file = File(_filePath!);
    if (!await file.exists()) return;

    final msg = jsonEncode({
      "type": "audio",
      "groupId": groupId,
      "sender": senderId,
      "chunk": base64Encode(await file.readAsBytes()),
    });

    _channel?.sink.add(msg);
  }

  // ------------------------------------------------------------
  // GROUPS
  // ------------------------------------------------------------
  void joinGroup(String id) {
    groupId = id.trim();
    _channel?.sink.add(jsonEncode({
      "type": "switch",
      "newGroupId": groupId,
    }));
    // Mirror room join to native Swift layer (works in background)
    if (Platform.isIOS) {
      platform.invokeMethod(
          'nativeJoinGroup', {'groupId': groupId}).catchError((_) {});
    }
    debugPrint("👥 Joined group $groupId");
  }

  // ------------------------------------------------------------
  // DISCONNECT
  // ------------------------------------------------------------
  Future<void> _onDisconnect() async {
    isConnected = false;

    try {
      await _wsSubscription?.cancel();
    } catch (_) {}
    try {
      await _channel?.sink.close();
    } catch (_) {}

    _wsSubscription = null;
    _channel = null;
    _pingTimer?.cancel();

    debugPrint("🔌 Disconnected");

    Future.delayed(const Duration(seconds: 2), () {
      if (!isConnected && senderId != null) connect(senderId!);
    });
  }

  Future<void> dispose() async {
    WidgetsBinding.instance.removeObserver(this);
    _pingTimer?.cancel();
    _netRetryTimer?.cancel();
    await _player.dispose();
    await _recorder.dispose();
    await _wsSubscription?.cancel();
    await _channel?.sink.close();

    debugPrint("🧹 Controller disposed");
  }

  // ------------------------------------------------------------
  // LIFECYCLE
  // ------------------------------------------------------------
  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.paused) {
      // Release the audio session so the native Swift NativePTTPlayer can
      // claim it cleanly for background playback. Fighting iOS here causes
      // background audio to be silenced.
      _player.stop();
      AudioSession.instance.then((s) => s.setActive(false));
    } else if (state == AppLifecycleState.resumed) {
      AudioSession.instance.then((s) async {
        await s.configure(const AudioSessionConfiguration.music());
        await s.setActive(true);
      });
    }
  }
}
