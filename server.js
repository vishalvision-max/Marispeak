import express from "express";
import { WebSocketServer } from "ws";
import http from "http";
import apn from "apn";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

const apnProvider = new apn.Provider({
  token: {
    key:    path.join(__dirname, "AuthKey_M983S4297F.p8"),
    keyId:  "M983S4297F",
    teamId: "RFCRATE6EZ",
  },
  production: true,
});

const groups         = {};
const users          = {};
const userVoipTokens = {};

function channelUUIDFromGroupId(groupId) {
  const md5 = crypto.createHash("md5").update(groupId).digest("hex");
  return [md5.slice(0,8),md5.slice(8,12),md5.slice(12,16),md5.slice(16,20),md5.slice(20,32)].join("-").toUpperCase();
}

function isValidToken(t) {
  return t && typeof t === "string" && t.length > 10;
}

function cleanupClient(ws) {
  if (ws.groupId && groups[ws.groupId]) {
    groups[ws.groupId].delete(ws);
    if (groups[ws.groupId].size === 0) delete groups[ws.groupId];
  }
  if (ws.userId && users[ws.userId] === ws) {
    delete users[ws.userId];
  }
}

const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) { cleanupClient(ws); ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on("close", () => clearInterval(heartbeatInterval));

wss.on("connection", (ws) => {
  console.log("Client connected");
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (msg) => {
    try {
      const data = JSON.parse(msg);

      if (data.type === "register") {
        users[data.userId] = ws;
        ws.userId = data.userId;
        if (isValidToken(data.voipToken)) {
          userVoipTokens[data.userId] = data.voipToken;
          console.log(`Saved VoIP token for ${data.userId}`);
        } else {
          console.log(`Register ${data.userId} — keeping existing token: ${isValidToken(userVoipTokens[data.userId]) ? "YES" : "NONE"}`);
        }
        console.log(`User registered: ${data.userId}`);
      }

      if (data.type === "ping") {
        ws.send(JSON.stringify({ type: "pong" }));
      }

      if (data.type === "switch") {
        if (ws.groupId && groups[ws.groupId]) {
          groups[ws.groupId].delete(ws);
          if (groups[ws.groupId].size === 0) delete groups[ws.groupId];
          console.log(`User ${ws.userId} left group ${ws.groupId}`);
        }
        const group = groups[data.newGroupId] || new Set();
        group.add(ws);
        groups[data.newGroupId] = group;
        ws.groupId = data.newGroupId;
        console.log(`User ${ws.userId} switched to group ${data.newGroupId}`);
      }

      if (data.type === "audio" && ws.groupId) {
        const connectedUserIds = new Set();
        groups[ws.groupId]?.forEach((client) => {
          if (client !== ws && client.readyState === 1) {
            client.send(JSON.stringify({ type: "audio", chunk: data.chunk, sender: data.sender }));
            if (client.userId) connectedUserIds.add(client.userId);
          }
        });

        const targetUserId = ws.groupId;
        if (targetUserId !== ws.userId && !connectedUserIds.has(targetUserId)) {
          const voipToken = userVoipTokens[targetUserId];
          if (isValidToken(voipToken)) {
            console.log(`Waking offline iOS user ${targetUserId} via VoIP push`);
            const channelId = channelUUIDFromGroupId(ws.groupId);
            const note = new apn.Notification();
            note.payload  = { groupId: ws.groupId, senderName: data.sender, "channel-id": channelId };
            note.topic    = "com.visionvivante.marispeak.voip-ptt";
            note.pushType = "voip";
            note.priority = 10;
            note.expiry   = Math.floor(Date.now() / 1000) + 60;
            apnProvider.send(note, voipToken).then((result) => {
              if (result.failed.length > 0) {
                console.error("APNs VoIP push failed:", JSON.stringify(result.failed, null, 2));
                const bad = ["BadDeviceToken","Unregistered","ExpiredToken"];
                if (result.failed.some(f => bad.includes(f.response?.reason))) {
                  userVoipTokens[targetUserId] = null;
                  console.log(`Cleared expired token for ${targetUserId}`);
                }
              } else {
                console.log(`VoIP push sent successfully to ${targetUserId}`);
              }
            });
          } else {
            console.log(`Cannot wake ${targetUserId} — no valid VoIP token stored`);
          }
        }
      }
    } catch (err) {
      console.error("Message error:", err);
    }
  });

  ws.on("close", () => {
    cleanupClient(ws);
    console.log(`Client disconnected: ${ws.userId || "unknown"}`);
  });
});

app.get("/", (req, res) => res.send("PTT WebSocket server running"));

const PORT = process.env.PORT || 3010;
server.listen(PORT, () => {
  console.log(`Server on port ${PORT}`);
  console.log(`APNs: production=true`);
});
