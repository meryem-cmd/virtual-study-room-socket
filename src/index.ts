import "dotenv/config";
import express from "express";
import { createServer } from "node:http";
import { Server } from "socket.io";
import cors from "cors";

const PORT = process.env.PORT || 3001;
const CLIENT_URL = process.env.CLIENT_URL || "http://localhost:3000";

const app = express();
app.use(cors({ origin: CLIENT_URL }));

// Simple health check — useful for confirming the server is alive,
// and Railway/Render will use something like this for deploy health checks later
app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

const httpServer = createServer(app);

const io = new Server(httpServer, {
  cors: { origin: CLIENT_URL, methods: ["GET", "POST"] },
});

type RoomJoinPayload = { roomCode: string; userName: string };
type RoomLeavePayload = { roomCode: string };

// In-memory roster: roomCode -> Map<socketId, userName>
// This resets if the server restarts — fine for now, but note for later:
// a production version would need this to survive server restarts/multiple instances.
const roomRosters = new Map<string, Map<string, string>>();

function getRoster(roomCode: string) {
  const roster = roomRosters.get(roomCode);
  if (!roster) return [];
  return Array.from(roster.entries()).map(([socketId, userName]) => ({
    socketId,
    userName,
  }));
}

// ---------- Timer (server-authoritative) ----------
type TimerMode = "focus" | "break";
type TimerState = {
  mode: TimerMode;
  status: "idle" | "running" | "paused";
  endsAt: number | null; // server timestamp (ms) when it finishes; only set while running
  remainingMs: number; // time left when idle/paused
};

const DURATIONS: Record<TimerMode, number> = {
  focus: 25 * 60 * 1000,
  break: 5 * 60 * 1000,
};

const roomTimers = new Map<string, TimerState>();
const timerTimeouts = new Map<string, NodeJS.Timeout>();

function getTimer(roomCode: string): TimerState {
  let t = roomTimers.get(roomCode);
  if (!t) {
    t = { mode: "focus", status: "idle", endsAt: null, remainingMs: DURATIONS.focus };
    roomTimers.set(roomCode, t);
  }
  return t;
}

// serverNow lets each browser work out how far its clock is from the server's
function timerPayload(roomCode: string) {
  return { ...getTimer(roomCode), serverNow: Date.now() };
}

function broadcastTimer(roomCode: string) {
  io.to(roomCode).emit("timer:state", timerPayload(roomCode));
}

// When a running timer hits zero, flip focus <-> break and stop, ready for the next round
function scheduleEnd(roomCode: string) {
  clearTimeout(timerTimeouts.get(roomCode));
  const t = getTimer(roomCode);
  if (t.status !== "running" || t.endsAt === null) return;

  const delay = Math.max(0, t.endsAt - Date.now());
  timerTimeouts.set(
    roomCode,
    setTimeout(() => {
      const nextMode: TimerMode = getTimer(roomCode).mode === "focus" ? "break" : "focus";
      roomTimers.set(roomCode, {
        mode: nextMode,
        status: "idle",
        endsAt: null,
        remainingMs: DURATIONS[nextMode],
      });
      broadcastTimer(roomCode);
    }, delay)
  );
}

// Free memory when the last person leaves a room
function cleanupRoomIfEmpty(roomCode: string) {
  const roster = roomRosters.get(roomCode);
  if (roster && roster.size === 0) {
    roomRosters.delete(roomCode);
    roomTimers.delete(roomCode);
    clearTimeout(timerTimeouts.get(roomCode));
    timerTimeouts.delete(roomCode);
  }
}

io.on("connection", (socket) => {
  console.log(`Client connected: ${socket.id}`);

  socket.on("room:join", ({ roomCode, userName }: RoomJoinPayload) => {
    socket.join(roomCode);
    socket.data.userName = userName;
    socket.data.roomCode = roomCode;

    if (!roomRosters.has(roomCode)) {
      roomRosters.set(roomCode, new Map());
    }
    const roster = roomRosters.get(roomCode)!;

    // If this same person (by name) already has a connection in this room
    // (e.g. a quick reconnect), remove the old entry first so we don't show them twice.
    for (const [existingSocketId, existingName] of roster.entries()) {
      if (existingName === userName && existingSocketId !== socket.id) {
        roster.delete(existingSocketId);
        socket.to(roomCode).emit("room:user-left", { socketId: existingSocketId });
      }
    }

    roster.set(socket.id, userName);

    socket.emit("room:roster", getRoster(roomCode));
    // Send the newcomer the current timer so they land on the right number
    socket.emit("timer:state", timerPayload(roomCode));

    socket.to(roomCode).emit("room:user-joined", {
      userName,
      socketId: socket.id,
    });

    console.log(`${userName} joined room ${roomCode}`);
  });

  socket.on("room:leave", ({ roomCode }: RoomLeavePayload) => {
    socket.leave(roomCode);
    roomRosters.get(roomCode)?.delete(socket.id);
    cleanupRoomIfEmpty(roomCode);

    socket.to(roomCode).emit("room:user-left", {
      socketId: socket.id,
    });
  });

  socket.on("disconnect", () => {
    const { roomCode } = socket.data;
    if (roomCode) {
      roomRosters.get(roomCode)?.delete(socket.id);
      cleanupRoomIfEmpty(roomCode);

      socket.to(roomCode).emit("room:user-left", {
        socketId: socket.id,
      });
    }
    console.log(`Client disconnected: ${socket.id}`);
  });

  socket.on("timer:start", ({ roomCode }: { roomCode: string }) => {
    const t = getTimer(roomCode);
    if (t.status === "running") return;
    t.status = "running";
    t.endsAt = Date.now() + t.remainingMs;
    scheduleEnd(roomCode);
    broadcastTimer(roomCode);
  });

  socket.on("timer:pause", ({ roomCode }: { roomCode: string }) => {
    const t = getTimer(roomCode);
    if (t.status !== "running" || t.endsAt === null) return;
    t.remainingMs = Math.max(0, t.endsAt - Date.now());
    t.status = "paused";
    t.endsAt = null;
    clearTimeout(timerTimeouts.get(roomCode));
    broadcastTimer(roomCode);
  });

  socket.on("timer:reset", ({ roomCode }: { roomCode: string }) => {
    clearTimeout(timerTimeouts.get(roomCode));
    roomTimers.set(roomCode, {
      mode: "focus",
      status: "idle",
      endsAt: null,
      remainingMs: DURATIONS.focus,
    });
    broadcastTimer(roomCode);
  });

    socket.on(
    "chat:message",
    (msg: { roomCode: string; userName: string; content: string; createdAt: string }) => {
      // Broadcast to everyone else in the room — the sender already shows their own message locally
      socket.to(msg.roomCode).emit("chat:message", msg);
    }
  );
    socket.on(
    "call:offer",
    ({ roomCode, offer, toSocketId }: { roomCode: string; offer: unknown; toSocketId: string }) => {
      io.to(toSocketId).emit("call:offer", { offer, fromSocketId: socket.id });
    }
  );

  socket.on(
    "call:answer",
    ({ answer, toSocketId }: { answer: unknown; toSocketId: string }) => {
      io.to(toSocketId).emit("call:answer", { answer, fromSocketId: socket.id });
    }
  );

  socket.on(
    "call:ice-candidate",
    ({ candidate, toSocketId }: { candidate: unknown; toSocketId: string }) => {
      io.to(toSocketId).emit("call:ice-candidate", { candidate, fromSocketId: socket.id });
    }
  );

  socket.on("call:end", ({ toSocketId }: { toSocketId: string }) => {
    io.to(toSocketId).emit("call:ended");
  });
});

httpServer.listen(PORT, () => {
  console.log(`Socket.IO server running on http://localhost:${PORT}`);
});