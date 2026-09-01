const http = require("http");
const net = require("net");
const fs = require("fs");
const { spawn } = require("child_process");
const { WebSocketServer } = require("ws");

const PORT = 8200;

const ICECAST_HOST = "127.0.0.1";
const ICECAST_PORT = 8100;
const ICECAST_MOUNT = "/jbradio";
const ICECAST_USER = "jbradio";

const PASSWORD_FILE =
  "/home/baz/jb-radio/config/source-password.txt";

let activeSocket = null;
let ffmpeg = null;
let icecastSocket = null;
let pcmReady = false;

function stopBroadcast(reason = "Broadcast stopped") {
  console.log(reason);

  pcmReady = false;

  if (activeSocket) {
    const socket = activeSocket;
    activeSocket = null;

    try {
      socket.close();
    } catch {}
  }

  if (ffmpeg) {
    const process = ffmpeg;
    ffmpeg = null;

    try {
      process.stdin.end();
    } catch {}

    try {
      process.kill("SIGTERM");
    } catch {}
  }

  if (icecastSocket) {
    const socket = icecastSocket;
    icecastSocket = null;

    try {
      socket.destroy();
    } catch {}
  }
}

function startFfmpeg(sampleRate) {
  const process = spawn(
    "ffmpeg",
    [
      "-hide_banner",
      "-loglevel",
      "warning",

      "-f",
      "s16le",

      "-ar",
      String(sampleRate),

      "-ac",
      "1",

      "-i",
      "pipe:0",

      "-vn",

      "-ac",
      "2",

      "-ar",
      "44100",

      "-codec:a",
      "libmp3lame",

      "-b:a",
      "128k",

      "-f",
      "mp3",

      "pipe:1",
    ],
    {
      stdio: ["pipe", "pipe", "pipe"],
    },
  );

  ffmpeg = process;

  process.stdout.on("data", (chunk) => {
    if (
      icecastSocket &&
      !icecastSocket.destroyed &&
      icecastSocket.writable
    ) {
      icecastSocket.write(chunk);
    }
  });

  process.stderr.on("data", (data) => {
    const message = data.toString().trim();

    if (message) {
      console.log(`FFmpeg: ${message}`);
    }
  });

  process.on("close", (code) => {
    console.log(
      `FFmpeg stopped with code ${code}`,
    );

    if (ffmpeg === process) {
      ffmpeg = null;
    }
  });

  pcmReady = true;

  console.log(
    `PCM relay started: ${sampleRate} Hz mono`,
  );
}

function startRelay(sampleRate) {
  const password = fs
    .readFileSync(PASSWORD_FILE, "utf8")
    .trim();

  const auth = Buffer.from(
    `${ICECAST_USER}:${password}`,
  ).toString("base64");

  const socket = net.createConnection({
    host: ICECAST_HOST,
    port: ICECAST_PORT,
  });

  icecastSocket = socket;

  let responseLogged = false;
  let responseBuffer = "";

  socket.on("connect", () => {
    const headers = [
      `PUT ${ICECAST_MOUNT} HTTP/1.0`,
      `Authorization: Basic ${auth}`,
      "Content-Type: audio/mpeg",
      "Ice-Public: 0",
      "Ice-Name: JB Radio",
      "Ice-Description: JazBaz Philippines Radio",
      "Connection: close",
      "",
      "",
    ].join("\r\n");

    socket.write(headers);

    startFfmpeg(sampleRate);
  });

  socket.on("data", (data) => {
    if (responseLogged) {
      return;
    }

    responseBuffer += data.toString("latin1");

    const lineEnd =
      responseBuffer.indexOf("\r\n");

    if (lineEnd === -1) {
      return;
    }

    const statusLine =
      responseBuffer.slice(0, lineEnd);

    const match =
      statusLine.match(
        /^HTTP\/\d+\.\d+\s+(\d+)/,
      );

    if (match) {
      console.log(
        `Icecast response: ${match[1]}`,
      );
    } else {
      console.log(
        `Icecast response: ${statusLine}`,
      );
    }

    responseLogged = true;
  });

  socket.on("error", (error) => {
    console.error(
      "Icecast error:",
      error.message,
    );

    if (socket === icecastSocket) {
      stopBroadcast(
        "Icecast connection failed",
      );
    }
  });

  socket.on("close", () => {
    if (
      socket === icecastSocket &&
      activeSocket
    ) {
      stopBroadcast(
        "Icecast connection closed",
      );
    }
  });
}

const server = http.createServer(
  (req, res) => {
    if (req.url === "/health") {
      res.writeHead(200, {
        "Content-Type":
          "application/json",
      });

      res.end(
        JSON.stringify({
          ok: true,
          broadcasting:
            Boolean(activeSocket),
          pcmReady,
        }),
      );

      return;
    }

    res.writeHead(404);
    res.end();
  },
);

const wss = new WebSocketServer({
  server,
});

wss.on("connection", (socket) => {
  if (activeSocket) {
    socket.close(
      1013,
      "Another DJ is already broadcasting",
    );

    return;
  }

  activeSocket = socket;

  console.log("DJ connected");

  socket.on(
    "message",
    (data, isBinary) => {
      if (!isBinary) {
        try {
          const message =
            JSON.parse(
              data.toString(),
            );

          if (
            message.type ===
              "pcm-start" &&
            message.format ===
              "s16le" &&
            message.channels === 1
          ) {
            const sampleRate =
              Number(
                message.sampleRate,
              );

            if (
              !Number.isFinite(
                sampleRate,
              ) ||
              sampleRate < 8000 ||
              sampleRate > 192000
            ) {
              throw new Error(
                "Invalid sample rate",
              );
            }

            if (
              !ffmpeg &&
              !icecastSocket
            ) {
              startRelay(
                sampleRate,
              );
            }
          }

          return;
        } catch (error) {
          console.error(
            "Control message error:",
            error instanceof Error
              ? error.message
              : error,
          );

          return;
        }
      }

      if (
        pcmReady &&
        ffmpeg &&
        ffmpeg.stdin.writable
      ) {
        ffmpeg.stdin.write(data);
      }
    },
  );

  socket.on("close", () => {
    if (socket === activeSocket) {
      stopBroadcast(
        "DJ disconnected",
      );
    }
  });

  socket.on("error", (error) => {
    console.error(
      "WebSocket error:",
      error.message,
    );

    if (socket === activeSocket) {
      stopBroadcast(
        "DJ WebSocket error",
      );
    }
  });
});

server.listen(
  PORT,
  "127.0.0.1",
  () => {
    console.log(
      `JB Radio PCM receiver listening on 127.0.0.1:${PORT}`,
    );
  },
);

process.on("SIGINT", () => {
  stopBroadcast(
    "Receiver shutting down",
  );

  server.close(() => {
    process.exit(0);
  });
});

process.on("SIGTERM", () => {
  stopBroadcast(
    "Receiver shutting down",
  );

  server.close(() => {
    process.exit(0);
  });
});