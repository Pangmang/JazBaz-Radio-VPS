const crypto = require("crypto");

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

const STREAM_TOKEN_SECRET_FILE =
  "/home/baz/jb-radio/config/stream-token-secret.txt";

const STREAM_TOKEN_SECRET = fs
  .readFileSync(
    STREAM_TOKEN_SECRET_FILE,
    "utf8",
  )
  .trim();

const usedNonces = new Map();

let activeSocket = null;

let ffmpeg = null;

let icecastSocket = null;

let pcmReady = false;

function cleanUsedNonces() {
  const now =
    Math.floor(Date.now() / 1000);

  for (const [
    nonce,
    expires,
  ] of usedNonces.entries()) {
    if (expires <= now) {
      usedNonces.delete(nonce);
    }
  }
}

function validateStreamToken(token) {
  if (!token) {
    return {
      valid: false,
      reason: "Missing token",
    };
  }

  const parts =
    token.split(".");

  if (parts.length !== 3) {
    return {
      valid: false,
      reason: "Invalid token format",
    };
  }

  const [
    expiresText,
    nonce,
    suppliedSignature,
  ] = parts;

  const expires =
    Number(expiresText);

  if (
    !Number.isInteger(expires) ||
    !nonce ||
    !suppliedSignature
  ) {
    return {
      valid: false,
      reason: "Invalid token",
    };
  }

  const now =
    Math.floor(Date.now() / 1000);

  if (expires <= now) {
    return {
      valid: false,
      reason: "Token expired",
    };
  }

  cleanUsedNonces();

  if (usedNonces.has(nonce)) {
    return {
      valid: false,
      reason: "Token already used",
    };
  }

  const payload =
    `${expiresText}.${nonce}`;

  const expectedSignature =
    crypto
      .createHmac(
        "sha256",
        STREAM_TOKEN_SECRET,
      )
      .update(payload)
      .digest("hex");

  const suppliedBuffer =
    Buffer.from(
      suppliedSignature,
      "utf8",
    );

  const expectedBuffer =
    Buffer.from(
      expectedSignature,
      "utf8",
    );

  if (
    suppliedBuffer.length !==
    expectedBuffer.length
  ) {
    return {
      valid: false,
      reason: "Invalid signature",
    };
  }

  if (
    !crypto.timingSafeEqual(
      suppliedBuffer,
      expectedBuffer,
    )
  ) {
    return {
      valid: false,
      reason: "Invalid signature",
    };
  }

  return {
    valid: true,
    nonce,
    expires,
  };
}

function rejectUpgrade(
  socket,
  statusCode,
  message,
) {
  try {
    socket.write(
      `HTTP/1.1 ${statusCode} ${message}\r\n` +
        "Connection: close\r\n" +
        "Content-Type: text/plain\r\n" +
        `Content-Length: ${Buffer.byteLength(
          message,
        )}\r\n` +
        "\r\n" +
        message,
    );
  } catch {}

  try {
    socket.destroy();
  } catch {}
}

function stopBroadcast(
  reason = "Broadcast stopped",
) {
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

function startFfmpeg() {
  const process = spawn(
    "ffmpeg",
    [
      "-hide_banner",
      "-loglevel",
      "warning",

      "-f",
      "webm",
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
      "192k",

      "-f",
      "mp3",

      "pipe:1",
    ],
    {
      stdio: [
        "pipe",
        "pipe",
        "pipe",
      ],
    },
  );

  ffmpeg = process;

  process.stdout.on(
    "data",
    (chunk) => {
      if (
        icecastSocket &&
        !icecastSocket.destroyed &&
        icecastSocket.writable
      ) {
        icecastSocket.write(
          chunk,
        );
      }
    },
  );

  process.stderr.on(
    "data",
    (data) => {
      const message =
        data
          .toString()
          .trim();

      if (message) {
        console.log(
          `FFmpeg: ${message}`,
        );
      }
    },
  );

  process.on(
    "close",
    (code) => {
      console.log(
        `FFmpeg stopped with code ${code}`,
      );

      if (
        ffmpeg === process
      ) {
        ffmpeg = null;
      }
    },
  );

  pcmReady = true;

  console.log(
    "Opus relay started: WebM/Opus -> stereo MP3 192k",
  );
}

function startRelay() {
  const password = fs
    .readFileSync(
      PASSWORD_FILE,
      "utf8",
    )
    .trim();

  const auth = Buffer.from(
    `${ICECAST_USER}:${password}`,
  ).toString("base64");

  const socket =
    net.createConnection({
      host: ICECAST_HOST,
      port: ICECAST_PORT,
    });

  icecastSocket = socket;

  let responseLogged = false;

  let responseBuffer = "";

  socket.on(
    "connect",
    () => {
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

      startFfmpeg();
    },
  );

  socket.on(
    "data",
    (data) => {
      if (responseLogged) {
        return;
      }

      responseBuffer +=
        data.toString(
          "latin1",
        );

      const lineEnd =
        responseBuffer.indexOf(
          "\r\n",
        );

      if (lineEnd === -1) {
        return;
      }

      const statusLine =
        responseBuffer.slice(
          0,
          lineEnd,
        );

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
    },
  );

  socket.on(
    "error",
    (error) => {
      console.error(
        "Icecast error:",
        error.message,
      );

      if (
        socket ===
        icecastSocket
      ) {
        stopBroadcast(
          "Icecast connection failed",
        );
      }
    },
  );

  socket.on(
    "close",
    () => {
      if (
        socket ===
          icecastSocket &&
        activeSocket
      ) {
        stopBroadcast(
          "Icecast connection closed",
        );
      }
    },
  );
}

const server =
  http.createServer(
    (req, res) => {
      if (
        req.url ===
        "/health"
      ) {
        res.writeHead(
          200,
          {
            "Content-Type":
              "application/json",
          },
        );

        res.end(
          JSON.stringify({
            ok: true,
            broadcasting:
              Boolean(
                activeSocket,
              ),
            pcmReady,
          }),
        );

        return;
      }

      res.writeHead(404);

      res.end();
    },
  );

const wss =
  new WebSocketServer({
    noServer: true,
  });

server.on(
  "upgrade",
  (
    request,
    socket,
    head,
  ) => {
    let requestUrl;

    try {
      requestUrl = new URL(
        request.url,
        "http://127.0.0.1",
      );
    } catch {
      rejectUpgrade(
        socket,
        400,
        "Bad Request",
      );

      return;
    }

    if (
      requestUrl.pathname !==
      "/dj-stream"
    ) {
      rejectUpgrade(
        socket,
        404,
        "Not Found",
      );

      return;
    }

    const token =
      requestUrl.searchParams.get(
        "token",
      );

    const validation =
      validateStreamToken(
        token,
      );

    if (!validation.valid) {
      console.log(
        `DJ connection rejected: ${validation.reason}`,
      );

      rejectUpgrade(
        socket,
        401,
        "Unauthorized",
      );

      return;
    }

    if (activeSocket) {
      rejectUpgrade(
        socket,
        503,
        "Another DJ is already broadcasting",
      );

      return;
    }

    usedNonces.set(
      validation.nonce,
      validation.expires,
    );

    wss.handleUpgrade(
      request,
      socket,
      head,
      (webSocket) => {
        wss.emit(
          "connection",
          webSocket,
          request,
        );
      },
    );
  },
);

wss.on(
  "connection",
  (socket) => {
    if (activeSocket) {
      socket.close(
        1013,
        "Another DJ is already broadcasting",
      );

      return;
    }

    activeSocket = socket;

    console.log(
      "DJ connected with valid stream token",
    );

    socket.on(
      "message",
      (
        data,
        isBinary,
      ) => {
        if (!isBinary) {
          try {
            const message =
              JSON.parse(
                data.toString(),
              );

            if (
              message.type ===
                "opus-start" &&
              message.format ===
                "webm"
            ) {
              if (
                !ffmpeg &&
                !icecastSocket
              ) {
                startRelay();
              }
            }

            return;
          } catch (
            error
          ) {
            console.error(
              "Control message error:",
              error instanceof
                Error
                ? error.message
                : error,
            );

            return;
          }
        }

        if (
          pcmReady &&
          ffmpeg &&
          ffmpeg.stdin
            .writable
        ) {
          const canContinue =
            ffmpeg.stdin.write(
              data,
            );

          if (!canContinue) {
            pcmReady = false;

            ffmpeg.stdin.once(
              "drain",
              () => {
                if (
                  ffmpeg &&
                  ffmpeg.stdin
                    .writable
                ) {
                  pcmReady = true;
                }
              },
            );
          }
        }
      },
    );

    socket.on(
      "close",
      () => {
        if (
          socket ===
          activeSocket
        ) {
          stopBroadcast(
            "DJ disconnected",
          );
        }
      },
    );

    socket.on(
      "error",
      (error) => {
        console.error(
          "WebSocket error:",
          error.message,
        );

        if (
          socket ===
          activeSocket
        ) {
          stopBroadcast(
            "DJ WebSocket error",
          );
        }
      },
    );
  },
);

server.listen(
  PORT,
  "127.0.0.1",
  () => {
    console.log(
      `JB Radio Opus receiver listening on 127.0.0.1:${PORT}`,
    );
  },
);

process.on(
  "SIGINT",
  () => {
    stopBroadcast(
      "Receiver shutting down",
    );

    server.close(() => {
      process.exit(0);
    });
  },
);

process.on(
  "SIGTERM",
  () => {
    stopBroadcast(
      "Receiver shutting down",
    );

    server.close(() => {
      process.exit(0);
    });
  },
);