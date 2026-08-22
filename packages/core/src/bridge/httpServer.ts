// 本地桥服务器：
//   - /mcp     给 Claude Code 的 MCP (Streamable HTTP) 端点（HTTP）
//   - /ws      Studio 插件 / 运行时 agent 的 WebSocket 通道（持久双向连接）
//   - /health  诊断（HTTP）
// 只绑定 127.0.0.1；HTTP 与 WS 升级都先过鉴权中间件。
//
// 传输已从长轮询切换为 WebSocket（Studio 的 HttpService:CreateWebStreamClient）。
// WS 处理器复用现有 CommandQueue：每连接跑一个 poll→send 推送循环，收到 response/event 即回灌。

import { createServer, IncomingMessage, Server, ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";
import type { Duplex } from "node:stream";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { buildMcpServer, SERVER_VERSION } from "../mcp/server.js";
import { authorizeRequest } from "../security/auth.js";
import { PROTOCOL_VERSION, HandshakeInfo, ResponseEnvelope } from "./envelope.js";
import type { CommandQueue } from "./commandQueue.js";
import type { ConfirmStore } from "../safety/confirm.js";
import type { Harness } from "../harness/harness.js";
import type { EventBus } from "./events.js";

export interface BridgeServerOptions {
  port: number;
  token: string;
  queue: CommandQueue;
  /** 运行时 server-agent 通道。 */
  agentQueue: CommandQueue;
  confirm: ConfirmStore;
  harness: Harness;
  events: EventBus;
  /** handshake 回调（用于通知 UI 有插件接入）。 */
  onHandshake?: (info: HandshakeInfo) => void;
}

const HOST = "127.0.0.1";
/** WS 保活 ping 间隔。 */
const HEARTBEAT_MS = 30_000;
/**
 * 连续这么多次没回 pong 才掐断（约 90s）。
 * 留足余量：插件执行长命令（run_luau 默认 30s）期间不能被误判成死连接。
 */
const MAX_MISSED_PONGS = 3;

export class BridgeServer {
  private server: Server | null = null;
  private wss: WebSocketServer | null = null;
  private mcpTransports = new Map<string, StreamableHTTPServerTransport>();
  /** 每个通道当前的活动 socket（plugin / agent 各一条），新连接会顶掉旧的。 */
  private activeSockets = new Map<string, WebSocket>();
  /** 保活：每条 socket 连续未回 pong 的次数。 */
  private missedPongs = new WeakMap<WebSocket, number>();
  private heartbeat: ReturnType<typeof setInterval> | null = null;
  private lastMcpAt = 0;
  private mcpClient: { name: string; version?: string } | null = null;

  constructor(private readonly opts: BridgeServerOptions) {}

  /** Claude Code 最近是否有 MCP 活动（用于 UI 判断"已连接"）。 */
  mcpActiveRecently(): boolean {
    return this.lastMcpAt > 0 && Date.now() - this.lastMcpAt < 60_000;
  }

  /** 最近一次 MCP initialize 上报的客户端（如 "claude-code" / "gemini-cli" / "cursor"）。 */
  getMcpClient(): { name: string; version?: string } | null {
    return this.mcpClient;
  }

  start(): Promise<void> {
    return new Promise((resolve, reject) => {
      const server = createServer((req, res) => {
        this.handle(req, res).catch((e) => {
          if (!res.headersSent) {
            res.writeHead(500, { "content-type": "application/json" });
          }
          res.end(JSON.stringify({ error: String(e) }));
        });
      });
      this.wss = new WebSocketServer({ noServer: true });
      server.on("upgrade", (req, socket, head) => this.handleUpgrade(req, socket as Duplex, head));
      server.on("error", reject);
      server.listen(this.opts.port, HOST, () => resolve());
      this.server = server;
      this.heartbeat = setInterval(() => this.sweepDeadSockets(), HEARTBEAT_MS);
      this.heartbeat.unref?.();
    });
  }

  async stop(): Promise<void> {
    if (this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = null;
    }
    this.activeSockets.clear();
    for (const t of this.mcpTransports.values()) {
      try {
        await t.close();
      } catch {
        // ignore
      }
    }
    this.mcpTransports.clear();
    if (this.wss) {
      for (const client of this.wss.clients) {
        try {
          client.terminate();
        } catch {
          // ignore
        }
      }
      this.wss.close();
      this.wss = null;
    }
    await new Promise<void>((resolve) => {
      if (!this.server) return resolve();
      this.server.close(() => resolve());
    });
    this.server = null;
  }

  /**
   * ping 所有 WS 客户端，掐掉连续 MAX_MISSED_PONGS 轮没回 pong 的。
   * Studio 进入 Test 时经常直接丢弃 socket 而不发关闭帧，服务端会一直以为它还开着；
   * 没有保活的话这些半开连接会永远占着通道。
   */
  private sweepDeadSockets(): void {
    if (!this.wss) return;
    for (const ws of this.wss.clients) {
      const missed = (this.missedPongs.get(ws) ?? 0) + 1;
      if (missed > MAX_MISSED_PONGS) {
        try {
          ws.terminate();
        } catch {
          // ignore
        }
        continue;
      }
      this.missedPongs.set(ws, missed);
      try {
        ws.ping();
      } catch {
        // ignore
      }
    }
  }

  // ---- WebSocket 升级（插件 / agent 通道） ----
  private handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void {
    const url = new URL(req.url ?? "/", `http://${HOST}:${this.opts.port}`);
    if (url.pathname !== "/ws") {
      socket.destroy();
      return;
    }
    const auth = authorizeRequest(req, { token: this.opts.token, port: this.opts.port });
    if (!auth.ok) {
      socket.write(`HTTP/1.1 ${auth.status ?? 401} Unauthorized\r\n\r\n`);
      socket.destroy();
      return;
    }
    this.wss!.handleUpgrade(req, socket, head, (ws) => this.handleWs(ws, url));
  }

  private handleWs(ws: WebSocket, url: URL): void {
    const isAgent = url.searchParams.get("role") === "agent";
    const channel = isAgent ? "agent" : "plugin";
    const queue = isAgent ? this.opts.agentQueue : this.opts.queue;
    /** 本连接在队列里的归属 id；0 = 还没握手。 */
    let connId = 0;
    let sid: string = randomUUID();

    this.missedPongs.set(ws, 0);
    ws.on("pong", () => this.missedPongs.set(ws, 0));

    // 推送循环：把队列里的命令通过 WS 即时发给插件。
    // 循环条件里必须带 isCurrentConnection：被新连接顶掉后要立刻退出，
    // 否则新旧两个循环会互相唤醒对方的 poll waiter，把事件循环彻底饿死。
    const pump = async (myConn: number) => {
      try {
        while (ws.readyState === WebSocket.OPEN && queue.isCurrentConnection(myConn)) {
          const env = await queue.poll(sid, myConn);
          if (env && ws.readyState === WebSocket.OPEN && queue.isCurrentConnection(myConn)) {
            ws.send(JSON.stringify({ type: "command", payload: env }));
          }
        }
      } catch {
        // 连接关闭等，退出循环即可。
      }
    };

    ws.on("message", (data) => {
      let msg: { type?: string; [k: string]: unknown };
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (!msg || typeof msg.type !== "string") return;

      if (msg.type === "handshake") {
        if (typeof msg.sessionId === "string" && msg.sessionId) sid = msg.sessionId;

        // 同一通道只保留最新一条连接：把上一条掐掉。
        // 用户在 Studio 点 Test 时，Studio 会丢掉旧 WebSocket 并让插件重连，
        // 而旧连接往往没发关闭帧，服务端还以为它开着。
        const previous = this.activeSockets.get(channel);
        if (previous && previous !== ws) {
          try {
            previous.terminate();
          } catch {
            // ignore
          }
        }
        this.activeSockets.set(channel, ws);

        connId = queue.attachConnection();
        queue.setConnectedSession(sid);
        if (isAgent) {
          this.opts.events.publish({ type: "agentState", state: "online" });
        } else {
          queue.setPluginTools(msg.tools as string[] | undefined);
          this.opts.onHandshake?.(msg as unknown as HandshakeInfo);
        }
        ws.send(
          JSON.stringify({ type: "handshake_ok", protocol: PROTOCOL_VERSION, serverVersion: SERVER_VERSION }),
        );
        void pump(connId);
      } else if (msg.type === "response") {
        // id 可能属于任一通道，两边都试一下。
        const r = msg as unknown as ResponseEnvelope;
        void (this.opts.queue.resolveResponse(r) || this.opts.agentQueue.resolveResponse(r));
      } else if (msg.type === "event") {
        const ev = msg.event as { type?: string } | undefined;
        if (ev && typeof ev.type === "string") this.opts.events.publish(ev as { type: string });
      }
    });

    ws.on("close", () => {
      // 已经被新连接顶掉的旧 socket：它的 close 迟到，不能把新连接判成离线。
      if (this.activeSockets.get(channel) === ws) this.activeSockets.delete(channel);
      if (connId === 0 || !queue.isCurrentConnection(connId)) return;
      queue.markDisconnected(connId);
      if (isAgent) this.opts.events.publish({ type: "agentState", state: "offline" });
    });
    ws.on("error", () => {
      // close 事件会随后触发，这里不额外处理。
    });
  }

  private async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? "/", `http://${HOST}:${this.opts.port}`);
    const path = url.pathname;

    // 鉴权（所有端点）。
    const auth = authorizeRequest(req, { token: this.opts.token, port: this.opts.port });
    if (!auth.ok) {
      res.writeHead(auth.status ?? 401, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: auth.message ?? "Unauthorized" }));
      return;
    }

    if (path === "/mcp") return this.handleMcp(req, res);
    if (path === "/health" && req.method === "GET") return this.handleHealth(res);

    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "Not found" }));
  }

  // ---- MCP (Streamable HTTP，带会话) ----
  private async handleMcp(req: IncomingMessage, res: ServerResponse): Promise<void> {
    this.lastMcpAt = Date.now();
    const sessionId = req.headers["mcp-session-id"] as string | undefined;
    let transport = sessionId ? this.mcpTransports.get(sessionId) : undefined;

    let body: unknown;
    if (req.method === "POST") body = await readJsonBody(req);

    if (!transport) {
      if (req.method === "POST" && isInitializeRequest(body)) {
        // 记录是哪个 AI 客户端连上来的（MCP initialize 会带 clientInfo）。
        const info = (body as { params?: { clientInfo?: { name?: string; version?: string } } })?.params
          ?.clientInfo;
        if (info?.name) this.mcpClient = { name: info.name, version: info.version };
        transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => randomUUID(),
          onsessioninitialized: (sid) => {
            this.mcpTransports.set(sid, transport!);
          },
        });
        transport.onclose = () => {
          if (transport!.sessionId) this.mcpTransports.delete(transport!.sessionId);
        };
        const mcp = buildMcpServer({
          queue: this.opts.queue,
          agentQueue: this.opts.agentQueue,
          confirm: this.opts.confirm,
          harness: this.opts.harness,
          events: this.opts.events,
          serverInfo: { port: this.opts.port, token: this.opts.token },
        });
        await mcp.connect(transport);
      } else {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            jsonrpc: "2.0",
            error: { code: -32000, message: "No valid session; send an initialize request first." },
            id: null,
          }),
        );
        return;
      }
    }

    await transport.handleRequest(req, res, body);
  }

  // ---- 健康检查 ----
  private handleHealth(res: ServerResponse): void {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        ok: true,
        serverVersion: SERVER_VERSION,
        pluginConnected: this.opts.queue.isPluginConnected(),
        agentConnected: this.opts.agentQueue.isPluginConnected(),
        queueDepth: this.opts.queue.queueDepth,
      }),
    );
  }
}

function readJsonBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      if (chunks.length === 0) return resolve(undefined);
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
      } catch (e) {
        reject(e);
      }
    });
    req.on("error", reject);
  });
}
