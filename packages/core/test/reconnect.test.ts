// Studio 里点 Test 会让插件重连，短时间内同一通道上会存在新旧两条 WS 连接。
// 这些用例锁住"只服务最新一条连接"的行为——旧代码在这种情况下会陷入微任务活锁，
// 把桌面 App 的事件循环饿死（App 卡死、AI Agent 连不上）。
import { describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import { BridgeServer } from "../src/bridge/httpServer";
import { CommandQueue } from "../src/bridge/commandQueue";
import { EventBus } from "../src/bridge/events";
import { ConfirmStore } from "../src/safety/confirm";
import { Harness } from "../src/harness/harness";

const TOKEN = "test-token";

function nextTick(ms = 50): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe("CommandQueue reconnect", () => {
  it("keeps the event loop alive when two connections poll the same queue", async () => {
    const queue = new CommandQueue(25_000);
    const first = queue.attachConnection();
    const second = queue.attachConnection();

    // 被顶掉的连接不再是活动连接，推送循环应当据此退出。
    expect(queue.isCurrentConnection(first)).toBe(false);
    expect(queue.isCurrentConnection(second)).toBe(true);

    // 即使旧连接继续 poll，也不会和新连接互相唤醒（不会 busy-loop）。
    let spins = 0;
    const LIMIT = 5_000;
    let ticks = 0;
    const iv = setInterval(() => { ticks++; }, 1);
    const stalePump = async () => {
      while (spins < LIMIT && queue.isCurrentConnection(first)) {
        spins++;
        await queue.poll("studio", first);
      }
    };
    const livePump = async () => {
      while (spins < LIMIT && queue.isCurrentConnection(second)) {
        spins++;
        await queue.poll("studio", second);
      }
    };
    void stalePump();
    void livePump();
    await nextTick(60);
    clearInterval(iv);

    expect(ticks).toBeGreaterThan(3); // 事件循环还在转
    queue.shutdown();
  });

  it("a stale poll never parks a waiter or steals commands", async () => {
    const queue = new CommandQueue(50);
    const stale = queue.attachConnection();
    const live = queue.attachConnection();
    queue.setConnectedSession("studio");

    const stalePoll = queue.poll("studio", stale);
    const livePoll = queue.poll("studio", live);
    const dispatched = queue.dispatch("get_run_state", {});

    // 命令必须交给新连接。
    const env = await livePoll;
    expect(env?.tool).toBe("get_run_state");
    // 旧连接只会在超时后拿到 null。
    await expect(stalePoll).resolves.toBeNull();

    queue.resolveResponse({ id: env!.id, ok: true, result: "ok" });
    await expect(dispatched).resolves.toBe("ok");
  });

  it("a late close from the replaced connection does not mark the queue offline", () => {
    const queue = new CommandQueue(50);
    const stale = queue.attachConnection();
    queue.setConnectedSession("studio");
    const live = queue.attachConnection();
    queue.setConnectedSession("studio");
    queue.setPluginTools(["get_run_state"]);

    queue.markDisconnected(stale); // 旧连接姗姗来迟的 close
    expect(queue.isPluginConnected()).toBe(true);
    expect(queue.isCurrentConnection(live)).toBe(true);
    expect(queue.supportsTool("get_run_state")).toBe(true);

    queue.markDisconnected(live);
    expect(queue.isPluginConnected()).toBe(false);
  });
});

describe("BridgeServer reconnect", () => {
  it("survives a plugin reconnect and keeps serving commands", async () => {
    const port = 39412;
    const queue = new CommandQueue(25_000);
    const agentQueue = new CommandQueue(25_000, "agent");
    const events = new EventBus();
    const server = new BridgeServer({
      port,
      token: TOKEN,
      queue,
      agentQueue,
      confirm: new ConfirmStore(),
      harness: new Harness(),
      events,
    });
    await server.start();

    const open = async () => {
      const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`, {
        headers: { Authorization: `Bearer ${TOKEN}`, "X-Roblox-MCP": "1" },
      });
      await new Promise((r) => ws.once("open", r));
      ws.send(JSON.stringify({
        type: "handshake",
        sessionId: "studio-session",
        pluginVersion: "1",
        tools: ["get_run_state"],
      }));
      return ws;
    };

    try {
      const before = await open();
      await nextTick();

      // 用户点 Test：Studio 丢掉旧连接并重连（旧 socket 没发关闭帧）。
      const after = await open();

      // 事件循环必须还活着。
      let ticks = 0;
      const iv = setInterval(() => { ticks++; }, 5);
      await nextTick(150);
      clearInterval(iv);
      expect(ticks).toBeGreaterThan(3);

      // 旧连接被服务端掐掉，新连接接管。
      expect(before.readyState).not.toBe(WebSocket.OPEN);
      expect(queue.isPluginConnected()).toBe(true);

      // 新连接照常收发命令。
      const gotCommand = new Promise<any>((resolve) => {
        after.on("message", (raw) => {
          const msg = JSON.parse(raw.toString());
          if (msg.type === "command") resolve(msg.payload);
        });
      });
      const result = queue.dispatch("get_run_state", {});
      const env = await gotCommand;
      after.send(JSON.stringify({ type: "response", id: env.id, ok: true, result: { state: "Edit" } }));
      await expect(result).resolves.toEqual({ state: "Edit" });

      after.terminate();
    } finally {
      await server.stop();
      queue.shutdown();
      agentQueue.shutdown();
    }
  }, 20_000);
});
