# 钉钉超时转后台延迟交付（Deferred Delivery）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 钉钉通道在模型回复超时后不再向用户报 `MODEL_REPLY_TIMEOUT` 错误，而是发送"转后台"通知，保留 `/stop` 控制权，并在任务到达终态（成功/失败/停止/中止）时经 sessionWebhook（有效时）或主动推送 API（`sendRobotText`）把结果送达用户。

**Architecture:** 三层改动：(1) 共享 `HarnessClient.ask()` 新增 `deferOnTimeout` 选项——超时时不抛错，返回延迟句柄并保留 control ownership；(2) 新建钉钉专用 `deferred-delivery.mjs` 模块——用 `HarnessReplyTracker` + 全局事件 mux（`watchHarnessEvents`）跟踪延迟 turn，终态时选择路由投递；(3) 钉钉 bridge 在 `#process` 里识别延迟句柄、发转后台通知并注册交付条目。复用飞书 `/watch` 已验证的 watcher 模式（事件过滤 `turn/end` + 断连后 `session.history` 对账）。

**Tech Stack:** Node.js ESM（.mjs）、`node:test`、无新增依赖。

**Spec:** 本计划内嵌规格（见下方"背景与规格"），源自本仓库调研会话（钉钉官方文档调研 + 代码侦察，结论已并入本节）。

## 背景与规格

### 平台事实（调研结论）

- 钉钉"回复"是独立 HTTP POST（消息自带 `sessionWebhook`），与 ACK（2.5s/3s，不可配）无关；`sessionWebhook` 有效期由每条入站消息的 `sessionWebhookExpiredTime` 字段给出（官方示例 ≈90 分钟）。
- 主动推送 API：`v1.0/robot/groupMessages/send`（群）/ `v1.0/robot/oToMessages/batchSend`（单聊），本仓库 `dingtalk-api.mjs` 已封装为 `sendRobotText`。
- 所有 turn 终态（成功/报错/停止/中止/取消）都收敛为带 `reason` 的 `turn/end` 事件。

### 现状（代码锚点）

- 超时是插件自身行为：`ask()` 轮询 history 到 `timeoutMs`（默认 600_000ms）后抛 `harness-reply-timeout`（`src/channels/shared/harness-client.mjs:1480`）→ 映射为 `MODEL_REPLY_TIMEOUT`。
- 超时后 finally 注销 ownership（`:1502-1504`）→ 之后 `/stop` 返回"没有正在运行的任务"（缺陷）。
- 钉钉 bridge `#process`（`src/channels/dingtalk/dingtalk-bridge.mjs:1044`）经 `askInWorkspaceSession`（`:1157`）问询，答案经 `#send`（webhook sendText）或 AI 卡片流投递；错误路径在 `:1241-1281`。
- `HarnessReplyTracker` 已导出（`shared/harness-client.mjs:416`）：`{promptRpcId, afterSeq}` 构造，`consumeAll(events)`、`finished/answer/reason/turn`。
- workspace scope 的 harness 是透传 Proxy（`bot-workspace-store.mjs:965`），`watchHarnessEvents` 天然可达。
- 飞书先例：`#ensureEventWatcher`（`feishu/bridge.mjs:2740-2762`）、`#onHarnessEvent` 过滤 `turn/end`（`:3090-3110`）、`#compensateSession` 经 history 对账（`:3154-3218`）。

### 范围裁定（用户明确指定）

**只做最小改动，仅覆盖"有终态的失败"（turn 会以 turn/end 结束的场景）。以下为明确非目标，不要实现：**

- **不做**后台看门狗/最大生命周期（turn 永不结束 → 用户收到转后台通知后沉默，接受）。
- **不做**重启持久化与对账（bridge 重启 → 内存中的延迟条目丢失，接受）。
- **不做**推送失败重试/机会性补投（webhook 与主动推送都失败 → 仅记日志，接受）。
- **不做**延迟 turn 的产物（artifact）交付（ask() finally 照旧 discard）。
- **不改**交互回复路径（`#processInteractionReply`）与飞书/其它渠道。
- `replyTimeoutMs` 语义变更：从"报错死线"变为"转后台并告知用户的时机"，默认 600_000 → 180_000。

## Global Constraints

- 测试框架 `node:test`；测试命令 `node --test test/*.test.mjs test/channels/*/*.test.mjs`（即 `npm test`）。
- 不新增任何 npm 依赖。
- 用户可见文案一律中文，经 `t()`（`src/channels/shared/i18n.mjs`）包裹，支持 `{param}` 插值。
- 终态文案必须带错误语义（不得把失败显示成"完成"——这是飞书先例的已知缺陷，不许照抄）。
- `sessionWebhookExpiredTime` 不确定时（字段缺失/0）默认信任 webhook 可用；仅当明确过期（留 120 秒余量）才直接走主动推送。
- 不修改共享 `message-failure.mjs` 的既有文案与映射（其它场景仍在用）。
- 每个任务独立可测、独立提交；提交信息沿用仓库风格（`feat:`/`test:`/`refactor:` 前缀）。

---

### Task 1: `ask()` 支持 `deferOnTimeout` 延迟句柄

**Files:**
- Modify: `src/channels/shared/harness-client.mjs:1276-1281`（选项解析）、`:1333-1334`（状态旗标）、`:1480`（超时分支）、`:1491-1509`（finally 守卫）
- Test: `test/channels/shared/harness-control.test.mjs`（在 :389 现有超时测试之后追加）

**Interfaces:**
- Consumes: 现有 `ask(sessionId, prompt, options)`、`#unregisterControlOwnership`、`#unregisterInteractionOwnership`、`baselineSeq`、`tracker`、`ownership`。
- Produces: `ask()` 新选项 `deferOnTimeout: true`。命中超时且 `tracker.turn !== null` 且未 stopRequested 时，**不抛错**，resolve 为延迟句柄：
  ```ts
  {
    deferred: true,          // 判别字段
    sessionId: string,
    turn: number,            // 目标 turn id
    promptRpcId: string,     // 绑定 user/message 的 rpcId
    afterSeq: number,        // 重放 history 的基线 seq
    releaseOwnership(): void // 交付后由调用方释放保留的 ownership
  }
  ```
  Task 2/3 依赖此形状。`tracker.turn === null`（排队中从未开turn）或默认模式 → 行为不变（仍抛 `harness-reply-timeout`）。

- [ ] **Step 1: 写失败测试**

在 `test/channels/shared/harness-control.test.mjs` 的 `test('HarnessClient marks a reply timeout after prompt admission', ...)`（:379-389）之后追加（复用该文件现有 `controlledTurn()` 夹具与 `HarnessTurnError` 导入）：

```js
test('HarnessClient defers a reply timeout with a delivery handle when asked', async () => {
  const turn = controlledTurn();
  const owner = {};
  const control = { owner, key: 'direct:defer' };
  const asking = turn.client.ask(turn.id, 'work', {
    timeoutMs: 1_500,
    deferOnTimeout: true,
    control,
  });
  await turn.admitted;
  const handle = await asking;
  assert.equal(handle.deferred, true);
  assert.equal(handle.sessionId, turn.id);
  assert.equal(typeof handle.turn, 'number');
  assert.equal(typeof handle.promptRpcId, 'string');
  assert.equal(typeof handle.afterSeq, 'number');
  assert.equal(typeof handle.releaseOwnership, 'function');
  // Ownership 在交接后保留，/stop 语义不丢失。
  assert.equal(await turn.client.hasActiveTurn(turn.id, control), true);
  handle.releaseOwnership();
  assert.equal(await turn.client.hasActiveTurn(turn.id, control), false);
});

test('HarnessClient keeps throwing reply timeouts without deferOnTimeout', async () => {
  const turn = controlledTurn();
  const asking = turn.client.ask(turn.id, 'work', { timeoutMs: 1_500 });
  await turn.admitted;
  await assert.rejects(asking, (error) => {
    assert.ok(error instanceof HarnessTurnError);
    assert.equal(error.code, 'harness-reply-timeout');
    assert.equal(error.promptAccepted, true);
    return true;
  });
});
```

注：`timeoutMs: 1_500` 保证至少跑一轮 300ms 轮询（`tracker.turn` 已绑定）。若夹具的 history 尚未把 ownership 标记为 active 导致 `hasActiveTurn` 断言失败，改用 `stopActiveTurn` 断言（返回 `true` 且 `turn.calls` 记录 `session.cancel`）替代第一条断言——本质契约是"释放后 ownership 消失"。

- [ ] **Step 2: 运行确认失败**

Run: `node --test test/channels/shared/harness-control.test.mjs`
Expected: FAIL —— 新测试因 `handle` 为 rejection（`harness-reply-timeout`）而失败。

- [ ] **Step 3: 最小实现**

`src/channels/shared/harness-client.mjs` 三处修改：

(a) 选项解析（:1278 `timeoutMs` 行后）：
```js
const deferOnTimeout = options.deferOnTimeout === true;
```

(b) 状态旗标（:1334 `let turnFinished = false;` 后）：
```js
let deferredOwnershipKept = false;
```

(c) 超时分支（替换 :1480 `throw new HarnessTurnError('harness-reply-timeout');`）：
```js
if (deferOnTimeout && tracker.turn !== null && !ownership?.stopRequested) {
  // 把仍在运行的 turn 交还调用方：control/interaction ownership 保持注册，
  // /stop 在渠道交付结果并调用 releaseOwnership() 之前一直有效。
  deferredOwnershipKept = true;
  return {
    deferred: true,
    sessionId,
    turn: tracker.turn,
    promptRpcId,
    afterSeq: baselineSeq,
    releaseOwnership: () => {
      if (!ownership) return;
      this.#unregisterControlOwnership(ownership);
      this.#unregisterInteractionOwnership(sessionId, ownership);
    },
  };
}
throw new HarnessTurnError('harness-reply-timeout');
```

(d) finally 守卫（:1502-1504 改为）：
```js
if (ownership && !deferredOwnershipKept) {
  this.#unregisterControlOwnership(ownership);
  this.#unregisterInteractionOwnership(sessionId, ownership);
}
```

不改动：`closeArtifactConsumer()`、artifact discard（延迟 turn 不交付产物，见非目标）、`interactionController.abort`（保持现状）。

- [ ] **Step 4: 运行确认通过**

Run: `node --test test/channels/shared/harness-control.test.mjs`
Expected: PASS（含既有 :379 超时测试，默认行为未变）。

- [ ] **Step 5: 提交**

```bash
git add src/channels/shared/harness-client.mjs test/channels/shared/harness-control.test.mjs
git commit -m "feat: ask() gains deferOnTimeout to hand still-running turns back to channels"
```

---

### Task 2: 钉钉延迟交付模块 `deferred-delivery.mjs`

**Files:**
- Create: `src/channels/dingtalk/deferred-delivery.mjs`
- Test: Create `test/channels/dingtalk/deferred-delivery.test.mjs`

**Interfaces:**
- Consumes: `HarnessReplyTracker`（`export class`，`src/channels/shared/harness-client.mjs:416`）；Task 1 的延迟句柄形状。
- Produces:
  ```ts
  deferredTerminalText(reason, answer): string          // 终态文案映射（导出，便于测试）
  createDeferredDeliverer({
    api,                  // 需有 sendRobotText({clientId, clientSecret, target, text, signal})
    clientId, clientSecret,
    harness,              // 需有 rpc('session.history', …) 与 watchHarnessEvents（可缺省，缺省则仅注册时对账）
    state,                // 可选 rememberOutboundMessage（optional call）
    status = null,        // 可选，成功后递增 messagesReplied/lastReplyAt
    logger = console, signal,
    sendText,             // (sessionWebhook, text, at) => Promise<providerMessageIds[]>（webhook 路径，由 bridge 注入 #send）
  }): { register({ key, deferred, route }): Promise<void> }
  // route = { sessionWebhook, sessionWebhookExpiredTime: number(0=未知), fallbackTarget, at }
  ```
  Task 3 消费 `createDeferredDeliverer` 与该 entry 形状。

- [ ] **Step 1: 写失败测试**

`test/channels/dingtalk/deferred-delivery.test.mjs`：

```js
import assert from 'node:assert/strict';
import test from 'node:test';

import {
  createDeferredDeliverer,
  deferredTerminalText,
} from '../../../src/channels/dingtalk/deferred-delivery.mjs';

async function eventually(predicate, message = 'condition was not met') {
  const deadline = Date.now() + 1_000;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.fail(message);
}

function historyFixture({
  turn = 1,
  promptRpcId = 'dingtalk-defer-1',
  answer = '后台完成的结果',
  reason = { kind: 'completed' },
  ended = true,
} = {}) {
  const events = [
    { seq: 1, type: 'turn/start', data: { turn } },
    { seq: 2, type: 'user/message', data: { turn, source: { rpcId: promptRpcId } } },
    {
      seq: 3,
      type: 'assistant/chunk',
      data: { turn, step: 0, chunk: { type: 'text-delta', index: 0, text: answer } },
    },
  ];
  if (ended) events.push({ seq: 4, type: 'turn/end', data: { turn, reason } });
  return { events };
}

function deferredFixture(overrides = {}) {
  return {
    deferred: true,
    sessionId: 'session-defer',
    turn: 1,
    promptRpcId: 'dingtalk-defer-1',
    afterSeq: -1,
    released: 0,
    releaseOwnership() { this.released += 1; },
    ...overrides,
  };
}

function delivererFixture({
  history = historyFixture(),
  sendText,
} = {}) {
  const listeners = [];
  const sent = [];
  const proactive = [];
  const remembered = [];
  const harness = {
    rpc: async (method) => (method === 'session.history' ? history : null),
    watchHarnessEvents: ({ signal, onSessionEvent, onReconnect }) => {
      listeners.push({ signal, onSessionEvent, onReconnect });
      return new Promise((resolve) => {
        if (signal.aborted) resolve();
        else signal.addEventListener('abort', resolve, { once: true });
      });
    },
  };
  const api = {
    sendRobotText: async ({ target, text }) => {
      proactive.push({ target, text });
      return {};
    },
  };
  const deliverer = createDeferredDeliverer({
    api,
    clientId: 'ding-client',
    clientSecret: 'host-secret',
    harness,
    state: { rememberOutboundMessage: async (entry) => remembered.push(entry) },
    logger: { warn: () => {} },
    sendText: sendText ?? (async (_webhook, text, _at) => {
      sent.push(text);
      return ['webhook-msg-1'];
    }),
  });
  return {
    deliverer, listeners, sent, proactive, remembered,
    setHistory: (next) => { history = next; },
  };
}

const P2P_ROUTE = {
  sessionWebhook: 'https://oapi.dingtalk.com/robot/reply?ticket=defer-1',
  sessionWebhookExpiredTime: 0,
  fallbackTarget: { type: 'user', userId: 'staff-approved', robotCode: 'ding-client' },
  at: undefined,
};

test('deferredTerminalText keeps error semantics for every terminal reason', () => {
  assert.equal(deferredTerminalText({ kind: 'completed' }, '答案'), '答案');
  assert.equal(deferredTerminalText(null, '答案'), '答案');
  assert.match(deferredTerminalText({ kind: 'completed' }, '   '), /没有可发送的文本结果/);
  assert.match(
    deferredTerminalText({ kind: 'error', error: { message: 'RATE_LIMIT' } }, ''),
    /任务失败：RATE_LIMIT/,
  );
  assert.match(deferredTerminalText('max-tokens', ''), /长度上限/);
  assert.match(deferredTerminalText('blocked', ''), /安全策略/);
  assert.equal(deferredTerminalText('stopped', ''), '任务已停止。');
  assert.equal(deferredTerminalText('cancelled', ''), '任务已停止。');
  assert.equal(deferredTerminalText('aborted', ''), '任务已中止。');
  assert.equal(deferredTerminalText('something-else', ''), '任务已结束。');
});

test('a deferred turn already finished at registration delivers immediately', async () => {
  const fx = delivererFixture();
  const deferred = deferredFixture();
  await fx.deliverer.register({ key: 'p2p:staff-approved', deferred, route: P2P_ROUTE });
  assert.deepEqual(fx.sent, ['后台完成的结果']);
  assert.equal(deferred.released, 1);
  assert.equal(fx.remembered.length, 1);
  assert.equal(fx.remembered[0].conversationKey, 'p2p:staff-approved');
  // 已交付条目不重复投递。
  fx.listeners[0].onSessionEvent({
    sessionId: 'session-defer',
    event: { type: 'turn/end', seq: 4, data: { turn: 1, reason: { kind: 'completed' } } },
  });
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(fx.sent.length, 1);
});

test('a deferred turn delivers through the watcher when it ends later', async () => {
  const fx = delivererFixture({ history: historyFixture({ ended: false }) });
  const deferred = deferredFixture();
  await fx.deliverer.register({ key: 'p2p:staff-approved', deferred, route: P2P_ROUTE });
  assert.equal(fx.sent.length, 0);
  fx.setHistory(historyFixture({ reason: 'stopped' }));
  fx.listeners[0].onSessionEvent({
    sessionId: 'session-defer',
    event: { type: 'turn/end', seq: 4, data: { turn: 1, reason: 'stopped' } },
  });
  await eventually(() => fx.sent.length === 1);
  assert.equal(fx.sent[0], '任务已停止。');
  assert.equal(deferred.released, 1);
});

test('an expired webhook window routes the result through the proactive API', async () => {
  const fx = delivererFixture();
  await fx.deliverer.register({
    key: 'group:conversation-1',
    deferred: deferredFixture(),
    route: {
      ...P2P_ROUTE,
      sessionWebhookExpiredTime: Date.now() - 1_000,
      fallbackTarget: {
        type: 'group',
        openConversationId: 'conversation-1',
        robotCode: 'ding-client',
      },
    },
  });
  assert.equal(fx.sent.length, 0);
  assert.equal(fx.proactive.length, 1);
  assert.deepEqual(fx.proactive[0].target, {
    type: 'group',
    openConversationId: 'conversation-1',
    robotCode: 'ding-client',
  });
});

test('a failed webhook send falls back to the proactive API', async () => {
  const fx = delivererFixture({
    sendText: async () => { throw new Error('webhook rejected'); },
  });
  const deferred = deferredFixture();
  await fx.deliverer.register({ key: 'p2p:staff-approved', deferred, route: P2P_ROUTE });
  assert.equal(fx.proactive.length, 1);
  assert.match(fx.proactive[0].text, /后台完成的结果/);
  assert.equal(deferred.released, 1);
});

test('reconnect compensation rescans turns that ended while offline', async () => {
  const fx = delivererFixture({ history: historyFixture({ ended: false }) });
  await fx.deliverer.register({ key: 'p2p:staff-approved', deferred: deferredFixture(), route: P2P_ROUTE });
  fx.setHistory(historyFixture());
  fx.listeners[0].onReconnect?.();
  await eventually(() => fx.sent.length === 1);
});
```

- [ ] **Step 2: 运行确认失败**

Run: `node --test test/channels/dingtalk/deferred-delivery.test.mjs`
Expected: FAIL —— 模块不存在（ERR_MODULE_NOT_FOUND）。

- [ ] **Step 3: 实现模块**

`src/channels/dingtalk/deferred-delivery.mjs`：

```js
import { HarnessReplyTracker } from '../shared/harness-client.mjs';
import { t } from '../shared/i18n.mjs';

// sessionWebhook 窗口（每条入站消息的 sessionWebhookExpiredTime）收口前，
// 为 POST 本身与时钟偏差留出的安全余量。
const WEBHOOK_EXPIRY_MARGIN_MS = 120_000;

export function deferredTerminalText(reason, answer) {
  const kind = reason?.kind ?? reason ?? null;
  if (kind === null || kind === 'completed') {
    if (typeof answer === 'string' && answer.trim()) return answer;
    return t('任务已结束，但没有可发送的文本结果。');
  }
  if (kind === 'error') {
    const detail = reason?.error?.message ?? reason?.failure?.message;
    return detail
      ? t('任务失败：{detail}', { detail: String(detail) })
      : t('任务失败：模型运行出错。');
  }
  if (kind === 'max-tokens') return t('任务已达到回复长度上限并结束。');
  if (kind === 'blocked') return t('任务被安全策略拦截。');
  if (['interrupted', 'stopped', 'cancelled', 'canceled'].includes(kind)) {
    return t('任务已停止。');
  }
  if (kind === 'aborted') return t('任务已中止。');
  return t('任务已结束。');
}

function webhookUsable(route, now = Date.now()) {
  const expiry = Number(route?.sessionWebhookExpiredTime) || 0;
  return expiry <= 0 || now < expiry - WEBHOOK_EXPIRY_MARGIN_MS;
}

/**
 * 钉钉延迟交付：ask 以 deferOnTimeout 交回仍在运行的 turn 后，
 * 在其终态（turn/end，任意 reason）时投递结果。路由优先当次会话的
 * sessionWebhook（未明确过期时），否则回退主动推送 sendRobotText。
 * 无看门狗、无持久化：条目仅存于内存（范围裁定见实施计划）。
 */
export function createDeferredDeliverer({
  api,
  clientId,
  clientSecret,
  harness,
  state,
  status = null,
  logger = console,
  signal,
  sendText,
}) {
  const entries = new Map(); // `${sessionId}\0${turn}` → entry
  const watcherSignal = signal ?? new AbortController().signal;
  const scans = new Map(); // entryKey → 在途 scan promise（串行防重复）
  let watcherStarted = false;

  const entryKeyOf = (deferred) => `${deferred.sessionId}\0${deferred.turn}`;

  async function deliver(entry, tracker) {
    const text = deferredTerminalText(tracker.reason, tracker.answer);
    let providerMessageIds = [];
    let delivered = false;
    if (webhookUsable(entry.route)) {
      try {
        providerMessageIds = await sendText(
          entry.route.sessionWebhook,
          text,
          entry.route.at,
        );
        delivered = true;
      } catch (error) {
        logger.warn?.(
          '[dsh-dingtalk] deferred webhook delivery failed, falling back to proactive send:',
          error?.message ?? error,
        );
      }
    }
    if (!delivered) {
      await api.sendRobotText({
        clientId,
        clientSecret,
        target: entry.route.fallbackTarget,
        text,
        signal: watcherSignal,
      });
    }
    entry.deferred.releaseOwnership?.();
    try {
      await state.rememberOutboundMessage?.({
        conversationKey: entry.key,
        text,
        sentAt: Date.now(),
        completedAt: Date.now(),
        providerMessageIds,
      });
    } catch (error) {
      logger.warn?.('[dsh-dingtalk] failed to remember a deferred outbound message:', error);
    }
    if (status) {
      status.messagesReplied = (status.messagesReplied ?? 0) + 1;
      status.lastReplyAt = new Date().toISOString();
    }
  }

  async function scanEntry(entry) {
    const key = entryKeyOf(entry.deferred);
    if (scans.has(key)) return scans.get(key);
    const task = (async () => {
      if (watcherSignal.aborted) return;
      const tracker = new HarnessReplyTracker({
        promptRpcId: entry.deferred.promptRpcId,
        afterSeq: entry.deferred.afterSeq,
      });
      const history = await harness.rpc(
        'session.history',
        { sessionId: entry.deferred.sessionId, maxMessages: 50 },
        30_000,
        { signal: watcherSignal },
      );
      tracker.consumeAll(history.events ?? []);
      if (tracker.finished && entries.delete(key)) {
        await deliver(entry, tracker);
      }
    })().catch((error) => {
      if (!watcherSignal.aborted) {
        logger.warn?.('[dsh-dingtalk] deferred turn scan failed:', error?.message ?? error);
      }
    }).finally(() => scans.delete(key));
    scans.set(key, task);
    return task;
  }

  function ensureWatcher() {
    if (watcherStarted || typeof harness?.watchHarnessEvents !== 'function') return;
    watcherStarted = true;
    try {
      const watcher = harness.watchHarnessEvents({
        signal: watcherSignal,
        onSessionEvent: ({ sessionId, event }) => {
          if (watcherSignal.aborted
            || event?.type !== 'turn/end'
            || event?.data?.turn === undefined
            || event?.data?.turn === null) return;
          const entry = entries.get(`${sessionId}\0${event.data.turn}`);
          if (entry) void scanEntry(entry);
        },
        onReconnect: () => {
          // 断连期间可能错过 turn/end，重连后对账。
          for (const entry of [...entries.values()]) void scanEntry(entry);
        },
      });
      Promise.resolve(watcher).catch((error) => {
        if (!watcherSignal.aborted) {
          logger.warn?.('[dsh-dingtalk] deferred event watcher stopped:', error.message);
        }
      });
    } catch (error) {
      watcherStarted = false;
      logger.warn?.('[dsh-dingtalk] deferred event watcher failed to start:', error.message);
    }
  }

  async function register({ key, deferred, route }) {
    if (!deferred || deferred.deferred !== true
      || typeof deferred.sessionId !== 'string' || !deferred.sessionId
      || deferred.turn === undefined || deferred.turn === null) {
      throw new TypeError('A deferred ask handle is required');
    }
    if (!route?.sessionWebhook || !route?.fallbackTarget) {
      throw new TypeError('A delivery route with sessionWebhook and fallbackTarget is required');
    }
    const entry = { key, deferred, route };
    entries.set(entryKeyOf(deferred), entry);
    ensureWatcher();
    // 竞态兜底：turn 可能在 ask 超时与本次注册之间已结束；history 是权威。
    await scanEntry(entry);
  }

  return { register };
}
```

实现要点（必须遵守）：
- `deliver()` 以 `entries.delete(key)` 成功为"领取令牌"（`scanEntry` 内），同一 turn 只投递一次；webhook 失败 → 单次主动推送兜底，两者都失败 → 异常进 `scanEntry` 的 catch 仅记日志（不重试，范围裁定）。
- 主动推送分支**不能用** `providerMessageIds.length === 0` 判断（真实 `api.sendText` 返回 `true`，id 恒为空），必须用显式 `delivered` 旗标。

- [ ] **Step 4: 运行确认通过**

Run: `node --test test/channels/dingtalk/deferred-delivery.test.mjs`
Expected: PASS（6 个测试全过）。

- [ ] **Step 5: 提交**

```bash
git add src/channels/dingtalk/deferred-delivery.mjs test/channels/dingtalk/deferred-delivery.test.mjs
git commit -m "feat: DingTalk deferred-turn deliverer with webhook-first routing and reason-aware copy"
```

---

### Task 3: 钉钉 bridge 接线（识别延迟句柄 + 转后台通知）

**Files:**
- Modify: `src/channels/dingtalk/dingtalk-bridge.mjs:70-72` 附近（导入与文案常量）、`:488-527`（私有字段与构造器）、`:1157-1182`（ask 调用与延迟分支）
- Test: `test/channels/dingtalk/dingtalk-bridge.test.mjs`（文件末尾追加；复用现有 `message()`、`stateFixture()`、`eventually()` 夹具）

**Interfaces:**
- Consumes: Task 1 延迟句柄（经 `askInWorkspaceSession` 返回的 `answer` 字段）、Task 2 `createDeferredDeliverer`；现有 `fileTarget(message, sender, clientId)`（`:365`）、`#send`（`:1603`）、`#finishStatusReaction`、`#batchInputs.complete`、`increment`。
- Produces: 行为——`#process` 中 `answer?.deferred === true` 时发转后台通知并注册交付，返回 `null`；不改其它路径。

- [ ] **Step 1: 写失败测试**

在 `test/channels/dingtalk/dingtalk-bridge.test.mjs` 末尾追加：

```js
// ── Deferred handoff: slow turns notify, then push on completion ──────────

function deferredHarnessFixture({ history = { events: [] } } = {}) {
  const listeners = [];
  const releaseCalls = [];
  const sent = [];
  const proactive = [];
  return {
    listeners,
    releaseCalls,
    sent,
    proactive,
    setHistory: (next) => { history = next; },
    harness: {
      sessionExists: async () => true,
      rpc: async (method) => (method === 'session.history' ? history : null),
      watchHarnessEvents: ({ signal, onSessionEvent, onReconnect }) => {
        listeners.push({ onSessionEvent, onReconnect });
        return new Promise((resolve) => {
          if (signal.aborted) resolve();
          else signal.addEventListener('abort', resolve, { once: true });
        });
      },
      ask: async () => ({
        deferred: true,
        sessionId: 'session-defer',
        turn: 1,
        promptRpcId: 'dingtalk-defer-1',
        afterSeq: -1,
        releaseOwnership: () => releaseCalls.push('release'),
      }),
    },
    api: {
      sendText: async ({ text }) => {
        sent.push(text);
        return { messageId: `ding-${sent.length}` };
      },
      sendRobotText: async ({ target, text }) => {
        proactive.push({ target, text });
        return {};
      },
    },
  };
}

test('DingTalk defers a slow turn instead of erroring, then pushes the result', async () => {
  const fixture = stateFixture();
  fixture.sessions.set('p2p:staff-approved', 'session-defer');
  const fx = deferredHarnessFixture();
  const bridge = new DingtalkHarnessBridge({
    api: fx.api,
    clientId: 'ding-client',
    clientSecret: 'host-secret',
    harness: fx.harness,
    state: fixture.state,
  });

  await bridge.accept(message('ding-defer-1', '慢任务', {
    sessionWebhookExpiredTime: Date.now() + 5_400_000,
  }));
  await bridge.waitForIdle();

  assert.match(fx.sent.at(-1), /任务仍在运行/);
  assert.equal(
    fx.sent.filter((text) => text.includes('MODEL_REPLY_TIMEOUT')
      || text.includes('等待模型回复超时')).length,
    0,
    'deferred handoff must not surface the timeout error',
  );

  fx.setHistory({ events: [
    { seq: 1, type: 'turn/start', data: { turn: 1 } },
    { seq: 2, type: 'user/message', data: { turn: 1, source: { rpcId: 'dingtalk-defer-1' } } },
    {
      seq: 3,
      type: 'assistant/chunk',
      data: { turn: 1, step: 0, chunk: { type: 'text-delta', index: 0, text: '后台完成的结果' } },
    },
    { seq: 4, type: 'turn/end', data: { turn: 1, reason: { kind: 'completed' } } },
  ] });
  fx.listeners[0].onSessionEvent({
    sessionId: 'session-defer',
    event: { type: 'turn/end', seq: 4, data: { turn: 1, reason: { kind: 'completed' } } },
  });
  await eventually(() => fx.sent.some((text) => text.includes('后台完成的结果')));
  assert.deepEqual(fx.releaseCalls, ['release']);
});

test('DingTalk deferred result falls back to proactive delivery after the webhook window', async () => {
  const fixture = stateFixture();
  fixture.sessions.set('p2p:staff-approved', 'session-defer');
  const fx = deferredHarnessFixture({
    history: { events: [
      { seq: 1, type: 'turn/start', data: { turn: 1 } },
      { seq: 2, type: 'user/message', data: { turn: 1, source: { rpcId: 'dingtalk-defer-1' } } },
      {
        seq: 3,
        type: 'assistant/chunk',
        data: { turn: 1, step: 0, chunk: { type: 'text-delta', index: 0, text: '后台完成的结果' } },
      },
      { seq: 4, type: 'turn/end', data: { turn: 1, reason: { kind: 'completed' } } },
    ] },
  });
  const bridge = new DingtalkHarnessBridge({
    api: fx.api,
    clientId: 'ding-client',
    clientSecret: 'host-secret',
    harness: fx.harness,
    state: fixture.state,
  });

  await bridge.accept(message('ding-defer-2', '慢任务', {
    sessionWebhookExpiredTime: Date.now() - 1_000,
  }));
  await bridge.waitForIdle();

  // 注册时 history 已终态 → 立即经主动推送交付，不等待 watcher 事件。
  assert.equal(fx.proactive.length, 1);
  assert.deepEqual(fx.proactive[0].target, {
    type: 'user',
    userId: 'staff-approved',
    robotCode: 'ding-client',
  });
  assert.match(fx.proactive[0].text, /后台完成的结果/);
});
```

- [ ] **Step 2: 运行确认失败**

Run: `node --test test/channels/dingtalk/dingtalk-bridge.test.mjs`
Expected: FAIL —— 转后台通知文案不存在（`fx.sent` 里只有旧错误文案或为空）。

- [ ] **Step 3: 实现 bridge 接线**

`src/channels/dingtalk/dingtalk-bridge.mjs` 四处修改：

(a) 导入（`import { t } from '../shared/i18n.mjs';`（:70）之后）：
```js
import { createDeferredDeliverer } from './deferred-delivery.mjs';
```

(b) 文案常量（`:72` `CARD_INITIAL_TEXT` 之后）：
```js
const DEFERRED_HANDOFF_NOTICE = '任务仍在运行，已转入后台。完成后我会把结果发到这里；发送 /stop 可停止任务。';
```

(c) 私有字段（`:492` `#approvals;` 附近）与构造器（`:525` `this.#replyTimeoutMs = replyTimeoutMs;` 之后）：
```js
#deferredDeliverer;
```
```js
this.#deferredDeliverer = createDeferredDeliverer({
  api,
  clientId: this.#clientId,
  clientSecret: this.#clientSecret,
  harness,
  state,
  status: this.#status,
  logger,
  signal,
  sendText: (sessionWebhook, text, at) => this.#send(sessionWebhook, text, at),
});
```

(d) `#process` 的 ask 调用（`:1166` `askOptions: {` 内加一行）：
```js
deferOnTimeout: true,
```

(e) `askInWorkspaceSession` 返回后（`:1182` 之后、`:1183` `if (batchSubmission)` 之前）插入延迟分支：
```js
if (answer?.deferred === true) {
  if (batchSubmission) {
    this.#batchInputs.complete(key, batchSubmission.token);
    batchSettled = true;
  }
  const notice = t(DEFERRED_HANDOFF_NOTICE);
  try {
    const streamed = cardStarted && await cardStream.finish(notice);
    if (!streamed) {
      await this.#send(sessionWebhook, notice, this.#atUsersFor(message));
    }
  } catch (error) {
    this.#logger.warn?.(
      '[dsh-dingtalk] deferred handoff notice failed:',
      error?.message ?? error,
    );
  }
  this.#deferredDeliverer.register({
    key,
    deferred: answer,
    route: {
      sessionWebhook,
      sessionWebhookExpiredTime: Number(message.sessionWebhookExpiredTime) || 0,
      fallbackTarget: fileTarget(message, sender, this.#clientId),
      at: this.#atUsersFor(message),
    },
  }).catch((error) => {
    this.#logger.warn?.(
      '[dsh-dingtalk] failed to register a deferred turn:',
      error?.message ?? error,
    );
  });
  this.#finishStatusReaction(statusReaction, 'clear');
  increment(this.#status, 'messagesReplied');
  this.#status.lastReplyAt = new Date().toISOString();
  return null;
}
```

不改：错误分支（:1241-1281）保留——非延迟场景（如 `tracker.turn === null` 的排队超时、交互路径）仍走原超时报错。

- [ ] **Step 4: 运行确认通过**

Run: `node --test test/channels/dingtalk/dingtalk-bridge.test.mjs`
Expected: PASS（含新增 2 个测试与既有全部测试）。

- [ ] **Step 5: 提交**

```bash
git add src/channels/dingtalk/dingtalk-bridge.mjs test/channels/dingtalk/dingtalk-bridge.test.mjs
git commit -m "feat: DingTalk defers slow turns to background delivery instead of surfacing MODEL_REPLY_TIMEOUT"
```

---

### Task 4: 默认超时 600_000 → 180_000（语义：转后台时机）

**Files:**
- Modify: `src/channels/dingtalk/dingtalk-bridge.mjs:505`（构造器默认）
- Modify: `src/channels/dingtalk/dingtalk-runtime.mjs:2-5`（导入）、`:158`（构造器默认）
- Modify: `plugin-src/host/channels/dingtalk/production.mjs:8`（导入区）、`:125`（装配默认）
- Test: `test/channels/dingtalk/dingtalk-bridge.test.mjs`（追加常量断言）

**Interfaces:**
- Consumes: 无（独立于 Task 1-3，但语义上依赖 Task 3——阈值动作已从报错变为转后台）。
- Produces: `export const DINGTALK_DEFAULT_REPLY_TIMEOUT_MS = 180_000;`（钉钉 bridge 模块导出，runtime 与 production 引用同一常量，消除三处魔数）。

- [ ] **Step 1: 写失败测试**

`test/channels/dingtalk/dingtalk-bridge.test.mjs`（更新顶部导入并追加）：

```js
// 顶部导入追加：
import {
  DINGTALK_DEFAULT_REPLY_TIMEOUT_MS,
  // …并入现有 DingtalkHarnessBridge 导入
} from '../../../src/channels/dingtalk/dingtalk-bridge.mjs';

test('DingTalk default reply timeout is a three-minute foreground window', () => {
  assert.equal(DINGTALK_DEFAULT_REPLY_TIMEOUT_MS, 180_000);
});
```

- [ ] **Step 2: 运行确认失败**

Run: `node --test test/channels/dingtalk/dingtalk-bridge.test.mjs`
Expected: FAIL —— 导出不存在（SyntaxError/undefined）。

- [ ] **Step 3: 实现三处默认值**

(a) `dingtalk-bridge.mjs`（`:72` 常量区，并在 `export const DingTalkHarnessBridge = DingtalkHarnessBridge;`（:1665）旁追加导出）：
```js
export const DINGTALK_DEFAULT_REPLY_TIMEOUT_MS = 180_000;
```
构造器默认（`:505`）：
```js
replyTimeoutMs = DINGTALK_DEFAULT_REPLY_TIMEOUT_MS,
```

(b) `dingtalk-runtime.mjs`：在现有 `./dingtalk-bridge.mjs` 导入（:2-5）中加入 `DINGTALK_DEFAULT_REPLY_TIMEOUT_MS`，构造器默认（`:158`）：
```js
replyTimeoutMs = DINGTALK_DEFAULT_REPLY_TIMEOUT_MS,
```

(c) `production.mjs`（导入区，:8 之后）：
```js
import { DINGTALK_DEFAULT_REPLY_TIMEOUT_MS } from '../../../../src/channels/dingtalk/dingtalk-bridge.mjs';
```
装配默认（`:125`）：
```js
replyTimeoutMs: config.replyTimeoutMs ?? DINGTALK_DEFAULT_REPLY_TIMEOUT_MS,
```

既有 `test/host.test.mjs:29` 显式传 `60_000`，不受影响；仓库无任何 `600_000` 断言（已核实）。

- [ ] **Step 4: 运行确认通过**

Run: `node --test test/channels/dingtalk/dingtalk-bridge.test.mjs test/channels/dingtalk/dingtalk-runtime.test.mjs test/host.test.mjs`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add src/channels/dingtalk/dingtalk-bridge.mjs src/channels/dingtalk/dingtalk-runtime.mjs plugin-src/host/channels/dingtalk/production.mjs test/channels/dingtalk/dingtalk-bridge.test.mjs
git commit -m "feat: lower DingTalk default reply timeout to a 3-minute foreground handoff window"
```

---

### Task 5: 全量回归验证

**Files:** 无新增（只验证）。

- [ ] **Step 1: 定向回归**

Run: `node --test test/channels/shared/harness-control.test.mjs test/channels/dingtalk/deferred-delivery.test.mjs test/channels/dingtalk/dingtalk-bridge.test.mjs test/channels/dingtalk/dingtalk-runtime.test.mjs test/channels/dingtalk/harness-client.test.mjs test/host.test.mjs`
Expected: 全部 PASS。

- [ ] **Step 2: 全量测试**

Run: `npm test`
Expected: 全部 PASS（含飞书/微信等共享 harness-client 的渠道——默认模式行为未变）。

- [ ] **Step 3: 若有回归**

按 superpowers:systematic-debugging 处理后重跑；禁止为过测试而删除断言。

---

## 已知限制（范围内接受，勿"顺手修复"）

1. **真卡死无二次告警**：turn 永不结束 → 用户仅收到转后台通知，之后无下文（无看门狗）。
2. **重启丢跟踪**：bridge/插件重启后内存延迟条目与 retained ownership 全部丢失；任务若完成，结果不补投。
3. **推送双失败即弃**：webhook 与 `sendRobotText` 都失败时仅记日志（单次 fallback，无重试）。
4. **延迟 turn 不交付产物**：`ask()` finally 照旧 discard 工具产物。
5. **排队未开 turn 的超时**：`tracker.turn === null` 时仍走旧超时报错（`MODEL_REPLY_TIMEOUT`）。
6. **交互回复路径**（`#processInteractionReply`）未接入 defer——超时仍报错。
7. **AI 卡片长任务体验**：转后台时卡片以通知文案收口，结果以新文本消息推送，不续用卡片流式。

## Self-Review 结论

- **规格覆盖**：用户要求的最小改动（不报错、转后台通知、终态推送、webhook→主动推送路由、/stop 保留、错误语义文案、默认阈值下调）分别由 Task 1/2/3/4 覆盖；用户明确排除的盲区兜底全部列入"已知限制"，无任务实现。
- **占位符扫描**：无 TBD/TODO/"适当处理"；所有代码步骤含完整代码。
- **类型一致性**：延迟句柄形状 `{deferred, sessionId, turn, promptRpcId, afterSeq, releaseOwnership}` 与 entry 形状 `{key, deferred, route{sessionWebhook, sessionWebhookExpiredTime, fallbackTarget, at}}` 在 Task 1/2/3 间一致；`delivered` 旗标规避了 `api.sendText` 返回 `true` 的 ids 陷阱。
