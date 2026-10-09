// @ts-nocheck
// Prime Agent <-> Herdr integration: pane state + resumable session identity.
//
// This file is the pane's only Herdr reporter: Prime's built-in reporter
// defers to any loaded file named `herdr-agent-state.ts`/`.js`, so this module
// replaces it entirely. Compared to the built-in it also reports the session
// identity and a resume command (resume_argv), which is what lets Herdr bring
// the same conversation back into the same pane after a Herdr server restart:
// the pane is restored and Herdr runs `prime-agent -r <session file>` there.
//
// The state machine (idle debounce, provider-error retry hold, blocked
// counting, monotonic seq) follows Prime's built-in reporter; the session
// reporting follows Herdr's file-based pi integration. Requires Herdr 0.9.3+
// (resume_argv support on pane.report_agent / pane.report_agent_session).
//
// Not managed by Herdr: `herdr integration install` writes only ~/.pi, so this
// file is never overwritten by integration updates. Outside a Herdr pane it is
// a complete no-op.

import net from "node:net";

const socketPath = process.env.HERDR_SOCKET_PATH;
const socketEndpoint =
  process.platform === "win32" && socketPath ? `\\\\.\\pipe\\${socketPath}` : socketPath;
const paneId = process.env.HERDR_PANE_ID;
const enabled = process.env.HERDR_ENV === "1" && !!socketPath && !!paneId;

// Custom source (must not use the `herdr:` prefix — that is reserved for
// Herdr's own integrations). Keep it stable: Herdr tracks report order per
// source and stores the resume command under it.
const source = "prime-herdr";
const agentLabel = "prime-agent";

function parseDurationEnv(name, fallback) {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

const idleDebounceMs = parseDurationEnv("HERDR_PI_IDLE_DEBOUNCE_MS", 250);
const retryGraceMs = parseDurationEnv("HERDR_PI_RETRY_GRACE_MS", 2500);

// Monotonic across all extension instances in this process. Herdr drops
// lower-seq reports per source, so a successor session instance (after /new,
// resume, fork, or reload) must never restart below a seq already used.
let reportSeq = Date.now() * 1000;
function nextReportSeq() {
  reportSeq = Math.max(reportSeq + 1, Date.now() * 1000);
  return reportSeq;
}

function sendRequest(request) {
  return new Promise((resolve) => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      socket.destroy();
      resolve(undefined);
    };
    const socket = net.createConnection(socketEndpoint);
    socket.on("error", finish);
    socket.on("connect", () => socket.write(`${JSON.stringify(request)}\n`));
    socket.on("data", finish);
    socket.on("end", finish);
    const timeout = setTimeout(finish, 500);
    timeout.unref?.();
  });
}

function lastAssistantMessage(messages) {
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    if (messages[i]?.role === "assistant") return messages[i];
  }
  return undefined;
}

// The agent auto-retries most provider errors after agent_end fires. Hold
// "working" through the retry grace window; if no retry starts, settle to
// blocked with the error message.
function errorHoldMessage(event) {
  const messages = Array.isArray(event?.messages) ? event.messages : [];
  const assistant = lastAssistantMessage(messages);
  if (assistant?.stopReason !== "error") return undefined;
  return String(assistant.errorMessage ?? "") || "provider error";
}

export default function (pi) {
  if (!enabled) {
    return;
  }

  let currentAgentSessionId;
  let currentAgentSessionPath;
  // Inline RLM child sessions share the parent's extension runner; bind the
  // reporter to the first (parent) session so subagent turns cannot flip the
  // pane state or stomp the session reference.
  let boundSessionManager;
  let sendInFlight = false;
  let queuedState;
  let activeDrain = Promise.resolve();
  let released = false;
  let agentActive = false;
  let retryHoldActive = false;
  let failureBlocked = false;
  let failureMessage;
  let blockedCount = 0;
  let blockedMessage;
  let lastState;
  let lastMessage;
  let idleTimer;
  let retryTimer;

  function isBoundSession(ctx) {
    if (boundSessionManager === undefined) return true;
    return ctx?.sessionManager === boundSessionManager;
  }

  function updateSessionRef(ctx) {
    try {
      const file = ctx?.sessionManager?.getSessionFile?.();
      currentAgentSessionPath =
        typeof file === "string" && file.startsWith("/") ? file : undefined;
    } catch {
      currentAgentSessionPath = undefined;
    }
    try {
      const id = ctx?.sessionManager?.getSessionId?.();
      currentAgentSessionId = typeof id === "string" && id.length > 0 ? id : undefined;
    } catch {
      currentAgentSessionId = undefined;
    }
  }

  // The pane comes back with the exact same conversation on Herdr restart.
  // `env TMPDIR=/tmp` pins Prime's daemon socket location (TMPDIR-scoped).
  function currentResumeArgv() {
    const ref = currentAgentSessionPath ?? currentAgentSessionId;
    if (!ref) return undefined;
    return ["env", "TMPDIR=/tmp", "prime-agent", "-r", ref];
  }

  function withSessionRef(params) {
    let next = params;
    if (currentAgentSessionPath) {
      next = { ...next, agent_session_path: currentAgentSessionPath };
    } else if (currentAgentSessionId) {
      next = { ...next, agent_session_id: currentAgentSessionId };
    }
    const argv = currentResumeArgv();
    return argv ? { ...next, resume_argv: argv } : next;
  }

  function sendState(state, message, seq = nextReportSeq()) {
    return sendRequest({
      id: `${source}:${Date.now()}:${Math.random().toString(36).slice(2)}`,
      method: "pane.report_agent",
      params: withSessionRef({
        pane_id: paneId,
        source,
        agent: agentLabel,
        state,
        message,
        seq,
      }),
    });
  }

  function queueState(state, message) {
    if (released) {
      // Released on quit; a late report would reclaim the pane.
      return;
    }
    queuedState = { state, message, seq: nextReportSeq() };
    if (!sendInFlight) {
      activeDrain = drainStateQueue();
    }
  }

  async function drainStateQueue() {
    if (sendInFlight) return;
    sendInFlight = true;
    try {
      while (queuedState) {
        const next = queuedState;
        queuedState = undefined;
        await sendState(next.state, next.message, next.seq);
      }
    } finally {
      sendInFlight = false;
      if (queuedState) {
        activeDrain = drainStateQueue();
      }
    }
  }

  function reportSession(sessionStartSource) {
    const ref = currentAgentSessionPath
      ? { agent_session_path: currentAgentSessionPath }
      : currentAgentSessionId
        ? { agent_session_id: currentAgentSessionId }
        : undefined;
    if (!ref) return Promise.resolve();
    return sendRequest({
      id: `${source}:session:${Date.now()}:${Math.random().toString(36).slice(2)}`,
      method: "pane.report_agent_session",
      params: withSessionRef({
        pane_id: paneId,
        source,
        agent: agentLabel,
        seq: nextReportSeq(),
        session_start_source: sessionStartSource,
        ...ref,
      }),
    });
  }

  async function releaseAgent() {
    // Stop new reports, drop queued ones, wait for the in-flight send so the
    // release is the last write on the wire.
    released = true;
    queuedState = undefined;
    await activeDrain.catch(() => undefined);
    return sendRequest({
      id: `${source}:release:${Date.now()}:${Math.random().toString(36).slice(2)}`,
      method: "pane.release_agent",
      params: { pane_id: paneId, source, agent: agentLabel, seq: nextReportSeq() },
    });
  }

  function clearTimer(timer) {
    if (timer) clearTimeout(timer);
  }

  function clearPendingTimers() {
    clearTimer(idleTimer);
    clearTimer(retryTimer);
    idleTimer = undefined;
    retryTimer = undefined;
  }

  function clearFailureState() {
    retryHoldActive = false;
    failureBlocked = false;
    failureMessage = undefined;
  }

  function desiredState() {
    if (blockedCount > 0) return { state: "blocked", message: blockedMessage };
    if (failureBlocked) return { state: "blocked", message: failureMessage };
    if (agentActive || retryHoldActive) return { state: "working", message: undefined };
    return { state: "idle", message: undefined };
  }

  function publishState(force = false) {
    const next = desiredState();
    if (!force && next.state === lastState && next.message === lastMessage) return;
    lastState = next.state;
    lastMessage = next.message;
    queueState(next.state, next.message);
  }

  function scheduleIdle() {
    clearPendingTimers();
    clearFailureState();
    idleTimer = setTimeout(() => {
      idleTimer = undefined;
      publishState();
    }, idleDebounceMs);
    idleTimer.unref?.();
  }

  function holdForRetry(message) {
    clearPendingTimers();
    retryHoldActive = true;
    failureBlocked = false;
    failureMessage = message;
    publishState();
    retryTimer = setTimeout(() => {
      retryTimer = undefined;
      retryHoldActive = false;
      failureBlocked = true;
      publishState();
    }, retryGraceMs);
    retryTimer.unref?.();
  }

  const unsubscribeBlocked = pi.events.on("herdr:blocked", (data) => {
    if (!data?.active) {
      blockedCount = Math.max(0, blockedCount - 1);
      if (blockedCount === 0) blockedMessage = undefined;
      publishState();
      return;
    }
    clearPendingTimers();
    if (retryHoldActive) {
      retryHoldActive = false;
      failureBlocked = true;
    }
    blockedCount += 1;
    blockedMessage = data.label;
    publishState();
  });

  pi.on("session_start", async (event, ctx) => {
    if (!isBoundSession(ctx)) return;
    if (boundSessionManager === undefined && ctx?.sessionManager !== undefined) {
      boundSessionManager = ctx.sessionManager;
    }
    updateSessionRef(ctx);
    // A reload can recreate this reporter mid-turn; seed from the session so
    // it does not report idle while the agent is still streaming.
    if (typeof ctx?.isIdle === "function") {
      try {
        agentActive = !ctx.isIdle();
      } catch {
        agentActive = false;
      }
    }
    // State report first (it holds the pane and carries the resume command),
    // then the session identity report.
    publishState(true);
    await activeDrain;
    await reportSession(event?.reason);
  });

  pi.on("agent_start", async (_event, ctx) => {
    if (!isBoundSession(ctx)) return;
    const before = currentAgentSessionPath ?? currentAgentSessionId;
    updateSessionRef(ctx);
    const after = currentAgentSessionPath ?? currentAgentSessionId;
    if (before !== after) {
      // Session switched (new/resume/fork): report the new identity and force a
      // state report so the new resume command reaches Herdr.
      publishState(true);
      await activeDrain;
      await reportSession();
      return;
    }
    clearPendingTimers();
    clearFailureState();
    agentActive = true;
    publishState();
  });

  pi.on("agent_end", (event, ctx) => {
    if (!isBoundSession(ctx)) return;
    if (!agentActive) {
      // Duplicate/late end events during a retry hold must not cancel it and
      // publish a false idle.
      return;
    }
    agentActive = false;
    const holdMessage = errorHoldMessage(event);
    if (holdMessage) {
      holdForRetry(holdMessage);
      return;
    }
    // Queued follow-up/steer messages start another loop right away; debounce
    // so the pane does not flicker done -> working.
    if (typeof ctx?.hasPendingMessages === "function" && ctx.hasPendingMessages()) {
      scheduleIdle();
      return;
    }
    clearPendingTimers();
    clearFailureState();
    publishState();
  });

  pi.on("session_shutdown", async (event, ctx) => {
    if (!isBoundSession(ctx)) return;
    clearPendingTimers();
    // Shared event bus across reloads and session replacements: drop this
    // instance's listener so a stale copy cannot keep reporting.
    unsubscribeBlocked();
    if (event?.reason !== "quit") {
      // Session replacement or reload: a successor instance re-reports
      // immediately; releasing here would race and clear its pane claim.
      released = true;
      queuedState = undefined;
      return;
    }
    await releaseAgent();
  });
}
