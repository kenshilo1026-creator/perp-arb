const DEBUGGER_VERSION = "1.3";
const ORDER_AUTOMATION_VERSION = "variational-order-automation-2026-07-26-10";
const MAX_QUEUE_SIZE = 1000;
const AUTO_RELOAD_COOLDOWN_MS = 5000;
const MAX_ORDER_RELOAD_RETRIES = 2;

const DEFAULT_CONFIG = {
  wsEndpoint: "ws://127.0.0.1:8766",
  restEndpoint: "ws://127.0.0.1:8767",
  commandEndpoint: "ws://127.0.0.1:8768",
  domainFilter: "variational",
  restAllowlist: [
    "https://omni.variational.io/api/quotes/indicative"
  ],
  wsAllowlist: [
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/events",
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio"
  ]
};

const state = {
  active: false,
  attachedTabId: null,
  config: { ...DEFAULT_CONFIG },
  configLoaded: false,
  pendingResponses: new Map(),
  websocketMeta: new Map(),
  lastError: null,
  lastAutoReloadAt: 0
};

class ForwardSocket {
  constructor(label, configKey) {
    this.label = label;
    this.configKey = configKey;
    this.ws = null;
    this.status = "disconnected";
    this.queue = [];
    this.retryTimer = null;
  }

  get endpoint() {
    return state.config[this.configKey];
  }

  connect() {
    if (!state.active) {
      return;
    }

    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    const endpoint = this.endpoint;
    if (!endpoint) {
      this.status = "disconnected";
      notifyStatus();
      return;
    }

    this.status = "connecting";
    notifyStatus();

    try {
      const socket = new WebSocket(endpoint);
      this.ws = socket;

      socket.onopen = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "connected";
        this.flush();
        if (this.configKey === "wsEndpoint") {
          autoReloadAttachedTab("forward receiver connected");
        }
        notifyStatus();
      };

      socket.onclose = () => {
        if (this.ws !== socket) {
          return;
        }
        this.ws = null;
        this.status = "disconnected";
        notifyStatus();
        this.scheduleReconnect();
      };

      socket.onerror = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "error";
        notifyStatus();
      };
    } catch (error) {
      this.status = "error";
      state.lastError = `${this.label} socket connect failed: ${error.message}`;
      notifyStatus();
      this.scheduleReconnect();
    }
  }

  send(payload) {
    const data = JSON.stringify(payload);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(data);
      return;
    }

    this.queue.push(data);
    if (this.queue.length > MAX_QUEUE_SIZE) {
      this.queue.shift();
    }
    this.connect();
  }

  flush() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return;
    }
    while (this.queue.length > 0) {
      this.ws.send(this.queue.shift());
    }
  }

  scheduleReconnect() {
    if (!state.active || this.retryTimer) {
      return;
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 1000);
  }

  restart() {
    this.close();
    this.connect();
  }

  close() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.status = "disconnected";
    notifyStatus();
  }
}

const wsForwarder = new ForwardSocket("websocket", "wsEndpoint");
const restForwarder = new ForwardSocket("rest", "restEndpoint");

class CommandSocket {
  constructor() {
    this.ws = null;
    this.status = "disconnected";
    this.retryTimer = null;
  }

  get endpoint() {
    return state.config.commandEndpoint;
  }

  connect() {
    if (!state.active) {
      return;
    }
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }
    if (!this.endpoint) {
      this.status = "disconnected";
      notifyStatus();
      return;
    }

    this.status = "connecting";
    notifyStatus();

    try {
      const socket = new WebSocket(this.endpoint);
      this.ws = socket;

      socket.onopen = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "connected";
        socket.send(JSON.stringify({ "type": "REGISTER", "role": "extension" }));
        notifyStatus();
      };

      socket.onmessage = (event) => {
        handleCommandMessage(event.data).catch((error) => {
          state.lastError = `Command handling failed: ${error.message}`;
          notifyStatus();
        });
      };

      socket.onclose = () => {
        if (this.ws !== socket) {
          return;
        }
        this.ws = null;
        this.status = "disconnected";
        notifyStatus();
        this.scheduleReconnect();
      };

      socket.onerror = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "error";
        notifyStatus();
      };
    } catch (error) {
      this.status = "error";
      state.lastError = `command socket connect failed: ${error.message}`;
      notifyStatus();
      this.scheduleReconnect();
    }
  }

  send(payload) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
      return;
    }
    this.connect();
  }

  scheduleReconnect() {
    if (!state.active || this.retryTimer) {
      return;
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 1000);
  }

  restart() {
    this.close();
    this.connect();
  }

  close() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.status = "disconnected";
    notifyStatus();
  }
}

const commandClient = new CommandSocket();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTransientFrameRemovalError(error) {
  const message = String(error?.message || error || "");
  return message.includes("Frame with ID")
    || message.includes("frame was removed")
    || message.includes("No frame with id")
    || message.includes("Cannot access contents of url")
    || message.includes("Extension context invalidated");
}

function isTransientOrderInjectionError(error) {
  const message = String(error?.message || error || "").toLowerCase();
  return isTransientFrameRemovalError(error)
    || message.includes("order injection returned no result");
}

async function runVariationalOrderInjection(payload) {
  let lastError = null;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      const [injectionResult] = await chrome.scripting.executeScript({
        target: { tabId: state.attachedTabId },
        world: "MAIN",
        func: executeVariationalOrder,
        args: [{ ...payload, automationVersion: ORDER_AUTOMATION_VERSION }]
      });
      return injectionResult;
    } catch (error) {
      lastError = error;
      if (!isTransientFrameRemovalError(error)) {
        throw error;
      }
      await waitForTabComplete(state.attachedTabId);
      await waitForVariationalOrderPageReady(payload);

      const submittingLimitOrder = !payload.previewOnly
        && !payload.prepareOnly
        && String(payload.orderType || "").toUpperCase() === "LIMIT";
      if (submittingLimitOrder) {
        try {
          const recoveredCheck = await checkVariationalOpenOrderAfterSubmit(
            {},
            payload,
            {},
            `recover-after-frame-removal-${attempt + 1}`
          );
          if (recoveredCheck.exists) {
            return {
              result: {
                ok: true,
                orderId: recoveredCheck.payload.orderId || null,
                details: {
                  automationVersion: ORDER_AUTOMATION_VERSION,
                  recoveredAfterFrameRemoval: true,
                  submitVerifiedOpenOrder: true,
                  needsCdpSubmitClick: false,
                  openOrderCheck: recoveredCheck.payload,
                }
              }
            };
          }
        } catch (checkError) {
          lastError = checkError;
          if (!isTransientFrameRemovalError(checkError)) {
            throw checkError;
          }
          await waitForTabComplete(state.attachedTabId);
          await waitForVariationalOrderPageReady(payload);
        }
      }

      if (attempt >= 2) {
        throw lastError;
      }
    }
  }
  throw lastError;
}

async function dispatchDebuggerEnterKey(tabId) {
  await sendDebuggerCommand(tabId, "Input.dispatchKeyEvent", {
    type: "rawKeyDown",
    key: "Enter",
    code: "Enter",
    windowsVirtualKeyCode: 13,
    nativeVirtualKeyCode: 13
  });
  await sendDebuggerCommand(tabId, "Input.dispatchKeyEvent", {
    type: "keyUp",
    key: "Enter",
    code: "Enter",
    windowsVirtualKeyCode: 13,
    nativeVirtualKeyCode: 13
  });
}

async function dispatchDebuggerMouseClick(tabId, point) {
  const x = Number(point?.x);
  const y = Number(point?.y);
  if (!Number.isFinite(x) || !Number.isFinite(y)) {
    throw new Error(`Invalid CDP click point: ${JSON.stringify(point)}`);
  }
  await sendDebuggerCommand(tabId, "Input.dispatchMouseEvent", {
    type: "mouseMoved",
    x,
    y,
    button: "none",
    buttons: 0,
    pointerType: "mouse"
  });
  await sendDebuggerCommand(tabId, "Input.dispatchMouseEvent", {
    type: "mousePressed",
    x,
    y,
    button: "left",
    buttons: 1,
    clickCount: 1,
    pointerType: "mouse"
  });
  await sendDebuggerCommand(tabId, "Input.dispatchMouseEvent", {
    type: "mouseReleased",
    x,
    y,
    button: "left",
    buttons: 0,
    clickCount: 1,
    pointerType: "mouse"
  });
}

async function focusSubmitButtonInMainWorld(tabId, selector, point = null) {
  if (!selector) {
    return null;
  }
  return await sendDebuggerCommand(tabId, "Runtime.evaluate", {
    expression: `
      (() => {
        const selector = ${JSON.stringify(selector)};
        const point = ${JSON.stringify(point)};
        let button = null;
        if (point && Number.isFinite(Number(point.x)) && Number.isFinite(Number(point.y))) {
          const pointEl = document.elementFromPoint(Number(point.x), Number(point.y));
          button = pointEl?.closest?.(selector) || null;
        }
        button = button || document.querySelector(selector);
        if (!button) return { ok: false, error: "submit selector not found", selector };
        button.scrollIntoView({ block: "center", inline: "center" });
        button.focus({ preventScroll: true });
        return {
          ok: true,
          text: (button.innerText || button.textContent || "").trim(),
          selector,
          activeElementText: (document.activeElement?.innerText || document.activeElement?.textContent || "").trim(),
          disabled: Boolean(button.disabled) || button.getAttribute("aria-disabled") === "true"
        };
      })()
    `,
    awaitPromise: true,
    returnByValue: true,
  });
}

async function checkVariationalOpenOrderAfterSubmit(result, payload, details, suffix) {
  const checkResult = await runVariationalCancelInjection({
    requestId: `${payload.requestId || "submit"}:${suffix}`,
    orderId: result.orderId || null,
    symbol: payload.symbol || details.market || null,
    side: payload.side || details.side || null,
    amount: payload.amount || details.amount || null,
    checkOnly: true,
  });
  const checkPayload = checkResult?.result || {};
  return {
    payload: checkPayload,
    exists: Boolean(checkPayload.exists || checkPayload.details?.exists),
  };
}

async function runVariationalSubmitFollowupInjection(payload) {
  const [injectionResult] = await chrome.scripting.executeScript({
    target: { tabId: state.attachedTabId },
    world: "MAIN",
    func: executeVariationalSubmitFollowup,
    args: [{ ...payload, automationVersion: ORDER_AUTOMATION_VERSION }]
  });
  return injectionResult?.result || {};
}

async function applyCdpSubmitClickIfRequested(result, payload) {
  const details = result?.details || {};
  const clickPoint = details.submitClickPoint;
  if (!result?.ok || !details.needsCdpSubmitClick || !clickPoint) {
    return result;
  }
  if (String(payload.orderType || details.orderType || "").toUpperCase() === "LIMIT") {
    await sleep(Number(payload.timeoutMs || 1500));
    const followupAfterSubmit = await runVariationalSubmitFollowupInjection({
      symbol: payload.symbol || details.market || null,
      side: payload.side || details.side || null,
      amount: payload.amount || details.amount || null,
    });
    if (followupAfterSubmit.clicked) {
      await sleep(Number(payload.timeoutMs || 1500));
    }
    const finalCheck = await checkVariationalOpenOrderAfterSubmit(result, payload, details, "verify-open-order-after-submit-activation");
    if (!finalCheck.exists) {
      return {
        ok: false,
        error: "Variational limit submit click did not create a matching open order.",
        details: {
          ...details,
          needsCdpSubmitClick: false,
          submitVerifiedOpenOrder: false,
          submitFollowupAfterSubmit: followupAfterSubmit,
          openOrderCheck: finalCheck.payload,
        }
      };
    }
    details.submitVerifiedOpenOrder = true;
    details.submitFollowupAfterSubmit = followupAfterSubmit;
    details.openOrderCheck = finalCheck.payload;
  } else {
    await sleep(Number(payload.timeoutMs || 1500));
  }
  return {
    ...result,
    details: {
      ...details,
      needsCdpSubmitClick: false
    }
  };
}

async function runOrderInjectionAttempt(payload) {
  const injectionResult = await runVariationalOrderInjection(payload);
  const result = injectionResult?.result;
  if (!result || typeof result !== "object" || !("ok" in result)) {
    throw new Error("Variational order injection returned no result.");
  }
  return await applyCdpSubmitClickIfRequested(result, payload);
}

async function runOrderInjectionAttemptWithFrameRecovery(payload) {
  const submittingOrder = !payload.previewOnly && !payload.prepareOnly;
  const submittingLimitOrder = submittingOrder
    && String(payload.orderType || "").toUpperCase() === "LIMIT";
  let lastError = null;

  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      return await runOrderInjectionAttempt(payload);
    } catch (error) {
      lastError = error;
      if (!isTransientOrderInjectionError(error)) {
        throw error;
      }

      await waitForTabComplete(state.attachedTabId);
      await waitForVariationalOrderPageReady(payload);

      if (submittingLimitOrder) {
        try {
          const recoveredCheck = await checkVariationalOpenOrderAfterSubmit(
            {},
            payload,
            {},
            `recover-full-submit-after-frame-removal-${attempt + 1}`
          );
          if (recoveredCheck.exists) {
            return {
              ok: true,
              orderId: recoveredCheck.payload.orderId || null,
              details: {
                automationVersion: ORDER_AUTOMATION_VERSION,
                recoveredAfterFrameRemoval: true,
                submitVerifiedOpenOrder: true,
                needsCdpSubmitClick: false,
                openOrderCheck: recoveredCheck.payload,
              }
            };
          }
        } catch (checkError) {
          lastError = checkError;
          if (!isTransientOrderInjectionError(checkError)) {
            throw checkError;
          }
          await waitForTabComplete(state.attachedTabId);
          await waitForVariationalOrderPageReady(payload);
        }
      } else if (submittingOrder) {
        throw new Error(
          "Variational page frame changed during market submit; execution status is unknown, so the order was not retried."
        );
      }

      if (attempt >= 2) {
        throw lastError;
      }
    }
  }

  throw lastError;
}

async function runVariationalCancelInjection(payload) {
  let lastError = null;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      const [injectionResult] = await chrome.scripting.executeScript({
        target: { tabId: state.attachedTabId },
        func: executeVariationalCancelOrder,
        args: [{ ...payload, automationVersion: ORDER_AUTOMATION_VERSION }]
      });
      return injectionResult;
    } catch (error) {
      lastError = error;
      if (attempt >= 2 || !isTransientFrameRemovalError(error)) {
        throw error;
      }
      await waitForTabComplete(state.attachedTabId);
      await sleep(1500);
    }
  }
  throw lastError;
}

async function runVariationalPricePreviewInjection(payload) {
  let lastError = null;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const [injectionResult] = await chrome.scripting.executeScript({
        target: { tabId: state.attachedTabId },
        func: executeVariationalLimitPricePreview,
        args: [{ ...payload, automationVersion: ORDER_AUTOMATION_VERSION, previewOnly: true }]
      });
      return injectionResult;
    } catch (error) {
      lastError = error;
      if (attempt >= 1 || !isTransientFrameRemovalError(error)) {
        throw error;
      }
      await sleep(1000);
    }
  }
  throw lastError;
}

async function handleCommandMessage(raw) {
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch (error) {
    return;
  }
  if (payload?.type === "PLACE_ORDER") {
    await handlePlaceOrderCommand(payload);
    return;
  }
  if (payload?.type === "CANCEL_ORDER") {
    await handleCancelOrderCommand(payload);
    return;
  }
  if (payload?.type === "PREPARE_MARKET_ORDER") {
    await handlePrepareMarketOrderCommand(payload);
    return;
  }
  if (payload?.type === "PREVIEW_LIMIT_ORDER_PRICE") {
    await handlePricePreviewCommand(payload);
    return;
  }
}

function isNotFoundError(error) {
  const msg = String(error || "").toLowerCase();
  if (msg.includes("could not find")) return true;
  return false;
}

function isSubmitDisabledAfterAmountError(error) {
  const msg = String(error || "").toLowerCase();
  return msg.includes("submit button stayed disabled after amount input");
}

function isQuotedPriceUnavailableError(error) {
  const msg = String(error || "").toLowerCase();
  return msg.includes("quoted price unavailable");
}

function isPostSubmitVerificationError(error) {
  const msg = String(error || "").toLowerCase();
  return msg.includes("limit submit click did not create a matching open order");
}

function collectVariationalOrderPageReadiness(command) {
  function visible(el) {
    if (!el) {
      return false;
    }
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== "hidden"
      && style.display !== "none"
      && rect.width > 0
      && rect.height > 0;
  }

  function normalizeSymbol(value) {
    return String(value || "")
      .trim()
      .toUpperCase()
      .replace(/[-_\s]?PERP$/i, "")
      .replace(/USDT$/i, "");
  }

  let currentSymbol = "";
  try {
    const url = new URL(window.location.href);
    const parts = url.pathname.split("/").filter(Boolean);
    const perpetualIndex = parts.findIndex((part) => part.toLowerCase() === "perpetual");
    currentSymbol = normalizeSymbol(parts[perpetualIndex + 1] || "");
  } catch (_error) {
    currentSymbol = "";
  }

  const expectedSymbol = normalizeSymbol(command.symbol || command.market);
  const quantityInput = document.querySelector('input[data-testid="quantity-input"]');
  const askText = String(document.querySelector('[data-testid="ask-price-display"]')?.textContent || "").trim();
  const bidText = String(document.querySelector('[data-testid="bid-price-display"]')?.textContent || "").trim();
  const quoteText = `${askText} ${bidText}`.trim();
  const quoteReady = /\$?\s*[0-9][0-9,]*(?:\.[0-9]+)?/.test(quoteText);
  const loadingButtons = Array.from(document.querySelectorAll("button"))
    .filter(visible)
    .filter((button) => /\bloading\b/i.test(String(button.textContent || "")));
  const tickerReady = Boolean(expectedSymbol)
    && Boolean(currentSymbol)
    && expectedSymbol === currentSymbol;

  return {
    ready: tickerReady
      && visible(quantityInput)
      && quoteReady
      && loadingButtons.length === 0,
    expectedSymbol,
    currentSymbol,
    quantityInputVisible: visible(quantityInput),
    quoteReady,
    quoteText,
    loadingButtonCount: loadingButtons.length,
    url: window.location.href,
  };
}

async function waitForVariationalOrderPageReady(payload, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  let lastReadiness = null;
  let lastError = null;

  while (Date.now() <= deadline) {
    try {
      const [injectionResult] = await chrome.scripting.executeScript({
        target: { tabId: state.attachedTabId },
        world: "MAIN",
        func: collectVariationalOrderPageReadiness,
        args: [payload]
      });
      lastReadiness = injectionResult?.result || null;
      if (lastReadiness?.ready) {
        return lastReadiness;
      }
    } catch (error) {
      lastError = error;
      if (!isTransientFrameRemovalError(error)) {
        throw error;
      }
    }
    await sleep(500);
  }

  const details = lastReadiness ? ` details=${JSON.stringify(lastReadiness)}` : "";
  const cause = lastError ? ` cause=${String(lastError.message || lastError)}` : "";
  throw new Error(
    `Variational order injection returned no result because the order page did not become ready.${details}${cause}`
  );
}

function waitForTabComplete(tabId, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let timeoutId = null;

    const cleanup = () => {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
      chrome.tabs.onUpdated.removeListener(onUpdated);
    };

    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      resolve();
    };

    const fail = (error) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      reject(error);
    };

    const onUpdated = (updatedTabId, changeInfo) => {
      if (updatedTabId === tabId && changeInfo.status === "complete") {
        finish();
      }
    };

    chrome.tabs.onUpdated.addListener(onUpdated);
    timeoutId = setTimeout(() => {
      fail(new Error(`Timed out waiting for tab ${tabId} to become ready`));
    }, timeoutMs);

    chrome.tabs.get(tabId, (tab) => {
      const err = chrome.runtime.lastError;
      if (err) {
        fail(new Error(err.message));
        return;
      }
      if (tab?.status === "complete") {
        finish();
      }
    });
  });
}

function reloadTabAndWaitForComplete(tabId, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let timeoutId = null;

    const cleanup = () => {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
      chrome.tabs.onUpdated.removeListener(onUpdated);
    };

    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      resolve();
    };

    const fail = (error) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      reject(error);
    };

    const onUpdated = (updatedTabId, changeInfo) => {
      if (updatedTabId === tabId && changeInfo.status === "complete") {
        finish();
      }
    };

    chrome.tabs.onUpdated.addListener(onUpdated);
    timeoutId = setTimeout(() => {
      fail(new Error(`Timed out waiting for tab ${tabId} to finish reload`));
    }, timeoutMs);

    chrome.tabs.reload(tabId, {}, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        fail(new Error(err.message));
      }
    });
  });
}

async function runOrderInjectionWithReload(payload) {
  let result = await runOrderInjectionAttemptWithFrameRecovery(payload);
  const submittingOrder = !payload.previewOnly && !payload.prepareOnly;

  for (let reloadAttempt = 0; result && !result.ok && reloadAttempt < MAX_ORDER_RELOAD_RETRIES; reloadAttempt += 1) {
    const quotedPriceUnavailable = isQuotedPriceUnavailableError(result.error);
    const postSubmitVerificationFailed = isPostSubmitVerificationError(result.error);
    if (submittingOrder && (!quotedPriceUnavailable || postSubmitVerificationFailed)) {
      break;
    }
    // For submitOnly, only reload when Variational explicitly reports that no
    // quoted price is available. Other failures may still rely on prepared state.
    const shouldReload = !postSubmitVerificationFailed && (
      quotedPriceUnavailable
      || (!payload.submitOnly && (isNotFoundError(result.error) || isSubmitDisabledAfterAmountError(result.error)))
    );
    if (!shouldReload) {
      break;
    }
    const reason = quotedPriceUnavailable
      ? (submittingOrder ? "quoted price unavailable before submit" : "quoted price unavailable")
      : (isSubmitDisabledAfterAmountError(result.error)
          ? "submit disabled after amount input"
          : "not found");
    console.log(`[variational] ${reason} — reloading page and retrying`);
    await reloadTabAndWaitForComplete(state.attachedTabId);
    await sleep(2000);
    result = await runOrderInjectionAttemptWithFrameRecovery(payload);
    if (result && !result.ok) {
      result.error = `[after reload] ${result.error || "unknown error"}`;
    }
  }

  return result;
}

async function handlePlaceOrderCommand(payload) {
  const requestId = payload.requestId;
  try {
    if (state.attachedTabId == null) {
      throw new Error("No Variational tab attached. Click Start in the extension popup first.");
    }
    let result = await runOrderInjectionWithReload(payload);
    if (!result || typeof result !== "object" || !("ok" in result)) {
      const [diagnosticResult] = await chrome.scripting.executeScript({
        target: { tabId: state.attachedTabId },
        func: collectVariationalPageDiagnostics
      });
      result = {
        ok: false,
        error: "Automation returned no result from Variational page.",
        details: {
          automationVersion: ORDER_AUTOMATION_VERSION,
          diagnostics: diagnosticResult?.result || null
        }
      };
    }
    commandClient.send({
      "type": "ORDER_RESULT",
      requestId,
      ok: Boolean(result.ok),
      orderId: result.orderId || null,
      error: result.error || null,
      details: {
        automationVersion: ORDER_AUTOMATION_VERSION,
        ...(result.details || {})
      },
      timestamp: nowIso()
    });
  } catch (error) {
    commandClient.send({
      "type": "ORDER_RESULT",
      requestId,
      ok: false,
      error: error.message,
      details: { automationVersion: ORDER_AUTOMATION_VERSION },
      timestamp: nowIso()
    });
  }
}

async function handleCancelOrderCommand(payload) {
  const requestId = payload.requestId;
  try {
    if (state.attachedTabId == null) {
      throw new Error("No Variational tab attached. Click Start in the extension popup first.");
    }
    const injectionResult = await runVariationalCancelInjection(payload);
    let result = injectionResult?.result || {};
    if (!result || typeof result !== "object" || !("ok" in result)) {
      const [diagnosticResult] = await chrome.scripting.executeScript({
        target: { tabId: state.attachedTabId },
        func: collectVariationalPageDiagnostics
      });
      result = {
        ok: false,
        error: "Cancel automation returned no result from Variational page.",
        details: {
          automationVersion: ORDER_AUTOMATION_VERSION,
          diagnostics: diagnosticResult?.result || null
        }
      };
    }
    commandClient.send({
      "type": "CANCEL_RESULT",
      requestId,
      ok: Boolean(result.ok),
      orderId: result.orderId || payload.orderId || null,
      error: result.error || null,
      details: {
        automationVersion: ORDER_AUTOMATION_VERSION,
        ...(result.details || {})
      },
      timestamp: nowIso()
    });
  } catch (error) {
    commandClient.send({
      "type": "CANCEL_RESULT",
      requestId,
      ok: false,
      orderId: payload.orderId || null,
      error: error.message,
      details: { automationVersion: ORDER_AUTOMATION_VERSION },
      timestamp: nowIso()
    });
  }
}

async function handlePrepareMarketOrderCommand(payload) {
  const requestId = payload.requestId;
  try {
    if (state.attachedTabId == null) {
      throw new Error("No Variational tab attached. Click Start in the extension popup first.");
    }
    let result = await runOrderInjectionWithReload({ ...payload, prepareOnly: true });
    if (!result || typeof result !== "object" || !("ok" in result)) {
      const [diagnosticResult] = await chrome.scripting.executeScript({
        target: { tabId: state.attachedTabId },
        func: collectVariationalPageDiagnostics
      });
      result = {
        ok: false,
        error: "Prepare market order automation returned no result.",
        details: { automationVersion: ORDER_AUTOMATION_VERSION, diagnostics: diagnosticResult?.result || null }
      };
    }
    commandClient.send({
      type: "PREPARE_RESULT",
      requestId,
      ok: Boolean(result.ok),
      prepared: Boolean(result.prepared),
      error: result.error || null,
      details: { automationVersion: ORDER_AUTOMATION_VERSION, ...(result.details || {}) },
      timestamp: nowIso()
    });
  } catch (error) {
    commandClient.send({
      type: "PREPARE_RESULT",
      requestId,
      ok: false,
      prepared: false,
      error: error.message,
      details: { automationVersion: ORDER_AUTOMATION_VERSION },
      timestamp: nowIso()
    });
  }
}

async function handlePricePreviewCommand(payload) {
  const requestId = payload.requestId;
  try {
    if (state.attachedTabId == null) {
      throw new Error("No Variational tab attached. Click Start in the extension popup first.");
    }
    const injectionResult = await runVariationalPricePreviewInjection(payload);
    let result = injectionResult?.result || {};
    if (!result || typeof result !== "object" || !("ok" in result)) {
      const [diagnosticResult] = await chrome.scripting.executeScript({
        target: { tabId: state.attachedTabId },
        func: collectVariationalPageDiagnostics
      });
      result = {
        ok: false,
        error: "Price preview returned no result from Variational page.",
        details: {
          automationVersion: ORDER_AUTOMATION_VERSION,
          diagnostics: diagnosticResult?.result || null
        }
      };
    }
    commandClient.send({
      "type": "PRICE_PREVIEW_RESULT",
      requestId,
      ok: Boolean(result.ok),
      price: result.price || null,
      error: result.error || null,
      details: {
        automationVersion: ORDER_AUTOMATION_VERSION,
        ...(result.details || {})
      },
      timestamp: nowIso()
    });
  } catch (error) {
    commandClient.send({
      "type": "PRICE_PREVIEW_RESULT",
      requestId,
      ok: false,
      price: null,
      error: error.message,
      details: { automationVersion: ORDER_AUTOMATION_VERSION },
      timestamp: nowIso()
    });
  }
}

function collectVariationalPageDiagnostics() {
  const clickableSelector = "button,[role='button'],a,[tabindex],div[class*='cursor-pointer'],div[class*='hover:bg']";

  function visible(el) {
    if (!el) {
      return false;
    }
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
  }

  function textOf(el) {
    return `${el.innerText || ""} ${el.textContent || ""} ${el.getAttribute("aria-label") || ""}`
      .replace(/\s+/g, " ")
      .trim();
  }

  return {
    url: window.location.href,
    title: document.title,
    buttons: Array.from(document.querySelectorAll(clickableSelector))
      .filter(visible)
      .slice(0, 120)
      .map((el) => ({
        text: textOf(el).slice(0, 160),
        disabled: Boolean(el.disabled) || el.getAttribute("aria-disabled") === "true",
        className: String(el.className || "").slice(0, 160)
      })),
    inputs: Array.from(document.querySelectorAll("input,textarea"))
      .filter(visible)
      .slice(0, 30)
      .map((el) => ({
        placeholder: String(el.getAttribute("placeholder") || "").slice(0, 160),
        value: String(el.value || "").slice(0, 60),
        type: String(el.getAttribute("type") || "").slice(0, 60),
        inputMode: String(el.getAttribute("inputmode") || "").slice(0, 60),
        className: String(el.className || "").slice(0, 160)
      }))
  };
}

function executeVariationalCancelOrder(command) {
  const automationVersion = command.automationVersion || "unknown";
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const clickableSelector = "button,[role='button'],a,[tabindex],div[class*='cursor-pointer'],div[class*='hover:bg']";

  function visible(el) {
    if (!el) {
      return false;
    }
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
  }

  function textOf(el) {
    return `${el.innerText || ""} ${el.textContent || ""} ${el.getAttribute("aria-label") || ""} ${el.getAttribute("title") || ""}`
      .replace(/\s+/g, " ")
      .trim();
  }

  function click(el) {
    try { el.scrollIntoView({ block: "center", inline: "center" }); } catch (_e) {}
    try { el.focus({ preventScroll: true }); } catch (_e) {}
    const rect = el.getBoundingClientRect();
    const cx = rect.left + rect.width / 2;
    const cy = rect.top + rect.height / 2;
    const base = { bubbles: true, cancelable: true, view: window, clientX: cx, clientY: cy, button: 0, buttons: 1 };
    const pointer = { ...base, pointerId: 1, pointerType: "mouse", isPrimary: true };
    // Full realistic gesture: some Variational controls (submit button, tabs, cancel)
    // respond to pointer events, not just a bare element.click().
    try { el.dispatchEvent(new PointerEvent("pointerover", pointer)); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mouseover", base));
    try { el.dispatchEvent(new PointerEvent("pointerenter", pointer)); } catch (_e) {}
    try { el.dispatchEvent(new PointerEvent("pointerdown", pointer)); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mousedown", base));
    try { el.dispatchEvent(new PointerEvent("pointerup", { ...pointer, buttons: 0 })); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mouseup", { ...base, buttons: 0 }));
    el.click();
  }

  function normalizeVariationalSymbol(value) {
    return String(value || "")
      .trim()
      .toUpperCase()
      .replace(/[-_\s]?PERP$/i, "")
      .replace(/USDT$/i, "");
  }

  function getOrderRow(el) {
    return el.closest('[data-testid="orders-table-row"]')
      || el.closest("tr")
      || el.closest("[role='row']")
      || el.parentElement
      || el;
  }

  function ensureOpenOrdersTabVisible() {
    const tabCandidates = Array.from(document.querySelectorAll("[role='tab'],button"))
      .filter(visible)
      .filter((el) => /open\s+orders/i.test(textOf(el)))
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")
      .filter((el) => getComputedStyle(el).pointerEvents !== "none");
    if (tabCandidates.length > 0) {
      click(tabCandidates[0]);
    }
  }

  function isCancelOnlyButton(el) {
    const t = textOf(el).toLowerCase();
    return /\bcancel\b/.test(t) && !/replace/i.test(t) && !/edit/i.test(t);
  }

  function normalizeAmountText(value) {
    return String(value || "")
      .trim()
      .replace(/,/g, "");
  }

  function normalizeSideText(value) {
    const text = String(value || "").trim().toLowerCase();
    if (!text) {
      return "";
    }
    if (["sell", "short", "ask"].includes(text)) {
      return "sell";
    }
    if (["buy", "long", "bid"].includes(text)) {
      return "buy";
    }
    return text;
  }

  function rowIncludesRequestedAmount(rowText, requestedAmount) {
    if (!requestedAmount) {
      return true;
    }
    const normalizedRequested = normalizeAmountText(requestedAmount);
    if (!normalizedRequested) {
      return true;
    }
    const normalizedRowText = normalizeAmountText(rowText);
    return normalizedRowText.includes(normalizedRequested);
  }

  function collectOrderRows() {
    const seen = new Set();
    return Array.from(document.querySelectorAll('[data-testid="orders-table-row"], tr, [role="row"]'))
      .filter(visible)
      .filter((el) => {
        if (seen.has(el)) {
          return false;
        }
        seen.add(el);
        return true;
      });
  }

  function rowActionCandidates(row) {
    return Array.from(row.querySelectorAll(clickableSelector))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true");
  }

  function rowTextMatches({
    rowText,
    normalizedSymbol,
    normalizedSide,
    normalizedAmount,
    requireSide,
    requireAmount,
  }) {
    const symbolMatches = normalizedSymbol && normalizeVariationalSymbol(rowText).includes(normalizedSymbol);
    if (!symbolMatches) {
      return false;
    }
    const rowTextLower = rowText.toLowerCase();
    const sideMatches = !requireSide || !normalizedSide || rowTextLower.includes(normalizedSide);
    const amountMatches = !requireAmount || rowIncludesRequestedAmount(rowText, normalizedAmount);
    return symbolMatches && sideMatches && amountMatches;
  }

  function findClickableCancelInRow(row) {
    const actions = rowActionCandidates(row);
    const exact = actions.find((el) => {
      const text = textOf(el);
      return /cancel/i.test(text) || /cancel order/i.test(text);
    });
    if (exact) {
      return exact;
    }
    const titled = actions.find((el) => {
      const label = `${el.getAttribute("title") || ""} ${el.getAttribute("aria-label") || ""}`.toLowerCase();
      return label.includes("cancel");
    });
    if (titled) {
      return titled;
    }
    return actions.length === 1 ? actions[0] : null;
  }

  function findCancelOrderButton(orderId, requestedSymbol, side, amount) {
    const normalizedSymbol = normalizeVariationalSymbol(requestedSymbol);
    const normalizedSide = normalizeSideText(side);
    const normalizedAmount = normalizeAmountText(amount);
    const rows = collectOrderRows();

    function rowMatches(el) {
      const rowText = textOf(getOrderRow(el));
      if (orderId && rowText.includes(String(orderId))) {
        return true;
      }
      return rowTextMatches({
        rowText,
        normalizedSymbol,
        normalizedSide,
        normalizedAmount,
        requireSide: true,
        requireAmount: true
      });
    }

    // Prefer exact title="Cancel Order" buttons first
    const exactCandidates = Array.from(document.querySelectorAll('button[title="Cancel Order"],button[aria-label="Cancel Order"]'))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true");
    const exactScoped = exactCandidates.filter(rowMatches);
    if (exactScoped.length >= 1) {
      return exactScoped[0];
    }

    // Fallback: any cancel-only button (excludes "Cancel & Replace")
    const candidates = Array.from(document.querySelectorAll(clickableSelector))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")
      .filter(isCancelOnlyButton);
    if (!candidates.length) {
      return null;
    }
    const scoped = candidates.filter(rowMatches);
    if (scoped.length >= 1) {
      return scoped[0];
    }

    const passes = [
      { requireSide: true, requireAmount: true },
      { requireSide: true, requireAmount: false },
      { requireSide: false, requireAmount: false }
    ];
    for (const pass of passes) {
      const matchedRows = rows.filter((row) =>
        rowTextMatches({
          rowText: textOf(row),
          normalizedSymbol,
          normalizedSide,
          normalizedAmount,
          requireSide: pass.requireSide,
          requireAmount: pass.requireAmount
        })
      );
      if (matchedRows.length !== 1) {
        continue;
      }
      const action = findClickableCancelInRow(matchedRows[0]);
      if (action) {
        return action;
      }
    }

    return candidates.length === 1 && !normalizedSymbol ? candidates[0] : null;
  }

  // Returns a matching open-order row if the target order is still present, else null.
  // Uses the same match passes as findCancelOrderButton so "present" is consistent.
  function findMatchingOrderRow(orderId, requestedSymbol, side, amount) {
    const normalizedSymbol = normalizeVariationalSymbol(requestedSymbol);
    const normalizedSide = normalizeSideText(side);
    const normalizedAmount = normalizeAmountText(amount);
    const rows = collectOrderRows();
    if (orderId) {
      const byId = rows.find((row) => textOf(getOrderRow(row)).includes(String(orderId)));
      if (byId) {
        return byId;
      }
    }
    const passes = [
      { requireSide: true, requireAmount: true },
      { requireSide: true, requireAmount: false },
      { requireSide: false, requireAmount: false }
    ];
    for (const pass of passes) {
      const matched = rows.filter((row) =>
        rowTextMatches({
          rowText: textOf(row),
          normalizedSymbol,
          normalizedSide,
          normalizedAmount,
          requireSide: pass.requireSide,
          requireAmount: pass.requireAmount
        })
      );
      if (matched.length >= 1) {
        return matched[0];
      }
    }
    return null;
  }

  function findCancelConfirmButton() {
    const confirmPatterns = [
      /\bconfirm\s+cancel\b/i,
      /\bcancel\s+order\b/i,
      /\byes\b/i,
      /\bconfirm\b/i,
      /確認/,
    ];
    return Array.from(document.querySelectorAll(clickableSelector))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")
      .find((el) => confirmPatterns.some((pattern) => pattern.test(textOf(el))));
  }

  function collectCancelDiagnostics() {
    return {
      url: window.location.href,
      title: document.title,
      rows: collectOrderRows()
        .slice(0, 40)
        .map((row) => ({
          text: textOf(row).slice(0, 240),
          actionCount: rowActionCandidates(row).length
        })),
      buttons: Array.from(document.querySelectorAll(clickableSelector))
        .filter(visible)
        .slice(0, 120)
        .map((el) => ({
          text: textOf(el).slice(0, 160),
          disabled: Boolean(el.disabled) || el.getAttribute("aria-disabled") === "true",
          className: String(el.className || "").slice(0, 160)
        }))
    };
  }

  return (async () => {
    const orderId = String(command.orderId || "").trim();
    const symbol = normalizeVariationalSymbol(command.symbol || command.market);
    const side = String(command.side || "").trim().toLowerCase();
    const amount = String(command.amount || "").trim();

    ensureOpenOrdersTabVisible();
    await sleep(1500);

    // checkOnly: report whether a matching open order still exists WITHOUT cancelling.
    if (command.checkOnly) {
      const row = findMatchingOrderRow(orderId, symbol, side, amount);
      return {
        ok: true,
        exists: Boolean(row),
        status: row ? "present" : "absent",
        orderId: orderId || null,
        details: { automationVersion, exists: Boolean(row), symbol, side, amount: amount || null, checkOnly: true }
      };
    }

    let cancelButton = findCancelOrderButton(orderId, symbol, side, amount);
    if (!cancelButton) {
      await sleep(3000);
      ensureOpenOrdersTabVisible();
      await sleep(1500);
      cancelButton = findCancelOrderButton(orderId, symbol, side, amount);
    }
    if (!cancelButton) {
      // The order may already be gone (filled or cancelled). If no matching open
      // order row exists, the cancel goal is already met — return idempotent
      // success instead of erroring. (Open Orders tab was made visible above.)
      const stillPresent = findMatchingOrderRow(orderId, symbol, side, amount);
      if (!stillPresent) {
        return {
          ok: true,
          orderId: orderId || null,
          status: "already_absent",
          details: {
            automationVersion,
            symbol,
            side,
            amount: amount || null,
            note: "No matching open order found — treated as already cancelled/filled."
          }
        };
      }
      return {
        ok: false,
        error: "Could not identify cancel button for Variational order.",
        details: {
          automationVersion,
          orderId: orderId || null,
          symbol,
          side,
          amount: amount || null,
          diagnostics: collectCancelDiagnostics()
        }
      };
    }

    click(cancelButton);
    await sleep(Number(command.confirmDelayMs || 300));
    const confirmButton = findCancelConfirmButton();
    if (confirmButton) {
      click(confirmButton);
      await sleep(300);
    }

    return {
      ok: true,
      orderId: orderId || null,
      status: "cancelled",
      details: {
        automationVersion,
        symbol,
        side,
        amount: amount || null,
        clickedCancelText: textOf(cancelButton),
        clickedConfirmText: confirmButton ? textOf(confirmButton) : null
      }
    };
  })();
}

function executeVariationalLimitPricePreview(command) {
  const automationVersion = command.automationVersion || "unknown";
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  function visible(el) {
    if (!el) {
      return false;
    }
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
  }

  function textOf(el) {
    return `${el.innerText || ""} ${el.textContent || ""} ${el.getAttribute("aria-label") || ""}`
      .replace(/\s+/g, " ")
      .trim();
  }

  function normalizedTextOf(el) {
    return textOf(el).toLowerCase().replace(/\s+/g, " ").trim();
  }

  function click(el) {
    try { el.scrollIntoView({ block: "center", inline: "center" }); } catch (_e) {}
    try { el.focus({ preventScroll: true }); } catch (_e) {}
    const rect = el.getBoundingClientRect();
    const cx = rect.left + rect.width / 2;
    const cy = rect.top + rect.height / 2;
    const base = { bubbles: true, cancelable: true, view: window, clientX: cx, clientY: cy, button: 0, buttons: 1 };
    const pointer = { ...base, pointerId: 1, pointerType: "mouse", isPrimary: true };
    // Full realistic gesture: some Variational controls (submit button, tabs, cancel)
    // respond to pointer events, not just a bare element.click().
    try { el.dispatchEvent(new PointerEvent("pointerover", pointer)); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mouseover", base));
    try { el.dispatchEvent(new PointerEvent("pointerenter", pointer)); } catch (_e) {}
    try { el.dispatchEvent(new PointerEvent("pointerdown", pointer)); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mousedown", base));
    try { el.dispatchEvent(new PointerEvent("pointerup", { ...pointer, buttons: 0 })); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mouseup", { ...base, buttons: 0 }));
    el.click();
  }

  function normalizeVariationalSymbol(value) {
    return String(value || "")
      .trim()
      .toUpperCase()
      .replace(/[-_\s]?PERP$/i, "")
      .replace(/USDT$/i, "");
  }

  function currentVariationalSymbol() {
    try {
      const url = new URL(window.location.href);
      const parts = url.pathname.split("/").filter(Boolean);
      const perpetualIndex = parts.findIndex((part) => part.toLowerCase() === "perpetual");
      if (perpetualIndex >= 0 && parts[perpetualIndex + 1]) {
        return normalizeVariationalSymbol(decodeURIComponent(parts[perpetualIndex + 1]));
      }
    } catch {
      // Fall back to document title below.
    }

    const titleMatch = String(document.title || "").match(/\b([A-Z0-9]+)\s*[-_\s]?\s*PERP\b/i);
    return titleMatch ? normalizeVariationalSymbol(titleMatch[1]) : "";
  }

  function inputContextText(el) {
    return [
      el.getAttribute("data-testid") || "",
      el.getAttribute("name") || "",
      el.getAttribute("id") || "",
      el.getAttribute("placeholder") || "",
      el.getAttribute("aria-label") || "",
      el.parentElement?.innerText || "",
      el.closest("label")?.innerText || "",
      el.closest("[data-testid]")?.getAttribute("data-testid") || "",
      el.closest("[data-testid]")?.innerText || "",
    ].join(" ");
  }

  function isLimitPriceInput(el) {
    const dataTestId = String(el.getAttribute("data-testid") || "").toLowerCase();
    if (dataTestId.includes("limit-price") || dataTestId.includes("price-input")) {
      return true;
    }
    return /\bprice\b/i.test(inputContextText(el));
  }

  function findLimitPriceInput() {
    const exactPriceInput = Array.from(document.querySelectorAll(
      'input[data-testid="limit-price-input"],textarea[data-testid="limit-price-input"]'
    ))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")[0];
    if (exactPriceInput) {
      return exactPriceInput;
    }
    return Array.from(document.querySelectorAll("input,textarea"))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")
      .find(isLimitPriceInput) || null;
  }

  function readLimitPriceValue(input) {
    if (!input) {
      return "";
    }
    return String(input.value || input.getAttribute("value") || "")
      .replace(/,/g, "")
      .trim();
  }

  function findPreviewLimitOrderTypeButton() {
    const buttons = Array.from(document.querySelectorAll("button"))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true");
    return buttons.find((el) => normalizedTextOf(el) === "limit")
      || buttons.find((el) => /\blimit\b/i.test(textOf(el)) && !/\bmarket\b/i.test(textOf(el)));
  }

  function findPreviewMidButton() {
    return Array.from(document.querySelectorAll("button"))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")
      .find((el) => normalizedTextOf(el) === "mid" || /\bmid\b/i.test(textOf(el)));
  }

  async function waitForPreviewLimitPrice(timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() <= deadline) {
      const priceInput = findLimitPriceInput();
      const price = readLimitPriceValue(priceInput);
      if (price && Number(price) > 0) {
        return { price, priceInput };
      }
      await sleep(100);
    }
    return { price: "", priceInput: findLimitPriceInput() };
  }

  function collectPreviewDiagnostics() {
    return {
      url: window.location.href,
      title: document.title,
      buttons: Array.from(document.querySelectorAll("button"))
        .filter(visible)
        .slice(0, 80)
        .map((el) => ({
          text: textOf(el).slice(0, 120),
          disabled: Boolean(el.disabled) || el.getAttribute("aria-disabled") === "true",
          className: String(el.className || "").slice(0, 120)
        })),
      inputs: Array.from(document.querySelectorAll("input,textarea"))
        .filter(visible)
        .slice(0, 20)
        .map((el) => ({
          placeholder: String(el.getAttribute("placeholder") || "").slice(0, 120),
          value: String(el.value || "").slice(0, 40),
          dataTestId: String(el.getAttribute("data-testid") || "").slice(0, 80),
          context: inputContextText(el).replace(/\s+/g, " ").trim().slice(0, 160)
        }))
    };
  }

  return (async () => {
    const requestedSymbol = normalizeVariationalSymbol(command.symbol || command.market);
    const currentSymbol = currentVariationalSymbol();
    if (!requestedSymbol || !currentSymbol || requestedSymbol !== currentSymbol) {
      return {
        ok: false,
        error: `Ticker mismatch: requested ${requestedSymbol || "unknown"} but current page is ${currentSymbol || "unknown"}.`,
        details: {
          automationVersion,
          previewOnly: true,
          requestedSymbol,
          currentSymbol,
          diagnostics: collectPreviewDiagnostics()
        }
      };
    }

    const limitButton = findPreviewLimitOrderTypeButton();
    if (!limitButton) {
      return {
        ok: false,
        error: "Could not switch Variational order form to Limit: Limit button not found.",
        details: { automationVersion, previewOnly: true, diagnostics: collectPreviewDiagnostics() }
      };
    }
    click(limitButton);
    await sleep(300);

    const midButton = findPreviewMidButton();
    if (!midButton) {
      return {
        ok: false,
        error: "Could not find Mid button for Variational limit price preview.",
        details: { automationVersion, previewOnly: true, clickedLimitText: textOf(limitButton), diagnostics: collectPreviewDiagnostics() }
      };
    }
    click(midButton);

    const priceResult = await waitForPreviewLimitPrice(Number(command.previewPriceTimeoutMs || 3000));
    if (!priceResult.price || Number(priceResult.price) <= 0) {
      return {
        ok: false,
        error: "Could not read Variational limit price after Mid click.",
        details: {
          automationVersion,
          previewOnly: true,
          clickedLimitText: textOf(limitButton),
          clickedMidText: textOf(midButton),
          diagnostics: collectPreviewDiagnostics()
        }
      };
    }

    return {
      ok: true,
      price: priceResult.price,
      details: {
        automationVersion,
        previewOnly: true,
        clickedLimitText: textOf(limitButton),
        clickedMidText: textOf(midButton)
      }
    };
  })();
}

function executeVariationalSubmitFollowup(command) {
  const automationVersion = command.automationVersion || "unknown";
  const clickableSelector = "button,[role='button'],a,[tabindex],div[class*='cursor-pointer'],div[class*='hover:bg']";

  function visible(el) {
    if (!el) {
      return false;
    }
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
  }

  function textOf(el) {
    return `${el.innerText || ""} ${el.textContent || ""} ${el.getAttribute("aria-label") || ""} ${el.getAttribute("title") || ""}`.trim();
  }

  function click(el) {
    try { el.scrollIntoView({ block: "center", inline: "center" }); } catch (_e) {}
    try { el.focus({ preventScroll: true }); } catch (_e) {}
    const rect = el.getBoundingClientRect();
    const cx = rect.left + rect.width / 2;
    const cy = rect.top + rect.height / 2;
    const base = { bubbles: true, cancelable: true, view: window, clientX: cx, clientY: cy, button: 0, buttons: 1 };
    const pointer = { ...base, pointerId: 1, pointerType: "mouse", isPrimary: true };
    try { el.dispatchEvent(new PointerEvent("pointerover", pointer)); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mouseover", base));
    try { el.dispatchEvent(new PointerEvent("pointerdown", pointer)); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mousedown", base));
    try { el.dispatchEvent(new PointerEvent("pointerup", { ...pointer, buttons: 0 })); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mouseup", { ...base, buttons: 0 }));
    el.click();
  }

  function normalizeSymbol(value) {
    return String(value || "")
      .trim()
      .toUpperCase()
      .replace(/[-_\s]?PERP$/i, "")
      .replace(/USDT$/i, "");
  }

  function modalCandidates() {
    const explicit = Array.from(document.querySelectorAll('[role="dialog"],[aria-modal="true"]'))
      .filter(visible);
    const fixed = Array.from(document.querySelectorAll("div,section"))
      .filter(visible)
      .filter((el) => {
        const style = window.getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return (style.position === "fixed" || style.position === "absolute")
          && rect.width >= 240
          && rect.height >= 120
          && rect.left < window.innerWidth
          && rect.top < window.innerHeight;
      })
      .filter((el) => /confirm|review|order|margin|submit|buy|sell/i.test(textOf(el)));
    return [...explicit, ...fixed];
  }

  function isRejected(text) {
    const normalized = text.toLowerCase().replace(/\s+/g, " ").trim();
    return /\bcancel\b/.test(normalized)
      || /\bclose\b/.test(normalized)
      || /\bedit\b/.test(normalized)
      || /\bback\b/.test(normalized)
      || /\bhistory\b/.test(normalized);
  }

  const symbol = normalizeSymbol(command.symbol || command.market);
  const side = String(command.side || "").trim().toUpperCase();
  const sideWord = side === "BUY" ? "buy" : "sell";
  const containers = modalCandidates();
  const buttons = containers.flatMap((container) =>
    Array.from(container.querySelectorAll(clickableSelector))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")
      .map((el) => ({ el, text: textOf(el).replace(/\s+/g, " ").trim(), containerText: textOf(container).replace(/\s+/g, " ").trim().slice(0, 240) }))
  );
  const candidates = buttons.filter((item) => {
    const lower = item.text.toLowerCase();
    if (isRejected(item.text)) {
      return false;
    }
    return /\bconfirm\b/.test(lower)
      || /\bsubmit\b/.test(lower)
      || /\bplace\b/.test(lower)
      || /\border\b/.test(lower)
      || (sideWord && lower.includes(sideWord) && (!symbol || lower.includes(symbol.toLowerCase())));
  });
  const chosen = candidates[0] || null;
  if (chosen) {
    click(chosen.el);
    return {
      ok: true,
      clicked: true,
      automationVersion,
      clickedText: chosen.text,
      containerText: chosen.containerText,
      modalCount: containers.length,
      candidateTexts: candidates.slice(0, 10).map((item) => item.text)
    };
  }
  return {
    ok: true,
    clicked: false,
    automationVersion,
    modalCount: containers.length,
    buttonTexts: buttons.slice(0, 20).map((item) => item.text),
    containerTexts: containers.slice(0, 5).map((el) => textOf(el).replace(/\s+/g, " ").trim().slice(0, 240))
  };
}

function executeVariationalOrder(command) {
  const automationVersion = command.automationVersion || "unknown";
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const clickableSelector = "button,[role='button'],a,[tabindex],div[class*='cursor-pointer'],div[class*='hover:bg']";

  function visible(el) {
    if (!el) {
      return false;
    }
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
  }

  function textOf(el) {
    return `${el.innerText || ""} ${el.textContent || ""} ${el.getAttribute("aria-label") || ""}`.trim();
  }

  function click(el) {
    try { el.scrollIntoView({ block: "center", inline: "center" }); } catch (_e) {}
    try { el.focus({ preventScroll: true }); } catch (_e) {}
    const rect = el.getBoundingClientRect();
    const cx = rect.left + rect.width / 2;
    const cy = rect.top + rect.height / 2;
    const base = { bubbles: true, cancelable: true, view: window, clientX: cx, clientY: cy, button: 0, buttons: 1 };
    const pointer = { ...base, pointerId: 1, pointerType: "mouse", isPrimary: true };
    // Full realistic gesture: some Variational controls (e.g. the order submit
    // button) respond to pointer events, not just a bare element.click().
    try { el.dispatchEvent(new PointerEvent("pointerover", pointer)); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mouseover", base));
    try { el.dispatchEvent(new PointerEvent("pointerenter", pointer)); } catch (_e) {}
    try { el.dispatchEvent(new PointerEvent("pointerdown", pointer)); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mousedown", base));
    try { el.dispatchEvent(new PointerEvent("pointerup", { ...pointer, buttons: 0 })); } catch (_e) {}
    el.dispatchEvent(new MouseEvent("mouseup", { ...base, buttons: 0 }));
    el.click();
  }

  function cdpClickPoint(el) {
    try { el.scrollIntoView({ block: "center", inline: "center" }); } catch (_e) {}
    try { el.focus({ preventScroll: true }); } catch (_e) {}
    const rect = el.getBoundingClientRect();
    const x = rect.left + rect.width / 2;
    const y = rect.top + rect.height / 2;
    const pointEl = document.elementFromPoint(x, y);
    return {
      x,
      y,
      width: rect.width,
      height: rect.height,
      elementFromPointText: pointEl ? textOf(pointEl).replace(/\s+/g, " ").trim().slice(0, 120) : null,
      elementFromPointTag: pointEl ? pointEl.tagName : null,
      elementFromPointClassName: pointEl ? String(pointEl.className || "").slice(0, 120) : null
    };
  }

  function setInputValue(input, value) {
    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")?.set;
    if (setter) {
      setter.call(input, value);
    } else {
      input.value = value;
    }
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
    input.dispatchEvent(new KeyboardEvent("keyup", { bubbles: true }));
  }

  function findButton(patterns) {
    const candidates = Array.from(document.querySelectorAll(clickableSelector))
      .filter(visible);
    return candidates.find((el) => patterns.some((pattern) => pattern.test(textOf(el))));
  }

  function findOrderTypeButton(orderType) {
    const pattern = orderType === "LIMIT" ? /\blimit\b/i : /\bmarket\b/i;
    return Array.from(document.querySelectorAll("button"))
      .filter(visible)
      .find((el) => pattern.test(textOf(el)));
  }

  function findLimitMidButton() {
    return Array.from(document.querySelectorAll("button"))
      .filter(visible)
      .find((el) => /\bmid\b/i.test(textOf(el)));
  }

  function isReduceOnlyEnabled(control) {
    const containers = [
      control,
      control.closest("label"),
      control.closest("button"),
      control.parentElement,
      control.parentElement?.parentElement,
    ].filter(Boolean);
    for (const container of containers) {
      const ariaChecked = container.getAttribute("aria-checked");
      if (ariaChecked === "true") {
        return true;
      }
      if (ariaChecked === "false") {
        return false;
      }
      const checkedInput = container.querySelector?.("input[type='checkbox'],input[type='radio']");
      if (checkedInput) {
        return Boolean(checkedInput.checked);
      }
      const className = String(container.className || "").toLowerCase();
      if (className.includes("bg-azure") && !className.includes("bg-transparent")) {
        return true;
      }
    }
    return false;
  }

  async function ensureReduceOnly(enabled) {
    if (!enabled) {
      return { ok: true, changed: false };
    }
    const control = Array.from(document.querySelectorAll(clickableSelector))
      .filter(visible)
      .find((el) => /\breduce\s+only\b/i.test(textOf(el)));
    if (!control) {
      return { ok: false, error: "Could not find Reduce Only control on Variational page." };
    }
    if (!isReduceOnlyEnabled(control)) {
      click(control);
      await sleep(200);
    }
    return { ok: true, changed: true };
  }

  function findLimitPriceInput() {
    const exactPriceInput = Array.from(document.querySelectorAll(
      'input[data-testid="limit-price-input"],textarea[data-testid="limit-price-input"]'
    ))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")[0];
    if (exactPriceInput) {
      return exactPriceInput;
    }
    const inputs = Array.from(document.querySelectorAll("input,textarea"))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true");
    return inputs.find(isLimitPriceInput) || null;
  }

  function readLimitPriceValue(input) {
    if (!input) {
      return "";
    }
    return String(input.value || input.getAttribute("value") || "")
      .replace(/,/g, "")
      .trim();
  }

  function inputContextText(el) {
    return [
      el.getAttribute("data-testid") || "",
      el.getAttribute("name") || "",
      el.getAttribute("id") || "",
      el.getAttribute("placeholder") || "",
      el.getAttribute("aria-label") || "",
      el.parentElement?.innerText || "",
      el.closest("label")?.innerText || "",
      el.closest("[data-testid]")?.getAttribute("data-testid") || "",
      el.closest("[data-testid]")?.innerText || "",
    ].join(" ");
  }

  function isLimitPriceInput(el) {
    const dataTestId = String(el.getAttribute("data-testid") || "").toLowerCase();
    if (dataTestId.includes("limit-price") || dataTestId.includes("price-input")) {
      return true;
    }
    const localText = inputContextText(el);
    const pricePatterns = [/\bprice\b/i, /\blimit\s+price\b/i];
    return pricePatterns.some((pattern) => pattern.test(localText));
  }

  async function waitForPriceInput(timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() <= deadline) {
      const input = findLimitPriceInput();
      const val = input ? String(input.value || "").replace(/,/g, "").trim() : "";
      if (val && Number(val) > 0) {
        return input;
      }
      await sleep(100);
    }
    return findLimitPriceInput();
  }

  async function setLimitPriceOrClickMid(explicitLimitPrice) {
    const priceInput = findLimitPriceInput();
    if (explicitLimitPrice) {
      if (!priceInput) {
        return { ok: false, usedMid: false, priceInput: null, error: "Could not find limit price input on Variational page." };
      }
      priceInput.focus();
      setInputValue(priceInput, explicitLimitPrice);
      await sleep(150);
      return { ok: true, usedMid: false, priceInput };
    }

    const midButton = findLimitMidButton();
    if (!midButton) {
      return { ok: false, usedMid: false, priceInput, error: "Could not find Mid button for Variational limit order." };
    }
    // Clear existing price so waitForPriceInput waits for the mid button to populate a fresh value
    const existingPriceInput = priceInput || findLimitPriceInput();
    if (existingPriceInput && existingPriceInput.value) {
      setInputValue(existingPriceInput, "");
      await sleep(80);
    }
    click(midButton);
    let populatedInput = await waitForPriceInput(3000);
    let priceVal = populatedInput ? String(populatedInput.value || "").replace(/,/g, "").trim() : "";
    if (!priceVal || Number(priceVal) <= 0) {
      // Retry: clear and click Mid again, wait longer
      const retryInput = populatedInput || findLimitPriceInput();
      if (retryInput && retryInput.value) {
        setInputValue(retryInput, "");
        await sleep(80);
      }
      click(midButton);
      populatedInput = await waitForPriceInput(6000);
      priceVal = populatedInput ? String(populatedInput.value || "").replace(/,/g, "").trim() : "";
    }
    if (!priceVal || Number(priceVal) <= 0) {
      return { ok: false, usedMid: true, priceInput: populatedInput, error: "Mid button clicked but price input did not populate within 9s." };
    }
    return { ok: true, usedMid: true, priceInput: populatedInput };
  }

  async function retryLimitMidAfterDisabledSubmit(side) {
    const priceResult = await setLimitPriceOrClickMid("");
    if (!priceResult.ok) {
      return {
        button: null,
        disabledButton: null,
        clickedMidAfterDisabledSubmit: false,
        error: priceResult.error,
      };
    }
    await sleep(250);
    const retryMidAfterDisabledSubmit = await waitForEnabledSubmitButton(side, 5000);
    return {
      ...retryMidAfterDisabledSubmit,
      clickedMidAfterDisabledSubmit: true,
    };
  }

  async function retryLimitMidAfterUnavailableSubmit(side) {
    const priceResult = await setLimitPriceOrClickMid("");
    if (!priceResult.ok) {
      return {
        button: null,
        disabledButton: null,
        unavailableButton: null,
        clickedMidAfterQuotedPriceUnavailable: false,
        error: priceResult.error,
      };
    }
    await sleep(750);
    const retryMidAfterUnavailableSubmit = await waitForEnabledSubmitButton(side, 7000);
    return {
      ...retryMidAfterUnavailableSubmit,
      clickedMidAfterQuotedPriceUnavailable: true,
    };
  }

  async function selectOrderType(orderType) {
    const button = findOrderTypeButton(orderType);
    if (!button) {
      return false;
    }
    if (!button.disabled && button.getAttribute("aria-disabled") !== "true") {
      click(button);
      if (orderType === "LIMIT") {
        // Wait for the limit-price-input to appear in the DOM (up to 2s)
        const deadline = Date.now() + 2000;
        while (Date.now() < deadline) {
          if (findLimitPriceInput()) break;
          await sleep(80);
        }
      } else {
        await sleep(150);
      }
    }
    return true;
  }

  function isRejectedTradeButtonText(text) {
    const normalized = text.toLowerCase().replace(/\s+/g, " ").trim();
    if (/^(buy|sell)\s+\$?[0-9][0-9,]*(?:\.[0-9]+)?(?:\s+\1\s+\$?[0-9][0-9,]*(?:\.[0-9]+)?)?$/.test(normalized)) {
      return true;
    }
    return [
      "order history",
      "trade history",
      "position history",
      "transaction history",
      "history",
      "orders",
      "open orders",
      "quoted price unavailable",
      "price unavailable"
    ].some((item) => normalized.includes(item));
  }

  function findQuotedPriceUnavailableButton() {
    return Array.from(document.querySelectorAll('button[data-testid="submit-button"]'))
      .filter(visible)
      .find((el) => /\bquoted\s+price\s+unavailable\b/i.test(textOf(el))) || null;
  }

  function submitButtonLooksLikeOrderAction(el, side) {
    const normalized = textOf(el).toLowerCase().replace(/\s+/g, " ").trim();
    const currentSymbol = currentVariationalSymbol().toLowerCase();
    const sideWord = side === "BUY" ? "buy" : "sell";
    if (!normalized.includes(sideWord) && !(side === "BUY" ? /\blong\b/.test(normalized) : /\bshort\b/.test(normalized))) {
      return false;
    }
    return (
      (currentSymbol && normalized.includes(currentSymbol))
      || /\bplace\b/.test(normalized)
      || /\bsubmit\b/.test(normalized)
      || /\bconfirm\b/.test(normalized)
      || /\border\b/.test(normalized)
    );
  }

  function findSubmitButton(side) {
    const explicitSubmitButtons = Array.from(document.querySelectorAll('button[data-testid="submit-button"]'))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")
      .filter((el) => !isRejectedTradeButtonText(textOf(el)));
    const sidePatterns = side === "BUY"
      ? [/\bplace\s+buy\b/i, /\bbuy\b/i, /\blong\b/i]
      : [/\bplace\s+sell\b/i, /\bsell\b/i, /\bshort\b/i];
    const explicitSideMatch = explicitSubmitButtons.find((el) => submitButtonLooksLikeOrderAction(el, side))
      || explicitSubmitButtons.find((el) => sidePatterns.some((pattern) => pattern.test(textOf(el))));
    if (explicitSideMatch) {
      return explicitSideMatch;
    }
    if (explicitSubmitButtons.length === 1) {
      return explicitSubmitButtons[0];
    }

    const candidates = Array.from(document.querySelectorAll(clickableSelector))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")
      .filter((el) => !isRejectedTradeButtonText(textOf(el)));
    const actionPatterns = [
      /\bplace\s+order\b/i,
      /\bplace\s+(buy|sell|long|short)\b/i,
      /\bsubmit\b/i,
      /\bconfirm\b/i
    ];
    return candidates.find((el) => sidePatterns.some((pattern) => pattern.test(textOf(el))))
      || candidates.find((el) => actionPatterns.some((pattern) => pattern.test(textOf(el))));
  }

  async function waitForEnabledSubmitButton(side, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    let disabledSubmitButton = null;
    let unavailableSubmitButton = null;
    while (Date.now() <= deadline) {
      const unavailableButton = findQuotedPriceUnavailableButton();
      if (unavailableButton) {
        unavailableSubmitButton = unavailableButton;
      }
      const submitButton = findSubmitButton(side);
      if (submitButton) {
        return { button: submitButton, disabledButton: disabledSubmitButton, unavailableButton: null };
      }
      disabledSubmitButton = Array.from(document.querySelectorAll('button[data-testid="submit-button"]'))
        .filter(visible)
        .filter((el) => !isRejectedTradeButtonText(textOf(el)))[0] || disabledSubmitButton;
      await sleep(100);
    }
    return {
      button: null,
      disabledButton: disabledSubmitButton || unavailableSubmitButton,
      unavailableButton: unavailableSubmitButton
    };
  }

  function findAmountInput(orderType, excludedInput = null) {
    // Try exact data-testid first — skip strict visibility so a transitioning Market tab doesn't block us
    for (const sel of ['input[data-testid="quantity-input"]', 'textarea[data-testid="quantity-input"]']) {
      const el = document.querySelector(sel);
      if (el && el !== excludedInput && !el.disabled && el.getAttribute("aria-disabled") !== "true") {
        const style = window.getComputedStyle(el);
        if (style.visibility !== "hidden" && style.display !== "none") {
          return el;
        }
      }
    }

    const allInputs = Array.from(document.querySelectorAll("input,textarea"))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")
      .filter((el) => el !== excludedInput)
      .filter((el) => !(orderType === "LIMIT" && isLimitPriceInput(el)));
    const exactSelectors = [
      'input[data-testid="quantity-input"]',
      'textarea[data-testid="quantity-input"]',
      "input[placeholder*='Amount' i]",
      "input[placeholder*='Size' i]",
      "input[placeholder*='Quantity' i]",
    ];
    for (const selector of exactSelectors) {
      const input = Array.from(document.querySelectorAll(selector))
        .filter((el) => allInputs.includes(el))[0];
      if (input) {
        return input;
      }
    }
    const amountPatterns = [/\bamount\b/i, /\bsize\b/i, /\bquantity\b/i, /\bqty\b/i];
    const contextMatch = allInputs.find((el) => {
      const localText = inputContextText(el);
      return amountPatterns.some((pattern) => pattern.test(localText));
    });
    if (contextMatch) {
      return contextMatch;
    }
    return allInputs.find((el) => el.getAttribute("inputmode") === "decimal")
      || allInputs.find((el) => el.getAttribute("type") === "number")
      || allInputs[0]
      || null;
  }

  function collectOrderDomDiagnostics() {
    const buttons = Array.from(document.querySelectorAll(clickableSelector))
      .filter(visible)
      .slice(0, 120)
      .map((el) => ({
        text: textOf(el).replace(/\s+/g, " ").trim().slice(0, 120),
        disabled: Boolean(el.disabled) || el.getAttribute("aria-disabled") === "true",
        className: String(el.className || "").slice(0, 120)
      }));
    const inputs = Array.from(document.querySelectorAll("input,textarea"))
      .filter(visible)
      .slice(0, 20)
      .map((el) => ({
        placeholder: String(el.getAttribute("placeholder") || "").slice(0, 120),
        value: String(el.value || "").slice(0, 40),
        dataTestId: String(el.getAttribute("data-testid") || "").slice(0, 80),
        type: String(el.getAttribute("type") || "").slice(0, 40),
        inputMode: String(el.getAttribute("inputmode") || "").slice(0, 40),
        className: String(el.className || "").slice(0, 120),
        context: inputContextText(el).replace(/\s+/g, " ").trim().slice(0, 160)
      }));
    return {
      url: window.location.href,
      title: document.title,
      buttons,
      inputs
    };
  }

  function normalizeVariationalSymbol(value) {
    return String(value || "")
      .trim()
      .toUpperCase()
      .replace(/[-_\s]?PERP$/i, "")
      .replace(/USDT$/i, "");
  }

  function currentVariationalSymbol() {
    try {
      const url = new URL(window.location.href);
      const parts = url.pathname.split("/").filter(Boolean);
      const perpetualIndex = parts.findIndex((part) => part.toLowerCase() === "perpetual");
      if (perpetualIndex >= 0 && parts[perpetualIndex + 1]) {
        return normalizeVariationalSymbol(decodeURIComponent(parts[perpetualIndex + 1]));
      }
    } catch {
      // Fall back to document title below.
    }

    const titleMatch = String(document.title || "").match(/\b([A-Z0-9]+)\s*[-_\s]?\s*PERP\b/i);
    return titleMatch ? normalizeVariationalSymbol(titleMatch[1]) : "";
  }

  return (async () => {
    const side = String(command.side || "").toUpperCase();
    const amount = String(command.amount || "").trim();
    const orderType = String(command.orderType || "MARKET").toUpperCase();
    const explicitLimitPrice = String(command.price || "").trim();
    const reduceOnly = Boolean(command.reduceOnly);
    const requestedSymbol = normalizeVariationalSymbol(command.symbol || command.market);
    const currentSymbol = currentVariationalSymbol();
    if (!["BUY", "SELL"].includes(side)) {
      return { ok: false, error: `Invalid side: ${command.side}` };
    }
    if (!["MARKET", "LIMIT"].includes(orderType)) {
      return { ok: false, error: `Invalid orderType: ${command.orderType}` };
    }
    if (!amount || Number(amount) <= 0) {
      return { ok: false, error: `Invalid amount: ${amount}` };
    }
    if (orderType === "LIMIT" && explicitLimitPrice && Number(explicitLimitPrice) <= 0) {
      return { ok: false, error: `Invalid limit price: ${explicitLimitPrice}` };
    }
    if (!requestedSymbol || !currentSymbol || requestedSymbol !== currentSymbol) {
      return {
        ok: false,
        error: `Ticker mismatch: requested ${requestedSymbol || "unknown"} but current page is ${currentSymbol || "unknown"}.`,
        details: {
          automationVersion,
          requestedSymbol,
          currentSymbol,
          diagnostics: collectOrderDomDiagnostics()
        }
      };
    }

    // submitOnly: page state from prepareOnly may have reset, so redo the full market order flow
    if (command.submitOnly) {
      await selectOrderType("MARKET"); // MARKET button may be absent if already active — that's OK
      const reduceOnlyResult = await ensureReduceOnly(reduceOnly);
      if (!reduceOnlyResult.ok) {
        return { ok: false, error: reduceOnlyResult.error, details: { automationVersion, reduceOnly, diagnostics: collectOrderDomDiagnostics() } };
      }
      const sideBtn = side === "BUY"
        ? findButton([/\bbuy\b/i, /\blong\b/i])
        : findButton([/\bsell\b/i, /\bshort\b/i]);
      if (!sideBtn) {
        return { ok: false, error: `Could not find ${side} button on Variational page (submitOnly).`, details: { automationVersion, diagnostics: collectOrderDomDiagnostics() } };
      }
      click(sideBtn);
      await sleep(150);
      const amountInput = findAmountInput("MARKET", null);
      if (!amountInput) {
        return { ok: false, error: "Could not find amount input on Variational page (submitOnly).", details: { automationVersion, diagnostics: collectOrderDomDiagnostics() } };
      }
      amountInput.focus();
      setInputValue(amountInput, amount);
      await sleep(300);
      const { button: submitButton, disabledButton, unavailableButton } = await waitForEnabledSubmitButton(side, Number(command.submitEnableTimeoutMs || 5000));
      if (!submitButton) {
        if (unavailableButton) {
          return { ok: false, error: "Quoted Price unavailable.", details: { automationVersion, amount, clickedSubmitText: textOf(unavailableButton), diagnostics: collectOrderDomDiagnostics() } };
        }
        if (disabledButton) {
          return { ok: false, error: "Submit button stayed disabled (submitOnly).", details: { automationVersion, amount, clickedSubmitText: textOf(disabledButton), diagnostics: collectOrderDomDiagnostics() } };
        }
        return { ok: false, error: "Could not find submit/order button on Variational page.", details: { automationVersion, diagnostics: collectOrderDomDiagnostics() } };
      }
      const submitClickPoint = cdpClickPoint(submitButton);
      click(submitButton);
      return {
        ok: true,
        details: {
          automationVersion,
          side,
          orderType,
          amount,
          submitOnly: true,
          reduceOnly,
          clickedSubmitText: textOf(submitButton),
          needsCdpSubmitClick: true,
          clickedViaPageGesture: true,
          submitSelector: 'button[data-testid="submit-button"]',
          submitClickPoint
        }
      };
    }

    const selectedOrderType = await selectOrderType(orderType);
    if (!selectedOrderType) {
      if (orderType === "LIMIT") {
        return {
          ok: false,
          error: `Could not find ${orderType} order type button on Variational page.`,
          details: { automationVersion, orderType, diagnostics: collectOrderDomDiagnostics() }
        };
      }
      // MARKET: button may be absent because Market is already the active order type — continue
    }

    let excludedAmountInput = null;
    if (orderType === "LIMIT") {
      const priceResult = await setLimitPriceOrClickMid(explicitLimitPrice);
      if (!priceResult.ok) {
        return {
          ok: false,
          error: priceResult.error,
          details: {
            automationVersion,
            orderType,
            explicitLimitPrice,
            diagnostics: collectOrderDomDiagnostics()
          }
        };
      }
      excludedAmountInput = priceResult.priceInput || null;
    }

    if (command.previewOnly) {
      const selectedPrice = readLimitPriceValue(excludedAmountInput || findLimitPriceInput());
      if (!selectedPrice || Number(selectedPrice) <= 0) {
        return {
          ok: false,
          error: "Could not read Variational limit price after Mid click.",
          details: { automationVersion, previewOnly: true, diagnostics: collectOrderDomDiagnostics() }
        };
      }
      return {
        ok: true,
        price: selectedPrice,
        details: {
          automationVersion,
          previewOnly: true,
          orderType,
          explicitLimitPrice: explicitLimitPrice || null
        }
      };
    }

    const reduceOnlyResult = await ensureReduceOnly(reduceOnly);
    if (!reduceOnlyResult.ok) {
      return {
        ok: false,
        error: reduceOnlyResult.error,
        details: { automationVersion, reduceOnly, diagnostics: collectOrderDomDiagnostics() }
      };
    }

    const sideButton = side === "BUY"
      ? findButton([/\bbuy\b/i, /\blong\b/i])
      : findButton([/\bsell\b/i, /\bshort\b/i]);
    if (!sideButton) {
      return {
        ok: false,
        error: `Could not find ${side} button on Variational page.`,
        details: { automationVersion, diagnostics: collectOrderDomDiagnostics() }
      };
    }
    click(sideButton);
    await sleep(150);

    // prepareOnly (MARKET taker pre-stage): side is selected, no amount input needed — submit triggered separately
    if (command.prepareOnly && orderType === "MARKET") {
      return { ok: true, prepared: true, details: { automationVersion, side, orderType, reduceOnly } };
    }

    const amountInput = findAmountInput(orderType, excludedAmountInput);
    if (!amountInput) {
      return {
        ok: false,
        error: "Could not find amount input on Variational page.",
        details: { automationVersion, diagnostics: collectOrderDomDiagnostics() }
      };
    }
    amountInput.focus();
    setInputValue(amountInput, amount);
    await sleep(150);

    // Variational only ENABLES the submit button when the limit price is committed
    // through its own "Mid" control — a programmatic value injection into the price
    // input isn't recognized by the ticket's validation. After the price + size are
    // entered, click Mid once more so the submit button becomes enabled.
    if (orderType === "LIMIT") {
      const midButton = findLimitMidButton();
      if (midButton) {
        click(midButton);
        await sleep(300);
      }
    }

    const { button: submitButton, disabledButton, unavailableButton } = await waitForEnabledSubmitButton(
      side,
      Number(command.submitEnableTimeoutMs || 5000)
    );
    let finalSubmitButton = submitButton;
    let finalDisabledButton = disabledButton;
    let finalUnavailableButton = unavailableButton;
    let clickedMidAfterDisabledSubmit = false;
    let clickedMidAfterQuotedPriceUnavailable = false;
    if (!finalSubmitButton && finalUnavailableButton && orderType === "LIMIT") {
      const retryResult = await retryLimitMidAfterUnavailableSubmit(side);
      if (retryResult.error) {
        return {
          ok: false,
          error: retryResult.error,
          details: {
            automationVersion,
            amount,
            clickedMidAfterQuotedPriceUnavailable,
            clickedSubmitText: textOf(finalUnavailableButton),
            diagnostics: collectOrderDomDiagnostics()
          }
        };
      }
      finalSubmitButton = retryResult.button;
      finalDisabledButton = retryResult.disabledButton || finalDisabledButton;
      finalUnavailableButton = retryResult.unavailableButton;
      clickedMidAfterQuotedPriceUnavailable = Boolean(retryResult.clickedMidAfterQuotedPriceUnavailable);
    }
    if (!finalSubmitButton && finalUnavailableButton) {
      return {
        ok: false,
        error: "Quoted Price unavailable.",
        details: {
          automationVersion,
          amount,
          clickedMidAfterQuotedPriceUnavailable,
          clickedSubmitText: textOf(finalUnavailableButton),
          diagnostics: collectOrderDomDiagnostics()
        }
      };
    }
    if (!finalSubmitButton && finalDisabledButton && orderType === "LIMIT") {
      const retryResult = await retryLimitMidAfterDisabledSubmit(side);
      if (retryResult.unavailableButton) {
        const unavailableRetryResult = await retryLimitMidAfterUnavailableSubmit(side);
        clickedMidAfterQuotedPriceUnavailable = Boolean(unavailableRetryResult.clickedMidAfterQuotedPriceUnavailable);
        if (unavailableRetryResult.error) {
          return {
            ok: false,
            error: unavailableRetryResult.error,
            details: {
              automationVersion,
              amount,
              clickedMidAfterDisabledSubmit,
              clickedMidAfterQuotedPriceUnavailable,
              clickedSubmitText: textOf(retryResult.unavailableButton),
              diagnostics: collectOrderDomDiagnostics()
            }
          };
        }
        finalSubmitButton = unavailableRetryResult.button;
        finalDisabledButton = unavailableRetryResult.disabledButton || finalDisabledButton;
        finalUnavailableButton = unavailableRetryResult.unavailableButton;
      }
      if (!finalSubmitButton && finalUnavailableButton) {
        return {
          ok: false,
          error: "Quoted Price unavailable.",
          details: {
            automationVersion,
            amount,
            clickedMidAfterDisabledSubmit,
            clickedMidAfterQuotedPriceUnavailable,
            clickedSubmitText: textOf(finalUnavailableButton),
            diagnostics: collectOrderDomDiagnostics()
          }
        };
      }
      if (!finalSubmitButton && retryResult.error) {
        return {
          ok: false,
          error: retryResult.error,
          details: {
            automationVersion,
            amount,
            clickedMidAfterDisabledSubmit,
            clickedMidAfterQuotedPriceUnavailable,
            clickedSubmitText: textOf(finalDisabledButton),
            diagnostics: collectOrderDomDiagnostics()
          }
        };
      }
      if (!finalSubmitButton) {
        finalSubmitButton = retryResult.button;
        finalDisabledButton = retryResult.disabledButton || finalDisabledButton;
      }
      clickedMidAfterDisabledSubmit = Boolean(retryResult.clickedMidAfterDisabledSubmit);
    }
    if (!finalSubmitButton) {
      if (finalDisabledButton) {
        return {
          ok: false,
          error: "Submit button stayed disabled after amount input.",
          details: {
            automationVersion,
            amount,
            clickedMidAfterDisabledSubmit,
            clickedMidAfterQuotedPriceUnavailable,
            clickedSubmitText: textOf(finalDisabledButton),
            diagnostics: collectOrderDomDiagnostics()
          }
        };
      }
      return {
        ok: false,
        error: "Could not find submit/order button on Variational page.",
        details: { automationVersion, diagnostics: collectOrderDomDiagnostics() }
      };
    }
    const submitClickPoint = cdpClickPoint(finalSubmitButton);
    click(finalSubmitButton);

    const usedLimitPrice = orderType === "LIMIT"
      ? (readLimitPriceValue(excludedAmountInput || findLimitPriceInput()) || null)
      : null;

    return {
      ok: true,
      details: {
        automationVersion,
        side,
        orderType,
        amount,
        reduceOnly,
        explicitLimitPrice: explicitLimitPrice || null,
        usedLimitPrice,
        clickedMidAfterDisabledSubmit,
        clickedMidAfterQuotedPriceUnavailable,
        market: command.market || null,
        clickedSubmitText: textOf(finalSubmitButton),
        needsCdpSubmitClick: true,
        clickedViaPageGesture: true,
        submitSelector: 'button[data-testid="submit-button"]',
        submitClickPoint
      }
    };
  })();
}

function autoReloadAttachedTab(reason) {
  if (!state.active || state.attachedTabId == null) {
    return;
  }
  const now = Date.now();
  if (now - state.lastAutoReloadAt < AUTO_RELOAD_COOLDOWN_MS) {
    return;
  }
  state.lastAutoReloadAt = now;

  chrome.tabs.reload(state.attachedTabId, {}, () => {
    const err = chrome.runtime.lastError;
    if (err) {
      state.lastError = `Auto reload failed (${reason}): ${err.message}`;
    } else {
      state.lastError = null;
    }
    notifyStatus();
  });
}

async function ensureConfigLoaded() {
  if (state.configLoaded) {
    return;
  }
  const stored = await chrome.storage.local.get("forwarderConfig");
  state.config = sanitizeConfig(stored.forwarderConfig);
  state.configLoaded = true;
}

function sanitizeConfig(incoming = {}) {
  return {
    wsEndpoint: asStringOrDefault(incoming.wsEndpoint, DEFAULT_CONFIG.wsEndpoint),
    restEndpoint: asStringOrDefault(incoming.restEndpoint, DEFAULT_CONFIG.restEndpoint),
    commandEndpoint: asStringOrDefault(incoming.commandEndpoint, DEFAULT_CONFIG.commandEndpoint),
    domainFilter: asStringOrDefault(incoming.domainFilter, DEFAULT_CONFIG.domainFilter),
    restAllowlist: sanitizeRestAllowlist(incoming.restAllowlist),
    wsAllowlist: sanitizeAllowlist(incoming.wsAllowlist, DEFAULT_CONFIG.wsAllowlist)
  };
}

function asStringOrDefault(value, fallback) {
  if (typeof value !== "string") {
    return fallback;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function nowIso() {
  return new Date().toISOString();
}

function sanitizeAllowlist(value, fallback) {
  if (!Array.isArray(value)) {
    return [...fallback];
  }
  const cleaned = value
    .filter((item) => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  if (!cleaned.length) {
    return [...fallback];
  }
  return cleaned;
}

function sanitizeRestAllowlist(value) {
  const cleaned = sanitizeAllowlist(value, DEFAULT_CONFIG.restAllowlist);
  const strict = cleaned.filter((item) => item === DEFAULT_CONFIG.restAllowlist[0]);
  if (!strict.length) {
    return [...DEFAULT_CONFIG.restAllowlist];
  }
  return strict;
}

function matchesDomainFilter(url) {
  const filter = state.config.domainFilter.trim().toLowerCase();
  if (!filter) {
    return true;
  }
  return (url || "").toLowerCase().includes(filter);
}

function normalizeUrlParts(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    return {
      originPath: `${parsed.origin}${parsed.pathname}`,
      full: parsed.toString()
    };
  } catch {
    return {
      originPath: rawUrl,
      full: rawUrl
    };
  }
}

function getMatchedRestPattern(url) {
  const patterns = state.config.restAllowlist || [];
  return getMatchedPattern(url, patterns);
}

function getMatchedWsPattern(url) {
  const patterns = state.config.wsAllowlist || [];
  return getMatchedPattern(url, patterns);
}

function getMatchedPattern(url, patterns) {
  if (!patterns.length) {
    return null;
  }

  const target = normalizeUrlParts(url);
  for (const pattern of patterns) {
    const normalizedPattern = normalizeUrlParts(pattern);
    if (target.originPath === normalizedPattern.originPath || target.full.startsWith(pattern)) {
      return pattern;
    }
  }
  return null;
}

async function debuggerAttach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.attach({ tabId }, DEBUGGER_VERSION, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function debuggerDetach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.detach({ tabId }, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function sendDebuggerCommand(tabId, method, params = {}) {
  return new Promise((resolve, reject) => {
    chrome.debugger.sendCommand({ tabId }, method, params, (result) => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve(result || {});
    });
  });
}

async function getActiveTabId() {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tabs.length || tabs[0].id == null) {
    throw new Error("No active tab found.");
  }
  return tabs[0].id;
}

async function startForwarding(tabId = null) {
  await ensureConfigLoaded();

  if (state.active) {
    return getStatus();
  }

  const targetTabId = tabId ?? (await getActiveTabId());
  await debuggerAttach(targetTabId);

  try {
    await sendDebuggerCommand(targetTabId, "Network.enable");
  } catch (error) {
    await debuggerDetach(targetTabId);
    throw error;
  }

  state.active = true;
  state.attachedTabId = targetTabId;
  state.lastError = null;
  wsForwarder.connect();
  restForwarder.connect();
  commandClient.connect();
  autoReloadAttachedTab("forwarder started");
  notifyStatus();
  return getStatus();
}

async function stopForwarding() {
  const attachedTabId = state.attachedTabId;
  cleanupForwardingState();
  if (attachedTabId != null) {
    try {
      await debuggerDetach(attachedTabId);
    } catch (error) {
      state.lastError = `Debugger detach failed: ${error.message}`;
    }
  }
  notifyStatus();
  return getStatus();
}

function cleanupForwardingState() {
  state.active = false;
  state.pendingResponses.clear();
  state.websocketMeta.clear();
  state.attachedTabId = null;
  state.lastAutoReloadAt = 0;
  wsForwarder.close();
  restForwarder.close();
  commandClient.close();
}

function getStatus() {
  return {
    active: state.active,
    attachedTabId: state.attachedTabId,
    config: state.config,
    sockets: {
      websocket: wsForwarder.status,
      rest: restForwarder.status,
      command: commandClient.status
    },
    lastError: state.lastError
  };
}

function notifyStatus() {
  chrome.runtime.sendMessage({ event: "status", status: getStatus() }).catch(() => {
    // No listeners (popup closed), safe to ignore.
  });
}

function trackResponse(params) {
  if (!params?.response?.url || !matchesDomainFilter(params.response.url)) {
    return;
  }
  if (params.type !== "Fetch" && params.type !== "XHR") {
    return;
  }

  const matchedPattern = getMatchedRestPattern(params.response.url);
  if (!matchedPattern) {
    return;
  }

  state.pendingResponses.set(params.requestId, {
    requestId: params.requestId,
    url: params.response.url,
    status: params.response.status,
    statusText: params.response.statusText,
    mimeType: params.response.mimeType,
    headers: params.response.headers,
    type: params.type,
    matchedPattern,
    capturedAt: nowIso()
  });
}

async function forwardResponseBody(requestId, encodedDataLength) {
  const meta = state.pendingResponses.get(requestId);
  if (!meta || state.attachedTabId == null) {
    return;
  }
  state.pendingResponses.delete(requestId);

  try {
    const result = await sendDebuggerCommand(state.attachedTabId, "Network.getResponseBody", { requestId });
    restForwarder.send({
      kind: "rest_response",
      requestId,
      timestamp: nowIso(),
      encodedDataLength,
      ...meta,
      body: result.body ?? "",
      base64Encoded: Boolean(result.base64Encoded)
    });
  } catch (error) {
    restForwarder.send({
      kind: "rest_response_error",
      requestId,
      timestamp: nowIso(),
      ...meta,
      error: error.message
    });
  }
}

function forwardWebSocketFrame(direction, params) {
  const meta = state.websocketMeta.get(params.requestId);
  if (!meta) {
    return;
  }

  wsForwarder.send({
    kind: "ws_frame",
    direction,
    requestId: params.requestId,
    url: meta.url,
    matchedPattern: meta.matchedPattern || "",
    timestamp: nowIso(),
    opcode: params.response?.opcode,
    mask: params.response?.mask,
    payloadData: params.response?.payloadData ?? ""
  });
}

async function handleDebuggerEvent(source, method, params) {
  if (!state.active || source.tabId !== state.attachedTabId) {
    return;
  }

  if (method === "Network.responseReceived") {
    trackResponse(params);
    return;
  }

  if (method === "Network.loadingFinished") {
    await forwardResponseBody(params.requestId, params.encodedDataLength);
    return;
  }

  if (method === "Network.loadingFailed") {
    state.pendingResponses.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketCreated") {
    const matchedPattern = getMatchedWsPattern(params.url);
    if (matchesDomainFilter(params.url) && matchedPattern) {
      state.websocketMeta.set(params.requestId, {
        url: params.url,
        matchedPattern,
        createdAt: nowIso()
      });
    }
    return;
  }

  if (method === "Network.webSocketClosed") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_closed",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso()
    });
    state.websocketMeta.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketFrameReceived") {
    forwardWebSocketFrame("received", params);
    return;
  }

  if (method === "Network.webSocketFrameSent") {
    forwardWebSocketFrame("sent", params);
    return;
  }

  if (method === "Network.webSocketFrameError") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_frame_error",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso(),
      errorMessage: params.errorMessage || "Unknown WebSocket frame error"
    });
  }
}

chrome.debugger.onEvent.addListener((source, method, params) => {
  handleDebuggerEvent(source, method, params).catch((error) => {
    state.lastError = `CDP event handling failed: ${error.message}`;
    notifyStatus();
  });
});

chrome.debugger.onDetach.addListener((source, reason) => {
  if (source.tabId !== state.attachedTabId) {
    return;
  }
  state.lastError = `Debugger detached: ${reason}`;
  cleanupForwardingState();
  notifyStatus();
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    await ensureConfigLoaded();

    if (message.action === "getStatus") {
      return { ok: true, status: getStatus() };
    }

    if (message.action === "updateConfig") {
      state.config = sanitizeConfig(message.config);
      await chrome.storage.local.set({ forwarderConfig: state.config });
      if (state.active) {
        wsForwarder.restart();
        restForwarder.restart();
        commandClient.restart();
      }
      notifyStatus();
      return { ok: true, status: getStatus() };
    }

    if (message.action === "start") {
      const status = await startForwarding(message.tabId ?? null);
      return { ok: true, status };
    }

    if (message.action === "stop") {
      const status = await stopForwarding();
      return { ok: true, status };
    }

    return { ok: false, error: `Unknown action: ${message.action}` };
  })()
    .then((response) => sendResponse(response))
    .catch((error) => sendResponse({ ok: false, error: error.message }));

  return true;
});

chrome.runtime.onInstalled.addListener(() => {
  ensureConfigLoaded().catch(() => {
    // Ignore config load errors during install.
  });
});
