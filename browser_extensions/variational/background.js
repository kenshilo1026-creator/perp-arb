const DEBUGGER_VERSION = "1.3";
const ORDER_AUTOMATION_VERSION = "variational-order-automation-2026-06-05-2";
const MAX_QUEUE_SIZE = 1000;
const AUTO_RELOAD_COOLDOWN_MS = 5000;

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

async function runVariationalOrderInjection(payload) {
  let lastError = null;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const [injectionResult] = await chrome.scripting.executeScript({
        target: { tabId: state.attachedTabId },
        func: executeVariationalOrder,
        args: [{ ...payload, automationVersion: ORDER_AUTOMATION_VERSION }]
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
  if (payload?.type !== "PLACE_ORDER") {
    return;
  }

  const requestId = payload.requestId;
  try {
    if (state.attachedTabId == null) {
      throw new Error("No Variational tab attached. Click Start in the extension popup first.");
    }
    const injectionResult = await runVariationalOrderInjection(payload);
    let result = injectionResult?.result || {};
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
    el.dispatchEvent(new MouseEvent("mouseover", { bubbles: true }));
    el.dispatchEvent(new MouseEvent("mousedown", { bubbles: true }));
    el.dispatchEvent(new MouseEvent("mouseup", { bubbles: true }));
    el.click();
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
    click(midButton);
    await sleep(150);
    return { ok: true, usedMid: true, priceInput: priceInput || findLimitPriceInput() };
  }

  async function selectOrderType(orderType) {
    const button = findOrderTypeButton(orderType);
    if (!button) {
      return false;
    }
    if (!button.disabled && button.getAttribute("aria-disabled") !== "true") {
      click(button);
      await sleep(150);
    }
    return true;
  }

  function isRejectedTradeButtonText(text) {
    const normalized = text.toLowerCase().replace(/\s+/g, " ").trim();
    return [
      "order history",
      "trade history",
      "position history",
      "transaction history",
      "history",
      "orders",
      "open orders"
    ].some((item) => normalized.includes(item));
  }

  function findSubmitButton(side) {
    const explicitSubmitButtons = Array.from(document.querySelectorAll('button[data-testid="submit-button"]'))
      .filter(visible)
      .filter((el) => !el.disabled && el.getAttribute("aria-disabled") !== "true")
      .filter((el) => !isRejectedTradeButtonText(textOf(el)));
    const sidePatterns = side === "BUY"
      ? [/\bplace\s+buy\b/i, /\bbuy\b/i, /\blong\b/i]
      : [/\bplace\s+sell\b/i, /\bsell\b/i, /\bshort\b/i];
    const explicitSideMatch = explicitSubmitButtons.find((el) => sidePatterns.some((pattern) => pattern.test(textOf(el))));
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
    while (Date.now() <= deadline) {
      const submitButton = findSubmitButton(side);
      if (submitButton) {
        return { button: submitButton, disabledButton: disabledSubmitButton };
      }
      disabledSubmitButton = Array.from(document.querySelectorAll('button[data-testid="submit-button"]'))
        .filter(visible)
        .filter((el) => !isRejectedTradeButtonText(textOf(el)))[0] || disabledSubmitButton;
      await sleep(100);
    }
    return { button: null, disabledButton: disabledSubmitButton };
  }

  function findAmountInput(orderType, excludedInput = null) {
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

    const selectedOrderType = await selectOrderType(orderType);
    if (!selectedOrderType) {
      return {
        ok: false,
        error: `Could not find ${orderType} order type button on Variational page.`,
        details: { automationVersion, orderType, diagnostics: collectOrderDomDiagnostics() }
      };
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

    const { button: submitButton, disabledButton } = await waitForEnabledSubmitButton(
      side,
      Number(command.submitEnableTimeoutMs || 3000)
    );
    if (!submitButton) {
      if (disabledButton) {
        return {
          ok: false,
          error: "Submit button stayed disabled after amount input.",
          details: {
            automationVersion,
            amount,
            clickedSubmitText: textOf(disabledButton),
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
    click(submitButton);
    await sleep(Number(command.timeoutMs || 1500));

      return {
        ok: true,
        details: {
          automationVersion,
          side,
          orderType,
          amount,
          explicitLimitPrice: explicitLimitPrice || null,
        market: command.market || null,
        clickedSubmitText: textOf(submitButton)
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
