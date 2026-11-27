// This function is serialized by chrome.scripting into the attached tab's
// ISOLATED world. Keep it self-contained. Cookies/tokens never leave that world.
// Only fixed same-origin routes are available; no generic HTTP proxy.
export async function executeNativeOrderRequest(command) {
  const protocol = "variational-native-v1";
  const start = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15000);
  let dispatched = false;
  let lastRequest = 0;
  let serverMs = null;
  const output = { protocol, started_ms: start, account: null, session_continuity: false };
  const validId = (s) => typeof s === "string" && /^[A-Za-z0-9_-]{1,128}$/.test(s);
  const address = (s) => typeof s === "string" && /^0x[0-9a-fA-F]{40}$/.test(s);
  const decimal = (s) => typeof s === "string" && /^(0|[1-9][0-9]*)(\.[0-9]+)?$/.test(s) && /[1-9]/.test(s);
  async function request(path, body, account) {
    if (Date.now() - start >= 15000) throw new Error("native_deadline");
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, 500 - (Date.now() - lastRequest))));
    if (controller.signal.aborted) throw new Error("native_deadline");
    lastRequest = Date.now();
    if (body !== undefined) throw new Error("native_ui_required");
    const response = await fetch(`/api${path}`, {
      method: "GET", credentials: "include",
      cache: "no-store", redirect: "error", signal: controller.signal,
      headers: { "content-type": "application/json", ...(account ? { "vr-connected-address": account } : {}) }
    });
    if (!response.ok) throw new Error(`native_http_${response.status}`);
    if (!(response.headers.get("content-type") || "").includes("application/json")) throw new Error("native_non_json");
    const date = Date.parse(response.headers.get("date") || "");
    serverMs = Number.isFinite(date) ? date : null;
    // Bound the stream before decoding, including chunked responses.
    const reader = response.body.getReader();
    const chunks = []; let size = 0;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.length;
        if (size > 1048576) throw new Error("native_response_too_large");
        chunks.push(value);
      }
    } finally { await reader.cancel(); }
    const bytes = new Uint8Array(size); let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
    return JSON.parse(new TextDecoder().decode(bytes));
  }
  async function accountIdentity() {
    const me = await request("/me", undefined, command.account);
    if (typeof me.token !== "string" || me.token.split(".").length !== 3) throw new Error("native_login_required");
    const encoded = me.token.split(".")[1].replaceAll("-", "+").replaceAll("_", "/");
    const claims = JSON.parse(atob(encoded.padEnd(Math.ceil(encoded.length / 4) * 4, "=")));
    // This is a fresh authenticated /me response, not a cached JWT supplied by
    // the caller. No signature claim is made by the local decoder.
    if (!address(claims.address) || !Number.isSafeInteger(claims.exp) || claims.exp * 1000 <= Date.now()) throw new Error("native_account_unavailable");
    const actual = claims.address.toLowerCase();
    if (command.account && actual !== command.account.toLowerCase()) throw new Error("native_account_mismatch");
    return actual;
  }
  const instrumentFields = ["underlying", "instrument_type", "settlement_asset", "funding_interval_s"];
  const orderFields = ["rfq_id", "side", "qty", "order_type", "limit_price", "price", "status", "clearing_status", "is_reduce_only", "is_auto_resize", "use_mark_price", "created_at"];
  const tradeFields = ["id", "source_rfq", "side", "qty", "price", "status", "trade_type", "created_at"];
  function project(row, fields) {
    if (!row || typeof row !== "object" || Array.isArray(row)) throw new Error("native_row_shape");
    const result = {};
    for (const key of fields) {
      const v = row[key];
      if (v === null || ["string", "boolean", "number"].includes(typeof v)) result[key] = v;
    }
    result.instrument = {};
    for (const key of instrumentFields) {
      const v = row.instrument?.[key];
      if (["string", "number"].includes(typeof v)) result.instrument[key] = v;
    }
    return result;
  }
  async function pages(path, fields, idKey, matchKey, pendingOnly = false) {
    const rows = []; const seen = new Set(); let complete = false;
    let duplicate = false; let count = null;
    // Bounded enumeration, not an invented /orders/{id} endpoint. Missing from
    // this window is UNKNOWN. Offset pagination is not an atomic snapshot.
    const maxPages = command.action === "preflight" ? 1 : 5;
    for (let page = 0; page < maxPages; page++) {
      const body = await request(`${path}?limit=100&offset=${page * 100}&order_by=created_at&order=desc${pendingOnly ? "&status=pending" : ""}`, undefined, output.account);
      const batch = Array.isArray(body) ? body : body?.result;
      if (!Array.isArray(batch) || batch.length > 100) throw new Error("native_page_shape");
      const n = body?.pagination?.object_count;
      if (n !== undefined && n !== null && (!Number.isSafeInteger(n) || n < 0)) throw new Error("native_page_count");
      if (count !== null && n !== count) throw new Error("native_pagination_changed");
      if (n !== undefined && n !== null) count = n;
      for (const raw of batch) {
        if (!validId(raw?.[idKey])) throw new Error("native_row_identity_missing");
        if (seen.has(raw[idKey])) duplicate = true;
        seen.add(raw[idKey]);
        if (pendingOnly && raw.status !== "pending") throw new Error("native_pending_filter_mismatch");
        if (!command.order_id || raw[matchKey] === command.order_id) rows.push({ ...project(raw, fields), response_server_ms: serverMs });
      }
      if (batch.length < 100 || (count !== null && (page + 1) * 100 >= count)) { complete = !duplicate; break; }
      // A positive RFQ match is useful without enumerating the entire account.
      // Do not label this as complete, or infer absence/cancel from this window.
      if (command.order_id && rows.length > 0) break;
    }
    return { source: `https://omni.variational.io/api${path}`, rows, window_complete: complete, atomic_snapshot: false, observed_ms: Date.now() };
  }
  async function inventoryRound() {
    const positions = await request("/positions", undefined, output.account);
    if (!Array.isArray(positions) || positions.length > 8192) throw new Error("native_positions_shape");
    const projected = positions.map(row => project(row.position_info, ["qty", "avg_entry_price", "updated_at"]));
    const portfolio = await request("/portfolio?compute_margin=true", undefined, output.account);
    if (!portfolio || typeof portfolio !== "object" || Array.isArray(portfolio)) throw new Error("native_portfolio_shape");
    const sub = portfolio.sub_accounts;
    const cleanPortfolio = {
      balance: typeof portfolio.balance === "string" ? portfolio.balance : null,
      cross_balance: typeof sub?.cross?.balance === "string" ? sub.cross.balance : null,
      isolated: Array.isArray(sub?.isolated) ? sub.isolated.map(row => project(row, ["balance", "allocated_qty"])) : null
    };
    const portfolioServerMs = serverMs;
    const orders = await pages("/orders/v2", orderFields, "rfq_id", "rfq_id", true);
    if (!orders.window_complete) throw new Error("native_inventory_incomplete");
    return { positions: projected, portfolio: cleanPortfolio, orders,
      positions_source: "https://omni.variational.io/api/positions",
      portfolio_source: "https://omni.variational.io/api/portfolio?compute_margin=true",
      portfolio_server_ms: portfolioServerMs, observed_ms: Date.now() };
  }
  try {
    if (location.origin !== "https://omni.variational.io" || command.protocol !== protocol) throw new Error("native_origin_or_protocol");
    if (command.account !== undefined && !address(command.account)) throw new Error("native_invalid_account");
    if (["submit", "cancel"].includes(command.action)) throw new Error("native_ui_required");
    if (!["identity", "inventory", "preflight", "lookup"].includes(command.action)) throw new Error("native_invalid_action");
    if (command.action !== "preflight" && !address(command.account)) throw new Error("native_expected_account_required");
    if (["lookup", "cancel"].includes(command.action) && !validId(command.order_id)) throw new Error("native_exact_rfq_required");
    output.account = await accountIdentity();
    if (command.action === "inventory") {
      if (command.order_id !== undefined) throw new Error("native_inventory_filter_forbidden");
      // Two independent bounded reads, never claim an atomic account snapshot.
      output.inventory = { rounds: [await inventoryRound(), await inventoryRound()] };
    } else if (command.action === "identity") {
      output.source = "https://omni.variational.io/api/me";
    } else {
      output.orders = await pages("/orders/v2", orderFields, "rfq_id", "rfq_id");
      output.trades = await pages("/trades", tradeFields, "id", "source_rfq");
      output.terminal_proven = false;
    }
    output.session_continuity = (await accountIdentity()) === output.account;
    if (!output.session_continuity) throw new Error("native_session_changed");
    return { ...output, ok: true, dispatched, finished_ms: Date.now() };
  } catch (error) {
    // Never expose a response body, exception text from a page, or credentials.
    const safe = /^native_[a-z0-9_]+$/.test(error?.message || "") ? error.message : "native_request_failed";
    return { ...output, ok: false, dispatched, error: safe, finished_ms: Date.now() };
  } finally { clearTimeout(timer); }
}

// Durable extension-side duplicate barrier. A failure after reservation remains
// unknown even after service-worker restart. The caller must reconcile, not retry.
export async function reserveNativeDispatch(storage, requestId) {
  if (!/^[A-Za-z0-9_-]{1,128}$/.test(requestId || "")) throw new Error("native_request_id_invalid");
  const key = "nativeOrderDispatchesV1";
  const data = await storage.get(key);
  const entries = data[key] || {};
  if (Object.hasOwn(entries, requestId)) throw new Error("native_duplicate_dispatch");
  if (Object.keys(entries).length >= 1000) throw new Error("native_dispatch_storage_full");
  await storage.set({ [key]: { ...entries, [requestId]: { reserved_ms: Date.now() } } });
}

export async function saveNativeReceipt(storage, command, receipt) {
  const key = "nativeOrderDispatchesV1";
  const data = await storage.get(key);
  const entries = data[key] || {};
  if (!Object.hasOwn(entries, command.requestId)) throw new Error("native_reservation_missing");
  const entry = entries[command.requestId];
  if (entry.receipt) throw new Error("native_receipt_already_saved");
  await storage.set({ [key]: { ...entries, [command.requestId]: {
    reserved_ms: entry.reserved_ms, action: command.action, account: command.account,
    order: command.order || null, receipt
  } } });
}

export async function readNativeReceipt(storage, requestId) {
  if (!/^[A-Za-z0-9_-]{1,128}$/.test(requestId || "")) throw new Error("native_request_id_invalid");
  const data = await storage.get("nativeOrderDispatchesV1");
  return data.nativeOrderDispatchesV1?.[requestId] || null;
}
