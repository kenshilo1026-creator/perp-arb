import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';

// Import the extension module without changing the repository's package type.
const text = await readFile(new URL('../browser_extensions/variational/native_orders.js', import.meta.url), 'utf8');
const { executeNativeOrderRequest: run, reserveNativeDispatch: reserve, saveNativeReceipt: save, readNativeReceipt: recover } = await import(`data:text/javascript;base64,${Buffer.from(text).toString('base64')}`);
test('popup starts read-only and only persists an explicitly checked native trading option', async () => {
  const source = await readFile(new URL('../browser_extensions/variational/popup.js', import.meta.url), 'utf8');
  const elements = new Map();
  const calls = [];
  const status = { active: false, config: { nativeTradingEnabled: false, restAllowlist: [] }, sockets: {} };
  const context = vm.createContext({
    document: { getElementById(id) {
      if (!elements.has(id)) elements.set(id, { value: '', checked: false, addEventListener(event, listener) { this[event] = listener; } });
      return elements.get(id);
    } },
    chrome: { runtime: { onMessage: { addListener() {} }, async sendMessage(message) {
      calls.push(message);
      if (message.action === 'updateConfig') status.config = message.config;
      return { ok: true, status };
    } } }
  });
  vm.runInContext(source, context);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(elements.get('nativeTradingEnabled').checked, false);
  await elements.get('start').click();
  assert.equal(calls.find(c => c.action === 'updateConfig').config.nativeTradingEnabled, false);
  elements.get('nativeTradingEnabled').checked = true;
  await elements.get('saveConfig').click();
  assert.equal(calls.filter(c => c.action === 'updateConfig').at(-1).config.nativeTradingEnabled, true);
});
const account = `0x${'a'.repeat(40)}`;
const other = `0x${'b'.repeat(40)}`;
const protocol = 'variational-native-v1';
const instrument = { underlying: 'ETH', instrument_type: 'perpetual_future', settlement_asset: 'USDC', funding_interval_s: 3600 };
const order = { rfq_id: 'rfq-1', instrument, qty: '1', side: 'buy', order_type: 'limit', limit_price: '10', status: 'cleared' };
function token(address) { return `e30.${Buffer.from(JSON.stringify({ address, exp: Math.floor(Date.now() / 1000) + 300 })).toString('base64url')}.signature`; }
function mock(responder) {
  globalThis.location = { origin: 'https://omni.variational.io' };
  const calls = [];
  globalThis.fetch = async (path, options) => {
    calls.push({ path, options });
    const body = await responder(path, options, calls);
    if (body instanceof Response) return body;
    return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json', date: new Date().toUTCString() } });
  };
  return calls;
}
function normal(path) {
  if (path === '/api/me') return { token: token(account), intercomUserJwt: 'NEVER_EXPORT' };
  if (path.startsWith('/api/orders/v2?')) return [order];
  if (path.startsWith('/api/trades?')) return [{ id: 'trade-1', source_rfq: 'rfq-1', instrument, qty: '1', price: '10', side: 'buy', status: 'confirmed', trade_type: 'trade' }];
  throw Error('unexpected route');
}
test('preflight only GETs fixed sources and never exports credentials', async () => {
  const calls = mock((path) => {
    const body = normal(path);
    if (Array.isArray(body)) body[0] = { ...body[0], authorization: 'NEVER_EXPORT', token: 'NEVER_EXPORT' };
    return body;
  });
  const r = await run({ protocol, action: 'preflight' });
  assert.equal(r.ok, true); assert.equal(r.account, account); assert.equal(r.dispatched, false);
  assert.equal(calls.length, 4); assert.ok(calls.every(c => c.options.method === 'GET'));
  assert.ok(!JSON.stringify(r).includes('NEVER_EXPORT'));
  assert.ok(r.orders.rows[0].response_server_ms > 0);
});
test('exact RFQ lookup never falls back to matching symbol', async () => {
  mock(normal);
  const r = await run({ protocol, action: 'lookup', account, order_id: 'another-rfq' });
  assert.equal(r.ok, true); assert.deepEqual(r.orders.rows, []); assert.deepEqual(r.trades.rows, []);
  assert.equal(r.terminal_proven, false);
});
test('wrong account stops before querying or submitting', async () => {
  const calls = mock(normal);
  const r = await run({ protocol, action: 'lookup', account: other, order_id: 'rfq-1' });
  assert.equal(r.error, 'native_account_mismatch'); assert.equal(r.dispatched, false); assert.equal(calls.length, 1);
});
test('session change rejects evidence after the query', async () => {
  let me = 0;
  mock((path) => path === '/api/me' ? { token: token(++me === 1 ? account : other) } : normal(path));
  const r = await run({ protocol, action: 'preflight' });
  assert.equal(r.ok, false); assert.equal(r.session_continuity, false);
});
test('unknown page shapes and HTTP errors are not absence', async () => {
  mock(path => path === '/api/me' ? normal(path) : { items: [] });
  const r = await run({ protocol, action: 'lookup', account, order_id: 'rfq-1' });
  assert.equal(r.ok, false); assert.equal(r.error, 'native_page_shape');
  mock(() => new Response('SECRET', { status: 429 }));
  const limited = await run({ protocol, action: 'preflight' });
  assert.equal(limited.error, 'native_http_429'); assert.ok(!JSON.stringify(limited).includes('SECRET'));
});
test('positive matches stop bounded paging without claiming a complete window', async () => {
  const calls = mock(path => {
    if (path === '/api/me') return normal(path);
    if (path.startsWith('/api/orders')) return Array.from({ length: 100 }, (_, i) => ({ ...order, rfq_id: i === 0 ? 'rfq-1' : `rfq-${i+1}` }));
    return normal(path);
  });
  const r = await run({ protocol, action: 'lookup', account, order_id: 'rfq-1' });
  assert.equal(r.ok, true); assert.equal(r.orders.rows.length, 1); assert.equal(r.orders.window_complete, false);
  assert.equal(calls.length, 4);
});
const payload = { order_type: 'limit', instrument, qty: '1', side: 'buy', limit_price: '10', is_reduce_only: true, is_auto_resize: false, use_mark_price: false, slippage_limit: '0' };
function inventoryResponse(path) {
  if (path === '/api/positions') return [{ position_info: {instrument, qty:'-2',avg_entry_price:'100', token:'NEVER_EXPORT'} }];
  if (path === '/api/portfolio?compute_margin=true') return {balance:'500',sub_accounts:{cross:{balance:'500'},isolated:[]},token:'NEVER_EXPORT'};
  if (path.startsWith('/api/orders')) { assert.ok(path.includes('status=pending')); return []; }
  return normal(path);
}
test('inventory reads positions collateral and unfiltered pending orders twice without mutation or credentials', async () => {
  const calls = mock(inventoryResponse);
  const r = await run({protocol,action:'inventory',account});
  assert.equal(r.ok,true); assert.equal(r.inventory.rounds.length,2);
  assert.equal(r.inventory.rounds[0].positions[0].qty,'-2');
  assert.equal(r.inventory.rounds[1].portfolio.cross_balance,'500');
  assert.equal(r.inventory.rounds[1].orders.window_complete,true);
  assert.equal(calls.length,8); assert.ok(calls.every(c=>c.options.method==='GET'));
  assert.ok(!JSON.stringify(r).includes('NEVER_EXPORT'));
});
test('inventory forbids RFQ filtering and never treats missing coverage as empty', async () => {
  mock(inventoryResponse);
  const filtered = await run({protocol,action:'inventory',account,order_id:'rfq-1'});
  assert.equal(filtered.ok,false);
  mock(path => path==='/api/positions' ? {} : inventoryResponse(path));
  const bad = await run({protocol,action:'inventory',account});
  assert.equal(bad.error,'native_positions_shape');
});
test('inventory rejects ignored pending filter and duplicate order pages', async () => {
  mock(path => path.startsWith('/api/orders') ? [order] : inventoryResponse(path));
  assert.equal((await run({protocol,action:'inventory',account})).error,'native_pending_filter_mismatch');
  mock(path => path.startsWith('/api/orders') ? [{...order,status:'pending'},{...order,status:'pending'}] : inventoryResponse(path));
  assert.equal((await run({protocol,action:'inventory',account})).error,'native_inventory_incomplete');
});
test('direct HTTP submit and cancel are disabled even with valid order parameters', async () => {
  for (const action of ['submit','cancel']) {
    const calls=mock(normal);
    const result=await run({protocol,action,account,order:payload,order_id:'rfq-1'});
    assert.equal(result.ok,false); assert.equal(result.error,'native_ui_required');
    assert.equal(result.dispatched,false); assert.equal(calls.length,0);
  }
});

test('persistent duplicate barrier survives new owner, storage failure blocks dispatch', async () => {
  let data = {};
  const storage = { get: async () => data, set: async value => { data = value; } };
  await reserve(storage, 'request-1');
  await assert.rejects(reserve({ ...storage }, 'request-1'), /duplicate/);
  await assert.rejects(reserve({ get: storage.get, set: async () => { throw Error('disk'); } }, 'request-2'), /disk/);
});
test('lost reply recovers the exact saved RFQ receipt without another HTTP call', async () => {
  let data = {};
  const storage = { get: async () => data, set: async value => { data = value; } };
  await reserve(storage, 'request-saved');
  const command = { requestId: 'request-saved', action: 'submit', account, order: payload };
  await save(storage, command, { rfq_id: 'real-rfq', dispatched: true, ok: true });
  const result = await recover({ ...storage }, 'request-saved');
  assert.equal(result.receipt.rfq_id, 'real-rfq'); assert.deepEqual(result.order, payload);
  assert.equal(await recover(storage, 'missing'), null);
  await assert.rejects(reserve(storage, 'request-saved'), /duplicate/);
  await assert.rejects(save(storage, command, {}), /already_saved/);
});
const background = await readFile(new URL('../browser_extensions/variational/background.js', import.meta.url), 'utf8');
function ownerHarness(inject) {
  const updates = new Set(); const sent = []; let reloads = 0;
  const noop = { addListener() {} };
  const context = vm.createContext({
    setTimeout, clearTimeout, console,
    executeNativeOrderRequest: run, reserveNativeDispatch: reserve, saveNativeReceipt: save, readNativeReceipt: recover,
    chrome: {
      debugger: { onEvent: noop, onDetach: noop },
      runtime: { onMessage: noop, onInstalled: noop, sendMessage: async () => {} },
      scripting: { executeScript: inject },
      tabs: { onUpdated: { addListener: fn => updates.add(fn), removeListener: fn => updates.delete(fn) },
        reload: (_id, _options, cb) => { reloads++; cb(); } }
    }
  });
  vm.runInContext(background.replace(/^import[^\n]+\n/gm, '') + `
    state.active = true; state.attachedTabId = 1;
    commandClient.send = value => globalThis.sent.push(value);
    globalThis.api = { command: handleCommandMessage, reload: autoReloadAttachedTab };
  `, context);
  context.sent = sent;
  return { ...context.api, sent, finishReload: () => { for (const fn of [...updates]) fn(1, { status: 'complete' }); }, reloads: () => reloads };
}
test('owner waits for auto reload completion before injecting a native read', async () => {
  let injections = 0;
  const owner = ownerHarness(async () => { injections++; return [{ result: { ok: true } }]; });
  owner.reload('test');
  const command = owner.command(JSON.stringify({ type: 'NATIVE_ORDER_REQUEST', action: 'preflight', requestId: 'read-1' }));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(injections, 0);
  owner.finishReload(); await command;
  assert.equal(injections, 1); assert.equal(owner.sent[0].ok, true);
});
test('native command owner blocks simultaneous legacy trading and reload', async () => {
  let release;
  const pending = new Promise(resolve => { release = resolve; });
  const owner = ownerHarness(async () => { await pending; return [{ result: { ok: true } }]; });
  const command = owner.command(JSON.stringify({ type: 'NATIVE_ORDER_REQUEST', action: 'preflight', requestId: 'read-1' }));
  await new Promise(resolve => setImmediate(resolve));
  owner.reload('during command');
  await owner.command(JSON.stringify({ type: 'PLACE_ORDER', requestId: 'legacy-1' }));
  assert.equal(owner.reloads(), 0); assert.equal(owner.sent[0].type, 'COMMAND_BUSY');
  release(); await command;
});
test('extension defaults reject native mutations before invoking the page', async () => {
  let injections = 0;
  const owner = ownerHarness(async () => { injections++; return []; });
  await owner.command(JSON.stringify({ type: 'NATIVE_ORDER_REQUEST', action: 'submit', requestId: 'submit-1' }));
  assert.equal(injections, 0); assert.equal(owner.sent[0].ok, false);
});
