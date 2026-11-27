// UI sends the request. CDP only validates/releases it and observes its reply.
// Never fetch/POST a mutation here, and never infer an RFQ from a similar row.
const ROOT = 'https://omni.variational.io/api';
const id = s => typeof s === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(s);
const number = s => {
  if (typeof s !== 'string' || !/^(0|[1-9][0-9]*)(\.[0-9]+)?$/.test(s)) return null;
  return s.replace(/\.0*$/, '').replace(/(\.[0-9]*?)0+$/, '$1');
};
export function validateUiMutation(command, request) {
  const expected = command.action === 'submit' ? '/orders/new/limit' : '/orders/cancel';
  if (request.method !== 'POST' || request.url !== ROOT + expected) throw Error('native_ui_unexpected_mutation');
  const headers = Object.entries(request.headers || {});
  const account = headers.find(([key]) => key.toLowerCase() === 'vr-connected-address')?.[1];
  if (account && account.toLowerCase() !== command.account) throw Error('native_ui_account_changed');
  let body;
  try { body = JSON.parse(request.postData); } catch { throw Error('native_ui_request_shape'); }
  if (command.action === 'cancel') {
    if (!id(command.order_id) || body.rfq_id !== command.order_id) throw Error('native_ui_cancel_identity');
    return;
  }
  const p = command.order;
  if (!p || !number(p.qty) || !number(p.limit_price) ||
      number(String(body.qty)) !== number(p.qty) || body.side !== p.side ||
      ['underlying', 'instrument_type', 'settlement_asset', 'funding_interval_s'].some(k => body.instrument?.[k] !== p.instrument?.[k]) ||
      (body.is_reduce_only ?? false) !== p.is_reduce_only ||
      (body.is_auto_resize ?? false) !== false || (body.use_mark_price ?? false) !== false) {
    throw Error('native_ui_order_mismatch');
  }
  if (number(String(body.limit_price)) !== number(p.limit_price)) throw Error('native_ui_mid_changed');
}

export async function runNativeUiMutation(command, { tabId, send, listen, identity, operate, waitMs = 5000 }) {
  const started = Date.now();
  let dispatched = false, admitted = false, networkId = null, response = null, completed = false;
  let resolve;
  const observed = new Promise(r => { resolve = r; });
  const finish = value => { if (!completed) { completed = true; resolve(value); } };
  const pending = new Set();
  const source = ROOT + (command.action === 'submit' ? '/orders/new/limit' : '/orders/cancel');
  const base = () => ({ protocol: 'variational-native-v1', started_ms: started, finished_ms: Date.now(),
    account: command.account, session_continuity: false, dispatched, transport: 'ui', source });
  const listener = async (origin, method, params) => {
    if (origin.tabId !== tabId) return;
    try {
      if (method === 'Fetch.requestPaused') {
        pending.add(params.requestId);
        if (params.request.method === 'GET') {
          await send('Fetch.continueRequest', { requestId: params.requestId });
          pending.delete(params.requestId); return;
        }
        try {
          if (admitted || completed) throw Error('native_ui_duplicate_blocked');
          validateUiMutation(command, params.request);
          if (!params.networkId) throw Error('native_ui_network_identity_missing');
        } catch (e) {
          await send('Fetch.failRequest', { requestId: params.requestId, errorReason: 'Aborted' });
          pending.delete(params.requestId);
          finish({ ok: false, error: e.message }); return;
        }
        // Reserve before the await: two paused requests cannot both pass.
        admitted = true; dispatched = true; networkId = params.networkId;
        await send('Fetch.continueRequest', { requestId: params.requestId });
        pending.delete(params.requestId);
      } else if (method === 'Network.responseReceived' && params.requestId === networkId) {
        if (params.response.url !== source) throw Error('native_ui_response_source');
        response = params.response;
      } else if (method === 'Network.loadingFailed' && params.requestId === networkId) {
        finish({ ok: false, error: 'native_ui_network_unknown' });
      } else if (method === 'Network.loadingFinished' && params.requestId === networkId) {
        if (!response || params.encodedDataLength > 1048576) throw Error('native_ui_response_unknown');
        if (response.status < 200 || response.status >= 300) {
          finish({ ok: false, error: `native_ui_http_${response.status}` }); return;
        }
        if (command.action === 'cancel') { finish({ ok: true, rfq_id: command.order_id }); return; }
        const wire = await send('Network.getResponseBody', { requestId: networkId });
        if (wire.base64Encoded || typeof wire.body !== 'string' || wire.body.length > 1048576) throw Error('native_ui_response_shape');
        const ack = JSON.parse(wire.body);
        if (!id(ack?.rfq_id)) throw Error('native_ui_rfq_missing');
        finish({ ok: true, rfq_id: ack.rfq_id });
      }
    } catch { finish({ ok: false, error: 'native_ui_observation_unknown' }); }
  };
  let remove = () => {};
  let timer;
  try {
    const before = await identity();
    if (!before.ok || before.account !== command.account) throw Error('native_ui_identity_failed');
    remove = listen(listener);
    await send('Fetch.enable', { patterns: [{ urlPattern: ROOT + '/orders/*', requestStage: 'Request' }] });
    let result;
    try { result = await operate(); } catch { result = { ok: false, error: 'native_ui_injection_unknown' }; }
    if (!admitted && /^native_ui_[a-z_]+$/.test(result?.error || '')) {
      finish({ ok: false, error: result.error });
    }
    const observation = await Promise.race([observed, new Promise(r => {
      timer = setTimeout(() => r({ ok: false, error: 'native_ui_ack_missing' }), waitMs);
    })]);
    const after = await identity();
    const continuity = after.ok && after.account === command.account;
    // UI row absence is never proof of no fill; a correlated network ACK wins.
    return { ...base(), ...observation, ok: observation.ok && continuity, session_continuity: continuity,
      ...(observation.ok && !continuity ? { error: 'native_ui_identity_changed' } : {}),
      ...(!observation.ok && result?.error ? { ui_error: String(result.error).slice(0, 300) } : {}) };
  } catch (error) {
    return { ...base(), ok: false, error: /^native_ui_[a-z_]+$/.test(error.message) ? error.message : 'native_ui_unknown' };
  } finally {
    clearTimeout(timer);
    completed = true;
    // Abort paused requests before disabling interception; do not release extras.
    for (const requestId of pending) {
      try { await send('Fetch.failRequest', { requestId, errorReason: 'Aborted' }); } catch {}
    }
    try { await send('Fetch.disable', {}); } catch {}
    remove();
  }
}
