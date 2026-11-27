import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
const source = await readFile(new URL('../browser_extensions/variational/ui_orders.js',import.meta.url),'utf8');
const { runNativeUiMutation:run, validateUiMutation:validate } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
const account='0x'+'a'.repeat(40);
const instrument={underlying:'ETH',instrument_type:'perpetual_future',settlement_asset:'USDC',funding_interval_s:3600};
const order={instrument,qty:'1',side:'buy',limit_price:'10',is_reduce_only:true};
const command={action:'submit',account,order};
const url='https://omni.variational.io/api/orders/new/limit';
function request(body=order){return {url,method:'POST',postData:JSON.stringify(body),headers:{'vr-connected-address':account}};}
function harness(c=command,{status=200,body={rfq_id:'real-rfq'},mutate=order,missing=false,duplicate=false,wrongTab=false}={}) {
  let listener,clicks=0;const calls=[];
  const emit=(method,params)=>listener({tabId:wrongTab?2:1},method,params);
  const deps={tabId:1,waitMs:2,identity:async()=>({ok:true,account}),
    listen:fn=>{listener=fn;return ()=>{listener=null;}},
    send:async(method,params)=>{calls.push({method,params});
      if(method==='Network.getResponseBody')return {body:JSON.stringify(body)};
      return {};
    },
    operate:async()=>{clicks++;
      const req=c.action==='cancel'?{url:'https://omni.variational.io/api/orders/cancel',method:'POST',postData:JSON.stringify(mutate)}:request(mutate);
      await emit('Fetch.requestPaused',{requestId:'fetch1',networkId:'net1',request:req});
      if(duplicate)await emit('Fetch.requestPaused',{requestId:'fetch2',networkId:'net2',request:req});
      if(!missing){await emit('Network.responseReceived',{requestId:'net1',response:{url:req.url,status}});
        await emit('Network.loadingFinished',{requestId:'net1',encodedDataLength:100});}
      return {ok:false,error:'UI row absent after immediate fill'};
    }};
  return {deps,calls,clicks:()=>clicks};
}
test('UI click sends once; exact network ACK binds RFQ even if order filled before row appears',async()=>{
  const h=harness();const r=await run(command,h.deps);
  assert.equal(r.ok,true);assert.equal(r.rfq_id,'real-rfq');assert.equal(r.transport,'ui');
  assert.equal(h.clicks(),1);assert.equal(h.calls.filter(c=>c.method==='Fetch.continueRequest').length,1);
  assert.equal(r.trades,undefined);
});
test('mismatched quantity side instrument price reduce-only account and cancel identity never leave browser',async()=>{
  for(const change of [{qty:'2'},{side:'sell'},{instrument:{...instrument,underlying:'BTC'}},{limit_price:'11'},{is_reduce_only:false}]){
    const h=harness(command,{mutate:{...order,...change}});const r=await run(command,h.deps);
    assert.equal(r.ok,false);assert.equal(r.dispatched,false);
    assert.equal(h.calls.filter(c=>c.method==='Fetch.continueRequest').length,0);
    assert.ok(h.calls.some(c=>c.method==='Fetch.failRequest'));
  }
  assert.throws(()=>validate(command,{...request(),headers:{'vr-connected-address':'wrong'}}),/account/);
  assert.throws(()=>validate({action:'cancel',order_id:'ours'}, {url:'https://omni.variational.io/api/orders/cancel',method:'POST',postData:'{"rfq_id":"manual"}'}),/identity/);
});
test('duplicate click is blocked; 400, missing ACK and wrong-tab evidence never become a resting order',async()=>{
  for(const options of [{duplicate:true},{status:400},{missing:true},{wrongTab:true},{body:{id:'trade-is-not-an-rfq'}}]){
    const h=harness(command,options);const r=await run(command,h.deps);
    assert.equal(r.ok,false);assert.equal(r.rfq_id,undefined);assert.equal(h.clicks(),1);
    assert.ok(h.calls.filter(c=>c.method==='Fetch.continueRequest').length<=1);
  }
});
test('UI cancel releases only matching RFQ and does not assert terminal fills',async()=>{
  const c={action:'cancel',account,order_id:'real-rfq'};
  const h=harness(c,{mutate:{rfq_id:'real-rfq'}});const r=await run(c,h.deps);
  assert.equal(r.ok,true);assert.equal(r.rfq_id,'real-rfq');assert.notEqual(r.terminal_proven,true);
});
test('blocked/read-only identity never clicks or sends a mutation',async()=>{
  const h=harness();h.deps.identity=async()=>({ok:false});
  assert.equal((await run(command,h.deps)).ok,false);assert.equal(h.clicks(),0);
});

test('prepared page ticket never clicks until commit; edited input and second commit are rejected',async()=>{
  const background=await readFile(new URL('../browser_extensions/variational/background.js',import.meta.url),'utf8');
  const from=background.indexOf("    if (command.prepareOnly && orderType === 'LIMIT') {");
  const to=background.indexOf('    const submitClickPoint = cdpClickPoint(finalSubmitButton);',from);
  const fn=background.slice(background.indexOf('function executeVariationalOrder(command) {'),background.indexOf('let autoReloadInFlight'));
  for(const change of ['none','quantity','price','detached']){
    let clicks=0;
    const priceInput={value:'10'},amountInput={value:'1',isConnected:true};
    const button={isConnected:true,disabled:false,getAttribute:()=>null};
    const ctx=vm.createContext({window:{location:{href:'https://omni.variational.io/perpetual/ETH'}},
      command:{prepareOnly:true,nativePrepareId:'ticket',symbol:'ETH'},orderType:'LIMIT',side:'BUY',amount:'1',reduceOnly:true,
      excludedAmountInput:priceInput,findLimitPriceInput:()=>priceInput,readLimitPriceValue:p=>p.value,
      amountInput,finalSubmitButton:button,click:()=>{clicks++;}});
    const prepared=vm.runInContext('(function(){'+background.slice(from,to)+'})()',ctx);
    assert.equal(prepared.price,'10');assert.equal(clicks,0);
    if(change==='quantity')amountInput.value='2';
    if(change==='price')priceInput.value='11';
    if(change==='detached')button.isConnected=false;
    vm.runInContext(fn,ctx);
    ctx.next={nativeCommitId:'ticket',symbol:'ETH',side:'BUY',amount:'1',price:'10',reduceOnly:true};
    const result=vm.runInContext('executeVariationalOrder(next)',ctx);
    assert.equal(result.ok,change==='none');assert.equal(clicks,change==='none'?1:0);
    assert.equal(vm.runInContext('executeVariationalOrder(next)',ctx).ok,false);
    assert.equal(clicks,change==='none'?1:0);
  }
});
