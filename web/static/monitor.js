/* Local, read-only monitor. All display text uses textContent, including venue data. */
(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const names = {aster:'Aster',hyperliquid:'Hyperliquid',lighter:'Lighter',variational:'Variational',entropy:'Entropy',arcus:'Arcus',ondo:'Ondo',binance_spot:'Binance 現貨'};
  const colors = {entry:'#568dff',exit:'#fb7079',long:'#41d7a0',short:'#43cbe3',reference:'#c4f85b'};
  const timeFormat = new Intl.DateTimeFormat('zh-HK',{timeZone:'Asia/Hong_Kong',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});
  const axisFormat = new Intl.DateTimeFormat('zh-HK',{timeZone:'Asia/Hong_Kong',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false});
  const state = {hours:24,mode:'maker_long',quantity:'0.01',catalog:[],snapshots:[],funding:{},reference:[],groups:[],
    showReference:true,follow:true,demo:false,generation:0,historyRequest:0,referenceRequest:0,socket:null,retry:null,retryCount:0,last:null,lastFrame:0,spreadChart:null,fundingChart:null};
  const finite = n => typeof n === 'number' && Number.isFinite(n);
  const fmt = (n,d=4) => finite(n) ? n.toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d}) : '—';
  const price = n => finite(n) ? fmt(n,n >= 100 ? 2 : n >= 1 ? 4 : 6) : '—';
  const pct = n => finite(n) ? `${n>0?'+':''}${fmt(n,4)}%` : '—';
  const time = n => finite(n) ? timeFormat.format(new Date(n)) : '—';
  function node(tag,cls,text){const n=document.createElement(tag);if(cls)n.className=cls;if(text!==undefined)n.textContent=text;return n;}
  function notice(text,kind=''){ $('notice').textContent=text; $('notice').className=`notice ${kind}`; }
  function connection(text,cls){$('connection').textContent=text;$('connection').className=`status ${cls}`;}
  function selection(){return {symbol:$('symbol').value,short:$('short').value,long:$('long').value,quantity:state.quantity};}
  function params(extra={}){return new URLSearchParams({...selection(),...extra});}
  async function get(path,query={}){const r=await fetch(`/api/monitor/${path}?${new URLSearchParams(query)}`);if(!r.ok)throw Error('資料讀取失敗');return r.json();}
  function setOptions(el,items,value){el.replaceChildren(...items.map(([id,label,disabled])=>{const o=new Option(label,id);o.disabled=!!disabled;return o;}));if([...el.options].some(o=>o.value===value&&!o.disabled))el.value=value;}
  async function catalog(refresh=false){
    const data=await get('catalog',{refresh});state.catalog=data.venues;
    const options=data.venues.map(v=>[v.id,`${v.name}${v.available?'':' · 暫不可用'}`,!v.available]);
    setOptions($('long'),options,$('long').value||'aster');setOptions($('short'),options.filter(v=>v[0]!=='binance_spot'),$('short').value||'hyperliquid');
    if($('long').value===$('short').value){const other=data.venues.find(v=>v.available&&v.id!=='binance_spot'&&v.id!==$('long').value);if(other)$('short').value=other.id;}
    commonSymbols();
  }
  function commonSymbols(){
    const old=$('symbol').value;
    const l=state.catalog.find(v=>v.id===$('long').value),s=state.catalog.find(v=>v.id===$('short').value);
    const common=(l?.symbols||[]).filter(x=>s?.symbols.includes(x));
    common.sort((a,b)=>{const order=['ETH','BTC','SOL'];const score=x=>order.includes(x)?order.indexOf(x):-1;return (score(a)<0?99:score(a))-(score(b)<0?99:score(b))||a.localeCompare(b);});
    setOptions($('symbol'),common.map(s=>[s,s]),old);$('symbol').disabled=!common.length;
    if(!common.length)notice('這兩個交易所目前沒有可確認的共同市場。','error');
  }
  function caption(){const s=selection();$('pair-caption').textContent=`${(names[s.long]||'—').toUpperCase()} / ${(names[s.short]||'—').toUpperCase()}`;
    $('contract-kind').textContent=s.long==='binance_spot'?'SPOT / PERP':'PERP / PERP';
    $('swap').disabled=s.long==='binance_spot';$('swap').title=s.long==='binance_spot'?'現貨配對固定為買入現貨／做空永續':'交換做多與做空方向';
    $('funding-detail').textContent=s.long==='binance_spot'?'現貨無資金費率 · 顯示永續空方費率':'空方收取 − 多方支付 · 預估';
    $('funding-long-name').textContent=names[s.long];$('funding-short-name').textContent=names[s.short];
    $('entry-basis').textContent=state.mode==='taker'?'可見深度估算':'掛單估算';
    $('quote-tag').textContent=[s.short,s.long].includes('variational')?'含參考報價':'未扣費用';}
  function disconnect(){clearTimeout(state.retry);if(state.socket){state.socket.onclose=null;state.socket.close();state.socket=null;}}
  async function restart(){
    const generation=++state.generation;disconnect();state.last=null;state.lastFrame=0;state.snapshots=[];state.reference=[];state.funding={};state.groups=[];
    caption();renderEmptyBooks();renderMetrics();renderStrategies();draw();
    if(state.demo){makeDemo();return;}
    if(!$('symbol').value||$('short').value===$('long').value){connection('配對未就緒','waiting');return;}
    notice('正在訂閱即時行情並載入歷史資料。');connection('連線中','waiting');
    connect(generation);await Promise.allSettled([loadHistory(generation),loadAux(generation)]);
  }
  function connect(generation){
    if(generation!==state.generation||state.demo)return;
    const ws=new WebSocket(`${location.protocol==='https:'?'wss':'ws'}://${location.host}/api/monitor/stream?${params()}`);state.socket=ws;
    ws.onmessage=e=>{if(generation!==state.generation)return;let data;try{data=JSON.parse(e.data);}catch{return;}
      if(data.type==='error'){notice(data.message,'error');connection('訂閱失敗','error');return;}
      if(data.type!=='snapshot')return;
      state.retryCount=0;state.last=data;state.lastFrame=Date.now();connection('已連線','live');
      append(data);renderSnapshot(data);draw();
    };
    ws.onclose=e=>{if(generation!==state.generation||state.demo)return;connection('連線中斷','error');invalidate();
      if(e.code===1008){notice('訂閱未通過，請重新載入市場或選擇其他配對。','error');return;}
      const delay=Math.min(30000,1000*2**Math.min(state.retryCount++,5));notice(`行情連線中斷，${delay/1000} 秒後重新連線。`,'error');state.retry=setTimeout(()=>connect(generation),delay);
    };
    ws.onerror=()=>connection('連線異常','error');
  }
  function append(snapshot){const bucket=Math.floor(snapshot.ts_ms/5000)*5000;const row={...snapshot,ts_ms:bucket};
    const last=state.snapshots.at(-1);if(last?.ts_ms===bucket)state.snapshots[state.snapshots.length-1]=row;else state.snapshots.push(row);
    const cutoff=Date.now()-state.hours*3600000;state.snapshots=state.snapshots.filter(r=>r.ts_ms>=cutoff).slice(-20000);
  }
  function invalidate(){state.last=null;renderMetrics();renderEmptyBooks();draw();}
  async function loadHistory(generation=state.generation,refreshReference=true){
    if(state.demo)return;
    if(state.showReference&&refreshReference)loadReference(generation);
    const request=++state.historyRequest;
    try{const s=selection();const data=await get('history',{...s,hours:state.hours,interval:$('interval').value});if(generation!==state.generation||request!==state.historyRequest)return;
      const merged=new Map(data.series.map(r=>[r.ts_ms,r]));for(const r of state.snapshots)merged.set(r.ts_ms,r);state.snapshots=[...merged.values()].sort((a,b)=>a.ts_ms-b.ts_ms);
      if(!state.showReference)$('history-note').textContent=`實際報價 · ${data.series.length} 筆歷史 · ${data.interval_seconds||$('interval').value} 秒顯示間隔`;
      draw();
    }catch{if(generation===state.generation&&!state.showReference)$('history-note').textContent='歷史讀取失敗；即時報價持續更新';}
  }
  async function loadReference(generation=state.generation){
    const request=++state.referenceRequest;
    $('history-note').textContent='正在讀取歷史參考 · 優先使用快取，只補缺少時段 · 最多 24 小時';
    try{const data=await get('reference',{symbol:$('symbol').value,short:$('short').value,long:$('long').value,hours:Math.min(state.hours,24)});
      if(generation!==state.generation||request!==state.referenceRequest||!state.showReference)return;state.reference=data.series;$('history-note').textContent=`${data.source} · 最多 24 小時 · ${data.series.length} 筆`;draw();
    }catch{if(generation===state.generation&&request===state.referenceRequest&&state.showReference)$('history-note').textContent='歷史參考暫時無法取得';}
  }
  async function loadAux(generation=state.generation){
    if(state.demo)return;
    const s=selection();
    await Promise.allSettled([
      get('funding',{symbol:s.symbol,short:s.short,long:s.long,days:Math.max(1,Math.ceil(state.hours/24))}).then(data=>{if(generation!==state.generation)return;state.funding=data.series;const count=Object.values(data.series).reduce((n,r)=>n+r.length,0);$('funding-note').textContent=count?`${data.source} · ${count} 筆 · 缺資料不補零`:'本地尚無此配對的費率紀錄；即時費率另外顯示';drawFunding();}),
      loadStrategies(generation)
    ]);
  }
  async function loadStrategies(generation=state.generation){
    if(state.demo||!$('symbol').value)return;
    try{const s=selection();const data=await get('strategies',{symbol:s.symbol,short:s.short,long:s.long});if(generation!==state.generation)return;state.groups=data.groups;renderStrategies();renderMetrics();}
    catch{if(generation===state.generation)$('strategy-update').textContent='策略紀錄讀取失敗';}
  }
  function renderMetrics(){const s=selection(),data=state.last,sp=data?.spreads?.[state.mode];
    $('entry-value').textContent=pct(sp?.entry);$('exit-value').textContent=pct(sp?.exit);
    const a=data?.funding?.[s.short],b=data?.funding?.[s.long];$('funding-value').textContent=s.long==='binance_spot'?(a?pct(a.hourly_pct):'—'):(a&&b?pct(a.hourly_pct-b.hourly_pct):'—');
    $('entry-detail').textContent=data?.spreads?.reason||(state.mode==='taker'?`兩腿各 ${state.quantity} ${s.symbol} · 深度加權`:`最佳限價估算 · ${state.quantity} ${s.symbol}`);
    const actual=actualQuoteSpread();if(actual!==null)$('entry-detail').textContent=`策略實際掛單價差 ${pct(actual)}（摘要）`;
    const liveGroups=state.groups.filter(g=>g.mode==='live'),observed=liveGroups.length?liveGroups:state.groups;
    const exposure=observed.reduce((sum,g)=>sum+Math.abs(Number(g.short?.quantity||0)+Number(g.long?.quantity||0)),0);
    $('exposure-value').textContent=observed.length?`${fmt(exposure,4)} ${s.symbol}`:'—';
    $('exposure-detail').textContent=observed.length?`${liveGroups.length?'實盤':'模擬'}殘餘絕對值合計 · ${observed.filter(g=>g.running).length} 組有心跳`:'無此配對的策略紀錄';
    for(const leg of ['long','short']){const rate=data?.funding?.[s[leg]];$(`funding-${leg}-rate`).textContent=s[leg]==='binance_spot'?'不適用（現貨）':(rate?`${pct(rate.raw_pct)} / ${rate.interval_hours}h`:'—');}
  }
  function actualQuoteSpread(){if(state.mode==='taker'||!state.last)return null;const s=selection(),leg=state.mode==='maker_long'?'long':'short';
    for(const g of state.groups.filter(g=>g.running&&g.mode==='live'))for(const o of g.orders||[]){
      if(o.venue!==s[leg]||o.purpose!=='quote_entry'||o.state!=='OPEN'||!o.price)continue;
      const q=Number(o.quantity)-Number(o.executed_quantity||0);const b=state.last.books[s[leg==='long'?'short':'long']];
      if(!b||!['live','indicative'].includes(b.status))continue;
      const levels=leg==='long'?b.bids:b.asks;let left=q,cost=0;for(const [p,size] of levels){const n=Math.min(left,size);cost+=n*p;left-=n;if(left<=1e-12)break;}
      if(left>1e-12||q<=0)continue;const hedge=cost/q,maker=Number(o.price);return leg==='long'?(hedge-maker)/maker*100:(maker-hedge)/hedge*100;
    }return null;
  }
  function renderSnapshot(data){renderMetrics();for(const leg of ['long','short'])renderBook($(`${leg}-book`),data.books[selection()[leg]],leg);
    $('quote-time').textContent=`最後更新 ${time(data.ts_ms)}`;
    if(!state.demo){const indicative=[selection().short,selection().long].includes('variational');notice(data.record_error||data.spreads.reason||(indicative?'Variational 為參考報價，未提供完整掛單深度；價差不保證可成交。':'即時報價已連線 · 圖表為未扣費用價差，平倉價差不等於持倉獲利。'),data.record_error?'error':'');}}
  function renderEmptyBooks(){for(const leg of ['long','short'])renderBook($(`${leg}-book`),{venue:selection()[leg],status:'waiting',bids:[],asks:[]},leg);}
  function renderBook(el,b,leg){if(!b)return;const header=node('div','book-header'),title=node('div','book-title');title.append(node('span','venue-symbol',(names[b.venue]||'?')[0]),node('span','',names[b.venue]),node('span',`side ${leg}`,leg==='long'?'多':'空'));
    const labels={live:'● 即時',stale:'● 已過期',waiting:'○ 等待中',indicative:'◌ 參考報價'};header.append(title,node('span',`book-health ${b.status==='stale'?'stale':''}`,state.demo?'◌ 示範':labels[b.status]));el.replaceChildren(header);
    if(b.venue==='variational'){const body=node('div','quote-only');for(const [label,value] of [['買入報價 Ask',b.ask],['賣出報價 Bid',b.bid]]){const row=node('div');row.append(node('span','muted',label),node('strong','',price(value)));body.append(row);}body.append(node('p','','此介面不提供完整買賣盤深度。參考報價不代表指定數量已可成交。'));el.append(body);}
    else{const cols=node('div','book-cols');['價格','數量','累計'].forEach(x=>cols.append(node('span','',x)));el.append(cols);
      const rows=(levels,side)=>{let total=0;const values=levels.slice(0,6).map(([p,q])=>({p,q,total:total+=q}));const max=total||1;if(side==='ask')values.reverse();for(let i=0;i<6;i++){const v=values[i];const row=node('div',`book-row ${side}`);row.style.setProperty('--depth',`${v?v.total/max*100:0}%`);row.append(node('span','',price(v?.p)),node('span','',v?fmt(v.q,3):'—'),node('span','',v?fmt(v.total,3):'—'));el.append(row);}};
      rows(b.asks,'ask');const mid=node('div','book-mid');mid.append(node('strong','',finite(b.bid)?price((b.bid+b.ask)/2):'—'),node('span','',finite(b.spread_pct)?`盤差 ${fmt(b.spread_pct,3)}%`:'無有效報價'));el.append(mid);rows(b.bids,'bid');}
    const foot=node('div','book-footer');foot.append(node('span','',b.status==='waiting'?'等待市場資料':`更新 ${time(b.received_ms)}`),node('span','',finite(b.age_ms)?`${Math.max(0,b.age_ms)} ms`:'—'));el.append(foot);
    el.title=`報價年齡 ${b.age_ms??'—'} ms；來源至接收 ${b.transport_ms??'—'} ms（包含時鐘偏差，不是成交延遲）`;
  }
  function plotOptions(el,series,height){return {width:el.clientWidth||700,height:height-24,legend:{show:true},
    cursor:{sync:{key:'monitor'},drag:{x:true,y:false}},series:[{label:'時間',value:(_,v)=>finite(v)?time(v*1000):'—'},...series.map(s=>({width:1.5,spanGaps:false,points:{show:true,size:2},value:(_,v)=>fmt(v,4),...s}))],
    scales:{x:{time:true},price:{auto:true}},axes:[{stroke:'#728b7b',grid:{stroke:'#19291f',width:1},ticks:{stroke:'#25382c'},font:'10px system-ui',values:(_,xs)=>xs.map(x=>axisFormat.format(new Date(x*1000))),size:34},
      {stroke:'#728b7b',grid:{stroke:'#19291f',width:1},ticks:{stroke:'#25382c'},font:'10px system-ui',size:62,values:(_,ys)=>ys.map(y=>`${fmt(y,3)}%`)}],
    hooks:{setSelect:[u=>{if(u.select.width>2){state.follow=false;$('follow').classList.remove('selected');}}]}};}
  function updatePlot(chart,data){const prior={...chart.scales.x};chart.setData(data);if(state.follow||!finite(prior.min))chart.setScale('x',{min:(Date.now()-state.hours*3600000)/1000,max:Date.now()/1000});else chart.setScale('x',{min:prior.min,max:prior.max});}
  function chartRows(){const interval=Number($('interval').value)*1000,buckets=new Map();for(const row of state.snapshots)buckets.set(Math.floor(row.ts_ms/interval)*interval,row);const out=[];let prev=null;for(const [stamp,row] of [...buckets].sort((a,b)=>a[0]-b[0])){if(prev!==null&&stamp-prev>interval*1.5)out.push({ts_ms:prev+interval});out.push({...row,ts_ms:stamp});prev=stamp;}return out;}
  function drawSpread(){
    $('reference').classList.toggle('selected',state.showReference);$('reference').setAttribute('aria-pressed',String(state.showReference));
    $('reference-legend').hidden=!state.showReference;$('entry-legend').hidden=state.showReference;$('exit-legend').hidden=state.showReference;
    $('prices').disabled=state.showReference;
    $('chart-empty-detail').textContent=state.showReference?'自動載入兩所共同時段的分鐘價格；沒有歷史來源的配對會保持空白。':'連線後開始錄製，未錄製的時段不補值。';
    const el=$('spread-chart'),showPrice=$('prices').checked&&!state.showReference,key=`${state.showReference}:${showPrice}:${state.mode}:${selection().short}:${selection().long}`;
    let rows=chartRows();let series=state.showReference?[{label:'歷史參考價差',stroke:colors.reference}]:[{label:'開倉價差',stroke:colors.entry},{label:'平倉價差',stroke:colors.exit}];
    const reference=[];for(const r of state.reference){const prior=reference.at(-1);if(!state.demo&&prior&&r.ts_ms-prior.ts_ms>90000)reference.push({ts_ms:prior.ts_ms+60000,reference:null});reference.push(r);}
    let data=state.showReference?[reference.map(r=>r.ts_ms/1000),reference.map(r=>r.reference)]:[rows.map(r=>r.ts_ms/1000),rows.map(r=>r.spreads?.[state.mode]?.entry??null),rows.map(r=>r.spreads?.[state.mode]?.exit??null)];
    if(showPrice){for(const leg of ['long','short']){series.push({label:names[selection()[leg]],stroke:colors[leg],scale:'price',width:1});data.push(rows.map(r=>{const b=r.books?.[selection()[leg]];return b&&['live','indicative'].includes(b.status)?(b.ask+b.bid)/2:null;}));}}
    if(!state.spreadChart||state.spreadChart._monitorKey!==key){state.spreadChart?.destroy();const opts=plotOptions(el,series,el.clientHeight||335);if(showPrice)opts.axes.push({scale:'price',side:1,stroke:'#647e6c',grid:{show:false},size:60,font:'10px system-ui',values:(_,v)=>v.map(price)});state.spreadChart=new uPlot(opts,data,el);state.spreadChart._monitorKey=key;}
    updatePlot(state.spreadChart,data);const count=data[1].filter(finite).length;$('chart-empty').style.display=count?'none':'flex';
  }
  function drawFunding(){
    const el=$('funding-chart'),s=selection(),min=Date.now()-state.hours*3600000,lists={};
    for(const leg of ['long','short']){lists[leg]=[...(state.funding[s[leg]]||[])];const current=state.last?.funding?.[s[leg]];if(current)lists[leg].push(current);lists[leg]=lists[leg].filter(r=>r.ts_ms>=min&&finite(r.hourly_pct)).sort((a,b)=>a.ts_ms-b.ts_ms);}
    const stamps=[...lists.long,...lists.short].map(r=>r.ts_ms);
    for(const list of Object.values(lists))for(let i=0;i<list.length-1;i++){const expires=list[i].ts_ms+list[i].interval_hours*3600000+1;if(finite(expires)&&expires<list[i+1].ts_ms)stamps.push(expires);}
    const times=[...new Set(stamps)].sort((a,b)=>a-b);
    const data=[times.map(t=>t/1000)];for(const leg of ['long','short']){let i=-1;data.push(times.map(t=>{while(i+1<lists[leg].length&&lists[leg][i+1].ts_ms<=t)i++;const r=lists[leg][i];return r&&t-r.ts_ms<=r.interval_hours*3600000?r.hourly_pct:null;}));}
    const key=`${s.long}:${s.short}`;if(!state.fundingChart||state.fundingChart._monitorKey!==key){state.fundingChart?.destroy();state.fundingChart=new uPlot(plotOptions(el,[{label:names[s.long],stroke:colors.long,points:{show:true,size:3}},{label:names[s.short],stroke:colors.short,points:{show:true,size:3}}],el.clientHeight||175),data,el);state.fundingChart._monitorKey=key;}
    updatePlot(state.fundingChart,data);
  }
  function draw(){if(typeof uPlot==='undefined')return;drawSpread();drawFunding();}
  const statuses={RUNNING:'運行設定',STOPPING:'停止中',PAUSED:'已暫停',STOPPED:'已停止',OPEN:'掛單中',PENDING_SUBMIT:'送出待確認',UNKNOWN:'結果待確認',FILLED:'已成交',CANCELED:'已撤單',REJECTED:'已拒絕',EXPIRED:'已失效'};
  function renderStrategies(){const el=$('strategies');el.replaceChildren();$('strategy-count').textContent=state.groups.length;$('strategy-update').textContent=`更新 ${time(Date.now())} · 心跳有效期 12 秒`;
    if(!state.groups.length){el.append(node('div','empty-state','此配對尚無策略紀錄。監控頁只讀取狀態，不會啟動或送出交易。'));return;}
    for(const g of state.groups){const summary=node('div','strategy-summary');summary.append(node('b','',`${g.mode==='live'?'實盤':'模擬'} · ${g.running?'● 運行中（有心跳）':'○ 上次保存狀態'}`),node('span','',statuses[g.status]||g.status),node('span','',`空 ${g.short?.quantity||0} ／ 多 ${g.long?.quantity||0}`),node('small','',`保存於 ${time(g.updated_ms)}`),node('small','',`均價 空 ${price(Number(g.short?.average))} ／ 多 ${price(Number(g.long?.average))}`),node('small','',`已實現價差損益 ${fmt(Number(g.short?.realized||0)+Number(g.long?.realized||0),2)} · 估算費用 ${fmt(Number(g.fees||0),2)}`));el.append(summary);
      const table=node('table','strategy-table'),head=node('tr');['交易所','用途','方向','委託數量','已成交','價格／均價','狀態','時間'].forEach(t=>head.append(node('th','',t)));const thead=node('thead');thead.append(head);table.append(thead);const body=node('tbody');
      for(const o of [...g.orders].reverse().slice(0,12)){const row=node('tr');const purpose={quote_entry:'開倉掛單',quote_exit:'平倉掛單',repair:'對沖修復',taker_entry:'吃單開倉',taker_exit:'吃單平倉',stop:'停止平倉'};
        [names[o.venue]||o.venue,purpose[o.purpose]||o.purpose,o.side==='BUY'?'買入':'賣出',o.quantity,o.executed_quantity,price(Number(o.average_price||o.price)),statuses[o.state]||o.state,time(o.updated_ms)].forEach(v=>row.append(node('td','',v??'—')));body.append(row);}table.append(body);el.append(table);}
  }
  function makeDemo(){const s=selection(),now=Date.now(),n=Math.min(6000,Math.ceil(state.hours*3600/Number($('interval').value)))+1;state.snapshots=[];state.funding={[s.short]:[],[s.long]:[]};let seed=37;const rand=()=>{seed=(seed*16807)%2147483647;return seed/2147483647;};
    for(let i=0;i<n;i++){const t=now-(n-1-i)*state.hours*3600000/n,mid=2480+Math.sin(i/31)*11+Math.sin(i/9)*3-i*.025,edge=.18+Math.sin(i/19)*.06+(rand()-.5)*.035+(i>280?.065:0);const books={};for(const [v,base] of [[s.long,mid],[s.short,mid*(1+edge/100)]]){const bids=[],asks=[];for(let j=0;j<6;j++){bids.push([base-.2-j*.12,+(2+rand()*18).toFixed(3)]);asks.push([base+.2+j*.12,+(2+rand()*18).toFixed(3)]);}books[v]={venue:v,status:v==='variational'?'indicative':'live',bid:base-.2,ask:base+.2,bids,asks,received_ms:t,source_ms:t-75,age_ms:75,transport_ms:75,spread_pct:.016};}
      const funding={[s.long]:{hourly_pct:.0015+Math.sin(i/28)*.0004,raw_pct:.012,interval_hours:8,ts_ms:t},[s.short]:{hourly_pct:.0045+Math.sin(i/25)*.001,raw_pct:.0045,interval_hours:1,ts_ms:t}};
      if(s.long==='binance_spot')delete funding[s.long];
      for(const rate of Object.values(funding))rate.raw_pct=rate.hourly_pct*rate.interval_hours;
      state.snapshots.push({ts_ms:t,books,funding,spreads:{taker:{entry:edge-.016,exit:edge+.016},maker_long:{entry:edge,exit:edge},maker_short:{entry:edge,exit:edge}}});if(i%10===0)for(const v of [s.short,s.long])if(funding[v])state.funding[v].push(funding[v]);}
    state.last=state.snapshots.at(-1);state.groups=[];state.reference=state.snapshots.map(r=>({ts_ms:r.ts_ms,reference:r.spreads.taker.entry+.008}));
    notice('示範模式 · 所有價格、費率與深度均為模擬資料，不代表市場機會。','demo');connection('示範資料','waiting');$('demo').textContent='返回即時行情';$('demo').classList.add('selected');
    $('history-note').textContent='示範資料 · 可操作圖表、設定與時間範圍';$('funding-note').textContent='示範費率 · 非歷史收益';renderSnapshot(state.last);renderStrategies();draw();
  }
  function resize(){for(const id of ['spread','funding']){const el=$(`${id}-chart`),chart=state[`${id}Chart`];if(chart&&el.clientWidth)chart.setSize({width:el.clientWidth,height:el.clientHeight-24});}}
  $('swap').onclick=()=>{if($('long').value==='binance_spot')return;const s=$('short').value;$('short').value=$('long').value;$('long').value=s;commonSymbols();restart();};
  for(const id of ['short','long'])$(id).onchange=()=>{if($('short').value===$('long').value){const other=id==='short'?'long':'short';const replacement=state.catalog.find(v=>v.available&&(other==='long'||v.id!=='binance_spot')&&v.id!==$(id).value);if(replacement)$(other).value=replacement.id;}commonSymbols();restart();};
  $('symbol').onchange=()=>restart();
  $('ranges').onclick=e=>{const b=e.target.closest('[data-hours]');if(!b)return;state.hours=Number(b.dataset.hours);$('ranges').querySelectorAll('button').forEach(x=>x.classList.toggle('selected',x===b));if(state.hours===168&&Number($('interval').value)<300)$('interval').value='300';state.follow=true;state.demo?makeDemo():(loadHistory(),loadAux());};
  $('interval').onchange=()=>state.demo?makeDemo():loadHistory(state.generation,false);$('prices').onchange=()=>draw();
  $('reference').onclick=()=>{state.showReference=!state.showReference;$('reference').classList.toggle('selected',state.showReference);if(state.showReference&&!state.demo)loadReference();else{if(!state.demo)loadHistory();draw();}};
  $('follow').onclick=()=>{state.follow=true;$('follow').classList.add('selected');draw();};
  $('fullscreen').onclick=()=>{document.body.classList.toggle('expanded');resize();};
  document.querySelectorAll('[data-tab]').forEach(b=>b.onclick=()=>{document.querySelectorAll('[data-tab]').forEach(x=>x.classList.toggle('active',x===b));document.body.classList.toggle('strategies-focus',b.dataset.tab==='strategies');resize();});
  $('settings-open').onclick=()=>$('settings').showModal();
  $('settings').addEventListener('close',()=>{if($('settings').returnValue!=='apply')return;state.mode=$('mode').value;state.quantity=$('quantity').value;restart();$('settings').returnValue='';});
  $('demo').onclick=()=>{state.demo=!state.demo;if(!state.demo){$('demo').textContent='示範預覽';$('demo').classList.remove('selected');}restart();};
  $('reconnect').onclick=async()=>{if(state.demo){makeDemo();return;}notice('重新確認共同市場…');try{await catalog(true);restart();}catch{notice('無法取得市場目錄，請稍後重試。','error');}};
  new ResizeObserver(resize).observe($('charts-section'));
  const timer=setInterval(()=>{$('clock').textContent=`${time(Date.now())} HKT`;if(!state.demo&&state.lastFrame&&Date.now()-state.lastFrame>3000){connection('資料中斷','error');invalidate();state.lastFrame=0;}},1000);
  let polling=false;const statusTimer=setInterval(async()=>{if(polling||document.hidden||state.demo)return;polling=true;try{await loadStrategies();}finally{polling=false;}},3000);
  window.addEventListener('pagehide',()=>{disconnect();clearInterval(timer);clearInterval(statusTimer);});
  setOptions($('long'),Object.entries(names),'aster');setOptions($('short'),Object.entries(names).filter(v=>v[0]!=='binance_spot'),'hyperliquid');renderEmptyBooks();
  (async()=>{try{await catalog();await restart();}catch{connection('目錄讀取失敗','error');notice('市場目錄無法載入，請按「重新連線」。可使用示範預覽檢視版面。','error');}})();
})();
