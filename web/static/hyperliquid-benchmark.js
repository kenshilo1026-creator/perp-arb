(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  let sockets=[],timers=[],states={},started=null,startWall=0,startHkt='',stopped=true,downloadUrl=null;
  const later=(f,t)=>{const id=setTimeout(f,t);timers.push(id);return id;};
  function dist(xs){const a=xs.filter(Number.isFinite).sort((x,y)=>x-y);if(!a.length)return null;const p=q=>+a[Math.min(a.length-1,Math.ceil(a.length*q)-1)].toFixed(2);return {count:a.length,min:a[0],median:p(.5),p95:p(.95),p99:p(.99),max:a.at(-1)};}
  function connect(name){return new Promise((resolve,reject)=>{
    const s=states[name]={frames:[],samples:[],latest:null,errors:[],ack:null,connected:false};
    const ws=new WebSocket('wss://api.hyperliquid.xyz/ws');sockets.push(ws);
    const timeout=later(()=>reject(Error(`${name} 未在 20 秒內收到有效買賣盤`)),20000);
    ws.onopen=()=>{s.connected=true;const subscription={type:'l2Book',coin:'ETH'};if(name==='fast')subscription.fast=true;ws.send(JSON.stringify({method:'subscribe',subscription}));};
    ws.onmessage=e=>{const mono=performance.now(),received_ms=Date.now();let message;try{message=JSON.parse(e.data);}catch{return;}
      if(message.channel==='subscriptionResponse'){s.ack=message.data;return;}
      if(message.channel==='error'){s.errors.push(JSON.stringify(message.data));reject(Error(`${name} 訂閱被拒絕`));return;}
      if(message.channel!=='l2Book'||message.data?.coin!=='ETH')return;
      const b=message.data,[bids,asks]=b.levels;if(!bids.length||!asks.length)return;
      const f={mono,received_ms,source_ms:b.time,transport_ms:received_ms-b.time,bid_levels:bids.length,ask_levels:asks.length,
        bid:Number(bids[0].px),ask:Number(asks[0].px),bid_size_top5:bids.slice(0,5).reduce((a,r)=>a+Number(r.sz),0),ask_size_top5:asks.slice(0,5).reduce((a,r)=>a+Number(r.sz),0)};
      s.latest=f;if(started!==null&&!stopped)s.frames.push(f);clearTimeout(timeout);resolve();};
    ws.onerror=()=>{s.errors.push('WebSocket error');reject(Error(`${name} WebSocket 連線錯誤`));};
    ws.onclose=()=>{s.connected=false;if(!stopped)s.errors.push('WebSocket closed during test');};
  });}
  function sample(){if(stopped||started===null)return;const elapsed_ms=performance.now()-started,now=Date.now();
    for(const s of Object.values(states)){const f=s.latest,age=now-f.source_ms;s.samples.push({elapsed_ms,age_ms:age,connected:s.connected,
      fresh:s.connected&&f.bid>0&&f.bid<=f.ask&&age>=-250&&age<=1000&&f.transport_ms<=500});}
    $('status').textContent=`量測中 ${Math.min(90,Math.floor(elapsed_ms/1000))} / 90 秒 · 一般 ${states.default.frames.length} 筆 · fast ${states.fast.frames.length} 筆`;
    if(elapsed_ms>=90000)finish(true);else later(sample,50);
  }
  function finish(complete){if(stopped)return;stopped=true;const duration=started===null?0:performance.now()-started;timers.forEach(clearTimeout);timers=[];
    sockets.forEach(ws=>ws.close());sockets=[];$('start').disabled=false;$('stop').disabled=true;
    if(!started||!complete){$('status').textContent='已停止；未完成完整比較';return;}
    const summary={client:'browser WebSocket',coin:'ETH',started_hkt:startHkt,duration_seconds:duration/1000,clock_drift_ms:(Date.now()-startWall)-duration,streams:{}};
    for(const [name,s] of Object.entries(states)){const f=s.frames,points=s.samples;let fresh=0,connected=0,total=0;
      for(let i=0;i<points.length-1;i++){const dt=points[i+1].elapsed_ms-points[i].elapsed_ms;total+=dt;if(points[i].fresh)fresh+=dt;if(points[i].connected)connected+=dt;}
      const differences=(a,key)=>a.slice(1).map((v,i)=>v[key]-a[i][key]);
      summary.streams[name]={frames:f.length,updates_per_second:f.length/(duration/1000),unique_source_times:new Set(f.map(r=>r.source_ms)).size,
        levels_per_side:[...new Set(f.map(r=>`${r.bid_levels}/${r.ask_levels}`))],receive_interval_ms:dist(differences(f,'mono')),source_interval_ms:dist(differences(f,'source_ms')),
        transport_ms:dist(f.map(r=>r.transport_ms)),sampled_age_ms:dist(points.map(r=>r.age_ms)),sample_interval_ms:dist(differences(points,'elapsed_ms')),
        fresh_time_pct:100*fresh/total,connected_time_pct:100*connected/total,top5_bid_quantity:dist(f.map(r=>r.bid_size_top5)),top5_ask_quantity:dist(f.map(r=>r.ask_size_top5)),ack:s.ack,errors:s.errors};}
    $('status').textContent=`完成 · ${startHkt} · ${Math.round(duration/1000)} 秒`;
    const metrics=[['收到更新筆數',s=>s.frames],['深度檔數（買／賣）',s=>s.levels_per_side.join(', ')],['接收間隔中位數 ms',s=>s.receive_interval_ms?.median],['接收間隔 P95 ms',s=>s.receive_interval_ms?.p95],['來源間隔中位數 ms',s=>s.source_interval_ms?.median],['來源至接收中位數 ms',s=>s.transport_ms?.median],['來源至接收 P95 ms',s=>s.transport_ms?.p95],['有效時間比例',s=>s.fresh_time_pct.toFixed(2)+'%'],['連線有效時間',s=>s.connected_time_pct.toFixed(2)+'%']];
    for(const [label,value]of metrics){const tr=document.createElement('tr');for(const x of[label,value(summary.streams.default),value(summary.streams.fast)]){const td=document.createElement('td');td.textContent=x??'—';tr.append(td);}$('metrics').append(tr);}
    $('result').textContent=JSON.stringify(summary,null,2);const artifact=JSON.stringify({summary,raw:states},null,2);$('raw').textContent=artifact;
    if(downloadUrl)URL.revokeObjectURL(downloadUrl);downloadUrl=URL.createObjectURL(new Blob([artifact],{type:'application/json'}));$('download').href=downloadUrl;$('download').hidden=false;
  }
  $('start').onclick=async()=>{stopped=false;started=null;states={};$('start').disabled=true;$('stop').disabled=false;$('download').hidden=true;$('metrics').replaceChildren();$('result').textContent='';$('raw').textContent='';$('status').textContent='建立連線與預熱中…';
    try{const a=connect('default');const b=new Promise((resolve,reject)=>later(()=>{if(stopped){reject(Error('已停止'));return;}connect('fast').then(resolve,reject);},3000));await Promise.all([a,b]);if(stopped)return;
      later(()=>{if(stopped)return;started=performance.now();startWall=Date.now();startHkt=new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Hong_Kong',dateStyle:'short',timeStyle:'medium'}).format(new Date())+' HKT';sample();},5000);
    }catch(e){finish(false);$('status').textContent=e.message;}
  };
  $('stop').onclick=()=>finish(false);window.addEventListener('pagehide',()=>finish(false));
})();
