const VERSION="0.1.0";
const WORKER_NAME="afo-fsl-v4-compress-mcp";
const CORS={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Allow-Headers':'Content-Type,Authorization,Mcp-Session-Id'};
const TOOLS=[{
  "name": "afo-fsl-v4-compress_status",
  "description": "Health check. Returns version, all binding statuses, and tool list.",
  "inputSchema": {
    "type": "object",
    "properties": {},
    "required": []
  }
},
{
  "name": "compress_repo_v4",
  "description": "Lossy V4 semantic compression: fetches GitHub repo tree, classifies each file (P/C/L/V/G/D), extracts a keyword-frequency fingerprint per file (or row/numeric/bool structural stats for data files) - and stores ONLY that fingerprint in D1. Raw file content is fetched transiently and never persisted (no R2, no blob storage). This is the core difference from raw-storage compressors: nothing here is recoverable verbatim.",
  "inputSchema": {
    "properties": {
      "branch": {
        "default": "main",
        "type": "string"
      },
      "max_files": {
        "default": 20,
        "type": "number"
      },
      "offset": {
        "default": 0,
        "type": "number"
      },
      "owner": {
        "type": "string"
      },
      "path": {
        "type": "string"
      },
      "repo": {
        "type": "string"
      }
    },
    "required": [
      "owner",
      "repo"
    ],
    "type": "object"
  }
},
{
  "name": "get_v4_codex",
  "description": "Derives the dash-codex (---:term mapping) for a compressed repo from aggregated keyword + path-segment frequency across all stored chunk fingerprints. Computed on demand, not stored - always reflects current chunk data.",
  "inputSchema": {
    "properties": {
      "branch": {
        "default": "main",
        "type": "string"
      },
      "owner": {
        "type": "string"
      },
      "repo": {
        "type": "string"
      }
    },
    "required": [
      "owner",
      "repo"
    ],
    "type": "object"
  }
},
{
  "name": "query_v4",
  "description": "Keyword/term search against the D1 chunk fingerprint index for a compressed repo. Returns chunk locations and the matched keyword:count fingerprint only - never original content, since none is stored.",
  "inputSchema": {
    "properties": {
      "branch": {
        "default": "main",
        "type": "string"
      },
      "limit": {
        "default": 20,
        "type": "number"
      },
      "owner": {
        "type": "string"
      },
      "repo": {
        "type": "string"
      },
      "term": {
        "type": "string"
      }
    },
    "required": [
      "owner",
      "repo",
      "term"
    ],
    "type": "object"
  }
},
{
  "name": "get_v4_tier1",
  "description": "Returns the Tier-1 global term-frequency table (split into P:prose-readable-word and U:structured/underscored-term groups, top 25 each) plus the Tier-1.5 vocabulary overflow list, aggregated from all stored chunk fingerprints for a repo/branch.",
  "inputSchema": {
    "properties": {
      "branch": {
        "default": "main",
        "type": "string"
      },
      "owner": {
        "type": "string"
      },
      "repo": {
        "type": "string"
      }
    },
    "required": [
      "owner",
      "repo"
    ],
    "type": "object"
  }
},
{
  "name": "export_v4",
  "description": "Assembles and returns the FULL .v4.fsl tiered text blob for a compressed repo/branch: -Z4- header, -S- type legend, -K- dash-codex, -0- chunk-type byte-range map, -1- term frequency, -1.5- vocabulary overflow, -2- per-chunk keyword lines, -2.5- term-to-chunk reverse index. This is the actual compressed artifact format - paste it directly to an LLM for structure-aware querying. Note: reconstructing original source from this output is best-effort/LLM-assisted only, never guaranteed, since no raw content is stored anywhere in this system.",
  "inputSchema": {
    "properties": {
      "branch": {
        "default": "main",
        "type": "string"
      },
      "owner": {
        "type": "string"
      },
      "repo": {
        "type": "string"
      }
    },
    "required": [
      "owner",
      "repo"
    ],
    "type": "object"
  }
},
{
  "name": "decompress_chunk_v4",
  "description": "Returns the stored fingerprint for one chunk (chunk_type + top_keywords) by chunk_id or file_path. UNLIKE raw-storage decompressors, this can never return original file content - none was ever persisted. For best-effort source reconstruction, call export_v4 and hand the result to an LLM.",
  "inputSchema": {
    "properties": {
      "branch": {
        "default": "main",
        "type": "string"
      },
      "chunk_id": {
        "type": "string"
      },
      "file_path": {
        "type": "string"
      },
      "owner": {
        "type": "string"
      },
      "repo": {
        "type": "string"
      }
    },
    "required": [
      "owner",
      "repo"
    ],
    "type": "object"
  }
},
{
  "name": "compression_stats_v4",
  "description": "Returns the latest V4 compression job stats for a repo/branch: ratio, byte counts, file counts, status.",
  "inputSchema": {
    "properties": {
      "branch": {
        "default": "main",
        "type": "string"
      },
      "owner": {
        "type": "string"
      },
      "repo": {
        "type": "string"
      }
    },
    "required": [
      "owner",
      "repo"
    ],
    "type": "object"
  }
}];
function rpc(id,r){return Response.json({jsonrpc:"2.0",id,result:r},{headers:CORS});}
function errResp(id,c,m){return Response.json({jsonrpc:"2.0",id,error:{code:c,message:m}},{headers:CORS});}
function tool(id,r){return rpc(id,{content:[{type:"text",text:JSON.stringify(r,null,2)}]});}
function genId(p){return p+"_"+Date.now().toString(36)+"_"+Math.random().toString(36).slice(2,8);}
function nowIso(){return new Date().toISOString();}
async function dbRun(db,sql,p){const s=db.prepare(sql);return p?s.bind(...p).run():s.run();}
async function dbFirst(db,sql,p){const s=db.prepare(sql);return p?s.bind(...p).first():s.first();}
async function dbAll(db,sql,p){const s=db.prepare(sql);const r=p?await s.bind(...p).all():await s.all();return r.results||[];}
async function ensureSchema(db){await db.prepare(`CREATE TABLE IF NOT EXISTS mcp_sessions (session_id TEXT PRIMARY KEY,worker_name TEXT NOT NULL,status TEXT DEFAULT 'active',parent_id TEXT,metadata TEXT,started_at TEXT NOT NULL,updated_at TEXT NOT NULL,finished_at TEXT)`).run();await db.prepare(`CREATE TABLE IF NOT EXISTS action_execution_logs (log_id TEXT PRIMARY KEY,session_id TEXT,worker_name TEXT NOT NULL,tool_name TEXT NOT NULL,status TEXT NOT NULL,input_json TEXT,output_summary TEXT,payload_uri TEXT,error_message TEXT,duration_ms INTEGER,input_tokens INTEGER,output_tokens INTEGER,vector_id TEXT,created_at TEXT NOT NULL)`).run();await db.prepare(`CREATE TABLE IF NOT EXISTS schema_migrations (migration_id TEXT PRIMARY KEY,worker_name TEXT NOT NULL,description TEXT,applied_at TEXT NOT NULL,checksum TEXT)`).run();}
async function handle(name,args,env,ctx){
  if(name==="afo_fsl_v4_compress_status"){const res={status:"ok",worker:WORKER_NAME,version:VERSION,generated_at:"2026-06-22T04:49:49.516Z",bindings:{},tools:TOOLS.map(t=>t.name)};
  try{await ensureSchema(env.DB);res.bindings.DB=true;}catch{res.bindings.DB=false;}
  return res;}
  await ensureSchema(env.DB);
  if (name === "compress_repo_v4") {
    const { owner, repo } = args;
    if (!owner) throw new Error("compress_repo_v4: owner required");
    if (!repo) throw new Error("compress_repo_v4: repo required");
    const owner=args.owner,repo=args.repo,branch=args.branch||'main',root=args.path||'';const repoKey=owner+'/'+repo;const max=Math.max(1,Math.min(args.max_files||20,30));const offset=Math.max(0,Math.floor(args.offset||0));const jobId=genId('fslv4'),ts=nowIso();const STOP=new Set(['the','and','for','that','with','this','from','are','was','were','have','has','not','but','you','your','can','will','all','any','its','our','out','use','via','def','self','int','str','class','return','import']);function classify(p,c){const ext=(p.split('.').pop()||'').toLowerCase();const base=p.split('/').pop();if(base.toUpperCase().includes('LICENSE'))return'L';const vm=(c.match(/^(human|assistant|user|speaker\d?)\s*:/img)||[]);if(vm.length>=2)return'V';if(['py','js','ts','jsx','tsx','go','rs','java','c','cpp','rb','php','sh'].includes(ext))return'C';if(['json','yaml','yml','toml','ini','cfg','env'].includes(ext))return'G';if(['csv','tsv','sql'].includes(ext))return'D';if(['md','txt','rst'].includes(ext))return'P';return'P'}function keywords(text,n){const counts={};const lower=text.toLowerCase();const re=/[a-z_][a-z0-9_]{2,}/g;let m;while((m=re.exec(lower))){const w=m[0];if(STOP.has(w))continue;counts[w]=(counts[w]||0)+1}return Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,n)}function dataStats(text){const lines=text.split('\n').filter(l=>l.trim());let numericValues=0,boolValues=0;for(const line of lines){const cells=line.split(/[,\t]/);for(const cell of cells){const c=cell.trim();if(/^-?\d+(\.\d+)?$/.test(c))numericValues++;if(/^(true|false)$/i.test(c))boolValues++}}return[['row_count',lines.length],['numeric_values',numericValues],['bool_values',boolValues]]}async function gh(url){return fetch(url,{headers:{Authorization:'Bearer '+env.GITHUB_TOKEN,Accept:'application/vnd.github+json','User-Agent':'fsl-v4-compress-mcp'}})}await dbRun(env.DB,'CREATE TABLE IF NOT EXISTS fsl_v4_jobs (job_id TEXT PRIMARY KEY, repo_key TEXT, branch TEXT, sha TEXT, files_found INTEGER, files_compressed INTEGER, orig_bytes INTEGER, compressed_bytes INTEGER, ratio REAL, status TEXT, created_at TEXT, updated_at TEXT)');await dbRun(env.DB,'CREATE TABLE IF NOT EXISTS fsl_v4_chunks (chunk_id TEXT PRIMARY KEY, repo_key TEXT, branch TEXT, file_path TEXT, chunk_type TEXT, orig_bytes INTEGER, top_keywords TEXT, created_at TEXT)');const ref=await gh('https://api.github.com/repos/'+owner+'/'+repo+'/git/ref/heads/'+encodeURIComponent(branch));let commit='unknown';if(ref.ok){const j=await ref.json();commit=(j.object&&j.object.sha)||'unknown'}const treeRes=await gh('https://api.github.com/repos/'+owner+'/'+repo+'/git/trees/'+encodeURIComponent(branch)+'?recursive=1');if(!treeRes.ok)throw new Error('GitHub tree fetch failed '+treeRes.status);const tj=await treeRes.json();const badExt=new Set(['png','jpg','jpeg','gif','ico','woff','woff2','ttf','pdf','zip','wasm','bin','mp4','mov','lock']);const badDir=['node_modules/','.git/','.wrangler/','dist/','build/','.next/','venv/','__pycache__/'];let files=(tj.tree||[]).filter(f=>f.type==='blob'&&(!root||f.path.startsWith(root))&&f.size<150000&&!badDir.some(d=>f.path.includes(d))&&!badExt.has((f.path.split('.').pop()||'').toLowerCase())&&f.path.split('/').pop()!=='.v4.fsl').map(f=>({path:f.path,size:f.size})).sort((a,b)=>a.path.localeCompare(b.path));const batch=files.slice(offset,offset+max);const next=offset+batch.length,done=next>=files.length;await dbRun(env.DB,'INSERT INTO fsl_v4_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',[jobId,repoKey,branch,commit.slice(0,12),files.length,0,0,0,0,'running',ts,ts]);let compressedCount=0;for(const file of batch){const enc=file.path.split('/').map(encodeURIComponent).join('/');const raw=await fetch('https://raw.githubusercontent.com/'+owner+'/'+repo+'/'+encodeURIComponent(branch)+'/'+enc,{headers:{Authorization:'Bearer '+env.GITHUB_TOKEN,'User-Agent':'fsl-v4-compress-mcp'}});if(!raw.ok)continue;const txt=await raw.text();if(!txt.trim())continue;const ctype=classify(file.path,txt);const kw=ctype==='D'?dataStats(txt):keywords(txt,8);const kwStr=kw.map(([t,c])=>t+':'+c).join(',');const chunkId=repoKey+'::'+branch+'::'+file.path;await dbRun(env.DB,'INSERT OR REPLACE INTO fsl_v4_chunks (chunk_id,repo_key,branch,file_path,chunk_type,orig_bytes,top_keywords,created_at) VALUES (?,?,?,?,?,?,?,?)',[chunkId,repoKey,branch,file.path,ctype,txt.length,kwStr,nowIso()]);compressedCount++}const allRows=await dbAll(env.DB,'SELECT file_path,orig_bytes,top_keywords FROM fsl_v4_chunks WHERE repo_key=? AND branch=?',[repoKey,branch]);const currentPaths=new Set(files.map(f=>f.path));let liveRows=allRows;if(done){for(const r of allRows){if(!currentPaths.has(r.file_path)){await dbRun(env.DB,'DELETE FROM fsl_v4_chunks WHERE repo_key=? AND branch=? AND file_path=?',[repoKey,branch,r.file_path])}}liveRows=allRows.filter(r=>currentPaths.has(r.file_path))}let totalOrig=0,totalCompressed=0;for(const r of liveRows){totalOrig+=r.orig_bytes;totalCompressed+=(r.top_keywords||'').length+r.file_path.length+12}const ratio=totalOrig>0?(totalOrig/Math.max(totalCompressed,1)):0;await dbRun(env.DB,'UPDATE fsl_v4_jobs SET files_compressed=?,orig_bytes=?,compressed_bytes=?,ratio=?,status=?,updated_at=? WHERE job_id=?',[liveRows.length,totalOrig,totalCompressed,ratio,done?'complete':'partial',nowIso(),jobId]);return{ok:true,job_id:jobId,repo:repoKey,branch,sha:commit.slice(0,12),files_found:files.length,files_compressed_this_batch:compressedCount,total_chunks_indexed:liveRows.length,next_offset:next,done,orig_bytes:totalOrig,compressed_bytes:totalCompressed,ratio:Number(ratio.toFixed(2)),lossy:true,note:'Raw content was fetched transiently and discarded. Only keyword fingerprints were stored.'};
  }

  if (name === "get_v4_codex") {
    const { owner, repo } = args;
    if (!owner) throw new Error("get_v4_codex: owner required");
    if (!repo) throw new Error("get_v4_codex: repo required");
    const owner=args.owner,repo=args.repo,branch=args.branch||'main',repoKey=owner+'/'+repo;const rows=await dbAll(env.DB,'SELECT file_path,top_keywords FROM fsl_v4_chunks WHERE repo_key=? AND branch=?',[repoKey,branch]);if(!rows.length)return{ok:false,error:'no chunks found; run compress_repo_v4 first'};const termFreq={},pathFreq={};for(const r of rows){for(const pair of (r.top_keywords||'').split(',')){const parts=pair.split(':');const t=parts[0];const c=Number(parts[1]||1);if(t)termFreq[t]=(termFreq[t]||0)+c}for(const seg of r.file_path.split('/')){if(seg.length>3)pathFreq[seg]=(pathFreq[seg]||0)+1}}const cand=new Map();for(const[term,count] of Object.entries(pathFreq)){if(count>=2&&term.length>=4)cand.set(term,{term,count,len:term.length})}for(const[term,count] of Object.entries(termFreq)){if(count>=3&&term.length>=8){const ex=cand.get(term);if(!ex||ex.count<count)cand.set(term,{term,count,len:term.length})}}const ranked=[...cand.values()].sort((a,b)=>(b.count-a.count)||(b.len-a.len));const codex=[];let dashLevel=3;for(const c of ranked){codex.push({dash:'-'.repeat(dashLevel),term:c.term,count:c.count});dashLevel++;if(dashLevel>24)break}return{ok:true,repo:repoKey,branch,codex,chunk_count:rows.length};
  }

  if (name === "query_v4") {
    const { owner, repo, term } = args;
    if (!owner) throw new Error("query_v4: owner required");
    if (!repo) throw new Error("query_v4: repo required");
    if (!term) throw new Error("query_v4: term required");
    const owner=args.owner,repo=args.repo,branch=args.branch||'main',repoKey=owner+'/'+repo,term=(args.term||'').toLowerCase();const limit=Math.min(args.limit||20,50);const rows=await dbAll(env.DB,'SELECT chunk_id,file_path,chunk_type,top_keywords,orig_bytes FROM fsl_v4_chunks WHERE repo_key=? AND branch=? AND (lower(top_keywords) LIKE ? OR lower(file_path) LIKE ?) LIMIT ?',[repoKey,branch,'%'+term+'%','%'+term+'%',limit]);return{ok:true,repo:repoKey,branch,term,count:rows.length,matches:rows,content_available:false,note:'V4 is lossy - matches show fingerprints only, not source content.'};
  }

  if (name === "get_v4_tier1") {
    const { owner, repo } = args;
    if (!owner) throw new Error("get_v4_tier1: owner required");
    if (!repo) throw new Error("get_v4_tier1: repo required");
    const owner=args.owner,repo=args.repo,branch=args.branch||'main',repoKey=owner+'/'+repo;const rows=await dbAll(env.DB,'SELECT top_keywords FROM fsl_v4_chunks WHERE repo_key=? AND branch=?',[repoKey,branch]);if(!rows.length)return{ok:false,error:'no chunks found; run compress_repo_v4 first'};const freq={};for(const r of rows){for(const pair of (r.top_keywords||'').split(',')){const parts=pair.split(':');const t=parts[0];const c=Number(parts[1]||1);if(t)freq[t]=(freq[t]||0)+c}}const P=[],U=[];for(const[term,count] of Object.entries(freq)){if(/^[a-z]+$/.test(term))P.push([term,count]);else U.push([term,count])}P.sort((a,b)=>b[1]-a[1]);U.sort((a,b)=>b[1]-a[1]);const overflow=[...P.slice(25),...U.slice(25)].sort((a,b)=>b[1]-a[1]).slice(0,200).map(e=>e[0]);return{ok:true,repo:repoKey,branch,tier1:{P:P.slice(0,25).map(([t,c])=>t+':'+c),U:U.slice(0,25).map(([t,c])=>t+':'+c)},tier1_5_count:overflow.length,tier1_5:overflow};
  }

  if (name === "export_v4") {
    const { owner, repo } = args;
    if (!owner) throw new Error("export_v4: owner required");
    if (!repo) throw new Error("export_v4: repo required");
    const owner=args.owner,repo=args.repo,branch=args.branch||'main',repoKey=owner+'/'+repo;const jobs=await dbAll(env.DB,'SELECT * FROM fsl_v4_jobs WHERE repo_key=? AND branch=? ORDER BY created_at DESC LIMIT 1',[repoKey,branch]);if(!jobs.length)return{ok:false,error:'no job found; run compress_repo_v4 first'};const job=jobs[0];const rows=await dbAll(env.DB,'SELECT chunk_id,file_path,chunk_type,orig_bytes,top_keywords FROM fsl_v4_chunks WHERE repo_key=? AND branch=? ORDER BY file_path',[repoKey,branch]);if(!rows.length)return{ok:false,error:'no chunks found; run compress_repo_v4 first'};const termFreq={},pathFreq={};rows.forEach(r=>{for(const pair of (r.top_keywords||'').split(',')){const parts=pair.split(':');const t=parts[0];const c=Number(parts[1]||1);if(t)termFreq[t]=(termFreq[t]||0)+c}for(const seg of r.file_path.split('/')){if(seg.length>3)pathFreq[seg]=(pathFreq[seg]||0)+1}});const cand=new Map();for(const[term,count] of Object.entries(pathFreq)){if(count>=2&&term.length>=4)cand.set(term,{term,count,len:term.length})}for(const[term,count] of Object.entries(termFreq)){if(count>=3&&term.length>=8){const ex=cand.get(term);if(!ex||ex.count<count)cand.set(term,{term,count,len:term.length})}}const ranked=[...cand.values()].sort((a,b)=>(b.count-a.count)||(b.len-a.len));const codex=[];let dashLevel=3;for(const c of ranked){codex.push({dash:'-'.repeat(dashLevel),term:c.term});dashLevel++;if(dashLevel>24)break}const P=[],U=[];for(const[term,count] of Object.entries(termFreq)){if(/^[a-z]+$/.test(term))P.push([term,count]);else U.push([term,count])}P.sort((a,b)=>b[1]-a[1]);U.sort((a,b)=>b[1]-a[1]);const overflow=[...P.slice(25),...U.slice(25)].sort((a,b)=>b[1]-a[1]).slice(0,200).map(e=>e[0]);let cum=0;const ranges=[];rows.forEach(r=>{const start=cum;cum+=r.orig_bytes;const last=ranges[ranges.length-1];if(last&&last.type===r.chunk_type)last.end=cum;else ranges.push({type:r.chunk_type,start,end:cum})});const t25={};const t2lines=rows.map((r,idx)=>{const terms=(r.top_keywords||'').split(',').map(p=>p.split(':')[0]).filter(Boolean);terms.forEach(t=>{t25[t]=t25[t]||[];if(!t25[t].includes(idx))t25[t].push(idx)});return idx+'/'+r.chunk_type+'/'+terms.join(',')});const lines=[];lines.push('-Z4-id:'+repo+'-orig:'+job.orig_bytes+'-chunks:'+rows.length+'-');lines.push('-S-P:prose,C:code,L:legal,V:conv,G:config,D:data-');lines.push('-K'+codex.map(c=>c.dash+':'+c.term).join(',')+'-');lines.push('-0-'+rows.length+'-');lines.push(ranges.map(r=>r.type+':'+r.start+'-'+r.end).join(','));lines.push('-1-');lines.push('P/'+P.slice(0,25).map(([t,c])=>t+':'+c).join(','));lines.push('U/'+U.slice(0,25).map(([t,c])=>t+':'+c).join(','));lines.push('-1.5-'+overflow.length+'-');lines.push(overflow.join('|'));lines.push('-2-');lines.push(...t2lines);lines.push('-2.5-');for(const term of Object.keys(t25).sort())lines.push(term+'/'+t25[term].join('-'));const text=lines.join('\n');return{ok:true,repo:repoKey,branch,content:text,byte_size:byteLen(text),compressed_bytes_db:job.compressed_bytes,orig_bytes:job.orig_bytes,ratio:Number(job.ratio).toFixed(2)};
  }

  if (name === "decompress_chunk_v4") {
    const { owner, repo } = args;
    if (!owner) throw new Error("decompress_chunk_v4: owner required");
    if (!repo) throw new Error("decompress_chunk_v4: repo required");
    const owner=args.owner,repo=args.repo,branch=args.branch||'main',repoKey=owner+'/'+repo;let row;if(args.chunk_id){const rows=await dbAll(env.DB,'SELECT * FROM fsl_v4_chunks WHERE chunk_id=? LIMIT 1',[args.chunk_id]);row=rows[0]}else if(args.file_path){const rows=await dbAll(env.DB,'SELECT * FROM fsl_v4_chunks WHERE repo_key=? AND branch=? AND file_path=? LIMIT 1',[repoKey,branch,args.file_path]);row=rows[0]}if(!row)return{ok:false,error:'chunk not found'};return{ok:true,chunk_id:row.chunk_id,file_path:row.file_path,chunk_type:row.chunk_type,orig_bytes:row.orig_bytes,fingerprint:row.top_keywords,content_available:false,lossy:true,note:'Original content was never stored. This keyword fingerprint is all that exists for this chunk.'};
  }

  if (name === "compression_stats_v4") {
    const { owner, repo } = args;
    if (!owner) throw new Error("compression_stats_v4: owner required");
    if (!repo) throw new Error("compression_stats_v4: repo required");
    const owner=args.owner,repo=args.repo,branch=args.branch||'main',repoKey=owner+'/'+repo;const rows=await dbAll(env.DB,'SELECT * FROM fsl_v4_jobs WHERE repo_key=? AND branch=? ORDER BY created_at DESC LIMIT 1',[repoKey,branch]);if(!rows.length)return{ok:false,error:'no job found for this repo/branch'};return{ok:true,job:rows[0]};
  }

  throw new Error("Unknown tool: "+name);}
export default{async fetch(request,env,ctx){if(request.method==="OPTIONS")return new Response(null,{status:204,headers:CORS});const url=new URL(request.url);if(url.pathname==="/health")return Response.json({status:"ok",worker:WORKER_NAME,version:VERSION},{headers:CORS});if(request.method!=="POST")return new Response("not found",{status:404,headers:CORS});let body;try{body=await request.json();}catch{return errResp(null,-32700,"Parse error");}const{id,method,params}=body;if(method==="initialize")return rpc(id,{protocolVersion:"2024-11-05",capabilities:{tools:{}},serverInfo:{name:WORKER_NAME,version:VERSION}});if(method==="notifications/initialized")return new Response(null,{status:204,headers:CORS});if(method==="ping")return rpc(id,{});if(method==="tools/list")return rpc(id,{tools:TOOLS});if(method==="tools/call"){try{return tool(id,await handle(params?.name,params?.arguments||{},env,ctx));}catch(e){return errResp(id,-32603,"Tool error: "+e.message);}}return errResp(id,-32601,"Method not found: "+method);}};