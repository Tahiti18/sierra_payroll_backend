<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Sierra → WBS Payroll (One-Click)</title>
  <style>
    :root{
      --bg:#0b1220; --card:#0f172a; --txt:#e2e8f0; --muted:#94a3b8;
      --border:#334155; --primary:#3b82f6; --accent:#0ea5e9;
      --ok:#10b981; --warn:#f59e0b; --err:#ef4444;
      --chip:#1f2937; --zone:#0b1020;
    }
    *{box-sizing:border-box}
    body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:var(--bg);color:var(--txt)}
    a{color:var(--accent);text-decoration:none}
    .wrap{max-width:980px;margin:36px auto;padding:20px}
    .card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:18px;margin-bottom:16px}
    h1{margin:0 0 6px;font-size:22px}
    p{margin:0 0 14px;color:var(--muted)}
    label{display:block;margin:8px 0 6px;color:var(--muted);font-size:13px}
    input[type=text]{width:100%;background:var(--zone);border:1px solid var(--border);border-radius:8px;padding:10px;color:var(--txt)}
    .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
    .btn{background:var(--primary);color:#fff;border:none;border-radius:8px;padding:10px 14px;font-weight:600;cursor:pointer}
    .btn.alt{background:var(--accent)}
    .btn.warn{background:var(--warn);color:#111}
    .btn:disabled{opacity:.55;cursor:not-allowed}
    .zone{border:2px dashed var(--border);background:var(--zone);border-radius:12px;padding:18px;text-align:center}
    .hint{font-size:12px;color:var(--muted);margin-top:6px}
    .status{margin-top:10px;font-size:14px}
    .ok{color:var(--ok)} .err{color:var(--err)} .warn{color:var(--warn)}
    .pill{display:inline-block;background:var(--chip);border:1px solid var(--border);border-radius:999px;padding:4px 8px;font-size:12px;color:var(--muted)}
    .stack{display:flex;flex-direction:column;gap:10px}
    .log{background:#0a0f1a;border:1px solid var(--border);border-radius:10px;padding:10px;min-height:120px;max-height:260px;overflow:auto;font-family:ui-monospace,Consolas,Menlo,monospace;font-size:12px;white-space:pre-wrap}
    .kbd{background:#0b1020;border:1px solid var(--border);border-radius:6px;padding:2px 6px;font-family:ui-monospace,Consolas,Menlo,monospace}
    .grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    @media (max-width:780px){ .grid{grid-template-columns:1fr} }
  </style>
</head>
<body>
  <div class="wrap">

    <!-- Header -->
    <div class="card">
      <h1>Sierra → WBS Payroll (One-Click)</h1>
      <p>Upload the weekly Sierra Excel. The server uses the pinned WBS template and returns a WBS-formatted file.</p>
      <div class="grid">
        <div>
          <label for="base">Backend URL</label>
          <input id="base" type="text" placeholder="https://your-railway-app.up.railway.app"
                 value="https://web-production-d09f2.up.railway.app"/>
          <div class="hint">Change if you redeploy somewhere else. Saved to your browser.</div>
        </div>
        <div>
          <label>&nbsp;</label>
          <div class="row">
            <button id="saveBase" class="btn alt">Save URL</button>
            <span id="baseStatus" class="pill">unsaved</span>
          </div>
        </div>
      </div>
    </div>

    <!-- Quick Checks -->
    <div class="card">
      <h1>Quick checks</h1>
      <div class="row">
        <button id="checkHealth" class="btn">Health</button>
        <button id="checkTemplate" class="btn">Template status</button>
        <button id="checkRoster" class="btn">Roster status</button>
        <button id="clearLog" class="btn warn">Clear log</button>
      </div>
      <div id="checkMsg" class="status"></div>
    </div>

    <!-- Converter -->
    <div class="card">
      <h1>Convert</h1>
      <div class="zone" id="dropzone">
        <div><strong>Drag & drop</strong> your Sierra file here or pick below.</div>
        <div class="hint">Accepted: <span class="kbd">.xlsx</span> or <span class="kbd">.xls</span></div>
      </div>
      <div class="row" style="margin-top:10px">
        <input id="file" type="file" accept=".xlsx,.xls"/>
        <button id="convert" class="btn">Convert to WBS</button>
      </div>
      <div id="convertMsg" class="status"></div>
    </div>

    <!-- Console -->
    <div class="card">
      <h1>Log console</h1>
      <div id="log" class="log"></div>
      <div class="hint">Shows each step with timestamps, response codes, and error text.</div>
    </div>

    <!-- Help -->
    <div class="card">
      <h1>Tips</h1>
      <div class="stack">
        <div>Make sure your Railway backend has the template at repo root named <span class="kbd">wbs_template.xlsx</span>.</div>
        <div>Optional roster file at repo root: <span class="kbd">roster.xlsx</span> or <span class="kbd">roster.csv</span>.</div>
        <div>Procfile should be: <span class="kbd">web: uvicorn server.main:app --host 0.0.0.0 --port $PORT</span></div>
        <div>CLI test (replace file path as needed):<br/>
          <span class="kbd">curl -f -X POST -F "file=@'Sierra Payroll 9_12_25 for Marwan.xlsx'" $(BASE)/process-payroll -o WBS_out.xlsx -D -</span>
        </div>
      </div>
    </div>

  </div>

  <script>
    // -------------- Small util --------------
    const $ = (id)=>document.getElementById(id);
    const logEl = $("log");
    function ts(){ return new Date().toLocaleTimeString(); }
    function log(line){ logEl.textContent += `[${ts()}] ${line}\n`; logEl.scrollTop = logEl.scrollHeight; }
    function setMsg(el, text, cls){
      el.textContent = text;
      el.className = "status " + (cls || "");
    }
    function saveBaseUrl(){
      const url = $("base").value.trim();
      localStorage.setItem("wbs_api_base", url);
      $("baseStatus").textContent = "saved";
      log(`Saved BASE: ${url}`);
    }
    function loadBaseUrl(){
      const s = localStorage.getItem("wbs_api_base");
      if (s){ $("base").value = s; $("baseStatus").textContent = "saved"; }
    }
    function getBase(){ return $("base").value.trim().replace(/\/+$/,""); }

    // -------------- Startup --------------
    loadBaseUrl();
    $("saveBase").onclick = saveBaseUrl;

    // -------------- Dropzone --------------
    const dz = $("dropzone");
    dz.addEventListener("dragover", (e)=>{ e.preventDefault(); dz.style.borderColor="#3b82f6"; });
    dz.addEventListener("dragleave", ()=>{ dz.style.borderColor="var(--border)"; });
    dz.addEventListener("drop", (e)=>{
      e.preventDefault();
      dz.style.borderColor="var(--border)";
      const f = e.dataTransfer.files && e.dataTransfer.files[0];
      if (f) { $("file").files = e.dataTransfer.files; log(`Dropped file: ${f.name} (${f.type||"unknown"})`); }
    });

    // -------------- Quick checks --------------
    $("checkHealth").onclick = async ()=>{
      const base = getBase();
      setMsg($("checkMsg"), "Checking health…"); log(`GET ${base}/health`);
      try{
        const r = await fetch(`${base}/health`, {mode:"cors", credentials:"omit"});
        const txt = await r.text(); let body = txt; try{body = JSON.stringify(JSON.parse(txt));}catch(_){}
        log(`→ ${r.status} ${r.statusText} ${body}`);
        setMsg($("checkMsg"), r.ok ? "Health OK" : `Health not OK: ${r.status}`, r.ok ? "ok":"warn");
      }catch(e){
        setMsg($("checkMsg"), `Health check failed: ${e.message}`, "err");
        log(`× Health error: ${e.message}`);
      }
    };

    $("checkTemplate").onclick = async ()=>{
      const base = getBase();
      setMsg($("checkMsg"), "Checking template status…"); log(`GET ${base}/template-status`);
      try{
        const r = await fetch(`${base}/template-status`, {mode:"cors", credentials:"omit"});
        const txt = await r.text(); let body = txt; try{body = JSON.stringify(JSON.parse(txt));}catch(_){}
        log(`→ ${r.status} ${r.statusText} ${body}`);
        const ok = r.ok && /"found"/i.test(body);
        setMsg($("checkMsg"), ok ? "Template: found" : `Template issue: ${body}`, ok ? "ok":"warn");
      }catch(e){
        setMsg($("checkMsg"), `Template check failed: ${e.message}`, "err");
        log(`× Template error: ${e.message}`);
      }
    };

    $("checkRoster").onclick = async ()=>{
      const base = getBase();
      setMsg($("checkMsg"), "Checking roster status…"); log(`GET ${base}/roster-status`);
      try{
        const r = await fetch(`${base}/roster-status`, {mode:"cors", credentials:"omit"});
        const txt = await r.text(); let body = txt; try{body = JSON.stringify(JSON.parse(txt));}catch(_){}
        log(`→ ${r.status} ${r.statusText} ${body}`);
        const ok = r.ok && /"found"/i.test(body);
        setMsg($("checkMsg"), ok ? "Roster: found" : `Roster: missing`, ok ? "ok":"warn");
      }catch(e){
        setMsg($("checkMsg"), `Roster check failed: ${e.message}`, "err");
        log(`× Roster error: ${e.message}`);
      }
    };

    $("clearLog").onclick = ()=>{ logEl.textContent = ""; };

    // -------------- Convert --------------
    $("convert").onclick = async ()=>{
      const base = getBase();
      const f = $("file").files[0];
      if(!f){ setMsg($("convertMsg"), "Choose a Sierra .xlsx first.", "err"); return; }
      if(!/\.(xlsx|xls)$/i.test(f.name)){ setMsg($("convertMsg"), "Unsupported file type. Use .xlsx or .xls.", "err"); return; }

      $("convert").disabled = true;
      setMsg($("convertMsg"), "Uploading and converting…");
      log(`POST ${base}/process-payroll (file=${f.name})`);

      const fd = new FormData(); fd.append("file", f);

      // Timeout guard
      const ac = new AbortController();
      const timer = setTimeout(()=>ac.abort(), 120000);

      try{
        const r = await fetch(`${base}/process-payroll`, {
          method:"POST",
          body: fd,
          mode:"cors",
          credentials:"omit",
          signal: ac.signal,
        });
        clearTimeout(timer);

        if (!r.ok){
          let detail = "";
          try{ detail = await r.text(); }catch(_){}
          log(`→ ERROR ${r.status}: ${detail || r.statusText}`);
          throw new Error(`Server ${r.status}: ${detail || r.statusText || "Unknown error"}`);
        }

        // success -> download
        const blob = await r.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        const outName = `WBS_Payroll_${new Date().toISOString().slice(0,10)}.xlsx`;
        a.href = url; a.download = outName; document.body.appendChild(a); a.click(); a.remove();
        URL.revokeObjectURL(url);

        setMsg($("convertMsg"), "Success — WBS file downloaded.", "ok");
        log(`→ OK 200: downloaded ${outName}`);
      }catch(e){
        clearTimeout(timer);
        if (e.name === "AbortError"){
          setMsg($("convertMsg"), "Timed out after 120s. Backend may be asleep or overloaded.", "err");
          log("× Timeout: request aborted at 120s");
        }else{
          setMsg($("convertMsg"), "Conversion failed: " + (e.message||e), "err");
          log(`× Convert error: ${e.message||e}`);
        }
      }finally{
        $("convert").disabled = false;
      }
    };
  </script>
</body>
</html>
