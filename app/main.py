<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>Sierra Roofing - Payroll Automation System (Debug)</title>
  <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fortawesome/fontawesome-free@6.4.0/css/all.min.css">
  <script src="https://cdn.jsdelivr.net/npm/react@18/umd/react.production.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/react-dom@18/umd/react-dom.production.min.js"></script>
  <style>
    .sierra-blue { background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%); }
    .sierra-accent { color: #f59e0b; }
    .upload-zone { border: 2px dashed #d1d5db; transition: all 0.3s ease; }
    .upload-zone:hover, .upload-zone.dragover { border-color: #3b82f6; background-color: #eff6ff; }
    .progress-bar { transition: width 0.3s ease; }
    .modal-overlay { background-color: rgba(0, 0, 0, 0.5); backdrop-filter: blur(4px); }
    .btn-primary { background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%); transition: all 0.3s ease; }
    .btn-primary:hover { background: linear-gradient(135deg, #1e40af 0%, #2563eb 100%); transform: translateY(-1px); }
    .card-shadow { box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06); }
    .sierra-logo { font-family: 'Arial Black', sans-serif; text-shadow: 2px 2px 4px rgba(0,0,0,0.3); }
    .debug-console { background: #1f2937; color: #f3f4f6; font-family: 'Courier New', monospace; font-size: 12px; max-height: 300px; overflow-y: auto; }
    .status-online { color: #10b981; } .status-offline { color: #ef4444; } .status-testing { color: #f59e0b; }
    .debug-toggle { position: fixed; top: 20px; right: 20px; z-index: 1000; }
  </style>
</head>
<body class="bg-gray-50 min-h-screen">
  <div id="root"></div>

  <script>
    const { useState, useEffect, useRef } = React;

    // ---- Debug logger (in-memory, and on screen) ----
    const debugLog = (setLogs) => (message, data = null, type = 'info') => {
      const timestamp = new Date().toLocaleTimeString();
      const entry = { timestamp, message, data, type };
      console[type === 'error' ? 'error' : (type === 'success' ? 'log' : 'log')](
        `[${timestamp}] ${type.toUpperCase()}: ${message}`, data || ''
      );
      setLogs(prev => {
        const next = [...prev, entry];
        return next.length > 300 ? next.slice(-300) : next;
      });
    };

    // ---- Simple fetch with 120s timeout and clear error text ----
    async function timedFetch(url, options = {}, timeoutMs = 120000) {
      const ac = new AbortController();
      const t = setTimeout(() => ac.abort(), timeoutMs);
      try {
        const res = await fetch(url, { ...options, signal: ac.signal, mode: 'cors', credentials: 'omit' });
        return res;
      } finally {
        clearTimeout(t);
      }
    }

    const App = () => {
      const [backendStatus, setBackendStatus] = useState('testing'); // testing | online | offline
      const [currentAPI, setCurrentAPI] = useState(
        localStorage.getItem('sierra_backend_base') || 'https://web-production-d09f2.up.railway.app'
      );
      const [activeTab, setActiveTab] = useState('upload'); // upload | debug
      const [uploadedFile, setUploadedFile] = useState(null);
      const [processing, setProcessing] = useState(false);
      const [progress, setProgress] = useState(0);
      const [processedFile, setProcessedFile] = useState(null);
      const [error, setError] = useState('');
      const [success, setSuccess] = useState('');
      const [debugMode, setDebugMode] = useState(false);
      const [logs, setLogs] = useState([]);
      const log = debugLog(setLogs);
      const fileInputRef = useRef(null);

      // --- Save/load base URL ---
      const saveBase = () => {
        localStorage.setItem('sierra_backend_base', currentAPI.trim());
        log('Saved backend URL', currentAPI, 'success');
      };

      // --- Health check + template/roster checks ---
      const checkHealth = async () => {
        setBackendStatus('testing'); setError(''); setSuccess('');
        const url = currentAPI.replace(/\/+$/, '') + '/health';
        log(`Testing health ${url} ...`);
        try {
          const r = await timedFetch(url, { method: 'GET' }, 10000);
          const txt = await r.text();
          let ok = false;
          try { ok = r.ok && JSON.parse(txt).ok === true; } catch(_){ ok = r.ok; }
          if (ok) {
            setBackendStatus('online');
            log('Health OK', txt, 'success');
            setSuccess('Health OK');
          } else {
            setBackendStatus('offline');
            log('Health not OK', { status: r.status, txt }, 'error');
            setError(`Health not OK: ${r.status} ${r.statusText} — ${txt}`);
          }
        } catch (e) {
          setBackendStatus('offline');
          log('Health check failed', e.message, 'error');
          setError(`Health check failed: ${e.message}`);
        }
      };

      const checkTemplate = async () => {
        setError(''); setSuccess('');
        const url = currentAPI.replace(/\/+$/, '') + '/template-status';
        log(`GET ${url}`);
        try {
          const r = await timedFetch(url, { method: 'GET' }, 15000);
          const txt = await r.text();
          log('Template response', txt, r.ok ? 'success' : 'error');
          if (r.ok && /"found"/i.test(txt)) setSuccess('Template: found');
          else setError(`Template issue: ${txt || r.statusText}`);
        } catch (e) {
          setError(`Template check failed: ${e.message}`);
          log('Template check failed', e.message, 'error');
        }
      };

      const checkRoster = async () => {
        setError(''); setSuccess('');
        const url = currentAPI.replace(/\/+$/, '') + '/roster-status';
        log(`GET ${url}`);
        try {
          const r = await timedFetch(url, { method: 'GET' }, 15000);
          const txt = await r.text();
          log('Roster response', txt, r.ok ? 'success' : 'error');
          if (r.ok && /"found"/i.test(txt)) setSuccess('Roster: found');
          else setError('Roster: missing');
        } catch (e) {
          setError(`Roster check failed: ${e.message}`);
          log('Roster check failed', e.message, 'error');
        }
      };

      // --- Initial auto health check ---
      useEffect(() => { checkHealth(); /* eslint-disable-next-line */ }, []);

      // --- File handling ---
      const handleFileUpload = (file) => {
        if (!file) return;
        if (!/\.(xlsx|xls)$/i.test(file.name)) {
          const msg = 'Please upload a valid Excel file (.xlsx or .xls)';
          setError(msg); setSuccess('');
          log('File validation failed', msg, 'error');
          return;
        }
        setUploadedFile(file);
        setProcessedFile(null);
        setError('');
        setSuccess('File uploaded — ready to process.');
        log('File selected', { name: file.name, size: file.size }, 'success');
      };

      // --- Convert Sierra -> WBS ---
      const processPayroll = async () => {
        if (!uploadedFile) { setError('Please upload a file first'); return; }
        if (backendStatus !== 'online') { setError('Backend offline — fix health first'); return; }

        setProcessing(true); setProgress(0); setError(''); setSuccess('');
        log('Starting payroll processing', { api: currentAPI, file: uploadedFile.name });

        // Fake progress to show life in UI
        const tick = setInterval(() => setProgress(p => (p >= 90 ? 90 : p + 10)), 200);

        try {
          const fd = new FormData(); fd.append('file', uploadedFile);
          const url = currentAPI.replace(/\/+$/, '') + '/process-payroll';
          log(`POST ${url} (multipart/form-data)`);

          const r = await timedFetch(url, { method: 'POST', body: fd }, 120000);

          if (!r.ok) {
            const text = await r.text().catch(()=>'');
            log('Server error on process', { status: r.status, text }, 'error');
            throw new Error(`Server ${r.status}: ${text || r.statusText || 'Unknown error'}`);
          }

          const blob = await r.blob();
          const outName = `WBS_Payroll_${new Date().toISOString().split('T')[0]}.xlsx`;
          const link = URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = link; a.download = outName; document.body.appendChild(a); a.click(); a.remove();
          URL.revokeObjectURL(link);

          setProgress(100);
          setProcessedFile({ blob, filename: outName });
          setSuccess('Payroll processed — file downloaded.');
          log('Payroll processed OK', { filename: outName, size: blob.size }, 'success');

        } catch (e) {
          if (e.name === 'AbortError') {
            setError('Timed out after 120s — backend may be cold or busy.');
            log('Timeout converting', null, 'error');
          } else {
            setError(`Error processing payroll: ${e.message}`);
            log('Convert failed', e.message, 'error');
          }
        } finally {
          clearInterval(tick);
          setProcessing(false);
        }
      };

      // --- Download again button ---
      const downloadProcessedFile = () => {
        if (!processedFile) return;
        const url = URL.createObjectURL(processedFile.blob);
        const a = document.createElement('a');
        a.href = url; a.download = processedFile.filename;
        document.body.appendChild(a); a.click(); a.remove();
        URL.revokeObjectURL(url);
        log('File re-downloaded', processedFile.filename, 'success');
      };

      // --- UI ---
      return React.createElement(
        React.Fragment,
        null,
        React.createElement("button", {
          onClick: () => setDebugMode(!debugMode),
          className: "debug-toggle bg-gray-800 text-white p-2 rounded-lg shadow-lg",
          title: "Toggle Debug Mode"
        }, React.createElement("i", { className: "fas fa-bug" })),

        React.createElement("header", { className: "sierra-blue text-white shadow-lg" },
          React.createElement("div", { className: "max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6" },
            React.createElement("div", { className: "flex items-center justify-between" },
              React.createElement("div", { className: "flex items-center space-x-4" },
                React.createElement("div", { className: "bg-white bg-opacity-20 p-3 rounded-lg" },
                  React.createElement("i", { className: "fas fa-hard-hat text-2xl sierra-accent" })
                ),
                React.createElement("div", null,
                  React.createElement("h1", { className: "sierra-logo text-3xl font-bold" }, "SIERRA ROOFING"),
                  React.createElement("p", { className: "text-blue-200" }, "Payroll Automation System - Debug Mode")
                )
              ),
              React.createElement("div", { className: "hidden md:flex items-center space-x-4" },
                React.createElement("div", { className: "text-right" },
                  React.createElement("p", { className: "text-sm text-blue-200" }, "Backend Status"),
                  React.createElement("p", {
                    className: `font-semibold ${
                      backendStatus === 'online' ? 'status-online' :
                      backendStatus === 'testing' ? 'status-testing' : 'status-offline'}`
                  },
                  React.createElement("i", {
                    className: `fas ${
                      backendStatus === 'online' ? 'fa-circle' :
                      backendStatus === 'testing' ? 'fa-spinner fa-spin' : 'fa-times-circle'
                    } text-xs mr-1`
                  }),
                  backendStatus === 'online' ? 'Online' : backendStatus === 'testing' ? 'Testing...' : 'Offline'),
                  backendStatus === 'offline' && React.createElement("button", {
                    onClick: checkHealth,
                    className: "text-xs bg-white bg-opacity-20 px-2 py-1 rounded mt-1"
                  }, "Retry Connection")
                )
              )
            )
          )
        ),

        backendStatus === 'offline' && React.createElement("div", { className: "bg-red-600 text-white px-4 py-3" },
          React.createElement("div", { className: "max-w-7xl mx-auto flex items-center justify-between" },
            React.createElement("div", { className: "flex items-center" },
              React.createElement("i", { className: "fas fa-exclamation-triangle mr-2" }),
              React.createElement("span", null, "Backend connection failed. Check CORS or update the backend URL.")
            ),
            React.createElement("button", {
              onClick: () => setDebugMode(true),
              className: "bg-white bg-opacity-20 px-3 py-1 rounded text-sm"
            }, "View Debug Info")
          )
        ),

        React.createElement("nav", { className: "bg-white shadow-sm border-b" },
          React.createElement("div", { className: "max-w-7xl mx-auto px-4 sm:px-6 lg:px-8" },
            React.createElement("div", { className: "flex space-x-8" },
              ['upload','debug'].map(id =>
                React.createElement("button", {
                  key: id,
                  onClick: () => setActiveTab(id),
                  className: `py-4 px-2 border-b-2 font-medium text-sm ${
                    activeTab === id ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'}`
                },
                React.createElement("i", { className: `${id==='upload'?'fas fa-upload':'fas fa-bug'} mr-2` }),
                id === 'upload' ? 'Process Payroll' : 'Debug Info')
              )
            )
          )
        ),

        React.createElement("main", {
          className: "max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8",
          style: { paddingBottom: debugMode ? '320px' : '2rem' }
        },
          error && React.createElement("div", { className: "mb-6 bg-red-50 border border-red-200 rounded-md p-4" },
            React.createElement("div", { className: "flex" },
              React.createElement("i", { className: "fas fa-exclamation-circle text-red-400 mr-3 mt-0.5" }),
              React.createElement("div", null,
                React.createElement("p", { className: "text-red-700" }, error),
                error.includes('CORS') && React.createElement("div", { className: "mt-2 text-sm text-red-600" },
                  React.createElement("p", null, React.createElement("strong", null, "CORS Issue Troubleshooting:")),
                  React.createElement("ul", { className: "list-disc ml-5 mt-1" },
                    React.createElement("li", null, "Ensure backend sends CORS headers"),
                    React.createElement("li", null, "Allow requests from this origin"),
                    React.createElement("li", null, "Open DevTools console for exact error text")
                  )
                )
              )
            )
          ),

          success && React.createElement("div", { className: "mb-6 bg-green-50 border border-green-200 rounded-md p-4" },
            React.createElement("div", { className: "flex" },
              React.createElement("i", { className: "fas fa-check-circle text-green-400 mr-3 mt-0.5" }),
              React.createElement("p", { className: "text-green-700" }, success)
            )
          ),

          activeTab === 'upload' && React.createElement(React.Fragment, null,
            React.createElement("div", { className: "bg-white rounded-lg shadow card-shadow p-6 mb-6" },
              React.createElement("h2", { className: "text-xl font-semibold text-gray-900 mb-4" },
                React.createElement("i", { className: "fas fa-server mr-2 text-blue-600" }),
                "Backend Connection"
              ),
              React.createElement("div", { className: "grid grid-cols-1 md:grid-cols-3 gap-4" },
                React.createElement("div", { className: "md:col-span-2" },
                  React.createElement("label", { className: "block text-sm text-gray-600 mb-1" }, "Backend URL"),
                  React.createElement("input", {
                    value: currentAPI, onChange: e => setCurrentAPI(e.target.value),
                    className: "w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500",
                    placeholder: "https://your-app.up.railway.app"
                  })
                ),
                React.createElement("div", { className: "flex items-end" },
                  React.createElement("button", { onClick: saveBase, className: "bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg font-medium" },
                    React.createElement("i", { className: "fas fa-save mr-2" }), "Save"
                  )
                )
              ),
              React.createElement("div", { className: "mt-4 flex items-center gap-2" },
                React.createElement("button", { onClick: checkHealth, className: "bg-blue-600 hover:bg-blue-700 text-white px-3 py-2 rounded" },
                  React.createElement("i", { className: "fas fa-heartbeat mr-2" }), "Health"
                ),
                React.createElement("button", { onClick: checkTemplate, className: "bg-gray-700 hover:bg-gray-800 text-white px-3 py-2 rounded" },
                  React.createElement("i", { className: "fas fa-file-excel mr-2" }), "Template"
                ),
                React.createElement("button", { onClick: checkRoster, className: "bg-gray-700 hover:bg-gray-800 text-white px-3 py-2 rounded" },
                  React.createElement("i", { className: "fas fa-users mr-2" }), "Roster"
                ),
                React.createElement("span", {
                  className: `${
                    backendStatus === 'online' ? 'text-green-600' :
                    backendStatus === 'testing' ? 'text-yellow-600' : 'text-red-600'
                  } ml-2`
                }, `Status: ${backendStatus}`)
              )
            ),

            React.createElement("div", { className: "bg-white rounded-lg shadow card-shadow p-6 mb-6" },
              React.createElement("h2", { className: "text-xl font-semibold text-gray-900 mb-4" },
                React.createElement("i", { className: "fas fa-upload mr-2 text-blue-600" }),
                "Upload Sierra Payroll File"
              ),
              React.createElement("div", {
                className: "upload-zone rounded-lg p-8 text-center cursor-pointer",
                onClick: () => fileInputRef.current && fileInputRef.current.click(),
                onDragOver: e => { e.preventDefault(); e.currentTarget.classList.add('dragover'); },
                onDragLeave: e => { e.preventDefault(); e.currentTarget.classList.remove('dragover'); },
                onDrop: e => {
                  e.preventDefault(); e.currentTarget.classList.remove('dragover');
                  const files = Array.from(e.dataTransfer.files);
                  if (files.length) handleFileUpload(files[0]);
                }
              },
                React.createElement("input", {
                  ref: fileInputRef, type: "file", accept: ".xlsx,.xls", className: "hidden",
                  onChange: e => handleFileUpload(e.target.files[0])
                }),
                uploadedFile
                  ? React.createElement("div", null,
                      React.createElement("i", { className: "fas fa-file-excel text-4xl text-green-500 mb-3" }),
                      React.createElement("p", { className: "text-lg font-medium text-gray-900 mb-2" }, uploadedFile.name),
                      React.createElement("p", { className: "text-sm text-gray-500" }, `Size: ${(uploadedFile.size/1024/1024).toFixed(2)} MB`),
                      React.createElement("button", {
                        onClick: e => { e.stopPropagation(); setUploadedFile(null); setProcessedFile(null); setSuccess(''); },
                        className: "mt-3 text-red-600 hover:text-red-800"
                      }, React.createElement("i", { className: "fas fa-times mr-1" }), "Remove File")
                    )
                  : React.createElement("div", null,
                      React.createElement("i", { className: "fas fa-cloud-upload-alt text-4xl text-gray-400 mb-3" }),
                      React.createElement("p", { className: "text-lg font-medium text-gray-900 mb-2" }, "Drop your Excel file here, or click to browse"),
                      React.createElement("p", { className: "text-sm text-gray-500" }, "Supports .xlsx and .xls files up to 10MB")
                    )
              )
            ),

            React.createElement("div", { className: "bg-white rounded-lg shadow card-shadow p-6" },
              React.createElement("h2", { className: "text-xl font-semibold text-gray-900 mb-4" },
                React.createElement("i", { className: "fas fa-cog mr-2 text-blue-600" }),
                "Process Payroll Data"
              ),
              processing && React.createElement("div", { className: "mb-4" },
                React.createElement("div", { className: "flex justify-between text-sm text-gray-600 mb-1" },
                  React.createElement("span", null, "Processing payroll data..."),
                  React.createElement("span", null, `${progress}%`)
                ),
                React.createElement("div", { className: "w-full bg-gray-200 rounded-full h-2" },
                  React.createElement("div", {
                    className: "progress-bar bg-blue-600 h-2 rounded-full",
                    style: { width: `${progress}%` }
                  })
                )
              ),
              React.createElement("div", { className: "flex items-center gap-3" },
                React.createElement("button", {
                  onClick: processPayroll,
                  disabled: !uploadedFile || processing || backendStatus !== 'online',
                  className: "btn-primary text-white px-6 py-3 rounded-lg font-medium disabled:opacity-50 disabled:cursor-not-allowed"
                },
                  processing
                    ? React.createElement(React.Fragment, null,
                        React.createElement("i", { className: "fas fa-spinner fa-spin mr-2" }), "Processing..."
                      )
                    : (backendStatus !== 'online'
                        ? React.createElement(React.Fragment, null,
                            React.createElement("i", { className: "fas fa-exclamation-triangle mr-2" }), "Backend Offline"
                          )
                        : React.createElement(React.Fragment, null,
                            React.createElement("i", { className: "fas fa-play mr-2" }), "Process Payroll"
                          )
                      )
                ),
                processedFile && React.createElement("button", {
                  onClick: downloadProcessedFile,
                  className: "bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded-lg font-medium"
                }, React.createElement("i", { className: "fas fa-download mr-2" }), "Download Again")
              )
            )
          ),

          activeTab === 'debug' && React.createElement(React.Fragment, null,
            React.createElement("div", { className: "bg-white rounded-lg shadow card-shadow p-6 mb-6" },
              React.createElement("h2", { className: "text-xl font-semibold text-gray-900 mb-4" },
                React.createElement("i", { className: "fas fa-bug mr-2 text-blue-600" }), "Debug Information"
              ),
              React.createElement("div", { className: "grid grid-cols-1 md:grid-cols-2 gap-6" },
                React.createElement("div", null,
                  React.createElement("h3", { className: "font-medium text-gray-900 mb-2" }, "System Status"),
                  React.createElement("div", { className: "space-y-2 text-sm" },
                    React.createElement("div", { className: "flex justify-between" },
                      React.createElement("span", null, "Frontend Domain:"),
                      React.createElement("span", { className: "font-mono" }, window.location.origin)
                    ),
                    React.createElement("div", { className: "flex justify-between" },
                      React.createElement("span", null, "Current API:"),
                      React.createElement("span", { className: "font-mono break-all" }, currentAPI)
                    ),
                    React.createElement("div", { className: "flex justify-between" },
                      React.createElement("span", null, "Backend Status:"),
                      React.createElement("span", { className: backendStatus==='online'?'text-green-600':backendStatus==='testing'?'text-yellow-600':'text-red-600' }, backendStatus)
                    )
                  )
                ),
                React.createElement("div", null,
                  React.createElement("h3", { className: "font-medium text-gray-900 mb-2" }, "Quick Actions"),
                  React.createElement("div", { className: "space-x-2" },
                    React.createElement("button", { onClick: checkHealth, className: "bg-blue-600 text-white px-3 py-1 rounded text-sm" }, "Health"),
                    React.createElement("button", { onClick: checkTemplate, className: "bg-gray-700 text-white px-3 py-1 rounded text-sm" }, "Template"),
                    React.createElement("button", { onClick: checkRoster, className: "bg-gray-700 text-white px-3 py-1 rounded text-sm" }, "Roster"),
                    React.createElement("button", { onClick: () => setLogs([]), className: "bg-red-600 text-white px-3 py-1 rounded text-sm" }, "Clear Logs")
                  )
                )
              )
            ),
            React.createElement("div", { className: "bg-white rounded-lg shadow card-shadow p-6" },
              React.createElement("h3", { className: "font-medium text-gray-900 mb-4" }, "Recent Debug Logs"),
              React.createElement("div", { className: "debug-console p-4 rounded" },
                logs.length
                  ? logs.slice(-150).map((logItem, i) =>
                      React.createElement("div", { key: i, className:
                        logItem.type==='error'?'text-red-400':
                        logItem.type==='success'?'text-green-400':
                        logItem.type==='request'?'text-yellow-400':'text-gray-300'
                      },
                        React.createElement("span", { className: "text-gray-500" }, `[${logItem.timestamp}] `),
                        logItem.message,
                        logItem.data ? React.createElement("pre", { className: "text-xs mt-1 text-gray-400" },
                          typeof logItem.data === 'string' ? logItem.data : JSON.stringify(logItem.data, null, 2)
                        ) : null
                      )
                    )
                  : React.createElement("div", { className: "text-gray-500" }, "No debug logs yet...")
              )
            )
          )
        ),

        React.createElement("footer", { className: "bg-gray-800 text-white mt-12" },
          React.createElement("div", { className: "max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8" },
            React.createElement("div", { className: "flex flex-col md:flex-row justify-between items-center" },
              React.createElement("div", { className: "flex items-center space-x-4 mb-4 md:mb-0" },
                React.createElement("div", { className: "bg-white bg-opacity-10 p-2 rounded" },
                  React.createElement("i", { className: "fas fa-hard-hat text-lg sierra-accent" })
                ),
                React.createElement("div", null,
                  React.createElement("p", { className: "font-semibold" }, "Sierra Roofing Payroll System - Debug Mode"),
                  React.createElement("p", { className: "text-sm text-gray-400" }, "Enhanced Debugging • API Testing • CORS Detection")
                )
              ),
              React.createElement("div", { className: "text-center md:text-right" },
                React.createElement("p", { className: "text-sm text-gray-400" }, "Debug Version • Backend: Railway • Frontend: Netlify"),
                React.createElement("p", { className: "text-xs text-gray-500 mt-1" }, "Timeout guard • Clear error text • Download-safe blob()")
              )
            )
          )
        ),

        debugMode && React.createElement("div", { className: "fixed bottom-0 left-0 right-0 z-50 bg-gray-900 border-t border-gray-700" },
          React.createElement("div", { className: "p-4" },
            React.createElement("div", { className: "flex justify-between items-center mb-2" },
              React.createElement("h3", { className: "text-white font-semibold" }, "Debug Console"),
              React.createElement("div", { className: "space-x-2" },
                React.createElement("button", { onClick: () => setLogs([]), className: "text-xs bg-gray-700 text-white px-2 py-1 rounded" }, "Clear"),
                React.createElement("button", { onClick: checkHealth, className: "text-xs bg-blue-600 text-white px-2 py-1 rounded" }, "Health")
              )
            ),
            React.createElement("div", { className: "debug-console p-2 rounded" },
              logs.length
                ? logs.slice(-80).map((logItem,i) =>
                    React.createElement("div", { key: i, className:
                      logItem.type==='error'?'text-red-400':
                      logItem.type==='success'?'text-green-400':
                      logItem.type==='request'?'text-yellow-400':'text-gray-300'
                    },
                      React.createElement("span", { className: "text-gray-500" }, `[${logItem.timestamp}] `),
                      logItem.message,
                      logItem.data ? React.createElement("pre", { className: "text-xs mt-1 text-gray-400" },
                        typeof logItem.data === 'string' ? logItem.data : JSON.stringify(logItem.data, null, 2)
                      ) : null
                    )
                  )
                : React.createElement("div", { className: "text-gray-500" }, "No debug logs yet...")
            )
          )
        )
      );
    };

    ReactDOM.render(React.createElement(App), document.getElementById('root'));
  </script>
</body>
</html>
