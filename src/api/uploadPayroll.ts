// src/api/uploadPayroll.ts
const API_BASE =
  import.meta?.env?.VITE_API_BASE ||
  (process.env.NODE_ENV === "production"
    ? "https://web-production-d09f2.up.railway.app"
    : "http://localhost:8000");

export async function uploadPayroll(file: File) {
  if (!file) throw new Error("No file provided");

  // Build multipart/form-data. The key MUST be exactly "file".
  const form = new FormData();
  form.append("file", file, file.name);

  const res = await fetch(`${API_BASE}/process-payroll`, {
    method: "POST",
    body: form,             // IMPORTANT: do NOT set Content-Type header
    credentials: "omit",    // or "include" if you rely on cookies
    redirect: "follow",
  });

  // FastAPI returns non-2xx for errors (422/500). Surface the message.
  const contentType = res.headers.get("content-type") || "";
  let payload: any = null;
  try {
    payload = contentType.includes("application/json") ? await res.json() : await res.text();
  } catch {
    /* ignore parse errors */
  }

  if (!res.ok) {
    const detail =
      (payload && (payload.detail || payload.message)) ||
      (typeof payload === "string" ? payload : `HTTP ${res.status}`);
    throw new Error(detail);
  }

  // On success your backend should return an Excel blob or a JSON link.
  // Handle both: if it's a file, force download; if JSON, return it.
  if (contentType.includes(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      )) {
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    return { type: "blob", url, filename: `WBS_${new Date().toISOString().slice(0,10)}.xlsx` };
  }

  // Otherwise assume JSON with a download URL or message.
  return { type: "json", data: payload };
}
