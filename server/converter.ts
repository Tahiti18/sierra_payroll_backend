import Fastify from "fastify";
import cors from "@fastify/cors";
import multipart from "@fastify/multipart";
import { convertSierraToWBS, auditFromBuffers } from "./converter.js";

const app = Fastify({ logger: true });
await app.register(cors, { origin: true });
await app.register(multipart, { limits: { fileSize: 30 * 1024 * 1024 } });

// simple upload form (served at "/")
app.get("/", async (_, reply) => {
  reply.type("text/html").send(`
<!doctype html>
<html>
<head><meta charset="utf-8"><title>Payroll Converter</title></head>
<body style="font-family:system-ui;max-width:680px;margin:40px auto;line-height:1.4">
  <h1>Payroll Converter</h1>
  <form id="f" method="post" action="/api/convert" enctype="multipart/form-data">
    <div>
      <label>Sierra file (.xlsx):</label>
      <input type="file" name="sierra_file" accept=".xlsx" required />
    </div>
    <div style="margin-top:8px">
      <label>Roster (optional .xlsx):</label>
      <input type="file" name="roster_file" accept=".xlsx" />
    </div>
    <div style="margin-top:12px">
      <button type="submit">Convert to WBS</button>
    </div>
  </form>

  <hr style="margin:24px 0">
  <form id="a" method="post" action="/api/audit" enctype="multipart/form-data">
    <div><b>Audit (diagnose mismatches)</b></div>
    <div>
      <label>Sierra file (.xlsx):</label>
      <input type="file" name="sierra_file" accept=".xlsx" required />
    </div>
    <div style="margin-top:8px">
      <label>Roster (optional .xlsx):</label>
      <input type="file" name="roster_file" accept=".xlsx" />
    </div>
    <div style="margin-top:12px">
      <button type="submit">Build Audit CSV</button>
    </div>
  </form>
</body>
</html>`);
});

// convert endpoint
app.post("/api/convert", async (req, reply) => {
  const parts = req.parts();
  let sierra: Buffer | undefined, roster: Buffer | undefined;

  for await (const p of parts) {
    if (p.type === "file") {
      const buf = await p.toBuffer();
      if (p.fieldname === "sierra_file") sierra = buf;
      if (p.fieldname === "roster_file") roster = buf;
    }
  }
  if (!sierra) return reply.code(400).send({ error: "Missing sierra_file" });

  const out = await convertSierraToWBS(sierra, roster);
  reply
    .header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    .header("Content-Disposition", 'attachment; filename="Payroll_Output.xlsx"')
    .send(out);
});

// audit endpoint (returns CSV of diffs + grand totals)
app.post("/api/audit", async (req, reply) => {
  const parts = req.parts();
  let sierra: Buffer | undefined, roster: Buffer | undefined;

  for await (const p of parts) {
    if (p.type === "file") {
      const buf = await p.toBuffer();
      if (p.fieldname === "sierra_file") sierra = buf;
      if (p.fieldname === "roster_file") roster = buf;
    }
  }
  if (!sierra) return reply.code(400).send({ error: "Missing sierra_file" });

  const csv = await auditFromBuffers(sierra, roster);
  reply
    .header("Content-Type", "text/csv")
    .header("Content-Disposition", 'attachment; filename="Audit.csv"')
    .send(csv);
});

app.listen({ port: Number(process.env.PORT || 3000), host: "0.0.0.0" });
