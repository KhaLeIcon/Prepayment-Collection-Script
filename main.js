const fs = require('fs');
const path = require('path');
const xlsx = require('xlsx');
const axios = require('axios');
const yaml = require('js-yaml');

const isPkg = typeof process.pkg !== 'undefined';
const baseDir = isPkg
  ? path.dirname(process.execPath) // when running as .exe, resolve to the folder containing the exe
  : path.join(__dirname, '..');    // in dev, resolve to project root

// === LOGGING SETUP (tee BEFORE any output) ===
const LOG_DIR = process.env.LOG_DIR || path.join(baseDir, 'logs');
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });
const logFile = path.join(LOG_DIR, `run_${new Date().toISOString().slice(0,10)}.log`);
const logStream = fs.createWriteStream(logFile, { flags: 'a' });

function logLine(level, ...args) {
  const msg = args.map(a => (typeof a === 'string' ? a : JSON.stringify(a))).join(' ');
  const line = `${new Date().toISOString()} [${level}] ${msg}\n`;
  process.stdout.write(line);
  logStream.write(line);
}

// Tee console to file
console.log  = (...a) => logLine('INFO',  ...a);
console.warn = (...a) => logLine('WARN',  ...a);
console.error= (...a) => logLine('ERROR', ...a);

// First visible line
console.log('Logging to file:', logFile);

// Surface any silent crashes
process.on('unhandledRejection', (e) => console.error('UnhandledRejection:', e?.stack || e));
process.on('uncaughtException',  (e) => console.error('UncaughtException:',  e?.stack || e));
// === END LOGGING ===

// insert in to fix duplicate
const fsp = fs.promises;

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

async function moveFileSafe(src, dest) {
  ensureDir(path.dirname(dest));
  try {
    await fsp.rename(src, dest);              // fast path (same drive)
  } catch (e) {
    await fsp.copyFile(src, dest);            // fallback (cross-drive or locked)
    await fsp.unlink(src);
  }
}

/**
 * Archive all CSVs except the newest. If newestName is falsy, auto-detect newest by mtime.
 */
async function archiveOldCsvs(dirPath, newestName) {
  const archiveDir = path.join(dirPath, 'archive');
  ensureDir(archiveDir);

  const files = fs.existsSync(dirPath)
    ? fs.readdirSync(dirPath).filter(f => /\.csv$/i.test(f) && !f.startsWith('~$'))
    : [];

  if (!files.length) {
    console.log(`No CSVs found to archive in ${dirPath}`);
    return;
  }

  let keep = newestName;
  if (!keep) {
    // auto-pick newest by mtime if not provided
    const sorted = files
      .map(f => ({ f, ts: fs.statSync(path.join(dirPath, f)).mtime.getTime() }))
      .sort((a, b) => b.ts - a.ts);
    keep = sorted[0].f;
  }

  const olds = files.filter(f => f !== keep);
  for (const f of olds) {
    const ts = fs.statSync(path.join(dirPath, f)).mtime.getTime();
    const archivedName = `${path.parse(f).name}.${ts}.csv`;
    await moveFileSafe(path.join(dirPath, f), path.join(archiveDir, archivedName));
  }
}

async function archiveCsv(dirPath, fileName) {
  const archiveDir = path.join(dirPath, 'archive');
  ensureDir(archiveDir);
  console.log('Archiving to folder:', archiveDir);
  const ts = Date.now();
  const archivedName = `${path.parse(fileName).name}.${ts}.csv`;
  await moveFileSafe(path.join(dirPath, fileName), path.join(archiveDir, archivedName));
}

// ---- Pre-sweep helpers to ensure every company folder gets an archive/ created
function listSubdirs(root) {
  if (!fs.existsSync(root)) return [];
  return fs.readdirSync(root)
    .map(name => path.join(root, name))
    .filter(p => {
      try { return fs.statSync(p).isDirectory(); } catch { return false; }
    });
}

/** Archive all CSVs except the newest one in a given folder (auto-detect newest). */
async function archiveAllButNewest(dirPath) {
  const csvs = fs.existsSync(dirPath)
    ? fs.readdirSync(dirPath).filter(f => /\.csv$/i.test(f) && !f.startsWith('~$'))
    : [];
  if (!csvs.length) {
    console.log(`Pre-sweep: no CSVs in ${dirPath}`);
    return;
  }
  await archiveOldCsvs(dirPath, /* newestName */ null); // null => auto-pick newest
  console.log(`Pre-sweep archived older CSVs in ${dirPath}`);
}

/** Sweep output folder and archive older CSVs in every subfolder (MAC1, AEC1, …) */
async function sweepArchiveAll(outputfolder) {
  if (!fs.existsSync(outputfolder)) {
    console.warn('Output folder does not exist for sweep:', outputfolder);
    return;
  }
  const subdirs = listSubdirs(outputfolder);
  if (!subdirs.length) {
    console.log('No company subfolders found for sweep in', outputfolder);
    return;
  }
  console.log(`Pre-sweep: found ${subdirs.length} company folders`);
  for (const d of subdirs) {
    try {
      await archiveAllButNewest(d);
    } catch (e) {
      console.error('Pre-sweep failed for', d, e?.message || e);
    }
  }
}

// lazy-require to avoid top-level require crashes in packaged exe
async function runDataScript() {
  console.log('Running GetPrepaymentSOCollectionInvoiceList.js…');
  try {
    const fetchAllData = require('./GetPrepaymentSOCollectionInvoiceList');
    if (typeof fetchAllData !== 'function') {
      console.error('GetPrepaymentSOCollectionInvoiceList did not export a function');
      return;
    }
    await fetchAllData();
  } catch (e) {
    console.error('Data script crashed while loading or running:', e?.stack || e);
  }
}

// --- Retry + error helpers ---
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function formatAxiosError(err, url, body) {
  return {
    url,
    requestBody: body,
    name: err?.name,
    message: err?.message,
    code: err?.code,
    stack: err?.stack,
    response: err?.response ? {
      status: err.response.status,
      statusText: err.response.statusText,
      data: err.response.data,
      headers: err.response.headers
    } : undefined
  };
}

async function postJsonWithRetry(url, body, options = {}, retryCfg = { retries: 2, backoffMs: 1000 }) {
  const { retries, backoffMs } = retryCfg;
  let attempt = 0;
  while (attempt <= retries) {
    try {
      const resp = await axios.post(url, body, { timeout: 30000, ...options });
      return resp;
    } catch (err) {
      console.error('CPI POST failed:', formatAxiosError(err, url, body));
      attempt++;
      if (attempt > retries) throw err;
      const wait = backoffMs * attempt; // linear backoff
      console.warn(`Retrying in ${wait} ms (attempt ${attempt}/${retries})...`);
      await sleep(wait);
    }
  }
}

function getNewestCsvForCompany(outputfolder, companyCode) {
  const dirPath = path.join(outputfolder, String(companyCode ?? '').trim());
  if (!fs.existsSync(dirPath)) return { dirPath, newestPath: null, newestName: null };

  const files = fs.readdirSync(dirPath)
    .filter(f => /\.csv$/i.test(f) && !f.startsWith('~$'))
    .map(f => ({
      name: f,
      mtime: fs.statSync(path.join(dirPath, f)).mtime.getTime()
    }))
    .sort((a, b) => b.mtime - a.mtime);

  if (!files.length) return { dirPath, newestPath: null, newestName: null };

  const newestName = files[0].name;
  const newestPath = path.join(dirPath, newestName);
  return { dirPath, newestPath, newestName };
}

function parseCsv(csvPath) {
  const content = fs.readFileSync(csvPath, 'utf8');
  const lines = content.split(/\r?\n/).filter(l => l.trim());
  const rows = lines.slice(1);
  return rows.map(line => {
    const [SalesOrder, SalesOrderItem, YY1_SALESFORCEID_I_SDI, Customer, AccountingDocument, CompanyCode, FiscalYear] = line.split(',');
    return { SalesOrder, SalesOrderItem, YY1_SALESFORCEID_I_SDI, Customer, AccountingDocument, CompanyCode, FiscalYear };
  });
}

async function main() {
  console.log('Startup diagnostics →', { isPkg, cwd: process.cwd(), execPath: process.execPath, baseDir });

  await runDataScript();

  // load config.yaml
  const configPath = path.join(baseDir, 'config.yaml');
  console.log('Config path:', configPath);
  if (!fs.existsSync(configPath)) {
    console.error('config.yaml not found at', configPath);
    return;
  }
  let config;
  try {
    config = yaml.load(fs.readFileSync(configPath, 'utf8'));
  } catch (e) {
    console.error('Failed to parse config.yaml:', e?.stack || e);
    return;
  }

  // NOTE: adjust to match your real structure
  const { username, password } = (config.credentials?.cpi ?? config.credentials ?? {});
  const automationUrl = config.cpi?.endpoints?.PrepaymentAutomation || config.cpi?.endpoints?.prepaymentAutomation;
  if (!automationUrl) {
    console.error('Missing PrepaymentAutomation endpoint in config.yaml');
    return;
  }
  const auth = { username, password };

  // Read Workbook
  const workbookPath = path.join(baseDir, 'CompanyCodeList.xlsx');
  console.log('Company list path:', workbookPath);
  if (!fs.existsSync(workbookPath)) {
    console.error('CompanyCodeList.xlsx not found at', workbookPath);
    return;
  }
  const workbook = xlsx.readFile(workbookPath);
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const companies = xlsx.utils.sheet_to_json(sheet);
  console.log(`Loaded ${companies.length} companies from CompanyCodeList.xlsx`);

  const outputfolder = config.outputfolder || path.join(baseDir, 'output');
  console.log('Output folder:', outputfolder);

  // 🔹 NEW: archive older CSVs in ALL company folders up-front
  await sweepArchiveAll(outputfolder);

  // ───────────────────────────────────────────────────────────
  // SINGLE-PASS over companies (no nested second loop)
  // ───────────────────────────────────────────────────────────
  for (const row of companies) {
    const codeRaw = row.CompanyCode;
    const invoiceType = row.InvoiceType;
    const scenario = row.Scenario;

    // Filter once
    if (invoiceType !== 'EInvoice' || scenario === 'B') {
      console.log(`Skipping ${codeRaw} due to InvoiceType/Scenario → InvoiceType=${invoiceType}, Scenario=${scenario}`);
      continue;
    }

    const code = String(codeRaw ?? '').trim();
    if (!code) {
      console.warn('Empty CompanyCode row in CompanyCodeList.xlsx — skipping');
      continue;
    }

    console.log(`Processing CompanyCode: ${code}`);

    const { dirPath, newestPath: csvPath, newestName } = getNewestCsvForCompany(outputfolder, code);

    if (!csvPath) {
      console.warn(`No CSV found for ${code} in ${dirPath}`);
      continue;
    }

    // Archive all older CSVs first — keep only the newest visible
    try {
      await archiveOldCsvs(dirPath, newestName);
      console.log(`Archived older CSVs for ${code} (kept newest: ${newestName})`);
    } catch (e) {
      console.error(`Archive older CSVs failed for ${code}:`, e?.message || e);
    }

    // Parse newest; if unreadable, archive it and continue (unchanged behavior)
    let records = [];
    try {
      records = parseCsv(csvPath);
      console.log(`Parsed ${records.length} records from ${newestName}`);
    } catch (e) {
      console.error(`Parse failed for ${csvPath}:`, e?.message || e);
      // Keep behavior: archive the problematic file and continue with next company
      try {
        await archiveCsv(dirPath, newestName);
        console.log(`Archived unreadable CSV for ${code}: ${newestName}`);
      } catch (ae) {
        console.error(`Failed to archive unreadable CSV for ${code}:`, ae?.message || ae);
      }
      continue;
    }

    // 🔸 Change #1: Do NOT archive empty CSVs — keep for inspection
    if (!records.length) {
      console.warn(`CSV for ${code} has no items. Keeping it (no archive) for inspection.`);
      continue;
    }

    // 🔸 Change #2: Only archive after at least one successful POST
    let successCount = 0;

    // Post each record with retry + rich error logging
    for (const rec of records) {
      const payload = {
        Accountingdocument: rec.AccountingDocument,
        SFID_I: rec.YY1_SALESFORCEID_I_SDI,
        Customer: rec.Customer,
        SalesDocument: rec.SalesOrder,
        SalesDocumentItem: rec.SalesOrderItem,
        Companycode: rec.CompanyCode,
        Fiscalyear: rec.FiscalYear
      };

      try {
        const resp = await postJsonWithRetry(
          automationUrl,
          payload,
          { auth },
          { retries: 2, backoffMs: 1500 }
        );
        successCount++;
        console.log(`→ Posted ${rec.AccountingDocument} for ${rec.CompanyCode}: ${resp.status}`);
      } catch (err) {
        console.error(`✗ Error Posting ${rec.AccountingDocument}/${rec.CompanyCode}`, formatAxiosError(err, automationUrl, payload));
        // continue to next record
      }
    }

    // Archive the processed newest CSV only if something actually posted successfully
    try {
      if (successCount > 0) {
        await archiveCsv(dirPath, newestName);
        console.log(`Archived processed CSV for ${code} (posted ${successCount} records) -> ${path.join(dirPath, 'archive')}`);
      } else {
        console.warn(`No successful posts for ${code}. Keeping CSV (no archive) for investigation.`);
      }
    } catch (e) {
      console.error(`Failed to handle post-process archive for ${code}:`, e?.message || e);
    }
  }

  // Optional: post-run sweep (keeps folders tidy even if new CSVs appeared mid-run)
  // await sweepArchiveAll(outputfolder);

  console.log('All companies processed.');
}

main().catch(err => console.error('Fatal error in main():', err?.stack || err));
