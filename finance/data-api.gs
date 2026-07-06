// ─────────────────────────────────────────────────────────────────────────────
// FINANCIAL DATA API — _Banks&Cards V3   (fixed 2026-07-06)
// ─────────────────────────────────────────────────────────────────────────────
// WHAT CHANGED vs the previous version:
//   • Classification aligned with weekly-digest.gs. _ExpenseLog col D holds raw
//     bank type codes (Sale, ACH_CREDIT, …) — the old `Type === 'Income'` test
//     never matched, and the deployed variant's amount-sign fallback was
//     inverted (sheet convention is NEGATIVE = money out). Category decides
//     transfers / reimbursables / income; sign decides the rest.
//   • Transfers & CC payments excluded from monthly income/expense series
//     (they double-counted); work reimbursements excluded from both and
//     reported as their own net figure.
//   • Optional shared-secret: set API_KEY in Project Settings → Script
//     properties, then call the web app with ?key=<value>. If no property is
//     set, the check is skipped (backwards compatible).
//
// HOW TO DEPLOY:
//   1. Open _Banks&Cards V3 → Extensions → Apps Script
//   2. Replace the data-api file contents with this file, save
//   3. Deploy → Manage deployments → Edit (pencil) → Version: New version → Deploy
//      (editing the existing deployment keeps the same URL)
//   4. If you set API_KEY, update the artifact's CONFIG.API_URL to append ?key=…
// ─────────────────────────────────────────────────────────────────────────────

const API_CFG = {
  LOG_SHEET: '_ExpenseLog',

  // _ExpenseLog columns (1-based)
  COL_DATE:      1,
  COL_DESC:      2,
  COL_AMOUNT:    3,  // signed: NEGATIVE = money out, POSITIVE = money in
  COL_TYPE:      4,  // raw bank type code — informational only
  COL_CATEGORY:  5,
  COL_ACCOUNT:   6,

  // Case-insensitive substrings matched against the reconciled category.
  TRANSFER_CATS: ['transfer', 'cc payment', 'payment', 'investment', 'hsa'],
  REIMB_CATS:    ['reimb'],
  INCOME_CATS:   ['income', 'paycheck'],
};

// Shared classifier — keep identical to the one in weekly-digest.gs
function classifyTxn_(amount, category) {
  const cat = String(category || '').toLowerCase();
  if (API_CFG.TRANSFER_CATS.some(s => cat.includes(s))) return 'transfer';
  if (API_CFG.REIMB_CATS.some(s => cat.includes(s)))    return 'reimb';
  if (API_CFG.INCOME_CATS.some(s => cat.includes(s)))   return 'income';
  return amount > 0 ? 'income' : 'expense';
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────
function doGet(e) {
  const out = ContentService.createTextOutput();
  out.setMimeType(ContentService.MimeType.JSON);

  try {
    const requiredKey = PropertiesService.getScriptProperties().getProperty('API_KEY');
    if (requiredKey && (!e || !e.parameter || e.parameter.key !== requiredKey)) {
      out.setContent(JSON.stringify({ error: 'unauthorised' }));
      return out;
    }
    out.setContent(JSON.stringify(buildPayload_()));
  } catch (err) {
    out.setContent(JSON.stringify({ error: err.message, stack: err.stack }));
  }

  return out;
}

function buildPayload_() {
  const rows = readLog_();
  return {
    generated:  new Date().toISOString(),
    monthly:    getMonthly_(rows),
    categories: getCategories_(rows),
    recent:     getRecent_(rows),
    accounts:   getAccounts_(rows),
  };
}

// Read _ExpenseLog once and normalise
function readLog_() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(API_CFG.LOG_SHEET);
  if (!sheet) return [];
  const data = sheet.getDataRange().getValues();
  const rows = [];
  for (let i = 1; i < data.length; i++) {
    const rawD = data[i][API_CFG.COL_DATE - 1];
    if (!rawD) continue;
    const date = rawD instanceof Date ? rawD : new Date(rawD);
    if (isNaN(date.getTime())) continue;
    const amount   = parseFloat(data[i][API_CFG.COL_AMOUNT - 1]) || 0;
    const category = String(data[i][API_CFG.COL_CATEGORY - 1] || '').trim();
    rows.push({
      date, amount, category,
      desc:    String(data[i][API_CFG.COL_DESC - 1] || '').trim(),
      account: String(data[i][API_CFG.COL_ACCOUNT - 1] || '').trim(),
      bucket:  classifyTxn_(amount, category),
    });
  }
  return rows;
}

// ─────────────────────────────────────────────────────────────────────────────
// Monthly income / spending series (transfers excluded, reimbursables netted)
// ─────────────────────────────────────────────────────────────────────────────
function getMonthly_(rows) {
  const monthly = {}; // "Mon-YY" → { inc, exp, reimb, sortKey }

  rows.forEach(r => {
    const mon = r.date.toLocaleString('en-US', { month: 'short' });
    const key = mon + '-' + String(r.date.getFullYear()).slice(-2);
    if (!monthly[key]) {
      monthly[key] = { inc: 0, exp: 0, reimb: 0, sortKey: r.date.getFullYear() * 12 + r.date.getMonth() };
    }
    if (r.bucket === 'income')       monthly[key].inc   += r.amount;
    else if (r.bucket === 'expense') monthly[key].exp   += Math.abs(r.amount);
    else if (r.bucket === 'reimb')   monthly[key].reimb += r.amount;
    // transfers intentionally excluded
  });

  const sorted = Object.entries(monthly).sort((a, b) => a[1].sortKey - b[1].sortKey);
  return {
    labels: sorted.map(([k])    => k),
    inc:    sorted.map(([, v]) => Math.round(v.inc)),
    exp:    sorted.map(([, v]) => Math.round(v.exp)),
    reimb:  sorted.map(([, v]) => Math.round(v.reimb)),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Category totals — current month + rolling 3 months (personal spending only)
// ─────────────────────────────────────────────────────────────────────────────
function getCategories_(rows) {
  const now        = new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const threeAgo   = new Date(now.getFullYear(), now.getMonth() - 3, 1);

  const thisMo = {}, last3 = {};
  rows.forEach(r => {
    if (r.bucket !== 'expense' || r.date < threeAgo) return;
    const cat = r.category || 'Uncategorised';
    const amt = Math.abs(r.amount);
    last3[cat] = (last3[cat] || 0) + amt;
    if (r.date >= monthStart) thisMo[cat] = (thisMo[cat] || 0) + amt;
  });

  const sort = obj => Object.entries(obj)
    .sort((a, b) => b[1] - a[1])
    .map(([k, v]) => ({ cat: k, amt: Math.round(v) }));

  return { thisMonth: sort(thisMo), last3Months: sort(last3) };
}

// ─────────────────────────────────────────────────────────────────────────────
// Recent transactions — last 30 days, newest first
// ─────────────────────────────────────────────────────────────────────────────
function getRecent_(rows) {
  const cutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  const tz     = Session.getScriptTimeZone();

  return rows
    .filter(r => r.date >= cutoff)
    .sort((a, b) => b.date - a.date)
    .slice(0, 100)
    .map(r => ({
      date:     Utilities.formatDate(r.date, tz, 'yyyy-MM-dd'),
      desc:     r.desc.substring(0, 50),
      amount:   Math.round(Math.abs(r.amount)),
      category: r.category,
      account:  r.account,
      type:     r.bucket,   // 'income' | 'expense' | 'transfer' | 'reimb'
    }));
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-account personal spending, current month
// ─────────────────────────────────────────────────────────────────────────────
function getAccounts_(rows) {
  const now        = new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);

  const accounts = {};
  rows.forEach(r => {
    if (r.bucket !== 'expense' || r.date < monthStart) return;
    const name = r.account || 'Unknown';
    accounts[name] = (accounts[name] || 0) + Math.abs(r.amount);
  });

  return Object.entries(accounts)
    .sort((a, b) => b[1] - a[1])
    .map(([name, amt]) => ({ name, amt: Math.round(amt) }));
}

// ─────────────────────────────────────────────────────────────────────────────
// Quick test — run in Apps Script to preview the JSON in the logs
// ─────────────────────────────────────────────────────────────────────────────
function testApi() {
  Logger.log(JSON.stringify(buildPayload_(), null, 2));
}
