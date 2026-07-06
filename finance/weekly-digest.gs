// ─────────────────────────────────────────────────────────────────────────────
// WEEKLY FINANCE DIGEST — _Banks&Cards V3   (fixed 2026-07-06)
// ─────────────────────────────────────────────────────────────────────────────
// WHAT CHANGED vs the previous version:
//   • Classification rewritten. _ExpenseLog col D holds raw bank type codes
//     (Sale, ACH_CREDIT, ACCT_XFER, …) — it NEVER contains the string "Income",
//     and amounts are sign-normalised as NEGATIVE = money out, POSITIVE = money
//     in. The old `rawAmt < 0 → income` fallback therefore inverted everything.
//     Now: category decides transfers / reimbursables / income; sign decides
//     the rest (negative → expense, positive → income).
//   • Transfers, CC payments and investment moves are excluded from income and
//     expense totals (they double-count otherwise).
//   • Work reimbursements are netted into their own stat line instead of
//     polluting income/spending.
//   • SKIP list now matches case-insensitive substrings ('Work Reimbursements'
//     never equalled 'Work Reimb', so the old exact-match skipped nothing).
//   • "Last 7 days" window no longer gets clipped at the 1st of the month.
//   • Savings rate shows "—" when month-to-date income is zero/tiny instead of
//     −6738%.
//
// HOW TO INSTALL:
//   1. Open _Banks&Cards V3 → Extensions → Apps Script
//   2. Replace the old digest file contents with this file, save
//   3. Existing sendWeeklyDigest trigger keeps working (same function name);
//      run testDigest() to preview immediately
// ─────────────────────────────────────────────────────────────────────────────

const DIGEST_CONFIG = {
  RECIPIENT:   'atangmh@pm.me',
  LOG_SHEET:   '_ExpenseLog',

  // ── Column positions in _ExpenseLog (1 = column A) ──────────────────────
  COL_DATE:      1,  // A — transaction date
  COL_DESC:      2,  // B — description / merchant
  COL_AMOUNT:    3,  // C — signed amount: NEGATIVE = money out, POSITIVE = in
  COL_TYPE:      4,  // D — raw bank type code (informational only, not used)
  COL_CATEGORY:  5,  // E — reconciled category
  COL_ACCOUNT:   6,  // F — card/account name

  // Case-insensitive substrings matched against the reconciled category.
  // Order matters: transfer check runs first, then reimbursables, then income.
  TRANSFER_CATS: ['transfer', 'cc payment', 'payment', 'investment', 'hsa'],
  REIMB_CATS:    ['reimb'],                 // Work Reimbursements etc.
  INCOME_CATS:   ['income', 'paycheck'],    // Income-Work, Income-Investments…

  LARGE_TXN_AMT: 500,   // flag single expenses at/above this
  TOP_N_CATS:    5,
};

// ─────────────────────────────────────────────────────────────────────────────
// Shared classifier — keep identical to the one in data-api.gs
//   returns 'transfer' | 'reimb' | 'income' | 'expense'
// ─────────────────────────────────────────────────────────────────────────────
function classifyTxn_(amount, category) {
  const cat = String(category || '').toLowerCase();
  if (DIGEST_CONFIG.TRANSFER_CATS.some(s => cat.includes(s))) return 'transfer';
  if (DIGEST_CONFIG.REIMB_CATS.some(s => cat.includes(s)))    return 'reimb';
  if (DIGEST_CONFIG.INCOME_CATS.some(s => cat.includes(s)))   return 'income';
  return amount > 0 ? 'income' : 'expense';
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN: build and send the digest
// ─────────────────────────────────────────────────────────────────────────────
function sendWeeklyDigest() {
  const ss    = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(DIGEST_CONFIG.LOG_SHEET);
  if (!sheet) {
    Logger.log('Sheet not found: ' + DIGEST_CONFIG.LOG_SHEET);
    return;
  }

  const allData = sheet.getDataRange().getValues();
  if (allData.length < 2) return;

  // Date windows — the week window is independent of the month window so the
  // first digest of a month still covers late last month.
  const now        = new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const weekAgo    = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const windowMin  = weekAgo < monthStart ? weekAgo : monthStart;

  // Accumulators
  let monthExpense = 0;   // personal spending (outflows, transfers/reimb excluded)
  let monthIncome  = 0;   // real income (paychecks + other inflows)
  let reimbNet     = 0;   // signed: + means reimbursed more than fronted
  let reimbOut     = 0;   // gross fronted this month
  const catTotals  = {};
  const largeTxns  = [];
  const weekTxns   = [];
  const uncategorised = [];

  for (let i = 1; i < allData.length; i++) {
    const row  = allData[i];
    const rawD = row[DIGEST_CONFIG.COL_DATE - 1];
    if (!rawD) continue;
    const date = rawD instanceof Date ? rawD : new Date(rawD);
    if (isNaN(date.getTime()) || date < windowMin) continue;

    const rawAmt   = parseFloat(row[DIGEST_CONFIG.COL_AMOUNT - 1]) || 0;
    const amount   = Math.abs(rawAmt);
    const category = String(row[DIGEST_CONFIG.COL_CATEGORY - 1] || '').trim();
    const desc     = String(row[DIGEST_CONFIG.COL_DESC - 1] || '').trim();
    const account  = String(row[DIGEST_CONFIG.COL_ACCOUNT - 1] || '').trim();
    const bucket   = classifyTxn_(rawAmt, category);

    const inMonth = date >= monthStart;
    const inWeek  = date >= weekAgo;

    if (inWeek && bucket === 'expense') {
      weekTxns.push({ date, desc, amount });
    }
    if (!inMonth) continue;

    switch (bucket) {
      case 'income':
        monthIncome += rawAmt;   // signed: refunds inside income cats net out
        break;
      case 'reimb':
        reimbNet += rawAmt;
        if (rawAmt < 0) reimbOut += amount;
        break;
      case 'transfer':
        break;                   // intentionally excluded from all totals
      case 'expense':
        monthExpense += amount;
        catTotals[category || 'Uncategorised'] = (catTotals[category || 'Uncategorised'] || 0) + amount;
        if (!category) uncategorised.push({ date, desc, amount, account });
        if (amount >= DIGEST_CONFIG.LARGE_TXN_AMT) {
          largeTxns.push({ date, desc, amount, category: category || '—', account });
        }
        break;
    }
  }

  // null = not meaningful yet (no income this month) → rendered as "—"
  const savingsRate = monthIncome > 0
    ? Math.round((1 - monthExpense / monthIncome) * 100)
    : null;

  const topCats = Object.entries(catTotals)
    .sort((a, b) => b[1] - a[1])
    .slice(0, DIGEST_CONFIG.TOP_N_CATS);

  const html    = buildHtml(now, monthIncome, monthExpense, savingsRate, reimbNet, reimbOut,
                            topCats, largeTxns, weekTxns, uncategorised);
  const subject = buildSubject(now, monthExpense, savingsRate);

  GmailApp.sendEmail(
    DIGEST_CONFIG.RECIPIENT,
    subject,
    'View this email in an HTML-capable client.',
    { htmlBody: html }
  );
  Logger.log('Digest sent · ' + subject);
}

// ─────────────────────────────────────────────────────────────────────────────
// HTML TEMPLATE
// ─────────────────────────────────────────────────────────────────────────────
function buildHtml(now, income, expense, savingsRate, reimbNet, reimbOut,
                   topCats, largeTxns, weekTxns, uncategorised) {
  const monthName = now.toLocaleString('en-US', { month: 'long' });
  const year      = now.getFullYear();
  const srKnown   = savingsRate !== null;
  const srColor   = !srKnown ? '#888780'
                  : savingsRate >= 30 ? '#3a7a1e'
                  : savingsRate >= 15 ? '#8e4f0a' : '#b83232';
  const srText    = srKnown ? savingsRate + '%' : '—';

  const fmt     = n => (n < 0 ? '-$' : '$') + Math.round(Math.abs(n)).toLocaleString('en-US');
  const fmtDate = d => `${d.getMonth() + 1}/${d.getDate()}`;

  const statCard = (label, value, color) => `
        <td width="25%" style="padding-right:8px">
          <div style="background:#ffffff;border:1px solid #e0ded9;border-radius:8px;padding:12px 14px">
            <div style="font-size:10px;color:#888780;text-transform:uppercase;letter-spacing:.4px;margin-bottom:4px">${label}</div>
            <div style="font-size:18px;font-weight:500;color:${color || '#2c2c2a'}">${value}</div>
          </div>
        </td>`;

  const statsHtml = `
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:16px">
      <tr>
        ${statCard('Month income', fmt(income))}
        ${statCard('Month spending', fmt(expense))}
        ${statCard('Savings rate', srText, srColor)}
        ${statCard('Reimb. net', (reimbNet >= 0 ? '+' : '') + fmt(reimbNet), reimbNet >= 0 ? '#3a7a1e' : '#8e4f0a')}
      </tr>
    </table>`;

  // Top categories table (personal spending only — transfers/reimb never appear)
  const catRows = topCats.map(([cat, total], i) => {
    const pct = expense > 0 ? Math.round(total / expense * 100) : 0;
    const bg  = i % 2 === 0 ? '#ffffff' : '#fafaf8';
    return `<tr style="background:${bg}">
      <td style="padding:8px 10px;border-bottom:1px solid #f1efe8;color:#5f5e5a;font-size:13px">${i + 1}. ${cat}</td>
      <td style="padding:8px 10px;border-bottom:1px solid #f1efe8;text-align:right;font-weight:500;font-size:13px">${fmt(total)}</td>
      <td style="padding:8px 10px;border-bottom:1px solid #f1efe8;text-align:right;color:#888780;font-size:12px;width:42px">${pct}%</td>
    </tr>`;
  }).join('');

  const catsHtml = topCats.length > 0 ? `
    <div style="background:#ffffff;border:1px solid #e0ded9;border-radius:10px;padding:14px 16px;margin-bottom:12px">
      <div style="font-size:11px;font-weight:500;color:#888780;text-transform:uppercase;letter-spacing:.4px;margin-bottom:10px">
        Top spending — ${monthName}
      </div>
      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse">${catRows}</table>
    </div>` : '';

  const largeSorted = largeTxns.sort((a, b) => b.amount - a.amount);
  const largeTxnRows = largeSorted.map((t, i) => {
    const bg = i % 2 === 0 ? '#ffffff' : '#fafaf8';
    return `<tr style="background:${bg}">
      <td style="padding:7px 8px;border-bottom:1px solid #f1efe8;color:#888780;font-size:12px;width:36px">${fmtDate(t.date)}</td>
      <td style="padding:7px 8px;border-bottom:1px solid #f1efe8;font-size:12px;color:#2c2c2a">${t.desc.substring(0, 38)}</td>
      <td style="padding:7px 8px;border-bottom:1px solid #f1efe8;font-size:11px;color:#888780">${t.category}</td>
      <td style="padding:7px 8px;border-bottom:1px solid #f1efe8;text-align:right;font-weight:500;font-size:12px">${fmt(t.amount)}</td>
    </tr>`;
  }).join('');

  const largeTxnHtml = largeTxns.length > 0 ? `
    <div style="background:#ffffff;border:1px solid #e0ded9;border-radius:10px;padding:14px 16px;margin-bottom:12px">
      <div style="font-size:11px;font-weight:500;color:#888780;text-transform:uppercase;letter-spacing:.4px;margin-bottom:10px">
        Large spending this month (≥${fmt(DIGEST_CONFIG.LARGE_TXN_AMT)})
      </div>
      <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse">${largeTxnRows}</table>
    </div>` : '';

  const weekTotal = weekTxns.reduce((s, t) => s + t.amount, 0);
  const weekBlurb = weekTxns.length > 0
    ? `<div style="background:#eaf3de;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:13px;color:#3a7a1e">
        <strong>Last 7 days:</strong> ${weekTxns.length} spending transactions totalling ${fmt(weekTotal)}${reimbOut > 0 ? ` &middot; ${fmt(reimbOut)} fronted for work this month` : ''}
       </div>`
    : '';

  const uncatBlurb = uncategorised.length > 0
    ? `<div style="background:#fdf3e7;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:12px;color:#8e4f0a">
        <strong>${uncategorised.length} uncategorised this month:</strong>
        ${uncategorised.slice(0, 5).map(t => `${t.desc.substring(0, 24)} (${fmt(t.amount)})`).join(' · ')}
       </div>`
    : '';

  return `
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;max-width:540px;margin:0 auto;color:#2c2c2a;padding:20px">
  <div style="background:#f8f7f4;border-radius:12px;padding:18px 20px;margin-bottom:16px">
    <div style="font-size:11px;color:#888780;letter-spacing:.4px;text-transform:uppercase;margin-bottom:3px">Weekly digest &middot; ${monthName} ${year}</div>
    <h1 style="font-size:22px;font-weight:500;margin:0 0 14px">Finance update</h1>
    ${statsHtml}
  </div>
  ${weekBlurb}
  ${uncatBlurb}
  ${catsHtml}
  ${largeTxnHtml}
  <div style="font-size:11px;color:#b4b2a9;text-align:center;padding-top:8px">
    Sent from Banks &amp; Cards V3 &middot;
    <a href="https://docs.google.com/spreadsheets/d/1D5b8rqHmdMX4lpGZ4oSp-3ufNlhEeTOpCSkpX-LWJhY/edit" style="color:#1854b0;text-decoration:none">Open sheet</a>
  </div>
</div>`;
}

function buildSubject(now, expense, savingsRate) {
  const month = now.toLocaleString('en-US', { month: 'short' });
  const fmt   = n => '$' + Math.round(Math.abs(n)).toLocaleString('en-US');
  const sr    = savingsRate !== null ? savingsRate + '% saved' : 'savings n/a';
  return `${month} finance digest · ${fmt(expense)} spent · ${sr}`;
}

// ─────────────────────────────────────────────────────────────────────────────
// TRIGGER SETUP — run once
// ─────────────────────────────────────────────────────────────────────────────
function createWeeklyTrigger() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'sendWeeklyDigest')
    .forEach(t => ScriptApp.deleteTrigger(t));

  ScriptApp.newTrigger('sendWeeklyDigest')
    .timeBased()
    .onWeekDay(ScriptApp.WeekDay.SUNDAY)
    .atHour(20)
    .create();

  SpreadsheetApp.getUi().alert('✓  Weekly trigger created.\nDigest will send every Sunday at 8–9 pm.');
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST — sends immediately
// ─────────────────────────────────────────────────────────────────────────────
function testDigest() {
  sendWeeklyDigest();
  SpreadsheetApp.getUi().alert('Test digest sent to ' + DIGEST_CONFIG.RECIPIENT);
}
