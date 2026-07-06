# Financial CSV Import Workflow — Full Review (2026-07-06)

Scope: the manual CSV export → Google Drive → Apps Script import → `_Banks&Cards V3`
pipeline, the weekly digest email, and the `data-api` / `banks-data.json` dashboard feed.

## The workflow as it exists today

```
Bank/card websites (manual CSV export, no API credentials shared)
        │
        ▼
Drive: _Fin/_Data/<Institution>/          AMEX · Chase (4 accts) · AppleCard ·
        │                                 Fidelity · Robinhood · Schwab · ADP
        ▼
Apps Script importer (Imp_Banks + V3-bound project)
  • reads CSVs per account folder, header-checked
  • dedups via 12-hex SHA-256 txn hash (AMEX: Reference#; others: date+desc+amount)
  • appends to raw tabs (AMEX, Chase-C2152, …), moves file → Processed_CSVs/
        │
        ▼
_Banks&Cards V3 sheet
  • raw tabs get `Clean Amount` (sign-normalised: negative = money OUT),
    `Reconciled Category` (Categories tab keyword/raw-cat mapping + Override col)
  • _ExpenseLog = QUERY union of all raw tabs → Date | Desc | Amount | Type | Category | Account
  • Dashboard / ByCat / inVout / SUMMARY pivots
        │
        ├──► weekly-digest.gs — Sunday 20:00 email
        └──► data-api.gs (web app) + banks-data.json — dashboard feed
```

The core design is sound: no third-party aggregator ever sees bank credentials,
dedup is content-addressed, files are archived after ingest, category
reconciliation is centralised in one tab with a manual override column.

---

## Bug #1 (the digest): income/expense classification is fully inverted

**Symptom** (Jul 5 digest): Month income $58, expenses $3,952, savings rate −6738%,
"Top spending #1: Work Reimbursements $3,927" — the $3,927 row is the
`TRINITY CONS … ACH_CREDIT` **deposit** (an expense reimbursement paid *to* you).

**Root cause — two wrong assumptions in `weekly-digest.gs`:**

1. `COL_TYPE` comment says the column holds `"Expense" or "Income"` and the code
   tests `typeStr === 'Income'`. In reality `_ExpenseLog` col D carries the **raw
   bank type codes** (`Sale`, `Purchase`, `ACH_CREDIT`, `ACCT_XFER`, `MISC_DEBIT`, …).
   The literal string `Income` appears in **zero** of 6,540 rows, so this test
   never matches.
2. The fallback `rawAmt < 0 → income` assumes "positive = expense, negative =
   credit" (it's even in the config comment). The sheet's actual convention is the
   opposite: the importer/`Clean Amount` normalises everything to
   **negative = money out, positive = money in** (verified: AMEX $14 Uber charge
   stored as −14; Chase `ACH_CREDIT` +3927.08 stored as +3927.08).

Net effect: **every expense is counted as income and every deposit as an
expense.** The July numbers decode exactly: "income" $58 = |−14| + |−18.8| + |−25|
(two Uber charges + a transfer out); "expenses" $3,952 = 3,927.08 (TRINITY credit)
+ 25 (transfer into savings). Savings rate (1 − 3952/57.8) = −6738%. ✓

**Same bug family in the deployed `data-api`** — `banks-data.json` shows AMEX
restaurant charges typed `income` and TRINITY payroll typed `expense`, so the
dashboard's monthly income/expense series and category totals are inverted too.
(The deployed web-app code has also drifted from the `data-api.gs` copy saved in
`_Fin/` — the copy classifies on `Type === 'Income'` only, which would yield zero
income; the deployed one clearly uses the amount-sign fallback. Neither is right.)

**Secondary digest bugs, visible in the same email:**

- `SKIP_CATS.includes(category)` is an **exact-match** against entries like
  `'Work Reimb'` — but the real category is `'Work Reimbursements'`, so nothing
  is ever skipped. That's why "Work Reimbursements" and "Transfers & Payments"
  headline Top Spending. (The data-api version correctly used case-insensitive
  substring matching; the digest didn't.)
- The row loop does `if (date < monthStart) continue` **before** the last-7-days
  check, so early in a month the "Last 7 days" block silently loses the
  cross-month days (Jun 29–30 were dropped from the Jul 5 digest).
- Savings rate divides by month-to-date income — on day 5 of a month that's
  noise (−6738%) even when classification is right. Needs an income>0 guard with
  a "—" display, not `0`.
- CC payments/transfers are not excluded from income/expense totals, so every
  AMEX autopay would be double-counted (once as card charges, once as the
  checking-account debit) even with correct sign handling.

**Corrected numbers** (from `_ExpenseLog`, classification = category-first, then sign):

| | June 2026 | July 2026 MTD |
|---|---|---|
| Paycheck income (Income-Work) | $3,191.70 | $0 |
| Personal spending | $7,239.36 | ~$0 |
| Reimbursables net (fronted vs repaid) | +$24.52 | +$3,894.28 |
| Top real category | Food: Dine Out $1,642 | — |

Fixed scripts are in this folder: **`weekly-digest.gs`** and **`data-api.gs`**
(shared classifier: transfer/CC-payment/investment categories excluded, work
reimbursements netted separately, sign decides the rest). Paste over the bound
script files in the V3 Apps Script project; redeploy the web app
(Deploy → Manage deployments → Edit → New version) so the dashboard picks it up.

> The SUMMARY tab has a milder version of the same blind spot: its "Top Spending
> Category" is `Work Reimbursements −$32,425` YTD and June expenses −$18,206
> include CC payments/transfers. Worth adding the same exclusions to those pivots.

---

## Bug #2 (importer): same-day identical transactions get silently dropped

Dedup keys for Chase and Apple Card are `date + normalised description + amount`.
Two genuinely separate, identical purchases the same day (two $5.75 coffees, two
same-fare Ubers) collide → the second row is treated as "already imported" and
discarded. AMEX is safe (Reference# is unique per charge). The log already shows
55 same-key row groups — the AMEX ones prove the pattern occurs in real data.

Options, best first:
1. **Import OFX/QFX instead of CSV** — see "Better way to import" below; `FITID`
   makes dedup exact for every institution.
2. For Chase CSVs, add the running `Balance` column into the hash (it
   disambiguates same-day duplicates on checking/savings).
3. Occurrence-count dedup: if the CSV contains N identical rows and the sheet
   has M, import N−M instead of 0.

## Bug #3 (data hygiene): silent gaps

- **16 uncategorised transactions in the last 90 days**, including a recurring
  $592.49 `WELLS FARGO AUTO DRAFT` (your car payment — one Categories-tab keyword
  rule fixes it forever) and one-offs like `SP TRMNL`, `FIRST LADY 92`, IKEA.
  Consider a digest section listing new uncategorised rows so they never rot.
- **Income series only sees Chase deposits.** ADP Data Log and the investment
  tabs aren't in the `_ExpenseLog` union — June shows $3,192 of payroll against
  $7,239 spending, so if part of salary lands elsewhere, savings-rate math will
  always look wrong. Either union the ADP tab in (with an `Income-Work`
  category) or annotate the digest that income = Chase deposits only.
- Coverage gaps between exports are invisible. Cheap fix: digest warns when an
  account's `Info` last-import timestamp is older than ~40 days, and (for
  checking/savings) verify the balance column chains across imports.

## Security notes (relevant given the "no third parties" stance)

- `data-api` is deployed as a web app with access "Anyone" — the full transaction
  log is protected only by the URL being unguessable, and that URL is embedded in
  a dashboard artifact. Add a shared-secret check
  (`e.parameter.key === PropertiesService.getScriptProperties().getProperty('API_KEY')`)
  and put the key in the artifact config; rotate the deployment URL after.
- `banks-data.json` sits in Drive — check it isn't link-shared.
- The Apps Script projects had no version control (the `.gs` copies in `_Fin/`
  already drifted from what's deployed). This folder is now the canonical home;
  consider `clasp pull/push` to sync the bound project with this repo.

## Is there a better way to import?

Your constraint (no aggregator gets credentials) rules out Plaid/Finicity/MX and
anything built on them (SimpleFIN Bridge, Tiller, Monarch, Copilot). Within that
constraint the manual-export-to-Drive pattern is genuinely the right architecture
— the improvements worth making are inside it:

1. **Switch exports from CSV to QFX/OFX where offered (Chase and AMEX both offer
   it).** Same manual download, but OFX carries a per-transaction `FITID` unique
   ID → dedup becomes exact (fixes Bug #2), amounts/signs are standardised
   (would have prevented Bug #1's convention drift), and date formats stop
   mattering. Apple Card only does CSV/OFX via Wallet export — its monthly OFX
   works too. The importer change is modest: parse OFX (simple SGML) per account
   instead of CSV.
2. **Reduce the manual steps, not the manual trust.** The Drive drop can be a
   one-tap iOS Shortcut ("Save to Drive → _Data/AMEX"). For institutions that
   email statements/exports, a small Gmail Apps Script can file attachments into
   the right folder — still no third party.
3. **Normalise at the door, not downstream.** Have the importer write a single
   canonical schema (date, desc, signed amount with one documented convention,
   raw type, account, hash) into the raw tabs so `_ExpenseLog` consumers never
   re-infer conventions. Today three different consumers (digest, data-api,
   SUMMARY) each re-derive "is this income?" — that's how this bug shipped.
   Best single fix: add a `Bucket` column (`Income / Spending / Transfer /
   Reimbursable`) computed once in `_ExpenseLog` via the Categories tab, and make
   every script and pivot read it.
