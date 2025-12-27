// scripts/daily.js
// 1) 오늘(KST) 실제 데이터 upsert
// 2) 누락 날짜 forward-fill

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const NOW_DATA_BASE_URL = process.env.NOW_DATA_BASE_URL;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !NOW_DATA_BASE_URL) {
  console.error('Missing env vars');
  process.exit(1);
}

function kstTodayISO() {
  const now = new Date();
  const kst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  return kst.toISOString().slice(0, 10);
}

async function fetchJson(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Fetch failed ${url}`);
  return r.json();
}

async function fetchActualData() {
  const fx = await fetchJson(`${NOW_DATA_BASE_URL}/.netlify/functions/fx`);
  const rates = await fetchJson(`${NOW_DATA_BASE_URL}/.netlify/functions/rates`);
  const kor = await fetchJson(`${NOW_DATA_BASE_URL}/.netlify/functions/kor10`);

  return {
    usdkrw_spot: fx.usdkrw_spot ?? fx.usdkrw ?? null,
    us_10y: rates.us_10y ?? null,
    us_1y: rates.us_1y ?? null,
    sofr_30d: rates.sofr_30d ?? rates.sofr ?? null,
    kor_10y: kor.kor_10y ?? kor.kor10 ?? null,
  };
}

async function callRpc(name, body = {}) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
    method: 'POST',
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`${name} failed: ${t}`);
  }
}

async function main() {
  const snapshot_date = kstTodayISO();
  const actual = await fetchActualData();

  await callRpc('upsert_market_snapshot', {
    p: {
      snapshot_date,
      ...actual,
      source_type: 'auto_actual',
    },
  });

  await callRpc('ffill_market_snapshots');
  console.log('Daily snapshot + ffill complete');
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
