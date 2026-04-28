const http = require('http');
const https = require('https');

// ── GDELT AUTO-SIGNAL ──
// ── ENDPOINT CACHE (5 хв) ──
const endpointCache = {};
function getCached(key) {
  const e = endpointCache[key];
  if (e && Date.now() - e.ts < 300000) return e.data;
  return null;
}
function setCache(key, data) {
  endpointCache[key] = { data, ts: Date.now() };
}

// ── GOOGLE TRENDS CROSS-SIGNAL ──
let trendsCache = { items: [], fetchedAt: 0 };

function getTrends() {
  const now = Date.now();
  if (now - trendsCache.fetchedAt < 900000 && trendsCache.items.length) {
    return Promise.resolve(trendsCache.items);
  }
  const geos = ['US','GB','DE','UA','IN','FR','BR','JP'];
  const fetchGeo = (geo) => new Promise((resolve) => {
    https.get({
      hostname: 'trends.google.com',
      path: '/trending/rss?geo='+geo,
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ResonanceBot/1.0)' }
    }, (r) => {
      let raw = ''; r.on('data', d => raw += d);
      r.on('end', () => {
        try {
          const re = /<title>(?:<!\[CDATA\[)?(.+?)(?:\]\]>)?<\/title>/g;
          const titles = [...raw.matchAll(re)].map(m=>m[1].trim()).slice(1,11);
          resolve(titles.map(t=>({title:t.toLowerCase(),geo})));
        } catch(e) { resolve([]); }
      });
    }).on('error', () => resolve([]));
  });
  return Promise.all(geos.map(fetchGeo)).then(results => {
    trendsCache.items = results.flat();
    trendsCache.fetchedAt = Date.now();
    return trendsCache.items;
  });
}

function fetchTrendsSignal(title, editors) {
  const words = title.toLowerCase().split(/\s+/).filter(w=>w.length>3);
  getTrends().then(trends => {
    const matches = trends.filter(t => words.some(w => t.title.includes(w)));
    if (!matches.length) return;
    const geos = [...new Set(matches.map(m=>m.geo))];
    console.log('Google Trends match:', title, '| geos:', geos.join(','));
    supabaseInsert('cross_signals', {
      type: 'WIKI+TRENDS',
      title: title,
      detail: 'trending in: '+geos.join(','),
      wiki_title: title,
      crypto_symbol: null,
      score: geos.length * 15
    });
    if (editors >= 2 && geos.length >= 2 && TELEGRAM_TOKEN) {
      sendTelegram(
        '📈 <b>Google Trends сигнал: ' + title + '</b>\n\n' +
        '🌍 Trending в: <b>' + geos.join(', ') + '</b>\n' +
        '👥 ' + editors + ' редактори на Wikipedia одночасно'
      );
    }
  }).catch(()=>{});
}

// ── POLYMARKET CROSS-SIGNAL ──
let polyCache = { items: [], fetchedAt: 0 };

async function getPolyMarkets() {
  const now = Date.now();
  if (now - polyCache.fetchedAt < 300000 && polyCache.items.length) return polyCache.items;
  return new Promise((resolve) => {
    https.get({
      hostname: 'gamma-api.polymarket.com',
      path: '/markets?closed=false&limit=300&order=volumeNum&ascending=false',
      headers: { 'User-Agent': 'ResonanceProxy/1.0' }
    }, (r) => {
      let raw = ''; r.on('data', d => raw += d);
      r.on('end', () => {
        try {
          const data = JSON.parse(raw);
          polyCache.items = data;
          polyCache.fetchedAt = Date.now();
          console.log('Polymarket cache updated:', data.length, 'markets');
          resolve(data);
        } catch(e) { resolve([]); }
      });
    }).on('error', () => resolve([]));
  });
}

function fetchPolymarketSignal(title, editors) {
  // Стоп-слова — занадто загальні
  const STOPWORDS = new Set(['will','with','from','that','this','have','their','they','what','when','where','about','after','before','election','party','company','president','minister','death','died','football','draft','league','match','series','final','finals','season','team','player','game','games','news','update','first','last','part','time','year','years','national','international','world','american','will','race','bowl','tournament','championship','prize','award','medal']);
  // Виключаємо роки
  const words = title.toLowerCase()
    .replace(/[^\w\s]/g,' ')
    .split(/\s+/)
    .filter(w => w.length > 3 && !STOPWORDS.has(w) && !/^\d{4}$/.test(w));

  if (!words.length) return;

  getPolyMarkets().then(markets => {
    const matches = markets.filter(m => {
      const q = (m.question||'').toLowerCase();
      // Вимагаємо 2+ матчів слів АБО матч слова з 6+ символами (унікальніше)
      const matchedWords = words.filter(w => q.includes(w));
      if (matchedWords.length >= 2) return true;
      if (matchedWords.length === 1 && matchedWords[0].length >= 7) return true;
      return false;
    });
    if (!matches.length) return;
    const top = matches[0];
    const prices = JSON.parse(top.outcomePrices||'[]');
    const yesProb = Math.round(parseFloat(prices[0]||0)*100);
    const vol = Math.round((top.volumeNum||0)/1000);
    console.log('Polymarket match:', title, '->', (top.question||'').slice(0,60), '| YES:', yesProb+'%', '| vol: $'+vol+'K');
    supabaseInsert('cross_signals', {
      type: 'WIKI+POLYMARKET',
      title: title,
      detail: 'YES:'+yesProb+'% vol:$'+vol+'K q:'+(top.question||'').slice(0,100),
      wiki_title: title,
      crypto_symbol: null,
      score: yesProb * Math.log(vol+1)
    });
    // Telegram тільки якщо подія справді актуальна (yesProb 5-95%, є взаємодія)
    if (editors >= 2 && vol > 100 && yesProb >= 5 && yesProb <= 95 && TELEGRAM_TOKEN) {
      const emoji = yesProb > 70 ? '🟢' : yesProb > 40 ? '🟡' : '🔴';
      sendTelegram(
        emoji + ' <b>Polymarket сигнал: ' + title + '</b>\n\n' +
        '📊 ' + (top.question||'').slice(0,80) + '\n' +
        '💰 YES: <b>' + yesProb + '%</b> | Обсяг: $' + vol + 'K\n' +
        '🔗 <a href="https://polymarket.com/event/' + (top.slug||'') + '">відкрити ринок</a>'
      );
    }
  }).catch(() => {});
}

// GDELT rate limiter — max 1 req per 6 sec
let lastGdeltCall = 0;
const gdeltQueue = [];
function gdeltRateLimited(url, cb) {
  gdeltQueue.push({url, cb});
  processGdeltQueue();
}
function processGdeltQueue() {
  if (!gdeltQueue.length) return;
  const now = Date.now();
  const wait = Math.max(0, 6000 - (now - lastGdeltCall));
  setTimeout(() => {
    if (!gdeltQueue.length) return;
    const {url, cb} = gdeltQueue.shift();
    lastGdeltCall = Date.now();
    https.get(url, { headers: { 'User-Agent': 'ResonanceProxy/1.0' } }, (r) => {
      let raw = ''; r.on('data', d => raw += d);
      r.on('end', () => cb(null, raw));
    }).on('error', e => cb(e, null));
    setTimeout(processGdeltQueue, 6100);
  }, wait);
}

function fetchGdeltSignal(title, lang, wiki, edits, editors) {
  const url = 'https://api.gdeltproject.org/api/v2/doc/doc?query='
    + encodeURIComponent(title)
    + '&mode=ArtList&maxrecords=5&timespan=1d&sort=HybridRel&format=json';

  gdeltRateLimited(url, (err, raw) => {
    if (err || !raw) return;
    try {
      if (raw.includes('limit requests')) return;
        const data = JSON.parse(raw);
        const articles = data.articles || [];
        if (!articles.length) return;

        // Avg tone
        const tones = articles.map(a => parseFloat(a.tone||0)).filter(t=>!isNaN(t));
        const avgTone = tones.length ? tones.reduce((a,b)=>a+b,0)/tones.length : 0;
        const countries = [...new Set(articles.map(a=>a.sourcecountry).filter(Boolean))];

        console.log('GDELT signal:', title, '| tone:', avgTone.toFixed(1), '| sources:', articles.length, '| countries:', countries.join(','));

        // Save cross-signal to Supabase
        if (articles.length >= 2) {
          supabaseInsert('cross_signals', {
            type: 'WIKI+GDELT',
            title: title,
            detail: 'tone:'+avgTone.toFixed(1)+' sources:'+articles.length+' countries:'+countries.slice(0,3).join(','),
            wiki_title: title,
            crypto_symbol: null,
            score: Math.abs(avgTone) * articles.length
          });
        }

        // Telegram alert if strong negative tone + multi-editor
        if (avgTone < -5 && editors >= 3 && TELEGRAM_TOKEN) {
          const toneEmoji = avgTone < -10 ? '🔴' : '🟡';
          sendTelegram(
            toneEmoji + ' <b>GDELT сигнал: ' + title + '</b>\n\n' +
            '📰 ' + articles.length + ' джерел у ' + countries.length + ' країнах\n' +
            '😟 Тональність: ' + avgTone.toFixed(1) + ' (негативна)\n' +
            '📝 ' + articles[0].title.slice(0, 100) + '\n\n' +
            '🔗 <a href="' + articles[0].url + '">читати</a>'
          );
        }
      } catch(e) {}
  });
}

// ── SUPABASE ──
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY || '';

function supabaseInsert(table, data, upsertOn) {
  if (!SUPABASE_URL || !SUPABASE_KEY) return;
  const body = JSON.stringify(upsertOn ? data : Array.isArray(data) ? data : data);
  const url = new URL(SUPABASE_URL + '/rest/v1/' + table);
  // For anomalies — upsert on title+wiki to avoid duplicates
  const prefer = upsertOn
    ? 'return=minimal,resolution=merge-duplicates'
    : 'return=minimal';
  const req = https.request({
    hostname: url.hostname,
    path: url.pathname + (upsertOn ? '?on_conflict='+upsertOn : ''),
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
      'apikey': SUPABASE_KEY,
      'Authorization': 'Bearer ' + SUPABASE_KEY,
      'Prefer': prefer
    }
  }, (res) => {
    if (res.statusCode >= 400) {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => console.log('Supabase error:', res.statusCode, d.slice(0, 100)));
    }
  });
  req.on('error', e => console.log('Supabase req error:', e.message));
  req.write(body);
  req.end();
}

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

// ── LANGUAGE TIERS ──
// Tier 1: large wikis — low threshold (2+ editors for dashboard, 4+ for Telegram)
const TIER1 = new Set(['enwiki','dewiki','frwiki','eswiki','ruwiki','jawiki','zhwiki','ptwiki']);
// Tier 2: medium wikis — mid threshold (3+ editors for dashboard, 5+ for Telegram)
const TIER2 = new Set(['ukwiki','plwiki','itwiki','arwiki','kowiki','nlwiki','svwiki','fawiki','trwiki','viwiki','idwiki']);
// Tier 3: small wikis — high threshold (4+ editors for dashboard, 6+ for Telegram)
const TIER3 = new Set(['tawiki','tewiki','mlwiki','hiwiki','bnwiki','urwiki','hewiki','fiwiki','cswiki','huwiki','rowiki','thwiki','elwiki','bgwiki','srwiki','hrwiki','skwiki','dawiki','nowiki']);

const ALL_WIKIS = new Set([...TIER1, ...TIER2, ...TIER3]);

function getTier(wiki) {
  if (TIER1.has(wiki)) return 1;
  if (TIER2.has(wiki)) return 2;
  if (TIER3.has(wiki)) return 3;
  return 3;
}

// Dashboard anomaly thresholds (unique editors in 60s)
function getDashThreshold(wiki) {
  const t = getTier(wiki);
  if (t === 1) return 2;
  if (t === 2) return 3;
  return 4;
}

// Telegram alert thresholds (unique editors in 300s)
function getTgThreshold(wiki) {
  const t = getTier(wiki);
  if (t === 1) return 5;
  if (t === 2) return 6;
  return 7;
}

// Telegram single spike thresholds (edits in 60s from one editor)
function getSpikeThreshold(wiki) {
  const t = getTier(wiki);
  if (t === 1) return 8;
  if (t === 2) return 10;
  return 12;
}

const anomWindow = {};
const sentAlerts = {};
const sentTrackedHits = {};
const pvcache = {};

// ── TELEGRAM ──
function sendTelegram(message) {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) return;
  const body = JSON.stringify({
    chat_id: TELEGRAM_CHAT_ID,
    text: message,
    parse_mode: 'HTML',
    disable_web_page_preview: false
  });
  const req = https.request({
    hostname: 'api.telegram.org',
    path: '/bot' + TELEGRAM_TOKEN + '/sendMessage',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
  }, (res) => {
    let d = ''; res.on('data', c => d += c);
    res.on('end', () => {
      try { const r = JSON.parse(d); if (!r.ok) console.log('TG error:', r.description); else console.log('TG OK:', message.slice(0,60).replace(/\n/g,' ')); } catch(e) {}
    });
  });
  req.on('error', e => console.log('TG req error:', e.message));
  req.write(body); req.end();
}

// ── WIKI INFO ──
async function getWikiInfo(title, lang) {
  return new Promise((resolve) => {
    const path = '/w/api.php?action=query&prop=categories|langlinks&titles=' +
      encodeURIComponent(title) + '&cllimit=30&clshow=!hidden&lllimit=500&format=json';
    const req = https.get({
      hostname: lang + '.wikipedia.org', path,
      headers: { 'User-Agent': 'ResonanceBot/1.0' }
    }, (res) => {
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const page = Object.values(JSON.parse(data).query?.pages || {})[0];
          if (!page) return resolve({ type: 'стаття', langCount: 1 });
          const cats = (page.categories || []).map(c => c.title.toLowerCase()).join(' ');
          const langCount = (page.langlinks || []).length + 1;
          let type = 'стаття';
          if (/deaths in 20|died 20/.test(cats))                                          type = '💀 СМЕРТЬ';
          else if (/politician|president|minister|senator|parliament|governor|mayor/.test(cats)) type = '🏛 ПОЛІТИК';
          else if (/businessperson|ceo|billionaire|executive|entrepreneur/.test(cats))    type = '💼 БІЗНЕС';
          else if (/sportsperson|athlete|footballer|tennis|basketball|olympic/.test(cats)) type = '⚽ СПОРТ';
          else if (/actor|musician|singer|director|comedian|rapper/.test(cats))           type = '🎭 КУЛЬТУРА';
          else if (/military officer|admiral|colonel|brigadier|armed forces|navy officer|army officer/.test(cats))  type = '🎖 ВІЙСЬКОВІ';
          else if (/scientist|professor|physicist|biologist|chemist/.test(cats))          type = '🔬 НАУКА';
          else if (/strait|canal|waterway|conflict|crisis|war|military operation/.test(cats)) type = '🌏 ГЕОПОЛІТИКА';
          else if (/football club|association football|league|championship/.test(cats))   type = '🏆 ФУТБОЛ';
          else if (/living people/.test(cats))                                             type = '👤 ПЕРСОНА';
          resolve({ type, langCount });
        } catch(e) { resolve({ type: 'стаття', langCount: 1 }); }
      });
    });
    req.on('error', () => resolve({ type: 'стаття', langCount: 1 }));
    req.setTimeout(6000, () => { req.destroy(); resolve({ type: 'стаття', langCount: 1 }); });
  });
}

// ── PAGEVIEWS ──
async function getViewsRatio(lang, title) {
  const key = lang + ':' + title;
  const now = Date.now();
  if (pvcache[key] && now - pvcache[key].fetchedAt < 3600000) return pvcache[key];
  return new Promise((resolve) => {
    const end = new Date(); const start = new Date(end - 8 * 86400000);
    const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
    const path = '/api/rest_v1/metrics/pageviews/per-article/' + lang +
      '.wikipedia/all-access/all-agents/' + encodeURIComponent(title.replace(/ /g,'_')) +
      '/daily/' + fmt(start) + '/' + fmt(end);
    https.get({ hostname: 'wikimedia.org', path, headers: { 'User-Agent': 'ResonanceBot/1.0' } }, (res) => {
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const items = JSON.parse(data).items || [];
          if (items.length < 2) return resolve(null);
          const today = items[items.length-1].views;
          const avg7 = Math.round(items.slice(0,-1).reduce((s,i) => s+i.views, 0) / (items.length-1));
          const ratio = avg7 > 0 ? +(today/avg7).toFixed(1) : 0;
          const result = { today, avg7, ratio, fetchedAt: Date.now() };
          pvcache[key] = result;
          resolve(result);
        } catch(e) { resolve(null); }
      });
    }).on('error', () => resolve(null));
  });
}

// ── TRENDING ──
let trendingCache = { items: [], fetchedAt: 0 };

async function fetchTrending() {
  const now = new Date();
  const yesterday = new Date(now - 86400000);
  const dayBefore = new Date(now - 172800000);
  const fmt = d => ({ year: d.getFullYear(), month: String(d.getMonth()+1).padStart(2,'0'), day: String(d.getDate()).padStart(2,'0') });
  const td = fmt(yesterday);
  const yd = fmt(dayBefore);

  function getTop(d) {
    return new Promise((resolve) => {
      const path = '/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/' + d.year + '/' + d.month + '/' + d.day;
      console.log('Fetching top:', d.year+'-'+d.month+'-'+d.day);
      https.get({ hostname: 'wikimedia.org', path, headers: { 'User-Agent': 'ResonanceBot/1.0' } }, (res) => {
        let data = ''; res.on('data', c => data += c);
        res.on('end', () => {
          try { resolve(JSON.parse(data).items?.[0]?.articles || []); }
          catch(e) { resolve([]); }
        });
      }).on('error', e => { console.log('getTop error:', e.message); resolve([]); });
    });
  }

  try {
    const [todayTop, yestTop] = await Promise.all([getTop(td), getTop(yd)]);
    console.log('Got', todayTop.length, 'today,', yestTop.length, 'yesterday');

    // Fallback: якщо сьогодні 0 (нічний час UTC) — використовуємо вчора vs позавчора
    const effectiveToday = todayTop.length > 0 ? todayTop : yestTop;
    const effectiveYest = todayTop.length > 0 ? yestTop : (await getTop(fmt(new Date(now - 172800000 - 86400000))));
    const isUsingFallback = todayTop.length === 0;
    if (isUsingFallback) console.log('Using yesterday as fallback (today=0)');

    const yestMap = {};
    yestTop.forEach(a => { yestMap[a.article] = a.views; });
    const trending = todayTop
      .filter(a => !['Main_Page','Special:Search','Wikipedia:','Portal:'].some(x => a.article.startsWith(x)))
      .map(a => {
        const prev = yestMap[a.article] || 0;
        const delta = prev > 0 ? +((a.views - prev) / prev * 100).toFixed(0) : 0;
        return { article: a.article.replace(/_/g,' '), views: a.views, prev, delta, rank: a.rank };
      })
      .filter(a => a.views > 1000)
      .sort((a,b) => b.delta - a.delta)
      .slice(0, 50);
    trendingCache = { items: trending, fetchedAt: Date.now() };
    console.log('Trending updated:', trending.length, 'articles, top:', trending[0]?.article, '+'+trending[0]?.delta+'%');
  } catch(e) { console.log('Trending error:', e.message); }
}

fetchTrending();
setInterval(fetchTrending, 1800000);

// ── ANOMALY CHECK ──
function looksLikeBot(user, isBot) {
  if (isBot) return true;
  if (!user) return false;
  // Wikipedia temporary accounts start with ~
  if (user.startsWith('~')) return true;
  // IP addresses
  if (/^\d+\.\d+\.\d+\.\d+$/.test(user)) return true;
  if (/^[0-9a-f:]+:[0-9a-f:]+$/i.test(user)) return true; // IPv6
  // Bot-like usernames
  if (/bot\d*$|BOT\d*$|Bot\d*$/i.test(user)) return true;
  if (/^[A-Z]\w*Bot/.test(user)) return true;
  return false;
}

async function checkAnomaly(title, wiki, user, isBot) {
  const lang = wiki.replace('wiki', '') || 'en';
  const key = wiki + ':' + title;
  const now = Date.now();

  if (!anomWindow[key]) {
    anomWindow[key] = { ts60:[], users60:new Set(), ts300:[], users300:new Set(), firedMulti:false, firedSingle:false, lastSeen:now };
  }
  const w = anomWindow[key];
  w.lastSeen = now;
  w.ts60.push(now); w.ts300.push(now);
  // Не рахуємо ботів і тимчасові акаунти як унікальних редакторів
  if (user && !looksLikeBot(user, isBot)) { w.users60.add(user); w.users300.add(user); }
  w.ts60  = w.ts60.filter(t => now - t < 60000);
  w.ts300 = w.ts300.filter(t => now - t < 300000);
  if (w.ts60.length === 0) w.users60 = new Set();
  if (w.ts300.length === 0) { w.users300 = new Set(); w.firedMulti = false; w.firedSingle = false; w.firedSupabase = false; w.firedSingleSupa = false; }

  const hits60  = w.ts60.length;
  const uniq60  = w.users60.size;
  const hits300 = w.ts300.length;
  const uniq300 = w.users300.size;

  const tgThreshold    = getTgThreshold(wiki);
  const spikeThreshold = getSpikeThreshold(wiki);
  const alertKey = key + ':' + Math.floor(now / 300000);

  // ── Supabase: записуємо при 2+ редакторах з повними даними ──
  if (uniq300 >= 2 && hits300 >= 2 && !w.firedSupabase) {
    w.firedSupabase = true;
    const wikiUrl = 'https://' + lang + '.wikipedia.org/wiki/' + encodeURIComponent(title.replace(/ /g,'_'));

    // Збираємо повні дані асинхронно перед записом
    Promise.all([
      getWikiInfo(title, lang),
      getViewsRatio(lang, title)
    ]).then(([info, pvData]) => {
      const typeWeights = {'СМЕРТЬ':50,'ГЕОПОЛІТИКА':20,'ПОЛІТИК':18,'ВІЙСЬКОВІ':15,
        'БІЗНЕС':12,'НАУКА':8,'КУЛЬТУРА':6,'СПОРТ':5,'ФУТБОЛ':4,'ПЕРСОНА':3};
      const atype = info.type || '';
      const lc = info.langCount || 0;
      const pvRatio = pvData ? pvData.ratio : 0;
      const trendItem = (trendingCache && trendingCache.items) ? trendingCache.items.find(t =>
        t.article.toLowerCase() === title.toLowerCase()) : null;
      const trendPct = trendItem ? trendItem.delta : null;

      const typeScore = typeWeights[atype.replace(/[^а-яА-ЯіІїЇєЄa-zA-Z]/g,'')] || 0;
      const langScore = Math.min(lc * 0.4, 30);
      const trendScore = trendPct ? Math.min(trendPct / 20, 20) : 0;
      const pvScore = pvRatio ? Math.min((pvRatio - 1) * 3, 15) : 0;
      const actScore = uniq300 * 2.5 + hits300 * 0.8;
      const score = typeScore + langScore + trendScore + pvScore + actScore;

      supabaseInsert('anomalies', {
        title: title,
        wiki: wiki,
        lang: lang,
        type: uniq300 >= 3 ? 'res' : 'mul',
        edits: hits300,
        editors: uniq300,
        lang_count: lc,
        article_type: atype,
        url: wikiUrl,
        score: Math.round(score),
        is_trending: trendPct !== null,
        trend_pct: trendPct
      }, 'title,wiki');

      // ── Baseline tracking + content delta ──
      updateBaseline(title, wiki, Array.from(w.users300)[0]);
      saveContentDelta(title, wiki, Array.from(w.users300)[0], w.comments[0]||'');

      // ── Oracle detector ──
      runOracleDetector(title, wiki, key);

      // ── Prediction markets ──
      checkPredictionSignals(title, wiki, uniq300, Math.round(score));

      // ── Wikidata граф ──
      setTimeout(() => checkWikidataGraph(title, wiki, Math.round(score)), 3000);
    }).catch(() => {
      // Fallback — записуємо без деталей
      supabaseInsert('anomalies', {
        title, wiki, lang, type: 'mul',
        edits: hits300, editors: uniq300,
        lang_count: 0, article_type: '', url: wikiUrl,
        score: uniq300 * 3 + hits300, is_trending: false, trend_pct: null
      }, 'title,wiki');
    });
  }

  // ── TELEGRAM: N+ unique editors in 5 min (tier-based) ──
  if (uniq300 >= tgThreshold && !w.firedMulti && !sentAlerts[alertKey + ':tg']) {
    w.firedMulti = true;
    sentAlerts[alertKey + ':tg'] = true;

    const [info, pvData] = await Promise.all([
      getWikiInfo(title, lang),
      getViewsRatio(lang, title)
    ]);
    const { type, langCount } = info;

    // ── Polymarket cross-signal ──
    fetchPolymarketSignal(title, uniq300);
    // ── Google Trends cross-signal ──
    fetchTrendsSignal(title, uniq300);
    // GDELT auto-enrich вимкнено — зберігаємо квоту для AI модалки

    // ── Записуємо всі аномалії в Supabase ──
    supabaseInsert('anomalies', {
      title: title,
      wiki: wiki,
      lang: lang,
      type: uniq300 >= 2 ? 'mul' : 'sng',
      edits: hits300,
      editors: uniq300,
      lang_count: info.langCount,
      article_type: (info.type || '').replace(/[^a-zA-Zа-яА-ЯіІїЇєЄ\s]/g,'').trim(),
      url: 'https://' + lang + '.wikipedia.org/wiki/' + encodeURIComponent(title.replace(/ /g,'_')),
      score: (uniq300 || uniq60) * 3 + (hits300 || hits60),
      is_trending: info.langCount >= 50,
      trend_pct: null
    }, 'title,wiki');

    // Strict importance filter for Telegram
    const isImportant =
      type.includes('СМЕРТЬ') ||
      type.includes('ПОЛІТИК') ||
      type.includes('БІЗНЕС') ||
      type.includes('ГЕОПОЛІТИКА') ||
      type.includes('ВІЙСЬКОВІ') ||
      (type.includes('ПЕРСОНА') && langCount >= 10) ||
      (type.includes('СПОРТ') && langCount >= 20) ||
      (type.includes('КУЛЬТУРА') && langCount >= 20) ||
      (type.includes('ФУТБОЛ') && langCount >= 25) ||
      (type.includes('НАУКА') && langCount >= 10) ||
      langCount >= 30;

    if (isImportant) {
      const wikiUrl = 'https://' + lang + '.wikipedia.org/wiki/' + encodeURIComponent(title.replace(/ /g,'_'));
      const langLabel = langCount >= 50 ? '🌍 глобальна (' + langCount + ' мов)'
        : langCount >= 20 ? '🌐 міжнар. (' + langCount + ' мов)'
        : langCount >= 10 ? '📍 регіон. (' + langCount + ' мов)'
        : '📌 локальна (' + langCount + ' мов)';
      const timeWindow = w.ts300.length > 1 ? Math.round((w.ts300[w.ts300.length-1] - w.ts300[0]) / 1000) : 0;
      const windowStr = timeWindow < 60 ? timeWindow + ' сек' : Math.round(timeWindow/60) + ' хв ' + (timeWindow%60) + ' сек';
      const tier = getTier(wiki);
      let pvLine = '';
      if (pvData && pvData.ratio >= 3) pvLine = '\n📈 Переглядів сьогодні: ' + pvData.today.toLocaleString() + ' (у ' + pvData.ratio + 'x більше звичайного)';
      else if (pvData && pvData.today > 0) pvLine = '\n👁 Переглядів: ' + pvData.today.toLocaleString();

      const msg =
        '⚡ <b>RESONANCE ALERT</b>\n\n' +
        '<b>' + title + '</b>\n' +
        type + ' · ' + langLabel + '\n\n' +
        '👥 ' + uniq300 + ' редактори · ' + hits300 + ' правок за ' + windowStr + '\n' +
        '🌐 ' + lang + '.wikipedia (tier ' + tier + ')' +
        pvLine + '\n\n' +
        '<a href="' + wikiUrl + '">Відкрити →</a>';

      sendTelegram(msg);
      console.log('TG MULTI:', title, '|', type, '|', langCount, 'langs |', uniq300, '/', tgThreshold, 'editors | tier', tier);
    }
  }

  // ── TELEGRAM: single spike — тільки смерть або 100+ мов, поріг 15+ ──
  if (hits60 >= 15 && uniq60 <= 1 && !w.firedSingle && !sentAlerts[alertKey + ':single']) {
    w.firedSingle = true;
    sentAlerts[alertKey + ':single'] = true;
    const info = await getWikiInfo(title, lang);
    // Single spikes тільки для смерті або дуже глобальних статей
    if (info.type.includes('СМЕРТЬ') || info.langCount >= 100) {
      const pvData = await getViewsRatio(lang, title);
      const wikiUrl = 'https://' + lang + '.wikipedia.org/wiki/' + encodeURIComponent(title.replace(/ /g,'_'));
      let pvLine = pvData && pvData.ratio >= 3 ? '\n📈 x' + pvData.ratio + ' переглядів (' + pvData.today.toLocaleString() + ')' : '';
      sendTelegram(
        '🔴 <b>SPIKE</b>\n\n<b>' + title + '</b>\n' +
        info.type + ' · ' + info.langCount + ' мов\n' +
        '⚡ ' + hits60 + ' правок / 60 сек · ' + lang + pvLine + '\n\n' +
        '<a href="' + wikiUrl + '">Відкрити →</a>'
      );
      console.log('TG SPIKE:', title, info.type, info.langCount, 'langs');
    }
  }

  // Cleanup
  if (now - w.lastSeen > 600000) delete anomWindow[key];
}

// Cleanups
setInterval(() => {
  const now = Date.now();
  const keys = Object.keys(sentAlerts);
  if (keys.length > 1000) keys.slice(0, 500).forEach(k => delete sentAlerts[k]);
  Object.keys(anomWindow).forEach(k => { if (now - anomWindow[k].lastSeen > 600000) delete anomWindow[k]; });
  Object.keys(pvcache).forEach(k => { if (now - pvcache[k].fetchedAt > 7200000) delete pvcache[k]; });
}, 300000);

// ── HTTP SERVER ──
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

  // /trending endpoint
  if (req.url === '/trending') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ items: trendingCache.items, fetchedAt: trendingCache.fetchedAt, count: trendingCache.items.length }));
    return;
  }

  // /ai endpoint — proxy to Claude API with web search
  if (req.url.startsWith('/ai?')) {
    const params = new URL('http://localhost' + req.url).searchParams;
    const article = params.get('q') || '';
    const today = new Date().toLocaleDateString('uk-UA', {day:'numeric', month:'long', year:'numeric'});
    const prompt = 'Стаття "' + article + '" сьогодні (' + today + ') різко зросла у переглядах на Wikipedia. Коротко поясни українською мовою (3-5 речень) що сталось з цією темою сьогодні або вчора. Якщо це персона — хто це і яка подія. Будь конкретним і фактичним. Не використовуй markdown форматування.';

    const body = JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 800,
      messages: [{ role: 'user', content: prompt }],
      tools: [{ type: 'web_search_20250305', name: 'web_search' }]
    });

    const apiReq = https.request({
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        'anthropic-version': '2023-06-01',
        'x-api-key': process.env.ANTHROPIC_API_KEY || ''
      }
    }, (apiRes) => {
      let data = '';
      apiRes.on('data', c => data += c);
      apiRes.on('end', () => {
        try {
          const json = JSON.parse(data);
          const text = (json.content || []).filter(b => b.type === 'text').map(b => b.text).join('\n').trim();
          res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
          res.end(JSON.stringify({ text, article }));
        } catch(e) {
          res.writeHead(500, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
          res.end(JSON.stringify({ error: e.message }));
        }
      });
    });
    apiReq.on('error', e => {
      res.writeHead(500, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ error: e.message }));
    });
    apiReq.write(body);
    apiReq.end();
    return;
  }

  // ── /gdelt — новини + тональність по темі ──
  if (req.url.startsWith('/gdelt?')) {
    const params = new URLSearchParams(req.url.slice(7));
    const q = params.get('q') || '';
    if (!q) { res.writeHead(400); res.end('{}'); return; }

    // Check cache first
    const cached = getCached('gdelt:'+q);
    if (cached) {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(cached);
      return;
    }

    // Single request — no rate limit issues
    const gdeltUrl = 'https://api.gdeltproject.org/api/v2/doc/doc?query='
      + encodeURIComponent(q)
      + '&mode=ArtList&maxrecords=10&timespan=1d&sort=HybridRel&format=json';

    https.get(gdeltUrl, { headers: { 'User-Agent': 'ResonanceProxy/1.0' } }, (gr) => {
      let raw = '';
      gr.on('data', d => raw += d);
      gr.on('end', () => {
        if (raw.includes('limit requests') || raw.includes('error code')) {
          res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
          res.end(JSON.stringify({ items: [], avgTone: 0, query: q, rateLimited: true }));
          return;
        }
        try {
          const data = JSON.parse(raw);
          const articles = (data.articles || []).map(a => ({
            title: a.title, url: a.url, source: a.domain,
            date: a.seendate, country: a.sourcecountry, lang: a.language
          }));
          const result = JSON.stringify({ items: articles, avgTone: 0, query: q, fetchedAt: Date.now() });
          setCache('gdelt:'+q, result);
          res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
          res.end(result);
        } catch(e) {
          res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
          res.end(JSON.stringify({ items: [], avgTone: 0, query: q }));
        }
      });
    }).on('error', () => {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify({ items: [], avgTone: 0, query: q }));
    });
    return;
  }


  // ── /rss — агрегований RSS з BBC/AP/Al Jazeera ──
  if (req.url.startsWith('/rss')) {
    const params = new URLSearchParams(req.url.split('?')[1]||'');
    const q = (params.get('q')||'').toLowerCase();

    const feeds = [
      { name: 'BBC World', url: 'https://feeds.bbci.co.uk/news/world/rss.xml' },
      { name: 'Al Jazeera', url: 'https://www.aljazeera.com/xml/rss/all.xml' },
      { name: 'Reuters', url: 'https://feeds.reuters.com/reuters/worldNews' },
      { name: 'AP News', url: 'https://rsshub.app/apnews/topics/world-news' },
    ];

    const fetchFeed = (feed) => new Promise((resolve) => {
      https.get(feed.url, { headers: { 'User-Agent': 'ResonanceProxy/1.0' } }, (r) => {
        let raw = ''; r.on('data', d => raw += d);
        r.on('end', () => {
          try {
            // Parse RSS titles and links with regex
            const titles = [...raw.matchAll(/<title>(?:<!\[CDATA\[)?([^<\]]+?)(?:\]\]>)?<\/title>/g)]
              .map(m=>m[1].trim()).slice(1,11);
            const links = [...raw.matchAll(/<link>(?!http:\/\/)(https?:\/\/[^<]+)<\/link>/g)]
              .map(m=>m[1].trim()).slice(0,10);
            const pubDates = [...raw.matchAll(/<pubDate>([^<]+)<\/pubDate>/g)]
              .map(m=>m[1].trim()).slice(0,10);
            const items = titles.map((title,i) => ({
              title, url: links[i]||'', source: feed.name, date: pubDates[i]||''
            })).filter(it=>it.title&&it.url);
            resolve(items);
          } catch(e) { resolve([]); }
        });
      }).on('error', () => resolve([]));
    });

    // Check RSS base cache (without filter)
    const rssBase = getCached('rss:base');
    const doFetch = (baseItems) => {
      let all = baseItems || [];
      if (q) {
        const words = q.split(/\s+/).filter(w=>w.length>3);
        all = all.filter(it => {
          const t = it.title.toLowerCase();
          return words.some(w => t.includes(w));
        });
      }
      all = all.slice(0, 20);
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify({ items: all, query: q, fetchedAt: Date.now() }));
    };

    if (rssBase) {
      doFetch(JSON.parse(rssBase));
    } else {
      Promise.all(feeds.map(fetchFeed)).then(results => {
        const all = results.flat();
        setCache('rss:base', JSON.stringify(all));
        doFetch(all);
      });
    }
    return;
  }

  // ── /trends — Google Trending searches по країнах ──
  if (req.url.startsWith('/trends')) {
    const params = new URLSearchParams(req.url.split('?')[1]||'');
    const geos = (params.get('geo') || 'US,GB,DE,UA,IN,FR').split(',');
    const q = (params.get('q') || '').toLowerCase();

    // Cache 15 хвилин
    const cacheKey = 'trends:'+geos.join(',');
    const cached = getCached(cacheKey);
    if (cached) {
      const data = JSON.parse(cached);
      // Filter by query if provided
      const result = q ? data.filter(t => t.title.toLowerCase().includes(q)) : data;
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify({ items: result, query: q, fetchedAt: Date.now() }));
      return;
    }

    const fetchGeo = (geo) => new Promise((resolve) => {
      https.get({
        hostname: 'trends.google.com',
        path: '/trending/rss?geo='+geo,
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ResonanceBot/1.0)' }
      }, (r) => {
        let raw = ''; r.on('data', d => raw += d);
        r.on('end', () => {
          try {
            const titles = [...raw.matchAll(/<title>(?:<!\[CDATA\[)?(.+?)(?:\]\]>)?<\/title>/g)].map(m=>m[1].trim()).slice(1,11);
            const traffic = [...raw.matchAll(/<ht:approx_traffic>(.+?)<\/ht:approx_traffic>/g)].map(m=>m[1].trim());
            const items = titles.map((title,i) => ({ title, traffic: traffic[i]||'?', geo }));
            resolve(items);
          } catch(e) { resolve([]); }
        });
      }).on('error', () => resolve([]));
    });

    Promise.all(geos.map(fetchGeo)).then(results => {
      const all = results.flat();
      setCache(cacheKey, JSON.stringify(all));
      const result = q ? all.filter(t => t.title.toLowerCase().includes(q)) : all;
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify({ items: result, total: all.length, query: q, fetchedAt: Date.now() }));
    });
    return;
  }

  // ── /hn — Hacker News search ──
  if (req.url.startsWith('/hn?')) {
    const q = new URLSearchParams(req.url.split('?')[1]||'').get('q') || '';
    if (!q) { res.writeHead(400); res.end('{}'); return; }
    const hnUrl = 'https://hn.algolia.com/api/v1/search?query='+encodeURIComponent(q)+'&tags=story&hitsPerPage=5';
    https.get(hnUrl, { headers: { 'User-Agent': 'ResonanceProxy/1.0' } }, (r) => {
      let raw = ''; r.on('data', d => raw += d);
      r.on('end', () => {
        try {
          const data = JSON.parse(raw);
          const items = (data.hits||[]).map(h => ({
            title: h.title,
            url: h.url || 'https://news.ycombinator.com/item?id='+h.objectID,
            source: 'Hacker News · '+h.points+'pts',
            date: h.created_at
          }));
          res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
          res.end(JSON.stringify({ items, query: q, fetchedAt: Date.now() }));
        } catch(e) {
          res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
          res.end(JSON.stringify({ items: [], query: q }));
        }
      });
    }).on('error', () => { res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}); res.end('{}'); });
    return;
  }

  // ── /fng — Fear & Greed Index ──
  if (req.url === '/fng') {
    https.get('https://api.alternative.me/fng/?limit=7', { headers: { 'User-Agent': 'ResonanceProxy/1.0' } }, (r) => {
      let raw = ''; r.on('data', d => raw += d);
      r.on('end', () => {
        try {
          const d = JSON.parse(raw);
          const current = d.data[0];
          const history = d.data.slice(0,7).map(x=>({
            value: parseInt(x.value),
            classification: x.value_classification,
            date: new Date(parseInt(x.timestamp)*1000).toISOString().slice(0,10)
          }));
          res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
          res.end(JSON.stringify({
            value: parseInt(current.value),
            classification: current.value_classification,
            history, fetchedAt: Date.now()
          }));
        } catch(e) {
          res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
          res.end('{}');
        }
      });
    }).on('error', () => { res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}); res.end('{}'); });
    return;
  }


  // /predictions endpoint
  if (req.url === '/predictions') {
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ items: predCache.items, fetchedAt: predCache.fetchedAt, count: predCache.items.length }));
    return;
  }

  // /sec endpoint
  if (req.url.startsWith('/sec')) {
    const params = new URLSearchParams(req.url.split('?')[1]||'');
    if (params.get('refresh') === '1') secCache.fetchedAt = 0;
    fetchSecFilings('S-1,S-1/A,8-K,SC 13D,425,DEFM14A').then(items => {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify({ items, fetchedAt: secCache.fetchedAt, count: items.length }));
    }).catch(() => {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify({ items: [], count: 0 }));
    });
    return;
  }


  // /daily endpoint — daily signal digest
  if (req.url === '/daily') {
    const cached = getCached('daily');
    if (cached) {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(cached);
      return;
    }
    // Якщо кеш порожній — повертаємо з dailyCache або тригеримо rebuild
    if (dailyCache.items.length) {
      const result = JSON.stringify({ items: dailyCache.items, fetchedAt: dailyCache.fetchedAt, count: dailyCache.items.length });
      setCache('daily', result);
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(result);
      return;
    }
    // Тригеримо rebuild і повертаємо порожній поки що
    buildDailyDigest().then(items => {
      const result = JSON.stringify({ items, fetchedAt: Date.now(), count: items.length });
      setCache('daily', result);
    });
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ items: [], building: true, count: 0 }));
    return;
  }

  // /daily/refresh — примусовий rebuild
  if (req.url === '/daily/refresh') {
    dailyCache.fetchedAt = 0;
    setCache('daily', null);
    buildDailyDigest().then(items => {
      const result = JSON.stringify({ items, fetchedAt: Date.now(), count: items.length });
      setCache('daily', result);
    });
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ status: 'building', message: 'digest is being rebuilt' }));
    return;
  }


  // /history/run — ручний запуск history batch
  if (req.url === '/history/run') {
    runHistoryBatch();
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ status: 'running', message: 'history batch started' }));
    return;
  }


  // /assets?title=X — asset mapping для статті
  if (req.url.startsWith('/assets?')) {
    const q = new URLSearchParams(req.url.split('?')[1]||'');
    const title = q.get('title') || '';
    const type = q.get('type') || '';
    const assets = mapAssets(title, type, '', null);
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ title, type, assets }));
    return;
  }


  // /baseline/flush — ручний запуск
  if (req.url === '/baseline/flush') {
    flushBaselines();
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ status: 'flushed', entities: Object.keys(baselineMemory).length }));
    return;
  }

  // /baseline/size — скільки entities в пам'яті
  if (req.url === '/baseline/size') {
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ 
      memory_size: Object.keys(baselineMemory).length,
      sample: Object.keys(baselineMemory).slice(0,10)
    }));
    return;
  }


  // /tracked/expand — розширити через Wikidata
  if (req.url === '/tracked/expand') {
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ status: 'started', message: 'expansion running in background, check logs' }));
    expandTrackedViaWikidata().then(n => console.log('Expansion result:', n, 'added'));
    return;
  }

  // /tracked/list — список tracked entities
  if (req.url.startsWith('/tracked/list')) {
    const params = new URLSearchParams(req.url.split('?')[1]||'');
    const type = params.get('type') || '';
    const url = SUPABASE_URL + '/rest/v1/tracked_entities?order=importance.desc' + (type ? '&entity_type=eq.'+type : '');
    https.get(url, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY } }, (r2) => {
      let d = ''; r2.on('data', c => d += c);
      r2.on('end', () => {
        res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
        res.end(d);
      });
    }).on('error', () => { res.writeHead(500); res.end('{}'); });
    return;
  }


  // /backtest/run — запустити backtest
  if (req.url === '/backtest/run') {
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ status: 'started', events: BACKTEST_EVENTS.length, message: 'check logs and /backtest/results' }));
    runBacktest().then(r => console.log('Backtest done. Summary:', JSON.stringify(r.summary)));
    return;
  }

  // /backtest/results — переглянути результати
  if (req.url === '/backtest/results') {
    const url = SUPABASE_URL + '/rest/v1/backtest_results?order=created_at.desc&limit=50';
    https.get(url, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY } }, (r2) => {
      let d = ''; r2.on('data', c => d += c);
      r2.on('end', () => {
        res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
        res.end(d);
      });
    }).on('error', () => { res.writeHead(500); res.end('{}'); });
    return;
  }


  // /github/backtest — запустити GitHub backtest
  if (req.url === '/github/backtest') {
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ status: 'started', events: GITHUB_BACKTEST_EVENTS.length }));
    runGithubBacktest().then(r => console.log('GH Backtest done:', JSON.stringify(r.summary)));
    return;
  }

  // /github/backtest/results — переглянути
  if (req.url === '/github/backtest/results') {
    const url = SUPABASE_URL + '/rest/v1/github_backtest?order=created_at.desc&limit=50';
    https.get(url, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY } }, (r2) => {
      let d = ''; r2.on('data', c => d += c);
      r2.on('end', () => {
        res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
        res.end(d);
      });
    }).on('error', () => { res.writeHead(500); res.end('{}'); });
    return;
  }


  // /edgar/validate — pre-registered unbiased validation experiment
  // Differs from /edgar/backtest (curated 20 events) — fetches ALL Form 4 in 4-week period
  // Pre-registered criteria: median 60d/90d excess return < SPY-3pp, p<0.10, n>=30 best cell
  // Takes 30-45 minutes to complete. Streams progress to logs.
  if (req.url === '/edgar/validate') {
    handleEdgarValidate(req, res);
    return;
  }

  // /edgar/backtest — запустити EDGAR backtest
  if (req.url === '/edgar/backtest') {
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ status: 'started', events: EDGAR_BACKTEST_EVENTS.length }));
    runEdgarBacktest().then(r => console.log('EDGAR backtest done'));
    return;
  }

  // /edgar/backtest/results
  if (req.url === '/edgar/backtest/results') {
    const url = SUPABASE_URL + '/rest/v1/edgar_backtest?order=created_at.desc&limit=50';
    https.get(url, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY } }, (r2) => {
      let d = ''; r2.on('data', c => d += c);
      r2.on('end', () => {
        res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
        res.end(d);
      });
    }).on('error', () => { res.writeHead(500); res.end('{}'); });
    return;
  }


  // /convergence — крос-платформа аналіз
  if (req.url === '/convergence') {
    buildConvergence().then(result => {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify(result, null, 2));
    }).catch(e => {
      res.writeHead(500, {'Content-Type':'application/json'});
      res.end(JSON.stringify({error: e.message}));
    });
    return;
  }


  // /edgar/signals — переглянути EDGAR+INSIDER cross signals
  if (req.url === '/edgar/signals') {
    const url = SUPABASE_URL + '/rest/v1/cross_signals?type=eq.EDGAR%2BINSIDER&order=created_at.desc&limit=30';
    https.get(url, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }}, (r2) => {
      let d = ''; r2.on('data', c => d += c);
      r2.on('end', () => {
        res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
        res.end(d);
      });
    }).on('error', () => { res.writeHead(500); res.end('{}'); });
    return;
  }

  // /cards — list pending action cards
  if (req.url.startsWith('/cards')) {
    const params = new URLSearchParams(req.url.split('?')[1]||'');
    const status = params.get('status') || 'pending';
    const url = SUPABASE_URL + '/rest/v1/action_cards?status=eq.' + status
      + '&order=created_at.desc&limit=30';
    https.get(url, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }}, (r2) => {
      let d = ''; r2.on('data', c => d += c);
      r2.on('end', () => {
        res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
        res.end(d);
      });
    }).on('error', () => { res.writeHead(500); res.end('[]'); });
    return;
  }

  // /card/decide?id=X&decision=approve|reject|watch
  if (req.url.startsWith('/card/decide')) {
    const params = new URLSearchParams(req.url.split('?')[1]||'');
    const id = params.get('id');
    const decision = params.get('decision');
    if (!id || !['approve','reject','watch'].includes(decision)) {
      res.writeHead(400); res.end('{"error":"need id and decision"}'); return;
    }
    const newStatus = decision === 'approve' ? 'approved' : decision === 'reject' ? 'rejected' : 'watching';
    const body = JSON.stringify({ status: newStatus, user_decision: decision, decision_at: new Date().toISOString() });
    const req2 = https.request({
      hostname: 'ovedzfpptsnxzxioyzkr.supabase.co',
      path: '/rest/v1/action_cards?id=eq.' + id,
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        'apikey': SUPABASE_KEY,
        'Authorization': 'Bearer ' + SUPABASE_KEY,
        'Prefer': 'return=minimal'
      }
    }, (r3) => {
      let d = ''; r3.on('data', c => d += c);
      r3.on('end', async () => {
        // Якщо approved або watching — фіксуємо entry price
        if (decision === 'approve' || decision === 'watch') {
          // Отримаємо ticker з картки
          const cardUrl = SUPABASE_URL + '/rest/v1/action_cards?id=eq.' + id + '&select=signal_source';
          https.get(cardUrl, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }}, async (cr) => {
            let cd = ''; cr.on('data', c => cd += c);
            cr.on('end', async () => {
              try {
                const cardArr = JSON.parse(cd);
                const ticker = cardArr?.[0]?.signal_source;
                if (ticker) {
                  const entry = await captureCardEntry(id, ticker);
                  console.log('Card #' + id + ' entry captured at $' + entry);
                }
              } catch(e) {}
            });
          });
        }
        res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
        res.end(JSON.stringify({ id, decision, status: newStatus }));
      });
    });
    req2.on('error', () => { res.writeHead(500); res.end('{}'); });
    req2.write(body); req2.end();
    return;
  }

  // /outcomes/check — manual outcome check
  if (req.url === '/outcomes/check') {
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ status: 'started' }));
    runOutcomeCheck().then(r => console.log('Manual outcome:', JSON.stringify(r)));
    return;
  }

  // /edgar/parse?ticker=X — діагностика парсингу
  if (req.url.startsWith('/edgar/parse')) {
    const params = new URLSearchParams(req.url.split('?')[1]||'');
    const ticker = params.get('ticker') || '';
    if (!ticker) { res.writeHead(400); res.end('{}'); return; }
    getEdgarStatsEnhanced(ticker).then(result => {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify(result, null, 2));
    });
    return;
  }

  // /edgar/check — manual trigger
  if (req.url === '/edgar/check') {
    res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({ status: 'started', message: 'check running in background' }));
    runDailyEdgarCheck().then(r => console.log('Manual EDGAR check:', JSON.stringify(r)));
    return;
  }

  // /edgar/stats?ticker=X — статистика для тикера
  if (req.url.startsWith('/edgar/stats')) {
    const params = new URLSearchParams(req.url.split('?')[1]||'');
    const ticker = params.get('ticker') || '';
    if (!ticker) { res.writeHead(400); res.end('{}'); return; }
    getEdgarStats(ticker).then(stats => {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify(stats || { error: 'not found' }));
    });
    return;
  }

  // /ping endpoint — keepalive
  if (req.url === '/ping') {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
    res.end('ok');
    return;
  }

  // /ddg endpoint — DuckDuckGo news proxy
  if (req.url.startsWith('/ddg?')) {
    const q = new URL('http://localhost' + req.url).searchParams.get('q') || '';
    const ddgCached = getCached('ddg:'+q);
    if (ddgCached) {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(ddgCached);
      return;
    }
    https.get({
      hostname: 'api.duckduckgo.com',
      path: '/?q=' + encodeURIComponent(q) + '&format=json&no_html=1&skip_disambig=1',
      headers: { 'User-Agent': 'ResonanceBot/1.0' }
    }, (apiRes) => {
      let data = '';
      apiRes.on('data', c => data += c);
      apiRes.on('end', () => {
        try {
          const json = JSON.parse(data);
          const items = [];
          // RelatedTopics as news-like items
          (json.RelatedTopics || []).slice(0,5).forEach(t => {
            if (t.Text && t.FirstURL) {
              items.push({ title: t.Text.slice(0,120), url: t.FirstURL, source: 'DuckDuckGo', date: '' });
            }
          });
          // Abstract
          if (json.AbstractText) {
            items.unshift({ title: json.AbstractText.slice(0,200), url: json.AbstractURL || '#', source: json.AbstractSource || 'DuckDuckGo', date: 'today' });
          }
          const ddgResult = JSON.stringify({ items });
          setCache('ddg:'+q, ddgResult);
          res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
          res.end(ddgResult);
        } catch(e) {
          res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
          res.end(JSON.stringify({ items: [] }));
        }
      });
    }).on('error', () => {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ items: [] }));
    });
    return;
  }

  // /gnews endpoint — GNews API proxy (free tier, no key needed for basic)
  if (req.url.startsWith('/gnews?')) {
    const q = new URL('http://localhost' + req.url).searchParams.get('q') || '';
    const GNEWS_KEY = process.env.GNEWS_API_KEY || '';
    const path = GNEWS_KEY
      ? '/v4/search?q=' + encodeURIComponent(q) + '&lang=en&max=5&token=' + GNEWS_KEY
      : '/v4/search?q=' + encodeURIComponent(q) + '&lang=en&max=5';

    https.get({
      hostname: 'gnews.io',
      path: path,
      headers: { 'User-Agent': 'ResonanceBot/1.0' }
    }, (apiRes) => {
      let data = '';
      apiRes.on('data', c => data += c);
      apiRes.on('end', () => {
        try {
          const json = JSON.parse(data);
          const items = (json.articles || []).map(a => ({
            title: a.title,
            url: a.url,
            source: a.source?.name || 'GNews',
            date: a.publishedAt ? new Date(a.publishedAt).toLocaleString('uk-UA', {day:'numeric',month:'short',hour:'2-digit',minute:'2-digit'}) : ''
          }));
          const ddgResult = JSON.stringify({ items });
          setCache('ddg:'+q, ddgResult);
          res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
          res.end(ddgResult);
        } catch(e) {
          res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
          res.end(JSON.stringify({ items: [], error: e.message }));
        }
      });
    }).on('error', e => {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      res.end(JSON.stringify({ items: [] }));
    });
    return;
  }

  // /github endpoint
  if (req.url === '/github') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      trending: githubEventCache.trending || [],
      events: githubEventCache.events || [],
      fetchedAt: githubEventCache.fetchedAt || 0
    }));
    return;
  }

  // /binance endpoint
  if (req.url === '/binance') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      items: binanceStatsCache.items || [],
      fetchedAt: binanceStatsCache.fetchedAt || 0,
      signals: findCrossSignals()
    }));
    return;
  }

  // SSE stream
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  // Send current client heartbeat every 25s
  const heartbeat = setInterval(() => {
    try { res.write(': ping\n\n'); } catch(e) { clearInterval(heartbeat); }
  }, 25000);

  req.on('close', () => {
    clearInterval(heartbeat);
    sseClients.delete(res);
    res.end();
  });

  sseClients.add(res);
});

// ── GLOBAL UPSTREAM — runs independently of clients ──
const sseClients = new Set();

function connectGlobalUpstream() {
  const req2 = https.get({
    hostname: 'stream.wikimedia.org',
    path: '/v2/stream/recentchange',
    headers: { 'Accept': 'text/event-stream', 'User-Agent': 'ResonanceProxy/1.0' }
  }, (upstream) => {
    console.log('Wikipedia upstream connected');
    upstream.on('data', chunk => {
      try {
        chunk.toString().split('\n').forEach(line => {
          if (!line.startsWith('data: ')) return;
          try {
            const data = JSON.parse(line.slice(6));
            if ((data.type === 'edit' || data.type === 'new') &&
                ALL_WIKIS.has(data.wiki) &&
                data.title && !data.title.includes(':') &&
                !/(^List of|^Deaths in|^Nekrolog|^Список|^Тисяча|^\d{4} in |^\d{4}–|^Index of|^Outline of)/i.test(data.title)) {
              checkAnomaly(data.title, data.wiki, data.user, data.bot);

              // Tracked entity — швидка перевірка на КОЖНІЙ правці
              if (!looksLikeBot(data.user, data.bot)) {
                const tracked = checkTracked(data.title);
                if (tracked) {
                  const key = data.wiki + ':' + data.title;
                  if (!sentTrackedHits[key] || Date.now() - sentTrackedHits[key] > 3600000) {
                    sentTrackedHits[key] = Date.now();
                    console.log('TRACKED HIT (SSE):', data.title, '|', tracked.entity_type, '|', tracked.related_ticker);
                    supabaseInsert('cross_signals', {
                      type: 'TRACKED+HIT',
                      title: data.title,
                      detail: tracked.entity_type + ' · ' + tracked.category + ' · ticker:' + tracked.related_ticker + ' · imp:' + tracked.importance + (tracked.notes ? ' · ' + tracked.notes : '') + ' · user:' + (data.user||'?'),
                      wiki_title: data.title,
                      crypto_symbol: tracked.related_ticker,
                      score: tracked.importance * 10
                    }, 'title,type');

                    if (tracked.importance >= 9 && TELEGRAM_TOKEN) {
                      sendTelegram(
                        '🎯 <b>TRACKED: ' + data.title + '</b>\n\n' +
                        tracked.entity_type + ' · ' + tracked.category + '\n' +
                        '💹 Ticker: <b>' + tracked.related_ticker + '</b>\n' +
                        '✏️ Editor: ' + (data.user||'?') + '\n' +
                        (tracked.notes ? '📝 ' + tracked.notes + '\n' : '') +
                        'ℹ️ Importance: ' + tracked.importance + '/10'
                      );
                    }
                  }
                }
              }
              const msg = 'data: ' + JSON.stringify({
                title: data.title, wiki: data.wiki,
                user: data.user, bot: data.bot,
                type: data.type, timestamp: data.timestamp,
                tier: getTier(data.wiki)
              }) + '\n\n';
              // Broadcast to all connected clients
              sseClients.forEach(client => {
                try { client.write(msg); } catch(e) { sseClients.delete(client); }
              });
            }
          } catch(e) {}
        });
      } catch(e) {}
    });
    upstream.on('end', () => {
      console.log('Upstream ended, reconnecting in 2s...');
      setTimeout(connectGlobalUpstream, 2000);
    });
    upstream.on('error', (e) => {
      console.log('Upstream error:', e.message, '— reconnecting');
      setTimeout(connectGlobalUpstream, 2000);
    });
  });
  req2.on('error', (e) => {
    console.log('Upstream req error:', e.message);
    setTimeout(connectGlobalUpstream, 5000);
  });
}

// Start global Wikipedia upstream immediately on launch

// ════════════════════════════════════════
// PREDICTION MARKETS
// ════════════════════════════════════════

let predCache = { items: [], fetchedAt: 0 };

async function fetchPolymarketDirect() {
  return new Promise((resolve) => {
    https.get({
      hostname: 'gamma-api.polymarket.com',
      path: '/markets?closed=false&limit=100&order=volumeNum&ascending=false',
      headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' }
    }, (res) => {
      let raw = ''; res.on('data', d => raw += d);
      res.on('end', () => {
        if (raw.startsWith('<!DOCTYPE') || raw.startsWith('<html')) { resolve([]); return; }
        try {
          const data = JSON.parse(raw);
          const items = (Array.isArray(data) ? data : []).map(m => {
            let prob = null;
            try { prob = parseFloat(JSON.parse(m.outcomePrices||'[]')[0]||0); } catch(e) {}
            return { source:'polymarket', id:'pm_'+(m.id||''), title:m.question||'', probability:prob,
              volume:Math.round((m.volumeNum||0)/1000), url:'https://polymarket.com/event/'+(m.slug||''),
              categories:'', activity:m.volumeNum||0, closeTime:m.endDateIso||null };
          }).filter(q=>q.title);
          console.log('Polymarket:', items.length, 'markets');
          resolve(items);
        } catch(e) { resolve([]); }
      });
    }).on('error', () => resolve([]));
  });
}

async function fetchManifold() {
  return new Promise((resolve) => {
    https.get({
      hostname: 'api.manifold.markets',
      path: '/v0/markets?limit=50&sort=last-bet-time',
      headers: { 'User-Agent': 'ResonanceBot/1.0', 'Accept': 'application/json' }
    }, (res) => {
      let raw = ''; res.on('data', d => raw += d);
      res.on('end', () => {
        try {
          const data = JSON.parse(raw);
          if (!Array.isArray(data)) { resolve([]); return; }
          const items = data.map(q => ({
            source:'manifold', id:'mf_'+q.id, title:q.question||'',
            probability:q.probability||null, volume:Math.round(q.volume||0),
            url:q.url||'https://manifold.markets/'+q.id,
            categories:(q.tags||[]).join(','), activity:q.volume||0,
            closeTime:q.closeTime?new Date(q.closeTime).toISOString():null
          })).filter(q=>q.title);
          console.log('Manifold:', items.length, 'markets');
          resolve(items);
        } catch(e) { resolve([]); }
      });
    }).on('error', () => resolve([]));
  });
}

async function fetchPredictIt() {
  return new Promise((resolve) => {
    https.get({
      hostname: 'www.predictit.org',
      path: '/api/marketdata/all/',
      headers: { 'User-Agent': 'ResonanceBot/1.0', 'Accept': 'application/json' }
    }, (res) => {
      let raw = ''; res.on('data', d => raw += d);
      res.on('end', () => {
        try {
          const data = JSON.parse(raw);
          const items = (data.markets||[]).map(m => {
            const contracts = m.contracts||[];
            const top = contracts.sort((a,b)=>(b.volume||0)-(a.volume||0))[0];
            const prob = top?(top.lastTradePrice||top.bestYesPrice||null):null;
            return { source:'predictit', id:'pi_'+m.id, title:m.name||'', probability:prob,
              volume:contracts.reduce((s,c)=>s+(c.volume||0),0),
              url:m.url||'https://www.predictit.org/markets/detail/'+m.id,
              categories:'', activity:m.dateEndKnown?1:0, closeTime:m.timeStamp||null };
          }).filter(q=>q.title);
          console.log('PredictIt:', items.length, 'markets');
          resolve(items);
        } catch(e) { resolve([]); }
      });
    }).on('error', () => resolve([]));
  });
}

async function fetchAllPredictions() {
  const now = Date.now();
  if (now - predCache.fetchedAt < 600000 && predCache.items.length) return predCache.items;
  const [poly, manifold, predictit] = await Promise.all([fetchPolymarketDirect(), fetchManifold(), fetchPredictIt()]);
  const all = [...poly, ...manifold, ...predictit];
  predCache = { items: all, fetchedAt: now };
  console.log('Predictions total:', all.length);
  return all;
}

function checkPredictionSignals(title, wiki, editors, score) {
  if (editors < 2) return;
  fetchAllPredictions().then(predictions => {
    const words = title.toLowerCase().split(/[\s,.-]+/).filter(w=>w.length>3);
    const matches = predictions.filter(p => {
      const t = (p.title||'').toLowerCase();
      const cnt = words.filter(w=>t.includes(w)).length;
      return cnt >= 2 || words.some(w=>w.length>6&&t.includes(w));
    }).slice(0,3);
    matches.forEach(match => {
      const prob = match.probability !== null ? Math.round(match.probability*100) : null;
      supabaseInsert('cross_signals', {
        type: 'WIKI+PREDICT', title,
        detail: (prob!==null?'YES:'+prob+'% ':'')+'vol:'+(match.volume>1000?Math.round(match.volume/1000)+'K':match.volume)+' '+match.source.toUpperCase()+' · '+match.title.slice(0,80),
        wiki_title: title, crypto_symbol: null,
        score: Math.round(score * (prob||50)/100),
        source_url: match.url
      });
    });
  }).catch(()=>{});
}

fetchAllPredictions();
setInterval(fetchAllPredictions, 600000);


// ════════════════════════════════════════
// SEC EDGAR
// ════════════════════════════════════════

let secCache = { items: [], fetchedAt: 0 };

const EIGHT_K_ITEMS = {
  '1.01':{ label:'M&A угода', score:80, emoji:'💼' },
  '1.03':{ label:'Банкрутство', score:90, emoji:'💥' },
  '2.04':{ label:'Дефолт', score:85, emoji:'🔴' },
  '5.02':{ label:'Зміна CEO/CFO', score:70, emoji:'👤' },
  '7.01':{ label:'Прес-реліз', score:30, emoji:'📢' },
  '8.01':{ label:'Інше', score:20, emoji:'📄' },
};

const SEC_FORMS = {
  'S-1':{ label:'IPO', emoji:'🚀', score:80 },
  'S-1/A':{ label:'IPO amend', emoji:'🚀', score:40 },
  '8-K':{ label:'Подія', emoji:'⚡', score:60 },
  'SC 13D':{ label:'Акціонер', emoji:'🎯', score:50 },
  '425':{ label:'M&A', emoji:'💼', score:70 },
  'DEFM14A':{ label:'Merger', emoji:'💼', score:75 },
};

async function fetchSecFilings(forms) {
  const now = Date.now();
  if (now - secCache.fetchedAt < 300000 && secCache.items.length) return secCache.items;
  const today = new Date().toISOString().slice(0,10);
  const yesterday = new Date(now-86400000).toISOString().slice(0,10);
  const formList = forms || Object.keys(SEC_FORMS).join(',');
  return new Promise((resolve) => {
    const path = '/LATEST/search-index?forms='+encodeURIComponent(formList)
      +'&dateRange=custom&startdt='+yesterday+'&enddt='+today
      +'&_source=file_date,display_names,period_ending,file_num,root_forms,biz_states,items&from=0&size=100';
    https.get({
      hostname: 'efts.sec.gov', path,
      headers: { 'User-Agent': 'ResonanceBot/1.0 contact@resonance.app', 'Accept': 'application/json' }
    }, (res) => {
      let raw = ''; res.on('data', d => raw += d);
      res.on('end', () => {
        try {
          const data = JSON.parse(raw);
          const seen = new Set();
          const items = [];
          for (const hit of (data.hits?.hits||[])) {
            const s = hit._source;
            const nameRaw = s.display_names?.[0]||'';
            const company = nameRaw.split('(')[0].trim();
            const tickerM = nameRaw.match(/\(([A-Z0-9]{1,5})\)/);
            const ticker = tickerM?tickerM[1]:'';
            const form = s.root_forms?.[0]||'';
            const key = company+':'+form;
            if (seen.has(key)) continue;
            seen.add(key);
            let meta = SEC_FORMS[form]||{ label:form, emoji:'📄', score:20 };
            const itemTypes = s.items||[];
            if (form==='8-K' && itemTypes.length) {
              const best = itemTypes.map(it=>EIGHT_K_ITEMS[it]||{label:it,score:0,emoji:'📄'}).sort((a,b)=>b.score-a.score)[0];
              if (best.score < 40) { seen.delete(key); continue; }
              meta = {...meta, label:best.label, emoji:best.emoji, score:Math.max(meta.score,best.score)};
            }
            items.push({ company:company.slice(0,60), ticker, form, label:meta.label, emoji:meta.emoji,
              score:meta.score, state:s.biz_states?.[0]||'', date:s.file_date,
              url:'https://www.sec.gov/cgi-bin/browse-edgar?company='+encodeURIComponent(company.replace(/[,.]/g,'').trim())+'&CIK=&type='+encodeURIComponent(form)+'&dateb=&owner=include&count=10&search_text=&action=getcompany',
              itemTypes });
          }
          items.sort((a,b)=>b.score-a.score);
          secCache = { items, fetchedAt: now };
          console.log('SEC updated:', items.length, 'filings');
          resolve(items);
        } catch(e) { console.log('SEC parse error:', e.message); resolve([]); }
      });
    }).on('error', e => { console.log('SEC fetch error:', e.message); resolve([]); });
  });
}

fetchSecFilings();
setInterval(fetchSecFilings, 900000);



// ── GROQ LLM ──
const GROQ_API_KEY = process.env.GROQ_API_KEY || '';
const GROQ_MODEL = 'llama-3.3-70b-versatile';
const groqCache = new Map();

async function classifyWithGroq(comments, title, wiki) {
  if (!GROQ_API_KEY) return null;
  const unique = [...new Set((comments||[]).filter(c => c && c.length > 5))].slice(0, 5);
  if (!unique.length) return null;
  const cacheKey = title + '|' + unique.join('|');
  if (groqCache.has(cacheKey)) return groqCache.get(cacheKey);
  const prompt = 'You are a financial signal detector analyzing Wikipedia edit comments.\nArticle: "' + title + '" (' + wiki + ')\nRecent edit comments:\n' + unique.map((c,i) => (i+1)+'. "'+c+'"').join('\n') + '\n\nRespond ONLY with valid JSON:\n{"event_type":"IPO|CRISIS|MILESTONE|DEATH|CORPORATE|GEOPOLITICAL|CRYPTO|NOISE","signal_strength":0.0,"affected_assets":[],"direction":"LONG|SHORT|STRADDLE|WATCH|NONE","pimino_score":0.0,"keywords":[],"reasoning":"one sentence max"}';
  return new Promise((resolve) => {
    const body = JSON.stringify({ model: GROQ_MODEL, max_tokens: 250, temperature: 0.1, messages: [{ role: 'user', content: prompt }] });
    const req = https.request({
      hostname: 'api.groq.com', path: '/openai/v1/chat/completions', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + GROQ_API_KEY, 'Content-Length': Buffer.byteLength(body) }
    }, (res) => {
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.error) { resolve(null); return; }
          const text = json.choices?.[0]?.message?.content || '{}';
          const result = JSON.parse(text.replace(/```json|```/g,'').trim());
          groqCache.set(cacheKey, result);
          setTimeout(() => groqCache.delete(cacheKey), 1800000);
          resolve(result);
        } catch(e) { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(8000, () => { req.destroy(); resolve(null); });
    req.write(body); req.end();
  });
}

// ════════════════════════════════════════
// DAILY SIGNAL DIGEST
// Запускається кожні 6г, повертає 7-10 карток
// ════════════════════════════════════════

let dailyCache = { items: [], fetchedAt: 0 };

async function buildDailyDigest() {
  console.log('Building daily digest...');
  try {
    // 1. Беремо топ аномалії за 24г з Supabase
    const since = new Date(Date.now() - 86400000).toISOString();
    const anomUrl = SUPABASE_URL + '/rest/v1/anomalies?created_at=gte.' + since
      + '&order=score.desc&limit=30';
    const xsUrl = SUPABASE_URL + '/rest/v1/cross_signals?created_at=gte.' + since
      + '&order=score.desc&limit=50';

    const [anomRes, xsRes] = await Promise.all([
      new Promise((resolve) => {
        https.get(anomUrl, {
          headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }
        }, (r) => {
          let d = ''; r.on('data', c => d += c);
          r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve([]); } });
        }).on('error', () => resolve([]));
      }),
      new Promise((resolve) => {
        https.get(xsUrl, {
          headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }
        }, (r) => {
          let d = ''; r.on('data', c => d += c);
          r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve([]); } });
        }).on('error', () => resolve([]));
      })
    ]);

    if (!Array.isArray(anomRes) || !anomRes.length) {
      console.log('Daily digest: no anomalies found');
      return [];
    }

    // 2. Групуємо cross signals по title
    const xsByTitle = {};
    if (Array.isArray(xsRes)) {
      xsRes.forEach(xs => {
        if (!xsByTitle[xs.wiki_title || xs.title]) xsByTitle[xs.wiki_title || xs.title] = [];
        xsByTitle[xs.wiki_title || xs.title].push(xs.type);
      });
    }

    // 3. Будуємо контекст для Groq
    const anomContext = anomRes.slice(0, 20).map(a => {
      const signals = xsByTitle[a.title] || [];
      return `- "${a.title}" | type:${a.article_type||'?'} | editors:${a.editors} | score:${a.score} | signals:[${signals.join(',')||'none'}] | keywords:${a.comment_keywords||''}`;
    }).join('\n');

    const prompt = `You are a financial intelligence analyst specializing in event-driven trading signals. Analyze Wikipedia anomaly data and identify ONLY signals with real financial market impact.

ANOMALY DATA (format: title | type | editors | score | cross-signals | keywords):
${anomContext}

STRICT INCLUSION CRITERIA — include ONLY if at least one applies:
1. IPO/listing/funding round preparation (keywords: IPO, S-1, listing, offering, funding)
2. Corporate crisis: bankruptcy, fraud, CEO change, M&A, acquisition
3. Geopolitical event affecting markets: election results, sanctions, military conflict, coup
4. Death/health crisis of a major political or business leader (50+ language Wikipedia)
5. Regulatory/legal action affecting a public company or sector
6. Macro event: central bank decision, major economic data, trade deal

STRICT EXCLUSION — never include:
- Sports results, championships, player statistics
- Historical figures without current relevance
- Local/regional elections with no macro impact
- Entertainment, culture, music awards
- Academic or scientific topics without market relevance
- Wikipedia maintenance edits (REVERT signals only)

For each included signal:
- pattern: IPO_PREP | CRISIS | CORPORATE_CHANGE | GEOPOLITICAL | REGULATORY | MACRO | DEATH
- urgency: URGENT (act now) | HIGH (act today) | MEDIUM (watch) | LOW (monitor)
- signals: list ONLY confirmed cross-signals from the data [WIKI+LLM, WIKI+PREDICT, EDITOR+OVERLAP, etc]
- assets: specific tickers or currency pairs (e.g. AAPL, EUR/USD, GOLD) — NOT generic "stocks"
- reasoning: ONE sentence, specific, actionable — WHY this matters for markets NOW
- convergence: count of independent confirming signals (1-7)

Respond ONLY with valid JSON array. If no signals meet criteria, return []:
[{"rank":1,"title":"...","pattern":"...","urgency":"...","signals":[],"reasoning":"...","assets":[],"convergence":3}]

Maximum 7 items. Quality over quantity.
Additional rules:
- DEATH of any person with 20+ language Wikipedia versions = always include (urgency URGENT)
- If today has no strong financial signals, include top 3 most significant events anyway with LOW urgency
- Never return empty array — always include at least 1-3 items`;

    // 4. Відправляємо в Groq
    const result = await new Promise((resolve) => {
      const body = JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        max_tokens: 2000,
        temperature: 0.1,
        messages: [{ role: 'user', content: prompt }]
      });
      const req = https.request({
        hostname: 'api.groq.com',
        path: '/openai/v1/chat/completions',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer ' + GROQ_API_KEY,
          'Content-Length': Buffer.byteLength(body)
        }
      }, (res) => {
        let data = ''; res.on('data', c => data += c);
        res.on('end', () => {
          try {
            const json = JSON.parse(data);
            if (json.error) { console.log('Groq daily error:', json.error.message?.slice(0,80)); resolve(null); return; }
            const text = json.choices?.[0]?.message?.content || '[]';
            const clean = text.replace(/```json|```/g, '').trim();
            resolve(JSON.parse(clean));
          } catch(e) { console.log('Groq daily parse error:', e.message); resolve(null); }
        });
      });
      req.on('error', () => resolve(null));
      req.setTimeout(15000, () => { req.destroy(); resolve(null); });
      req.write(body); req.end();
    });

    if (!result || !Array.isArray(result)) return [];

    console.log('Daily digest:', result.length, 'signals');

    // 5. Зберігаємо в Supabase
    result.forEach(item => {
      supabaseInsert('daily_signals', {
        rank: item.rank,
        title: item.title,
        pattern: item.pattern,
        urgency: item.urgency,
        signals: item.signals,
        reasoning: item.reasoning,
        assets: item.assets,
        convergence: item.convergence
      });
    });

    // 6. Telegram summary
    if (TELEGRAM_TOKEN && result.length) {
      const urgent = result.filter(r => r.urgency === 'URGENT' || r.urgency === 'HIGH').slice(0,3);
      if (urgent.length) {
        const msg = '📊 <b>Daily Digest</b> · ' + new Date().toLocaleTimeString('uk-UA', {hour:'2-digit',minute:'2-digit'}) + '\n\n'
          + urgent.map(r => {
            const urgEmoji = r.urgency==='URGENT'?'🔴':r.urgency==='HIGH'?'🟡':'🔵';
            return urgEmoji + ' <b>' + r.title + '</b>\n'
              + r.pattern + ' · conv: ' + r.convergence + '/7\n'
              + (r.assets?.length ? '💹 ' + r.assets.join(', ') + '\n' : '')
              + '<i>' + r.reasoning + '</i>';
          }).join('\n\n');
        sendTelegram(msg);
      }
    }

    dailyCache = { items: result, fetchedAt: Date.now() };
    return result;

  } catch(e) {
    console.log('Daily digest error:', e.message);
    return [];
  }
}

// Запускаємо кожні 6 годин
buildDailyDigest();
setInterval(buildDailyDigest, 21600000);


// ════════════════════════════════════════
// WIKIPEDIA HISTORY BATCH — нічний аналіз
// Визначає BRANCH (підготовка) vs FLY (реакція)
// ════════════════════════════════════════

async function analyzeHistoryPattern(title, lang, editors, score) {
  if (!GROQ_API_KEY) return null;

  // Тягнемо revision history за 30 днів через Vercel /api/retro
  const today = new Date().toISOString().slice(0,10);
  const retroUrl = 'https://resonance-dashboard-7a1u.vercel.app/api/retro?title='
    + encodeURIComponent(title) + '&event=' + today + '&lang=' + (lang||'en') + '&days=30';

  const retroData = await new Promise((resolve) => {
    https.get(retroUrl, { headers: { 'User-Agent': 'ResonanceBot/1.0' } }, (r) => {
      let raw = ''; r.on('data', d => raw += d);
      r.on('end', () => { try { resolve(JSON.parse(raw)); } catch(e) { resolve(null); } });
    }).on('error', () => resolve(null));
  });

  if (!retroData?.found || !retroData.timeline?.length) return null;

  // Будуємо контекст для Groq
  const tl = retroData.timeline;
  const timelineStr = tl.map(d =>
    `T${d.t>0?'+':''}${d.t}д: ${d.edits}правок ${d.editors}ред ${d.signal!=='none'?'['+d.signal+']':''} ${d.comments?.slice(0,2).join(' | ')||''}`
  ).join('\n');

  const firstSignal = retroData.firstSignal;

  const prompt = `You are analyzing Wikipedia edit patterns to determine if this is a BRANCH (preparation before event) or FLY (reaction to already-happened event).

Article: "${title}" (editors today: ${editors}, score: ${score})
First anomalous signal: ${firstSignal ? 'T' + firstSignal.t + ' days, ' + firstSignal.signal : 'none'}

30-day edit timeline (T=0 is today):
${timelineStr}

Analyze the pattern:
- BRANCH: activity INCREASES in days BEFORE today (T-3 to T-1), suggests event is COMING
- FLY: activity SPIKES at T0 or after, suggests event ALREADY HAPPENED and people are reacting
- SUSTAINED: steady high activity over many days, suggests ongoing situation

Also determine:
- lead_time: how many days before today the signal started (negative = days ago)
- confidence: 0.0-1.0 how confident you are in the pattern
- next_72h: what likely happens in next 72 hours

Respond ONLY with valid JSON:
{"pattern":"BRANCH|FLY|SUSTAINED","lead_time":-3,"confidence":0.8,"reasoning":"one sentence","next_72h":"brief prediction","signal_quality":"STRONG|MED|WEAK"}`;

  return new Promise((resolve) => {
    const body = JSON.stringify({
      model: GROQ_MODEL, max_tokens: 300, temperature: 0.1,
      messages: [{ role: 'user', content: prompt }]
    });
    const req = https.request({
      hostname: 'api.groq.com', path: '/openai/v1/chat/completions', method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + GROQ_API_KEY,
        'Content-Length': Buffer.byteLength(body)
      }
    }, (res) => {
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.error) { resolve(null); return; }
          const text = json.choices?.[0]?.message?.content || '{}';
          const result = JSON.parse(text.replace(/```json|```/g,'').trim());
          console.log('History pattern:', title, '|', result.pattern, '| conf:', result.confidence, '| lead:', result.lead_time);
          resolve(result);
        } catch(e) { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(10000, () => { req.destroy(); resolve(null); });
    req.write(body); req.end();
  });
}

async function runHistoryBatch() {
  if (!GROQ_API_KEY) return;
  console.log('Running history batch...');

  try {
    // Беремо топ-10 аномалій за тиждень
    const since = new Date(Date.now() - 7 * 86400000).toISOString();
    const url = SUPABASE_URL + '/rest/v1/anomalies?created_at=gte.' + since
      + '&order=score.desc&limit=10';

    const anomalies = await new Promise((resolve) => {
      https.get(url, {
        headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }
      }, (r) => {
        let d = ''; r.on('data', c => d += c);
        r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve([]); } });
      }).on('error', () => resolve([]));
    });

    if (!Array.isArray(anomalies) || !anomalies.length) return;

    const results = [];

    for (const anom of anomalies.slice(0, 10)) {
      // Затримка між запитами щоб не вичерпати Groq rate limit
      await new Promise(r => setTimeout(r, 2000));

      const pattern = await analyzeHistoryPattern(anom.title, anom.lang, anom.editors, anom.score);
      if (!pattern) continue;

      results.push({ title: anom.title, ...pattern });

      // Зберігаємо в cross_signals
      supabaseInsert('cross_signals', {
        type: 'WIKI+HISTORY',
        title: anom.title,
        detail: pattern.pattern + ' · lead:' + pattern.lead_time + 'д · conf:' + pattern.confidence
          + ' · ' + pattern.reasoning
          + ' · next72h: ' + (pattern.next_72h||''),
        wiki_title: anom.title,
        crypto_symbol: null,
        score: Math.round(pattern.confidence * 80)
      }, 'title,type');

      // Telegram для BRANCH з високою впевненістю
      if (pattern.pattern === 'BRANCH' && pattern.confidence >= 0.7 && TELEGRAM_TOKEN) {
        sendTelegram(
          '🌿 <b>BRANCH Signal: ' + anom.title + '</b>\n\n' +
          '📅 Перший сигнал: T' + pattern.lead_time + ' днів\n' +
          '💡 ' + pattern.reasoning + '\n' +
          '🔮 Наступні 72г: ' + (pattern.next_72h||'невідомо') + '\n' +
          '📊 Впевненість: ' + Math.round(pattern.confidence*100) + '%'
        );
      }
    }

    console.log('History batch done:', results.length, 'analyzed');

    // Оновлюємо daily digest після batch
    if (results.length) buildDailyDigest();

  } catch(e) {
    console.log('History batch error:', e.message);
  }
}

// Запускаємо щоночі о 2:00 UTC
function scheduleHistoryBatch() {
  const now = new Date();
  const next2am = new Date(now);
  next2am.setUTCHours(2, 0, 0, 0);
  if (next2am <= now) next2am.setUTCDate(next2am.getUTCDate() + 1);
  const msUntil = next2am - now;
  console.log('History batch scheduled in', Math.round(msUntil/3600000), 'hours');
  setTimeout(() => {
    runHistoryBatch();
    setInterval(runHistoryBatch, 86400000); // потім кожні 24г
  }, msUntil);
}

scheduleHistoryBatch();

// Також /history/run для ручного запуску

// ════════════════════════════════════════
// ORACLE DETECTOR
// Визначає паттерн BRANCH vs FLY
// на основі поведінки редакторів
// ════════════════════════════════════════

// Фінансові keywords в коментарях
const FINANCIAL_KW = ['ipo','funding','acquisition','merger','bankrupt','fraud',
  'sec filing','s-1','listing','offering','chapter 11','liquidity','collapse',
  'initial public','went public','arrested','indicted','takeover'];

// Паніка/реакція keywords (FLY сигнали)
const PANIC_KW = ['reverted','undone','breaking','scandal','crisis','hack',
  'exploit','stolen','emergency','urgent','developing'];

function detectOraclePattern(title, wiki, anomData) {
  const { comments, users300, ts300, firstSeen } = anomData;
  const now = Date.now();

  if (!comments || !comments.length) return null;

  const commentsText = comments.join(' ').toLowerCase();
  const uniqueEditors = users300 ? users300.size : 0;
  const totalEdits = ts300 ? ts300.length : 0;
  const spanMin = (now - (firstSeen || now)) / 60000;

  // Фінансові коментарі
  const hasFinancialKW = FINANCIAL_KW.some(kw => commentsText.includes(kw));
  const hasPanicKW = PANIC_KW.some(kw => commentsText.includes(kw));

  // Детектуємо мови в коментарях (non-ASCII = не англійська)
  const nonAsciiComments = comments.filter(c => c && c.split('').some(ch => ch.charCodeAt(0) > 127)).length;
  const isMultiLingual = nonAsciiComments >= 2;

  // BRANCH ознаки:
  // - Мало редакторів але фінансові коментарі
  // - Концентрований редактор (багато правок від одного)
  const editorsList = anomData.editorsList || [];
  const maxEditorEdits = editorsList.length > 0 
    ? Math.max(...editorsList.map(e => e.count || 1)) 
    : 0;
  const isConcentrated = maxEditorEdits >= 3 && uniqueEditors <= 2;
  const isBranchPattern = hasFinancialKW && (uniqueEditors <= 3 || isConcentrated);

  // FLY ознаки:
  // - Раптовий вибух редакторів
  // - Різномовні коментарі (паніка роздрібних)
  // - Panic keywords
  const isBurst = uniqueEditors >= 8 && spanMin <= 30;
  const isFlyPattern = (isBurst || isMultiLingual || hasPanicKW) && !hasFinancialKW;

  // SUSTAINED:
  // - Стабільна активність протягом годин
  const isSustained = spanMin >= 120 && uniqueEditors >= 3 && !isFlyPattern && !isBranchPattern;

  let pattern = null;
  let confidence = 0;
  let signal_type = null;

  if (isBranchPattern) {
    pattern = 'BRANCH';
    confidence = hasFinancialKW && isConcentrated ? 0.85 : 0.65;
    signal_type = 'UPCOMING_EVENT';
  } else if (isFlyPattern) {
    pattern = 'FLY';
    confidence = isBurst && isMultiLingual ? 0.9 : isBurst ? 0.75 : 0.6;
    signal_type = 'REACTION';
  } else if (isSustained) {
    pattern = 'SUSTAINED';
    confidence = 0.6;
    signal_type = 'ONGOING';
  }

  if (!pattern) return null;

  return {
    pattern,
    confidence,
    signal_type,
    indicators: {
      hasFinancialKW,
      hasPanicKW,
      isMultiLingual,
      isConcentrated,
      isBurst,
      uniqueEditors,
      totalEdits,
      spanMin: Math.round(spanMin)
    }
  };
}

function runOracleDetector(title, wiki, key) {
  const w = anomWindow[key];
  if (!w || w.comments.length < 2) return;

  // Будуємо editorsList з users300
  const editorsList = [];
  if (w.users300) {
    w.users300.forEach(user => {
      const count = w.comments.filter ? 1 : 1; // approximate
      editorsList.push({ user, count: 1 });
    });
  }

  const result = detectOraclePattern(title, wiki, {
    comments: w.comments,
    users300: w.users300,
    ts300: w.ts300,
    firstSeen: w.firstSeen || Date.now(),
    editorsList
  });

  if (!result) return;
  if (result.confidence < 0.6) return;

  console.log('ORACLE:', title, '|', result.pattern, '| conf:', result.confidence, '| indicators:', JSON.stringify(result.indicators));

  // Зберігаємо в cross_signals
  supabaseInsert('cross_signals', {
    type: 'WIKI+ORACLE',
    title,
    detail: result.pattern
      + ' · conf:' + result.confidence
      + ' · ' + result.signal_type
      + ' · editors:' + result.indicators.uniqueEditors
      + (result.indicators.hasFinancialKW ? ' · FINANCIAL_KW' : '')
      + (result.indicators.isBurst ? ' · BURST' : '')
      + (result.indicators.isMultiLingual ? ' · MULTILINGUAL' : ''),
    wiki_title: title,
    crypto_symbol: null,
    score: Math.round(result.confidence * 90)
  }, 'title,type');

  // Telegram для BRANCH з high confidence
  if (result.pattern === 'BRANCH' && result.confidence >= 0.75 && TELEGRAM_TOKEN) {
    sendTelegram(
      '🌿 <b>ORACLE BRANCH: ' + title + '</b>\n\n' +
      '📊 Впевненість: ' + Math.round(result.confidence * 100) + '%\n' +
      '👥 Редакторів: ' + result.indicators.uniqueEditors + '\n' +
      (result.indicators.hasFinancialKW ? '💰 Фінансові keywords в коментарях\n' : '') +
      (result.indicators.isConcentrated ? '🎯 Концентрований редактор\n' : '') +
      '⏱ За ' + result.indicators.spanMin + ' хвилин\n\n' +
      '🔮 Тип: UPCOMING_EVENT — щось готується'
    );
  }

  // Telegram для FLY з burst
  if (result.pattern === 'FLY' && result.confidence >= 0.8 && TELEGRAM_TOKEN) {
    sendTelegram(
      '🪰 <b>ORACLE FLY: ' + title + '</b>\n\n' +
      '📊 Впевненість: ' + Math.round(result.confidence * 100) + '%\n' +
      '👥 ' + result.indicators.uniqueEditors + ' редакторів за ' + result.indicators.spanMin + ' хв\n' +
      (result.indicators.isMultiLingual ? '🌍 Різномовні коментарі — глобальна реакція\n' : '') +
      '⚡ Тип: REACTION — подія вже відбувається'
    );
  }
}


// ════════════════════════════════════════
// ASSET MAPPER
// Wikipedia категорії → фінансові активи
// ════════════════════════════════════════

const COUNTRY_CURRENCIES = {
  'hungary':'HUF','turkey':'TRY','ukraine':'UAH','russia':'RUB',
  'china':'CNY','japan':'JPY','india':'INR','brazil':'BRL',
  'argentina':'ARS','mexico':'MXN','south korea':'KRW',
  'australia':'AUD','canada':'CAD','united kingdom':'GBP',
  'european union':'EUR','switzerland':'CHF','norway':'NOK',
  'poland':'PLN','czech':'CZK','romania':'RON'
};

const SECTOR_ETFS = {
  'cryptocurrency':'BTC-USD',
  'bitcoin':'BTC-USD',
  'ethereum':'ETH-USD',
  'artificial intelligence':'QQQ',
  'semiconductor':'SOXX',
  'defense':'ITA',
  'oil':'USO',
  'gold':'GLD',
  'bank':'XLF',
  'pharmaceutical':'XPH',
  'airline':'JETS',
  'real estate':'VNQ',
  'energy':'XLE',
  'technology':'QQQ',
  'retail':'XRT',
};

const KNOWN_COMPANIES = {
  'apple':'AAPL','microsoft':'MSFT','google':'GOOGL','alphabet':'GOOGL',
  'amazon':'AMZN','meta':'META','nvidia':'NVDA','tesla':'TSLA',
  'boeing':'BA','airbus':'AIR.PA','toyota':'TM','volkswagen':'VOW3.DE',
  'samsung':'005930.KS','sony':'SONY','tencent':'TCEHY','alibaba':'BABA',
  'jpmorgan':'JPM','goldman sachs':'GS','blackrock':'BLK',
  'berkshire':'BRK-B','exxon':'XOM','chevron':'CVX',
  'thatgamecompany':'SONY', // Sony публікує їх ігри
  'annapurna':'AAPL', // Apple Arcade партнер
  'ftx':'BTC-USD','binance':'BNB-USD','coinbase':'COIN',
  'svb':'XLF','credit suisse':'CS','ubs':'UBS',
  'openai':'MSFT','anthropic':'GOOGL',
};

function mapAssets(title, articleType, categories, groqAssets) {
  const assets = new Set();
  const text = (title + ' ' + (categories||'') + ' ' + (articleType||'')).toLowerCase();

  // 1. Groq вже дав assets — перевіряємо і залишаємо валідні
  if (groqAssets && Array.isArray(groqAssets)) {
    groqAssets.forEach(a => {
      if (a && a.length <= 10 && /^[A-Z0-9.\-]+$/.test(a)) assets.add(a);
    });
  }

  // 2. Відомі компанії
  Object.entries(KNOWN_COMPANIES).forEach(([name, ticker]) => {
    if (text.includes(name)) assets.add(ticker);
  });

  // 3. Країни → валюти
  Object.entries(COUNTRY_CURRENCIES).forEach(([country, currency]) => {
    if (text.includes(country)) assets.add(currency);
  });

  // 4. Сектори → ETF
  Object.entries(SECTOR_ETFS).forEach(([sector, etf]) => {
    if (text.includes(sector)) assets.add(etf);
  });

  // 5. Типи статей (кирилиця і латиниця)
  if (articleType) {
    const at = articleType.toLowerCase();
    if (at.includes('геополіт') || at.includes('військ') || at.includes('geo') || at.includes('milit')) {
      assets.add('GLD'); assets.add('USO');
    }
    if (at.includes('бізнес') || at.includes('business')) assets.add('SPY');
    if ((at.includes('смерть') || at.includes('death')) && text.includes('politic')) assets.add('SPY');
    if (at.includes('політик') || at.includes('politic')) {
      // Шукаємо країну в заголовку
      Object.entries(COUNTRY_CURRENCIES).forEach(([country, currency]) => {
        if (title.toLowerCase().includes(country)) assets.add(currency);
      });
    }
  }

  // 6. Орбан/Угорщина — явний кейс
  const titleLow = title.toLowerCase();
  if (titleLow.includes('orbán') || titleLow.includes('orban') || titleLow.includes('hungarian') || titleLow.includes('hungary') || titleLow.includes('magyar') || titleLow.includes('fidesz')) {
    assets.add('HUF');
  }
  if (titleLow.includes('erdoğan') || titleLow.includes('erdogan') || titleLow.includes('turkey') || titleLow.includes('turkish')) {
    assets.add('TRY');
  }
  if (titleLow.includes('ukraine') || titleLow.includes('zelensky') || titleLow.includes('ukrainian')) {
    assets.add('UAH'); assets.add('GLD');
  }
  // US Politicians → SPY
  const usStates = ['california','texas','florida','new york','georgia','arizona','michigan','pennsylvania'];
  const isUSPolitician = usStates.some(s => text.includes(s)) || 
    text.includes('congress') || text.includes('senate') || 
    text.includes('representative') || text.includes('governor');
  if (isUSPolitician) { assets.add('SPY'); assets.add('USD'); }

  // Дефолт для політиків без знайдених активів
  if (assets.size === 0 && articleType) {
    const at = articleType.toLowerCase();
    if (at.includes('політик') || at.includes('politic')) assets.add('SPY');
    if (at.includes('бізнес') || at.includes('business')) assets.add('SPY');
    if (at.includes('геополіт') || at.includes('geo')) { assets.add('GLD'); assets.add('USO'); }
  }

  return [...assets].slice(0, 5); // максимум 5 активів
}

// Оновлюємо anomaly запис з mapped assets
function enrichWithAssets(title, articleType, groqResult) {
  const assets = mapAssets(
    title,
    articleType,
    '',
    groqResult?.affected_assets
  );
  if (!assets.length) return;

  // Оновлюємо cross_signals якщо є groq результат
  if (groqResult && assets.length) {
    console.log('Asset mapping:', title, '->', assets.join(','));
  }
  return assets;
}


// ════════════════════════════════════════
// BASELINE TRACKING + CONTENT DELTA
// ════════════════════════════════════════

// Отримуємо diff через Wikipedia API
async function fetchWikiDiff(title, lang) {
  return new Promise((resolve) => {
    const path = '/w/api.php?action=query&prop=revisions&titles='
      + encodeURIComponent(title)
      + '&rvprop=content|comment|size&rvlimit=2&rvdiffto=prev&format=json';
    https.get({
      hostname: lang + '.wikipedia.org', path,
      headers: { 'User-Agent': 'ResonanceBot/1.0' }
    }, (r) => {
      let raw = ''; r.on('data', d => raw += d);
      r.on('end', () => {
        try {
          const page = Object.values(JSON.parse(raw).query?.pages || {})[0];
          if (!page?.revisions?.length) { resolve(null); return; }
          const rev = page.revisions[0];
          resolve({
            size: rev.size || 0,
            comment: rev.comment || '',
            diff: rev.diff?.['*'] || ''
          });
        } catch(e) { resolve(null); }
      });
    }).on('error', () => resolve(null));
  });
}

// Парсимо diff HTML щоб витягти added/removed text
function parseDiffText(diffHtml) {
  if (!diffHtml) return { added: '', removed: '', section: '' };
  // Додані рядки — td class="diff-addedline"
  const addedMatches = [...diffHtml.matchAll(/<td[^>]*class="[^"]*diff-addedline[^"]*"[^>]*>([\s\S]*?)<\/td>/g)];
  const removedMatches = [...diffHtml.matchAll(/<td[^>]*class="[^"]*diff-deletedline[^"]*"[^>]*>([\s\S]*?)<\/td>/g)];
  const stripTags = s => s.replace(/<[^>]+>/g, '').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').trim();
  const added = addedMatches.map(m => stripTags(m[1])).filter(Boolean).join(' ').slice(0, 500);
  const removed = removedMatches.map(m => stripTags(m[1])).filter(Boolean).join(' ').slice(0, 500);
  // Секція — з comment типу /* Funding */
  return { added, removed, section: '' };
}

// Зберігаємо content delta
async function saveContentDelta(title, wiki, editor, comment) {
  const lang = wiki.replace('wiki','') || 'en';

  // Спрощена версія — без diff HTML (він занадто великий)
  // Витягуємо секцію з comment /* Section */
  const sectionMatch = comment.match(/\/\*\s*([^*]+)\s*\*\//);
  const section = sectionMatch ? sectionMatch[1].trim() : '';

  // Беремо останні 2 ревізії для розрахунку delta_bytes
  const path = '/w/api.php?action=query&prop=revisions&titles='
    + encodeURIComponent(title)
    + '&rvprop=size|comment&rvlimit=2&format=json';

  return new Promise((resolve) => {
    https.get({
      hostname: lang + '.wikipedia.org', path,
      headers: { 'User-Agent': 'ResonanceBot/1.0' }
    }, (r) => {
      let raw = ''; r.on('data', d => raw += d);
      r.on('end', () => {
        try {
          const page = Object.values(JSON.parse(raw).query?.pages || {})[0];
          const revs = page?.revisions || [];
          const deltaBytes = revs.length >= 2 ? (revs[0].size - revs[1].size) : 0;

          supabaseInsert('content_deltas', {
            title: title.slice(0, 200),
            wiki,
            editor: (editor||'').slice(0, 100),
            added_text: null, // не тягнемо diff для економії
            removed_text: null,
            section: section.slice(0, 100),
            delta_bytes: deltaBytes
          });
          resolve({ deltaBytes, section });
        } catch(e) { resolve(null); }
      });
    }).on('error', () => resolve(null));
    setTimeout(() => resolve(null), 5000);
  });
}

// Baseline — оновлюємо профіль статті
function updateBaseline(title, wiki, editor) {
  // Просто додаємо в пам'ять — батч запишеться щоночі
  if (!baselineMemory[title+'|'+wiki]) {
    baselineMemory[title+'|'+wiki] = { edits: 0, editors: new Set(), hours: {}, lastSeen: 0 };
  }
  const b = baselineMemory[title+'|'+wiki];
  b.edits++;
  if (editor) b.editors.add(editor);
  const hour = new Date().getUTCHours();
  b.hours[hour] = (b.hours[hour]||0) + 1;
  b.lastSeen = Date.now();
}

const baselineMemory = {};

// Щоночі пишемо baseline в Supabase
async function flushBaselines() {
  console.log('Flushing baselines:', Object.keys(baselineMemory).length, 'entities');
  const entries = Object.entries(baselineMemory).slice(0, 500); // обмежуємо щоб не перевантажити
  for (const [key, b] of entries) {
    const [title, wiki] = key.split('|');
    const typicalHours = Object.entries(b.hours)
      .sort((a,b)=>b[1]-a[1])
      .slice(0,5)
      .map(([h])=>parseInt(h));

    supabaseInsert('baseline_profiles', {
      entity_type: 'article',
      entity_key: key,
      avg_daily_edits: b.edits / 30,
      avg_daily_editors: b.editors.size / 30,
      typical_hours: typicalHours,
      total_edits_30d: b.edits,
      last_seen: new Date(b.lastSeen).toISOString()
    }, 'entity_key');
  }
  // Очищаємо після flush
  Object.keys(baselineMemory).forEach(k => delete baselineMemory[k]);
}

// Запускаємо flush о 3:00 UTC щодня
function scheduleBaselineFlush() {
  const now = new Date();
  const next3am = new Date(now);
  next3am.setUTCHours(3, 0, 0, 0);
  if (next3am <= now) next3am.setUTCDate(next3am.getUTCDate() + 1);
  const msUntil = next3am - now;
  console.log('Baseline flush scheduled in', Math.round(msUntil/3600000), 'hours');
  setTimeout(() => {
    flushBaselines();
    setInterval(flushBaselines, 86400000);
  }, msUntil);
}
scheduleBaselineFlush();

// Endpoint для ручного запуску + перегляду

// ════════════════════════════════════════
// TRACKED ENTITIES — focused detection
// ════════════════════════════════════════

const trackedEntities = new Map(); // wiki_title -> {entity_type, category, country, related_ticker, importance, notes}

async function loadTrackedEntities() {
  return new Promise((resolve) => {
    const url = SUPABASE_URL + '/rest/v1/tracked_entities?order=importance.desc';
    https.get(url, {
      headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }
    }, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => {
        try {
          const rows = JSON.parse(d);
          if (Array.isArray(rows)) {
            trackedEntities.clear();
            rows.forEach(row => trackedEntities.set(row.wiki_title, row));
            console.log('Tracked entities loaded:', trackedEntities.size);
          }
        } catch(e) {}
        resolve();
      });
    }).on('error', () => resolve());
  });
}

// Перевіряємо чи title в tracked list (exact або fuzzy match)
function checkTracked(title) {
  if (trackedEntities.has(title)) return trackedEntities.get(title);
  // Fuzzy — без діакритики
  const normalize = s => s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const normTitle = normalize(title);
  for (const [key, val] of trackedEntities) {
    if (normalize(key) === normTitle) return val;
  }
  return null;
}

// Завантажуємо при старті і оновлюємо кожну годину
loadTrackedEntities();
setInterval(loadTrackedEntities, 3600000);


// ════════════════════════════════════════
// TRACKED EXPAND — Wikidata subsidiaries
// ════════════════════════════════════════

async function expandTrackedViaWikidata() {
  console.log('Expanding tracked entities via Wikidata...');
  const url = SUPABASE_URL + '/rest/v1/tracked_entities?order=importance.desc';
  const parents = await new Promise((resolve) => {
    https.get(url, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY } }, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve([]); } });
    }).on('error', () => resolve([]));
  });

  const onlyTankers = parents.filter(p => !(p.notes||'').startsWith('SUB of'));
  console.log('Tankers to expand:', onlyTankers.length);

  const seen = new Set(parents.map(p => p.wiki_title));
  let added = 0;

  for (const parent of onlyTankers.slice(0, 40)) { // обмежуємо 40 за запуск
    // Get QID
    const qidPath = '/w/api.php?action=wbsearchentities&search='
      + encodeURIComponent(parent.wiki_title) + '&language=en&limit=1&format=json';
    const qid = await new Promise((resolve) => {
      https.get({ hostname: 'www.wikidata.org', path: qidPath, headers: { 'User-Agent': 'ResonanceBot/1.0' } }, (r) => {
        let d = ''; r.on('data', c => d += c);
        r.on('end', () => { try { resolve(JSON.parse(d).search?.[0]?.id || null); } catch(e) { resolve(null); } });
      }).on('error', () => resolve(null));
    });
    if (!qid) continue;

    // SPARQL related
    const sparql = 'SELECT DISTINCT ?relLabel ?relatedLabel WHERE {'
      + 'VALUES ?item { wd:' + qid + ' } '
      + 'VALUES ?rel { wdt:P355 wdt:P749 wdt:P127 wdt:P3320 wdt:P169 wdt:P1037 wdt:P488 wdt:P112 wdt:P159 } '
      + '{ ?item ?rel ?related . } UNION { ?related ?rel ?item . } '
      + '?related rdfs:label ?relatedLabel FILTER(LANG(?relatedLabel)=\'en\') . '
      + '?relProp wikibase:directClaim ?rel ; rdfs:label ?relLabel FILTER(LANG(?relLabel)=\'en\') . '
      + 'FILTER(STRLEN(?relatedLabel) < 60) } LIMIT 25';

    const related = await new Promise((resolve) => {
      const url = '/sparql?query=' + encodeURIComponent(sparql) + '&format=json';
      https.get({ hostname: 'query.wikidata.org', path: url,
        headers: { 'User-Agent': 'ResonanceBot/1.0', 'Accept': 'application/json' }
      }, (r) => {
        let d = ''; r.on('data', c => d += c);
        r.on('end', () => {
          try {
            const bindings = JSON.parse(d).results?.bindings || [];
            resolve(bindings.map(b => ({
              title: b.relatedLabel?.value,
              relation: b.relLabel?.value || ''
            })).filter(x => x.title));
          } catch(e) { resolve([]); }
        });
      }).on('error', () => resolve([]));
      setTimeout(() => resolve([]), 10000);
    });

    for (const rel of related) {
      if (!rel.title || rel.title.length < 3 || rel.title.length > 60) continue;
      if (/^\d{4}$/.test(rel.title)) continue;
      if (['United States','Human','Company','Corporation','Private company','Public company'].includes(rel.title)) continue;
      if (seen.has(rel.title)) continue;
      seen.add(rel.title);

      supabaseInsert('tracked_entities', {
        wiki_title: rel.title,
        entity_type: 'SUB',
        category: parent.category,
        country: parent.country,
        related_ticker: parent.related_ticker,
        importance: Math.max(3, parent.importance - 3),
        notes: 'SUB of ' + parent.wiki_title + ' · ' + rel.relation
      }, 'wiki_title');
      added++;
    }

    console.log('Expanded', parent.wiki_title, '->', related.length, 'related');
    // Rate limit Wikidata — 2с
    await new Promise(r => setTimeout(r, 2000));
  }

  console.log('Expansion done — added:', added);
  return added;
}


// ════════════════════════════════════════
// BACKTEST ENGINE — ретроспективна перевірка
// ════════════════════════════════════════

const BACKTEST_EVENTS = [
  { date: '2025-03-28', title: 'CoreWeave', lang: 'en', type: 'IPO', ticker: 'CRWV', expected_pattern: 'BRANCH', importance: 9 },
  { date: '2025-07-31', title: 'Figma', lang: 'en', type: 'IPO', ticker: 'FIG', expected_pattern: 'BRANCH', importance: 9 },
  { date: '2025-06-05', title: 'Circle Internet Group', lang: 'en', type: 'IPO', ticker: 'CRCL', expected_pattern: 'BRANCH', importance: 8 },
  { date: '2024-04-04', title: 'Reddit', lang: 'en', type: 'IPO', ticker: 'RDDT', expected_pattern: 'BRANCH', importance: 8 },
  { date: '2022-11-11', title: 'FTX', lang: 'en', type: 'CRISIS', ticker: 'BTC-USD', expected_pattern: 'FLY', importance: 10 },
  { date: '2023-03-10', title: 'Silicon Valley Bank', lang: 'en', type: 'CRISIS', ticker: 'XLF', expected_pattern: 'FLY', importance: 10 },
  { date: '2023-03-19', title: 'Credit Suisse', lang: 'en', type: 'CRISIS', ticker: 'UBS', expected_pattern: 'FLY', importance: 9 },
  { date: '2024-07-19', title: 'CrowdStrike', lang: 'en', type: 'CRISIS', ticker: 'CRWD', expected_pattern: 'FLY', importance: 9 },
  { date: '2025-01-27', title: 'DeepSeek', lang: 'en', type: 'EVENT', ticker: 'NVDA', expected_pattern: 'FLY', importance: 10 },
  { date: '2023-11-17', title: 'Sam Altman', lang: 'en', type: 'CORPORATE', ticker: 'MSFT', expected_pattern: 'FLY', importance: 10 },
  { date: '2024-08-23', title: 'Pavel Durov', lang: 'en', type: 'CRISIS', ticker: 'TON-USD', expected_pattern: 'FLY', importance: 9 },
  { date: '2024-11-05', title: '2024 United States presidential election', lang: 'en', type: 'ELECTION', ticker: 'SPY', expected_pattern: 'FLY', importance: 10 },
  { date: '2024-11-23', title: 'Javier Milei', lang: 'en', type: 'ELECTION', ticker: 'ARS', expected_pattern: 'BRANCH', importance: 8 },
  { date: '2024-07-04', title: '2024 United Kingdom general election', lang: 'en', type: 'ELECTION', ticker: 'GBP', expected_pattern: 'BRANCH', importance: 8 },
  { date: '2022-09-08', title: 'Elizabeth II', lang: 'en', type: 'DEATH', ticker: 'GBP', expected_pattern: 'FLY', importance: 10 },
  { date: '2025-04-21', title: 'Pope Francis', lang: 'en', type: 'DEATH', ticker: null, expected_pattern: 'FLY', importance: 10 },
  { date: '2022-02-24', title: '2022 Russian invasion of Ukraine', lang: 'en', type: 'GEOPOLITICAL', ticker: 'RUB', expected_pattern: 'FLY', importance: 10 },
  { date: '2023-10-07', title: 'October 7 attacks', lang: 'en', type: 'GEOPOLITICAL', ticker: 'GLD', expected_pattern: 'FLY', importance: 10 },
  { date: '2024-12-08', title: 'Fall of the Assad regime', lang: 'en', type: 'GEOPOLITICAL', ticker: 'USO', expected_pattern: 'FLY', importance: 9 },
  { date: '2024-01-10', title: 'Bitcoin', lang: 'en', type: 'CRYPTO', ticker: 'BTC-USD', expected_pattern: 'BRANCH', importance: 10 },
  { date: '2022-11-30', title: 'ChatGPT', lang: 'en', type: 'PRODUCT', ticker: 'MSFT', expected_pattern: 'FLY', importance: 10 },
  { date: '2025-01-19', title: 'TikTok', lang: 'en', type: 'REGULATORY', ticker: 'META', expected_pattern: 'FLY', importance: 9 },

  // M&A
  { date: '2022-10-27', title: 'Twitter, Inc.', lang: 'en', type: 'MA', ticker: 'TSLA', expected_pattern: 'BRANCH', importance: 9 },
  { date: '2023-10-13', title: 'Cisco Systems', lang: 'en', type: 'MA', ticker: 'CSCO', expected_pattern: 'BRANCH', importance: 7, notes: 'Splunk acquisition' },
  { date: '2024-01-26', title: 'Hewlett Packard Enterprise', lang: 'en', type: 'MA', ticker: 'HPE', expected_pattern: 'BRANCH', importance: 6, notes: 'Juniper deal' },
  { date: '2025-05-30', title: 'OpenAI', lang: 'en', type: 'CORPORATE', ticker: 'MSFT', expected_pattern: 'BRANCH', importance: 9, notes: 'io acquisition' },
  
  // FDA / PHARMA
  { date: '2023-06-08', title: 'Eli Lilly and Company', lang: 'en', type: 'PHARMA', ticker: 'LLY', expected_pattern: 'BRANCH', importance: 8, notes: 'Mounjaro/Zepbound prep' },
  { date: '2023-08-08', title: 'Novo Nordisk', lang: 'en', type: 'PHARMA', ticker: 'NVO', expected_pattern: 'BRANCH', importance: 8, notes: 'Wegovy heart trial' },
  { date: '2024-02-21', title: 'Pfizer', lang: 'en', type: 'EARNINGS', ticker: 'PFE', expected_pattern: 'FLY', importance: 6 },

  // EARNINGS REACTIONS
  { date: '2023-05-24', title: 'Nvidia', lang: 'en', type: 'EARNINGS', ticker: 'NVDA', expected_pattern: 'FLY', importance: 10, notes: 'AI breakout earnings' },
  { date: '2024-02-21', title: 'Nvidia', lang: 'en', type: 'EARNINGS', ticker: 'NVDA', expected_pattern: 'FLY', importance: 9 },
  { date: '2024-02-22', title: 'Eli Lilly and Company', lang: 'en', type: 'EARNINGS', ticker: 'LLY', expected_pattern: 'FLY', importance: 7 },
  { date: '2024-08-29', title: 'Nvidia', lang: 'en', type: 'EARNINGS', ticker: 'NVDA', expected_pattern: 'FLY', importance: 9 },
  
  // BANKRUPTCIES / FAILURES
  { date: '2023-08-21', title: 'WeWork', lang: 'en', type: 'CRISIS', ticker: null, expected_pattern: 'BRANCH', importance: 8, notes: 'going concern warning' },
  { date: '2023-11-06', title: 'WeWork', lang: 'en', type: 'CRISIS', ticker: null, expected_pattern: 'FLY', importance: 7, notes: 'Chapter 11 filing' },
  { date: '2024-04-22', title: 'Boeing', lang: 'en', type: 'CRISIS', ticker: 'BA', expected_pattern: 'FLY', importance: 8, notes: 'Whistleblower events' },
  { date: '2024-01-05', title: 'Boeing', lang: 'en', type: 'CRISIS', ticker: 'BA', expected_pattern: 'FLY', importance: 9, notes: 'Alaska Airlines door blowout' },
  
  // TECH PRODUCT LAUNCHES
  { date: '2024-09-09', title: 'iPhone 16', lang: 'en', type: 'PRODUCT', ticker: 'AAPL', expected_pattern: 'BRANCH', importance: 7 },
  { date: '2023-09-12', title: 'iPhone 15', lang: 'en', type: 'PRODUCT', ticker: 'AAPL', expected_pattern: 'BRANCH', importance: 7 },
  { date: '2024-02-02', title: 'Apple Vision Pro', lang: 'en', type: 'PRODUCT', ticker: 'AAPL', expected_pattern: 'BRANCH', importance: 8 },
  { date: '2025-02-01', title: 'Grok (chatbot)', lang: 'en', type: 'PRODUCT', ticker: 'TSLA', expected_pattern: 'BRANCH', importance: 7, notes: 'Grok 3' },
  
  // ELECTIONS — додаткові
  { date: '2023-11-19', title: 'Javier Milei', lang: 'en', type: 'ELECTION', ticker: 'ARS', expected_pattern: 'BRANCH', importance: 8, notes: 'Argentina runoff' },
  { date: '2024-06-30', title: '2024 French legislative election', lang: 'en', type: 'ELECTION', ticker: 'EUR', expected_pattern: 'BRANCH', importance: 8 },
  { date: '2024-10-27', title: '2024 Japanese general election', lang: 'en', type: 'ELECTION', ticker: 'JPY', expected_pattern: 'BRANCH', importance: 7 },
  
  // GEOPOLITICS — додаткові
  { date: '2024-07-13', title: 'Donald Trump', lang: 'en', type: 'EVENT', ticker: 'SPY', expected_pattern: 'FLY', importance: 9, notes: 'Assassination attempt' },
  { date: '2025-06-13', title: 'Iran–Israel war', lang: 'en', type: 'GEOPOLITICAL', ticker: 'USO', expected_pattern: 'FLY', importance: 10 },
  { date: '2024-09-24', title: 'Hezbollah pager explosions', lang: 'en', type: 'GEOPOLITICAL', ticker: 'GLD', expected_pattern: 'FLY', importance: 9 },
  
  // CRYPTO
  { date: '2022-05-09', title: 'Terra (blockchain)', lang: 'en', type: 'CRISIS', ticker: 'BTC-USD', expected_pattern: 'FLY', importance: 10, notes: 'UST collapse' },
  { date: '2023-06-05', title: 'Binance', lang: 'en', type: 'REGULATORY', ticker: 'BNB-USD', expected_pattern: 'FLY', importance: 9, notes: 'SEC charges' },
  { date: '2024-05-23', title: 'Ethereum', lang: 'en', type: 'CRYPTO', ticker: 'ETH-USD', expected_pattern: 'BRANCH', importance: 8, notes: 'Spot ETF approval' },
  
  // SCANDALS / RESIGNATIONS
  { date: '2023-08-31', title: 'Mitch McConnell', lang: 'en', type: 'CRISIS', ticker: 'SPY', expected_pattern: 'FLY', importance: 6, notes: 'Health freeze' },
  { date: '2024-07-21', title: 'Joe Biden', lang: 'en', type: 'POLITICS', ticker: 'SPY', expected_pattern: 'BRANCH', importance: 9, notes: 'Withdrawal from race' },
  { date: '2025-09-25', title: 'Charlie Kirk', lang: 'en', type: 'EVENT', ticker: null, expected_pattern: 'FLY', importance: 8 },
  
  // CORPORATE
  { date: '2024-01-05', title: 'Claudine Gay', lang: 'en', type: 'CORPORATE', ticker: null, expected_pattern: 'BRANCH', importance: 6, notes: 'Harvard president resignation' },
  { date: '2024-12-04', title: 'Brian Thompson', lang: 'en', type: 'CRISIS', ticker: 'UNH', expected_pattern: 'FLY', importance: 9, notes: 'UnitedHealth CEO killed' },
  { date: '2024-09-17', title: 'Hezbollah', lang: 'en', type: 'GEOPOLITICAL', ticker: 'GLD', expected_pattern: 'FLY', importance: 9, notes: 'Pager attacks' },
  
  // MARKETS / FED
  { date: '2024-08-05', title: 'Black Monday', lang: 'en', type: 'CRISIS', ticker: 'SPY', expected_pattern: 'FLY', importance: 9, notes: 'Yen carry trade unwind' },
  { date: '2025-04-09', title: '2025 stock market crash', lang: 'en', type: 'CRISIS', ticker: 'SPY', expected_pattern: 'FLY', importance: 10, notes: 'Tariff crash' },
];

// Аналіз timeline через детектори
function analyzeTimeline(timeline, allUsers, rareEditors) {
  if (!timeline || !timeline.length) {
    return { detected: false, lead_time: 0, confidence: 0, pattern: null, signals: [], reasoning: 'no timeline' };
  }

  const signals = [];
  let firstSignalT = 0;
  let detectedPattern = null;
  let confidence = 0;

  // Шукаємо найперший день з editors >= 2
  const earlyMultiEditor = timeline.find(d => d.t < 0 && d.editors >= 2);
  if (earlyMultiEditor) {
    signals.push('multi_editor_at_T' + earlyMultiEditor.t);
    if (earlyMultiEditor.t < firstSignalT) firstSignalT = earlyMultiEditor.t;
  }

  // Рідкісні редактори за T-30..T-1
  const rareInPeriod = timeline.filter(d => d.t < 0 && d.rareUsers && d.rareUsers.length > 0);
  if (rareInPeriod.length >= 2) {
    signals.push('rare_editor_cluster');
    const earliest = Math.min(...rareInPeriod.map(d => d.t));
    if (earliest < firstSignalT) firstSignalT = earliest;
  }

  // Фінансові keywords в коментарях за T<0
  const FIN_KW = ['ipo','funding','acquisition','merger','bankrupt','fraud','sec filing','s-1','listing','offering','chapter 11','liquidity','collapse','initial public','went public','arrested','indicted'];
  const finDays = timeline.filter(d => d.t < 0 && d.comments && d.comments.some(c => 
    FIN_KW.some(kw => (c||'').toLowerCase().includes(kw))
  ));
  if (finDays.length > 0) {
    signals.push('financial_keyword_at_T' + finDays[0].t);
    if (finDays[0].t < firstSignalT) firstSignalT = finDays[0].t;
  }

  // STRONG signal в історії (T<0)
  const strongDays = timeline.filter(d => d.t < 0 && d.signal === 'STRONG');
  if (strongDays.length > 0) {
    signals.push('strong_signal_at_T' + strongDays[0].t);
    if (strongDays[0].t < firstSignalT) firstSignalT = strongDays[0].t;
  }

  // Burst — багато редакторів за день
  const burstDays = timeline.filter(d => d.t < 0 && d.editors >= 5);
  if (burstDays.length > 0) {
    signals.push('editor_burst_at_T' + burstDays[0].t);
  }

  // Концентрований редактор з фін. керy
  const concentrated = timeline.find(d => 
    d.t < 0 && d.users && d.users.some(u => u.count >= 3 && !u.isIP) &&
    d.comments && d.comments.some(c => FIN_KW.some(kw => (c||'').toLowerCase().includes(kw)))
  );
  if (concentrated) signals.push('concentrated_editor_with_finkw');

  // Класифікація паттерну
  if (signals.includes('rare_editor_cluster') || signals.includes('concentrated_editor_with_finkw') || 
      signals.some(s => s.includes('financial_keyword'))) {
    detectedPattern = 'BRANCH';
    confidence = 0.7 + (signals.length * 0.05);
  } else if (signals.includes('strong_signal_at_T0') || signals.some(s => s.includes('editor_burst'))) {
    detectedPattern = 'FLY';
    confidence = 0.6 + (signals.length * 0.05);
  } else if (signals.length >= 2) {
    detectedPattern = 'WEAK';
    confidence = 0.4;
  }

  // Якщо firstSignalT = 0 і є multi_editor — теж сигнал
  if (firstSignalT === 0 && earlyMultiEditor) firstSignalT = earlyMultiEditor.t;

  const detected = !!detectedPattern && firstSignalT < 0;
  
  return {
    detected,
    lead_time: -firstSignalT, // позитивне число = за скільки днів до події
    confidence: Math.min(confidence, 1),
    pattern: detectedPattern,
    signals,
    reasoning: detected 
      ? `${detectedPattern} pattern detected, first signal at T${firstSignalT}d`
      : 'no clear preparatory signal'
  };
}

async function fetchRetroData(title, eventDate, lang, days) {
  return new Promise((resolve) => {
    const url = 'https://resonance-dashboard-7a1u.vercel.app/api/retro?title='
      + encodeURIComponent(title) + '&event=' + eventDate + '&lang=' + (lang||'en') + '&days=' + days;
    https.get(url, { headers: { 'User-Agent': 'ResonanceBot/1.0' }}, (r) => {
      let raw = ''; r.on('data', d => raw += d);
      r.on('end', () => { try { resolve(JSON.parse(raw)); } catch(e) { resolve(null); } });
    }).on('error', () => resolve(null));
    setTimeout(() => resolve(null), 30000);
  });
}

async function runBacktest() {
  console.log('Running backtest on', BACKTEST_EVENTS.length, 'events...');
  const results = [];

  for (const event of BACKTEST_EVENTS) {
    console.log('Backtest:', event.title, event.date);
    const days = event.expected_pattern === 'BRANCH' ? 60 : 30;
    const data = await fetchRetroData(event.title, event.date, event.lang, days);

    if (!data || !data.found) {
      results.push({ ...event, detected: false, reasoning: 'no data' });
      console.log('  → no data');
      continue;
    }

    const analysis = analyzeTimeline(data.timeline, data.allUsers, data.rareEditors);
    
    const result = {
      event_date: event.date,
      event_title: event.title,
      event_type: event.type,
      expected_pattern: event.expected_pattern,
      detected: analysis.detected,
      detected_pattern: analysis.pattern,
      lead_time_days: analysis.lead_time,
      confidence: analysis.confidence,
      signals_found: analysis.signals,
      rare_editors_found: (data.rareEditors||[]).slice(0,5).map(e => e.user),
      total_revisions: data.total || 0,
      reasoning: analysis.reasoning
    };

    results.push(result);

    // Записуємо в Supabase
    supabaseInsert('backtest_results', result);

    console.log('  →', analysis.detected ? 'DETECTED' : 'MISSED', 
      '|', analysis.pattern, '| lead:', analysis.lead_time + 'd',
      '| conf:', analysis.confidence.toFixed(2),
      '| signals:', analysis.signals.length);

    // Rate limit
    await new Promise(r => setTimeout(r, 2000));
  }

  // Підсумок
  const detected = results.filter(r => r.detected);
  const tpExpected = results.filter(r => r.detected && r.detected_pattern === r.expected_pattern);
  console.log('\n═══ BACKTEST SUMMARY ═══');
  console.log('Total events:', results.length);
  console.log('Detected:', detected.length, '(' + Math.round(detected.length/results.length*100) + '%)');
  console.log('Pattern match:', tpExpected.length, '(' + Math.round(tpExpected.length/results.length*100) + '%)');
  
  const branches = results.filter(r => r.expected_pattern === 'BRANCH');
  const flies = results.filter(r => r.expected_pattern === 'FLY');
  console.log('BRANCH events:', branches.filter(r => r.detected).length, '/', branches.length);
  console.log('FLY events:', flies.filter(r => r.detected).length, '/', flies.length);
  
  const avgLead = detected.length ? (detected.reduce((s,r) => s + r.lead_time_days, 0) / detected.length).toFixed(1) : 0;
  console.log('Avg lead time:', avgLead, 'days');
  
  return { results, summary: { total: results.length, detected: detected.length, avgLead } };
}


// ════════════════════════════════════════
// GITHUB BACKTEST — підтвердження концепції на іншій платформі
// ════════════════════════════════════════

// GitHub API helper з токеном (опціонально)
const GITHUB_TOKEN = process.env.GITHUB_TOKEN || '';

async function ghApi(path) {
  return new Promise((resolve) => {
    const headers = { 'User-Agent': 'ResonanceBot/1.0', 'Accept': 'application/vnd.github.v3+json' };
    if (GITHUB_TOKEN) headers['Authorization'] = 'token ' + GITHUB_TOKEN;
    https.get({ hostname: 'api.github.com', path, headers }, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => {
        try {
          const result = JSON.parse(d);
          if (result.message?.includes('rate limit')) {
            console.log('GitHub rate limited');
            resolve(null);
          } else resolve(result);
        } catch(e) { resolve(null); }
      });
    }).on('error', () => resolve(null));
    setTimeout(() => resolve(null), 15000);
  });
}

// Тягнемо commits за період
async function getCommitsInRange(owner, repo, since, until) {
  const path = '/repos/' + owner + '/' + repo + '/commits?since=' + since + '&until=' + until + '&per_page=100';
  const commits = await ghApi(path);
  if (!Array.isArray(commits)) return [];
  return commits.map(c => ({
    sha: c.sha?.slice(0,7),
    author: c.author?.login || c.commit?.author?.name || 'unknown',
    date: c.commit?.author?.date,
    message: (c.commit?.message||'').slice(0,150)
  }));
}

// Тягнемо branches
async function getBranches(owner, repo) {
  const branches = await ghApi('/repos/' + owner + '/' + repo + '/branches?per_page=100');
  if (!Array.isArray(branches)) return [];
  return branches.map(b => b.name);
}

// Тягнемо releases
async function getReleases(owner, repo) {
  const releases = await ghApi('/repos/' + owner + '/' + repo + '/releases?per_page=30');
  if (!Array.isArray(releases)) return [];
  return releases.map(r => ({
    tag: r.tag_name,
    date: r.published_at,
    name: (r.name||'').slice(0,80)
  }));
}

// Аналіз GitHub timeline на наявність preparation pattern
function analyzeGithubTimeline(commits, eventDate, branches, releases) {
  if (!commits.length) {
    return { detected: false, lead_time: 0, confidence: 0, signals: [], reasoning: 'no commits' };
  }

  const eventTs = new Date(eventDate).getTime();
  const signals = [];
  let firstSignalT = 0;

  // Групуємо commits по днях
  const byDay = {};
  const authorsAll = new Set();
  const authorCounts = {};
  for (const c of commits) {
    const day = c.date?.slice(0,10);
    if (!day) continue;
    const t = Math.round((new Date(day).getTime() - eventTs) / 86400000);
    if (!byDay[day]) byDay[day] = { commits: 0, authors: new Set(), messages: [], t };
    byDay[day].commits++;
    byDay[day].authors.add(c.author);
    byDay[day].messages.push(c.message);
    authorsAll.add(c.author);
    authorCounts[c.author] = (authorCounts[c.author]||0) + 1;
  }

  const days = Object.values(byDay).sort((a,b) => a.t - b.t);

  // Сигнал 1: commit velocity sprint
  const beforeDays = days.filter(d => d.t < 0 && d.t >= -30);
  if (beforeDays.length >= 5) {
    const avgCommits = beforeDays.reduce((s,d) => s + d.commits, 0) / beforeDays.length;
    const burstDay = beforeDays.find(d => d.commits >= avgCommits * 3 && d.commits >= 5);
    if (burstDay) {
      signals.push('commit_burst_at_T' + burstDay.t);
      if (burstDay.t < firstSignalT) firstSignalT = burstDay.t;
    }
  }

  // Сигнал 2: rare contributors (1-2 commits загалом за період)
  const rareAuthors = Object.entries(authorCounts).filter(([_,n]) => n <= 2);
  const rareInPeriod = beforeDays.filter(d => 
    [...d.authors].some(a => rareAuthors.find(([name]) => name === a))
  );
  if (rareInPeriod.length >= 2) {
    signals.push('rare_contributor_cluster');
    const earliest = Math.min(...rareInPeriod.map(d => d.t));
    if (earliest < firstSignalT) firstSignalT = earliest;
  }

  // Сигнал 3: release/launch keywords в commit messages
  const LAUNCH_KW = ['release','launch','prod','production','public','v1.0','v2.0','beta','rc1','rc2','final','official','announce'];
  const launchCommits = beforeDays.filter(d =>
    d.messages.some(m => LAUNCH_KW.some(kw => m.toLowerCase().includes(kw)))
  );
  if (launchCommits.length > 0) {
    signals.push('launch_keyword_at_T' + launchCommits[0].t);
    if (launchCommits[0].t < firstSignalT) firstSignalT = launchCommits[0].t;
  }

  // Сигнал 4: суспіцыйні гілки (release-*, prod-*, launch-*)
  const suspBranches = (branches||[]).filter(b => /^(release|prod|launch|main-|v\d+|public|deploy)/i.test(b));
  if (suspBranches.length > 0) {
    signals.push('release_branches:' + suspBranches.length);
  }

  // Сигнал 5: release tags перед подією
  const preReleases = (releases||[]).filter(r => {
    if (!r.date) return false;
    const t = Math.round((new Date(r.date).getTime() - eventTs) / 86400000);
    return t < 0 && t >= -30;
  });
  if (preReleases.length > 0) {
    signals.push('pre_event_releases:' + preReleases.length);
    const earliestT = Math.min(...preReleases.map(r => 
      Math.round((new Date(r.date).getTime() - eventTs) / 86400000)
    ));
    if (earliestT < firstSignalT) firstSignalT = earliestT;
  }

  let pattern = null;
  let confidence = 0;
  if (signals.length >= 3) {
    pattern = 'BRANCH';
    confidence = 0.7 + Math.min(signals.length * 0.05, 0.25);
  } else if (signals.length >= 2) {
    pattern = 'WEAK_BRANCH';
    confidence = 0.5;
  } else if (signals.length === 1) {
    pattern = 'LOW';
    confidence = 0.3;
  }

  const detected = !!pattern && firstSignalT < 0;

  return {
    detected,
    lead_time: -firstSignalT,
    confidence: Math.min(confidence, 1),
    pattern,
    signals,
    total_commits: commits.length,
    unique_authors: authorsAll.size,
    rare_authors: rareAuthors.length,
    reasoning: detected
      ? `GitHub ${pattern} pattern, first signal at T${firstSignalT}d, ${signals.length} indicators`
      : 'no clear preparatory pattern in commits'
  };
}

// Список подій з відомими GitHub repos
const GITHUB_BACKTEST_EVENTS = [
  { date: '2025-01-27', title: 'DeepSeek launch', owner: 'deepseek-ai', repo: 'DeepSeek-V3', type: 'PRODUCT' },
  { date: '2025-01-27', title: 'DeepSeek R1', owner: 'deepseek-ai', repo: 'DeepSeek-R1', type: 'PRODUCT' },
  { date: '2024-04-18', title: 'Llama 3 release', owner: 'meta-llama', repo: 'llama3', type: 'PRODUCT' },
  { date: '2024-09-25', title: 'Llama 3.2 release', owner: 'meta-llama', repo: 'llama-models', type: 'PRODUCT' },
  { date: '2024-12-26', title: 'DeepSeek V3', owner: 'deepseek-ai', repo: 'DeepSeek-V3', type: 'PRODUCT' },
  { date: '2024-07-23', title: 'Llama 3.1 release', owner: 'meta-llama', repo: 'llama-models', type: 'PRODUCT' },
  { date: '2024-02-20', title: 'Gemma release', owner: 'google-deepmind', repo: 'gemma', type: 'PRODUCT' },
  { date: '2024-12-09', title: 'Veo 2 release', owner: 'google-deepmind', repo: 'veo', type: 'PRODUCT' },
  { date: '2025-03-12', title: 'Gemma 3 release', owner: 'google-deepmind', repo: 'gemma', type: 'PRODUCT' },
  { date: '2024-09-17', title: 'Hezbollah pager attack', owner: 'apolocron', repo: 'hezbollah-pager-mystery', type: 'EVENT', notes: 'community analysis repos' },
  { date: '2022-05-09', title: 'Terra collapse', owner: 'terra-money', repo: 'core', type: 'CRISIS' },
  { date: '2022-11-11', title: 'FTX collapse', owner: 'ftxus', repo: 'serum-ts', type: 'CRISIS' },
  { date: '2024-04-19', title: 'Bitcoin halving', owner: 'bitcoin', repo: 'bitcoin', type: 'CRYPTO' },
  { date: '2024-01-10', title: 'BTC ETF approval', owner: 'bitcoin', repo: 'bitcoin', type: 'CRYPTO' },
  { date: '2023-03-14', title: 'GPT-4 release', owner: 'openai', repo: 'openai-python', type: 'PRODUCT' },
];

async function runGithubBacktest() {
  console.log('Running GitHub backtest on', GITHUB_BACKTEST_EVENTS.length, 'events...');
  const results = [];

  for (const event of GITHUB_BACKTEST_EVENTS) {
    console.log('GitHub backtest:', event.title, event.owner + '/' + event.repo);

    const eventDate = new Date(event.date);
    const since = new Date(eventDate.getTime() - 60 * 86400000).toISOString();
    const until = new Date(eventDate.getTime() + 7 * 86400000).toISOString();

    const [commits, branches, releases] = await Promise.all([
      getCommitsInRange(event.owner, event.repo, since, until),
      getBranches(event.owner, event.repo),
      getReleases(event.owner, event.repo)
    ]);

    const analysis = analyzeGithubTimeline(commits, event.date, branches, releases);

    const result = {
      event_date: event.date,
      event_title: event.title,
      event_type: event.type,
      platform: 'GITHUB',
      repo: event.owner + '/' + event.repo,
      detected: analysis.detected,
      detected_pattern: analysis.pattern,
      lead_time_days: analysis.lead_time,
      confidence: analysis.confidence,
      signals_found: analysis.signals,
      total_commits: analysis.total_commits || 0,
      unique_authors: analysis.unique_authors || 0,
      rare_authors: analysis.rare_authors || 0,
      reasoning: analysis.reasoning
    };

    results.push(result);

    supabaseInsert('github_backtest', result);

    console.log('  →', analysis.detected ? 'DETECTED' : 'MISSED',
      '|', analysis.pattern, '| lead:', analysis.lead_time + 'd',
      '| commits:', commits.length, '| signals:', analysis.signals.length);

    await new Promise(r => setTimeout(r, 2500)); // GitHub rate limit
  }

  const detected = results.filter(r => r.detected);
  console.log('\n═══ GITHUB BACKTEST SUMMARY ═══');
  console.log('Total:', results.length);
  console.log('Detected:', detected.length, '(' + Math.round(detected.length/results.length*100) + '%)');
  return { results, summary: { total: results.length, detected: detected.length } };
}


// ════════════════════════════════════════
// SEC EDGAR BACKTEST
// ════════════════════════════════════════

const SEC_HEADERS = { 'User-Agent': 'ResonanceBot abobiy@gmail.com' };

async function secFetch(host, path) {
  return new Promise((resolve) => {
    https.get({ hostname: host, path, headers: SEC_HEADERS }, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve(null); } });
    }).on('error', () => resolve(null));
    setTimeout(() => resolve(null), 15000);
  });
}

let secTickerCache = null;
async function getCIKForTicker(ticker) {
  if (!secTickerCache) {
    secTickerCache = await secFetch('www.sec.gov', '/files/company_tickers.json');
  }
  if (!secTickerCache) return null;
  const found = Object.values(secTickerCache).find(t => t.ticker === ticker.toUpperCase());
  return found ? String(found.cik_str).padStart(10, '0') : null;
}

// Тягнемо filings компанії за період
async function getFilingsInRange(cik, startDate, endDate) {
  const sub = await secFetch('data.sec.gov', '/submissions/CIK' + cik + '.json');
  if (!sub?.filings?.recent) return [];
  const f = sub.filings.recent;
  const results = [];
  for (let i = 0; i < (f.form?.length || 0); i++) {
    const filingDate = f.filingDate[i];
    if (filingDate < startDate || filingDate > endDate) continue;
    results.push({
      form: f.form[i],
      filingDate,
      accessionNumber: f.accessionNumber[i],
      primaryDocument: f.primaryDocument[i],
      reportDate: f.reportDate[i] || null
    });
  }
  return results;
}

// Аналіз filings перед подією
function analyzeEdgarFilings(filings, eventDate) {
  const eventTs = new Date(eventDate).getTime();
  const signals = [];
  let firstSignalT = 0;

  // Перетворюємо в days-from-event
  const filingsT = filings.map(f => {
    const t = Math.round((new Date(f.filingDate).getTime() - eventTs) / 86400000);
    return { ...f, t };
  }).filter(f => f.t < 0 && f.t >= -90); // 90 днів до події

  // Сигнал 1: Form 4 (insider trades) — concentrated activity
  const form4s = filingsT.filter(f => f.form === '4');
  if (form4s.length >= 3) {
    signals.push('insider_activity:' + form4s.length);
    const earliest = Math.min(...form4s.map(f => f.t));
    if (earliest < firstSignalT) firstSignalT = earliest;
  }

  // Сигнал 2: 8-K filings (material events) — burst
  const eightKs = filingsT.filter(f => f.form === '8-K');
  if (eightKs.length >= 2) {
    signals.push('8k_burst:' + eightKs.length);
    const earliest = Math.min(...eightKs.map(f => f.t));
    if (earliest < firstSignalT) firstSignalT = earliest;
  }

  // Сигнал 3: S-1 / S-1/A (IPO docs)
  const s1s = filingsT.filter(f => /^S-1/.test(f.form));
  if (s1s.length > 0) {
    signals.push('s1_filings:' + s1s.length);
    const earliest = Math.min(...s1s.map(f => f.t));
    if (earliest < firstSignalT) firstSignalT = earliest;
  }

  // Сигнал 4: SC 13D/G (large stake announcements)
  const stakeFilings = filingsT.filter(f => /^SC 13/.test(f.form));
  if (stakeFilings.length > 0) {
    signals.push('large_stake:' + stakeFilings.length);
  }

  // Сигнал 5: 425 (M&A communications), DEFM14A (merger proxy)
  const maFilings = filingsT.filter(f => /^(425|DEFM14A|S-4)/.test(f.form));
  if (maFilings.length > 0) {
    signals.push('ma_communications:' + maFilings.length);
  }

  // Сигнал 6: Прискорення filings — більше за останні 30 днів ніж попередні 60
  const last30 = filingsT.filter(f => f.t >= -30).length;
  const prior60 = filingsT.filter(f => f.t < -30 && f.t >= -90).length;
  if (last30 > prior60 && last30 >= 3) {
    signals.push('filing_acceleration:' + last30 + 'vs' + prior60);
  }

  let pattern = null;
  let confidence = 0;
  if (signals.length >= 3) {
    pattern = 'BRANCH';
    confidence = 0.75 + Math.min(signals.length * 0.05, 0.25);
  } else if (signals.length >= 2) {
    pattern = 'WEAK_BRANCH';
    confidence = 0.5;
  } else if (signals.length === 1) {
    pattern = 'LOW';
    confidence = 0.3;
  }

  return {
    detected: !!pattern && firstSignalT < 0,
    lead_time: -firstSignalT,
    confidence: Math.min(confidence, 1),
    pattern,
    signals,
    total_filings: filingsT.length,
    reasoning: pattern ? `${pattern}: ${signals.length} indicator(s), first at T${firstSignalT}d` : 'no preparation pattern'
  };
}

// Список подій з відомими тикерами
const EDGAR_BACKTEST_EVENTS = [
  { date: '2025-03-28', title: 'CoreWeave IPO', ticker: 'CRWV', type: 'IPO' },
  { date: '2025-07-31', title: 'Figma IPO', ticker: 'FIG', type: 'IPO' },
  { date: '2025-06-05', title: 'Circle IPO', ticker: 'CRCL', type: 'IPO' },
  { date: '2024-04-04', title: 'Reddit IPO', ticker: 'RDDT', type: 'IPO' },
  { date: '2023-03-10', title: 'SVB collapse', ticker: 'SIVBQ', type: 'CRISIS' },
  { date: '2024-07-19', title: 'CrowdStrike outage', ticker: 'CRWD', type: 'CRISIS' },
  { date: '2024-12-04', title: 'UnitedHealth CEO killed', ticker: 'UNH', type: 'CRISIS' },
  { date: '2024-01-05', title: 'Boeing door blowout', ticker: 'BA', type: 'CRISIS' },
  { date: '2023-05-24', title: 'Nvidia AI earnings beat', ticker: 'NVDA', type: 'EARNINGS' },
  { date: '2024-02-21', title: 'Nvidia earnings', ticker: 'NVDA', type: 'EARNINGS' },
  { date: '2024-08-29', title: 'Nvidia earnings', ticker: 'NVDA', type: 'EARNINGS' },
  { date: '2024-02-22', title: 'Eli Lilly earnings', ticker: 'LLY', type: 'EARNINGS' },
  { date: '2024-02-21', title: 'Pfizer earnings', ticker: 'PFE', type: 'EARNINGS' },
  { date: '2023-11-06', title: 'WeWork bankruptcy', ticker: 'WE', type: 'CRISIS' },
  { date: '2024-01-26', title: 'HPE-Juniper deal', ticker: 'HPE', type: 'MA' },
  { date: '2023-10-13', title: 'Cisco-Splunk deal', ticker: 'CSCO', type: 'MA' },
  { date: '2022-10-27', title: 'Twitter takeover', ticker: 'TWTR', type: 'MA' },
  { date: '2023-08-21', title: 'WeWork going concern', ticker: 'WE', type: 'CRISIS' },
  { date: '2024-04-22', title: 'Boeing whistleblower', ticker: 'BA', type: 'CRISIS' },
  { date: '2025-01-30', title: 'Microsoft earnings', ticker: 'MSFT', type: 'EARNINGS' }
];

async function runEdgarBacktest() {
  console.log('Running EDGAR backtest on', EDGAR_BACKTEST_EVENTS.length, 'events...');
  const results = [];

  for (const event of EDGAR_BACKTEST_EVENTS) {
    console.log('EDGAR backtest:', event.title, '|', event.ticker);

    const cik = await getCIKForTicker(event.ticker);
    if (!cik) {
      console.log('  → CIK not found for', event.ticker);
      results.push({
        event_date: event.date, event_title: event.title, event_type: event.type,
        platform: 'EDGAR', ticker: event.ticker, detected: false,
        reasoning: 'CIK not found (delisted or never public)'
      });
      continue;
    }

    const eventDate = new Date(event.date);
    const startDate = new Date(eventDate.getTime() - 90 * 86400000).toISOString().slice(0,10);
    const endDate = event.date;

    const filings = await getFilingsInRange(cik, startDate, endDate);
    const analysis = analyzeEdgarFilings(filings, event.date);

    const result = {
      event_date: event.date,
      event_title: event.title,
      event_type: event.type,
      platform: 'EDGAR',
      ticker: event.ticker,
      cik,
      detected: analysis.detected,
      detected_pattern: analysis.pattern,
      lead_time_days: analysis.lead_time,
      confidence: analysis.confidence,
      signals_found: analysis.signals,
      total_filings: analysis.total_filings,
      reasoning: analysis.reasoning
    };

    results.push(result);
    supabaseInsert('edgar_backtest', result);

    console.log('  →', analysis.detected ? 'DETECTED' : 'MISSED',
      '|', analysis.pattern, '| lead:', analysis.lead_time + 'd',
      '| filings:', analysis.total_filings, '| signals:', analysis.signals.length);

    await new Promise(r => setTimeout(r, 1500)); // SEC rate limit ~10/sec
  }

  const detected = results.filter(r => r.detected);
  console.log('\n═══ EDGAR BACKTEST SUMMARY ═══');
  console.log('Total:', results.length);
  console.log('Detected:', detected.length, '(' + Math.round(detected.length/results.length*100) + '%)');
  return { results };
}


// ════════════════════════════════════════
// CONVERGENCE ANALYSIS — крос-платформ
// ════════════════════════════════════════

async function fetchTable(table) {
  return new Promise((resolve) => {
    https.get(SUPABASE_URL + '/rest/v1/' + table + '?limit=200', {
      headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }
    }, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve([]); } });
    }).on('error', () => resolve([]));
  });
}

async function buildConvergence() {
  const wiki = await fetchTable('backtest_results');
  const github = await fetchTable('github_backtest');
  const edgar = await fetchTable('edgar_backtest');

  const eventsMap = {};

  // Нормалізатор назв — забираємо суфікси
  const normalize = s => s.toLowerCase()
    .replace(/\s+(ipo|earnings|deal|bankruptcy|collapse|outage|takeover|going concern|whistleblower|door blowout|ceo killed|ai earnings beat|launch|release|approval|halving)\s*$/i, '')
    .replace(/^\d{4}\s+/, '')
    .trim();

  for (const w of wiki) {
    const key = normalize(w.event_title);
    if (!eventsMap[key]) eventsMap[key] = { title: w.event_title, date: w.event_date, type: w.event_type };
    eventsMap[key].wiki = {
      detected: w.detected, pattern: w.detected_pattern,
      lead: w.lead_time_days, conf: w.confidence,
      signals: (w.signals_found||[]).length
    };
  }

  for (const g of github) {
    const key = normalize(g.event_title);
    if (!eventsMap[key]) eventsMap[key] = { title: g.event_title, date: g.event_date, type: g.event_type };
    eventsMap[key].github = {
      detected: g.detected, pattern: g.detected_pattern,
      lead: g.lead_time_days, conf: g.confidence,
      signals: (g.signals_found||[]).length
    };
  }

  for (const e of edgar) {
    const key = normalize(e.event_title);
    if (!eventsMap[key]) eventsMap[key] = { title: e.event_title, date: e.event_date, type: e.event_type };
    eventsMap[key].edgar = {
      detected: e.detected, pattern: e.detected_pattern,
      lead: e.lead_time_days, conf: e.confidence,
      signals: (e.signals_found||[]).length,
      ticker: e.ticker
    };
  }

  const conv = [];
  for (const [key, ev] of Object.entries(eventsMap)) {
    const sources = [];
    const attempted = [];
    if (ev.wiki) attempted.push('WIKI');
    if (ev.github) attempted.push('GITHUB');
    if (ev.edgar) attempted.push('EDGAR');
    if (ev.wiki?.detected) sources.push('WIKI');
    if (ev.github?.detected) sources.push('GITHUB');
    if (ev.edgar?.detected) sources.push('EDGAR');

    const confs = [ev.wiki?.conf, ev.github?.conf, ev.edgar?.conf].filter(c => c !== undefined);
    const avgConf = confs.length ? confs.reduce((s,c)=>s+c,0)/confs.length : 0;

    conv.push({
      title: ev.title, date: ev.date, type: ev.type,
      detected_in: sources, attempted_in: attempted,
      score: sources.length,
      avg_conf: avgConf,
      wiki_lead: ev.wiki?.lead || null,
      github_lead: ev.github?.lead || null,
      edgar_lead: ev.edgar?.lead || null,
      max_lead: Math.max(ev.wiki?.lead||0, ev.github?.lead||0, ev.edgar?.lead||0),
      ticker: ev.edgar?.ticker || null
    });
  }

  conv.sort((a,b) => b.score - a.score || b.avg_conf - a.avg_conf);

  return {
    total_events: conv.length,
    triple: conv.filter(c => c.score === 3),
    double: conv.filter(c => c.score === 2),
    single: conv.filter(c => c.score === 1),
    none: conv.filter(c => c.score === 0).map(c => c.title),
    summary: {
      triple_count: conv.filter(c => c.score === 3).length,
      double_count: conv.filter(c => c.score === 2).length,
      single_count: conv.filter(c => c.score === 1).length,
      none_count: conv.filter(c => c.score === 0).length
    }
  };
}


// ════════════════════════════════════════
// LIVE EDGAR MONITORING — daily insider activity
// ════════════════════════════════════════

// Кеш: ticker → 90-day baseline filings count
const edgarBaseline = {};

// Тягнемо filings за період і повертаємо стат
async function getEdgarStats(ticker) {
  const cik = await getCIKForTicker(ticker);
  if (!cik) return null;

  const sub = await secFetch('data.sec.gov', '/submissions/CIK' + cik + '.json');
  if (!sub?.filings?.recent) return null;

  const f = sub.filings.recent;
  const now = Date.now();
  const today = new Date().toISOString().slice(0,10);
  const day30 = new Date(now - 30 * 86400000).toISOString().slice(0,10);
  const day90 = new Date(now - 90 * 86400000).toISOString().slice(0,10);

  let last30_form4 = 0, last30_8k = 0, last30_total = 0;
  let prior60_form4 = 0, prior60_8k = 0, prior60_total = 0;
  let form4_recent = []; // окремо тримаємо Form 4 з accession

  for (let i = 0; i < (f.form?.length||0); i++) {
    const date = f.filingDate[i];
    const form = f.form[i];

    if (date >= day30 && date <= today) {
      last30_total++;
      if (form === '4') {
        last30_form4++;
        form4_recent.push({
          form, date,
          primaryDocument: f.primaryDocument[i],
          accessionNumber: f.accessionNumber[i]
        });
      }
      if (form === '8-K') last30_8k++;
    } else if (date >= day90 && date < day30) {
      prior60_total++;
      if (form === '4') prior60_form4++;
      if (form === '8-K') prior60_8k++;
    }
  }

  // Нормалізуємо за днями: prior60 за 60 днів, last30 за 30
  const prior60_form4_perday = prior60_form4 / 60;
  const last30_form4_perday = last30_form4 / 30;
  const form4_ratio = prior60_form4_perday > 0 
    ? last30_form4_perday / prior60_form4_perday 
    : (last30_form4 > 0 ? 99 : 0);

  return {
    ticker, cik,
    last30_form4, last30_8k, last30_total,
    prior60_form4, prior60_8k, prior60_total,
    form4_ratio: Math.round(form4_ratio * 100) / 100,
    has_8k_burst: last30_8k >= 3,
    form4_recent: form4_recent.slice(0, 10)
  };
}

// Daily run — перевіряємо всі tracked entities з ticker

// ════════════════════════════════════════
// ACTION CARDS — структуровані картки до дії
// ════════════════════════════════════════

async function buildActionCard(signal) {
  // signal: {type, ticker, direction, ratio, buys, sells, net_value, insiders, entity, etc}
  
  // 1. Перевірка чи ринок вже знає (price + news)
  const marketAware = await checkMarketAwareness(signal.ticker);
  
  // 2. Тягнемо контекст з нашої системи
  const wikiContext = signal.entity?.wiki_title 
    ? await getRecentWikiActivity(signal.entity.wiki_title)
    : null;
  
  // 3. Тягнемо живу ціну з Yahoo Finance
  const priceData = signal.ticker ? await getYahooPrice(signal.ticker) : null;
  
  // 4. Будуємо картку через Groq
  const prompt = buildCardPrompt(signal, marketAware, wikiContext, priceData);
  const cardData = await groqClassify(prompt, true); // expect JSON
  
  if (!cardData || !cardData.instrument) {
    console.log('Action card skipped — no clear trade idea');
    return null;
  }
  
  // 4. Зберігаємо
  const card = {
    signal_type: signal.type,
    signal_source: signal.ticker || signal.title,
    what_happened: cardData.what_happened || signal.detail,
    market_aware: marketAware.aware,
    market_signals: marketAware.summary + (priceData?.price ? ' · price: $' + priceData.price : ''),
    asymmetry: cardData.asymmetry || '',
    instrument: cardData.instrument,
    direction: cardData.direction,
    position_size: cardData.position_size || '1-2% portfolio',
    lead_time_days: cardData.lead_time_days || signal.lead_time || 30,
    invalidation: cardData.invalidation || '',
    target: cardData.target || '',
    stop_loss: cardData.stop_loss || '',
    timeframe: cardData.timeframe || '30d',
    confidence: cardData.confidence || 0.5,
    asymmetry_score: cardData.asymmetry_score || 0.5,
    status: 'pending'
  };
  
  // Запис в Supabase
  const insertResult = await new Promise((resolve) => {
    const body = JSON.stringify(card);
    const req = https.request({
      hostname: 'ovedzfpptsnxzxioyzkr.supabase.co',
      path: '/rest/v1/action_cards',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        'apikey': SUPABASE_KEY,
        'Authorization': 'Bearer ' + SUPABASE_KEY,
        'Prefer': 'return=representation'
      }
    }, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.write(body); req.end();
  });
  
  const cardId = insertResult?.[0]?.id;
  
  // Telegram з картою якщо confidence висока
  if (card.confidence >= 0.7 && TELEGRAM_TOKEN) {
    sendTelegram(formatCardForTelegram(card, cardId));
  }
  
  return { id: cardId, card };
}

// Перевіряємо чи ринок вже знає (price action + news count)
async function checkMarketAwareness(ticker) {
  if (!ticker) return { aware: false, summary: 'no ticker' };
  
  // Поки спрощено — без price API. Покажемо що є з cross_signals на цей тикер
  const url = SUPABASE_URL + '/rest/v1/cross_signals?crypto_symbol=eq.' + ticker
    + '&created_at=gte.' + new Date(Date.now() - 7*86400000).toISOString()
    + '&limit=10';
  
  const recent = await new Promise((resolve) => {
    https.get(url, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }}, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve([]); } });
    }).on('error', () => resolve([]));
  });
  
  const recentTypes = [...new Set((recent||[]).map(r => r.type))];
  return {
    aware: recentTypes.includes('WIKI+POLYMARKET'),
    summary: recentTypes.length ? recentTypes.join(', ') : 'тиша в інших джерелах'
  };
}

// Контекст з Wikipedia за 7 днів
async function getRecentWikiActivity(wikiTitle) {
  const url = SUPABASE_URL + '/rest/v1/anomalies?title=eq.' + encodeURIComponent(wikiTitle)
    + '&created_at=gte.' + new Date(Date.now() - 7*86400000).toISOString()
    + '&limit=5';
  
  return new Promise((resolve) => {
    https.get(url, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }}, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve([]); } });
    }).on('error', () => resolve([]));
  });
}

// Промпт для Groq
function buildCardPrompt(signal, marketAware, wikiContext, priceData) {
  const wikiSummary = (wikiContext||[]).length 
    ? `Wikipedia activity: ${wikiContext.length} аномалій за тиждень, типи: ${[...new Set(wikiContext.map(w=>w.type))].join(',')}`
    : 'Wikipedia: тиша';
  
  const priceLine = priceData?.price 
    ? `\nПОТОЧНА ЦІНА: $${priceData.price} (${priceData.currency})`
    : '';
  
  return `Ти аналітик. Маєш сигнал з RESONANCE системи. Поверни ТІЛЬКИ JSON без markdown.

СИГНАЛ:
Тип: ${signal.type}
Джерело: ${signal.ticker || signal.title}
Деталі: ${signal.detail}
${signal.direction ? 'Напрямок інсайдерів: ' + signal.direction : ''}
${signal.buys !== undefined ? 'Buys: ' + signal.buys + ', Sells: ' + signal.sells : ''}
${signal.net_value !== undefined ? 'Net value: $' + signal.net_value : ''}
${signal.insiders ? 'Insiders: ' + signal.insiders.join(', ') : ''}
${signal.entity ? 'Linked entity: ' + signal.entity.wiki_title + ' (' + signal.entity.entity_type + ')' : ''}${priceLine}

КОНТЕКСТ РИНКУ:
${marketAware.summary}
${wikiSummary}

ЗАВДАННЯ: Сформуй структуровану trade idea. Якщо немає чіткого asymmetric setup — поверни {"skip": true, "reason": "..."}.

ВАЖЛИВО:
- Якщо є поточна ціна — давай target/stop_loss В ДОЛАРАХ (не в %), розрахуй від поточної ціни
- Для опцій вказуй конкретний страйк (ATM, OTM 5%, etc)
- Для EDGAR insider signal — lead_time 60-90 днів (не менше)
- Position size має враховувати confidence: висока conf = 1.5-2%, середня = 0.5-1%

JSON формат:
{
  "what_happened": "1 речення пояснення сигналу",
  "asymmetry": "що ринок ще не врахував — конкретно",
  "instrument": "тікер або options ('GS Jul17 920 puts')",
  "direction": "LONG | SHORT | STRADDLE | WATCH",
  "position_size": "1.5% portfolio",
  "lead_time_days": число (60-90 для EDGAR, 21-30 для WIKI),
  "invalidation": "що спростує сигнал — конкретний event",
  "target": "ціна USD або %",
  "stop_loss": "ціна USD або %",
  "timeframe": "30d | 60d | 90d",
  "confidence": 0.0-1.0,
  "asymmetry_score": 0.0-1.0
}`;
}

// Yahoo Finance price
async function getYahooPrice(ticker) {
  return new Promise((resolve) => {
    const path = '/v8/finance/chart/' + ticker + '?interval=1d&range=5d';
    https.get({
      hostname: 'query1.finance.yahoo.com', path,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    }, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => {
        try {
          const data = JSON.parse(d);
          const meta = data.chart?.result?.[0]?.meta;
          if (!meta) { resolve(null); return; }
          resolve({
            ticker,
            price: meta.regularMarketPrice,
            previousClose: meta.previousClose,
            currency: meta.currency
          });
        } catch(e) { resolve(null); }
      });
    }).on('error', () => resolve(null));
    setTimeout(() => resolve(null), 5000);
  });
}

// Форматуємо для Telegram
function formatCardForTelegram(card, cardId) {
  const dirEmoji = card.direction === 'LONG' ? '🟢' : card.direction === 'SHORT' ? '🔴' : card.direction === 'STRADDLE' ? '🎯' : '👁';
  return '🎴 <b>ACTION CARD #' + (cardId||'?') + '</b>\n\n' +
    dirEmoji + ' <b>' + (card.direction||'WATCH') + ' ' + (card.instrument||'?') + '</b>\n\n' +
    '<b>Що сталось:</b>\n' + (card.what_happened||'').slice(0,200) + '\n\n' +
    '<b>Асиметрія:</b>\n' + (card.asymmetry||'').slice(0,200) + '\n\n' +
    '⏱ Lead time: ' + (card.lead_time_days||'?') + 'd · timeframe: ' + (card.timeframe||'?') + '\n' +
    '🎯 Target: ' + (card.target||'?') + '\n' +
    '🛑 Stop: ' + (card.stop_loss||'?') + '\n' +
    '❌ Invalidation: ' + (card.invalidation||'').slice(0,150) + '\n' +
    '📊 Confidence: ' + Math.round((card.confidence||0)*100) + '% · Asymmetry: ' + Math.round((card.asymmetry_score||0)*100) + '%\n' +
    '💼 Size: ' + (card.position_size||'?') + '\n\n' +
    '<i>Status: pending — approve/reject в дашборді</i>';
}

// Helper для Groq classify повертаючий JSON
async function groqClassify(prompt, expectJson) {
  return new Promise((resolve) => {
    if (!GROQ_API_KEY) { resolve(null); return; }
    const body = JSON.stringify({
      model: 'llama-3.3-70b-versatile',
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.3,
      max_tokens: 1500,
      response_format: expectJson ? { type: 'json_object' } : undefined
    });
    const req = https.request({
      hostname: 'api.groq.com', path: '/openai/v1/chat/completions', method: 'POST',
      headers: {
        'Content-Type': 'application/json', 'Authorization': 'Bearer ' + GROQ_API_KEY,
        'Content-Length': Buffer.byteLength(body)
      }
    }, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => {
        try {
          const resp = JSON.parse(d);
          const text = resp.choices?.[0]?.message?.content || '';
          if (expectJson) resolve(JSON.parse(text.replace(/```json|```/g,'').trim()));
          else resolve(text);
        } catch(e) { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.write(body); req.end();
    setTimeout(() => resolve(null), 30000);
  });
}

// Парсинг Form 4 XML
async function parseForm4(cik, accession, primaryDoc) {
  const accessionClean = accession.replace(/-/g,'');
  const cikInt = parseInt(cik);
  const indexPath = '/Archives/edgar/data/' + cikInt + '/' + accessionClean + '/' + accession + '-index.htm';
  
  const index = await new Promise((resolve) => {
    https.get({ hostname: 'www.sec.gov', path: indexPath, headers: SEC_HEADERS }, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => resolve(d));
    }).on('error', () => resolve(''));
    setTimeout(() => resolve(''), 8000);
  });

  const xmlMatches = [...index.matchAll(/href="\/Archives\/edgar\/data\/[^"]+\/([^"\/]+\.xml)"/g)];
  const xmlName = xmlMatches.find(m => !m[1].includes('xslF345') && !m[1].includes('FilingSummary'))?.[1];
  if (!xmlName) return null;

  const xmlPath = '/Archives/edgar/data/' + cikInt + '/' + accessionClean + '/' + xmlName;
  const xml = await new Promise((resolve) => {
    https.get({ hostname: 'www.sec.gov', path: xmlPath, headers: SEC_HEADERS }, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => resolve(d));
    }).on('error', () => resolve(''));
    setTimeout(() => resolve(''), 8000);
  });

  if (!xml) return null;

  const reporter = xml.match(/<rptOwnerName>([^<]+)<\/rptOwnerName>/)?.[1] || 'Unknown';
  const title = xml.match(/<officerTitle>([^<]+)<\/officerTitle>/)?.[1] || '';
  const codes = [...xml.matchAll(/<transactionCode>([^<]+)<\/transactionCode>/g)].map(m => m[1]);
  const shares = [...xml.matchAll(/<transactionShares>\s*<value>([\d.]+)<\/value>/g)].map(m => parseFloat(m[1]));
  const prices = [...xml.matchAll(/<transactionPricePerShare>\s*<value>([\d.]+)<\/value>/g)].map(m => parseFloat(m[1]));
  const ad = [...xml.matchAll(/<transactionAcquiredDisposedCode>\s*<value>([AD])<\/value>/g)].map(m => m[1]);

  let totalBought = 0, totalSold = 0, totalValue = 0;
  for (let i = 0; i < codes.length; i++) {
    const code = codes[i];
    const sh = shares[i] || 0;
    const pr = prices[i] || 0;
    const value = sh * pr;
    if (code === 'P' || (ad[i] === 'A' && code !== 'M' && code !== 'F')) {
      totalBought += sh;
      totalValue += value;
    } else if (code === 'S' || (ad[i] === 'D' && code !== 'M' && code !== 'F')) {
      totalSold += sh;
      totalValue -= value;
    }
  }

  return {
    reporter, title,
    bought: totalBought,
    sold: totalSold,
    netValue: totalValue,
    direction: totalSold > totalBought ? 'SELL' : totalBought > totalSold ? 'BUY' : 'NEUTRAL'
  };
}

// Розширений stats з парсингом до 5 form 4
async function getEdgarStatsEnhanced(ticker) {
  const baseStats = await getEdgarStats(ticker);
  if (!baseStats) return null;

  // Парсимо тільки якщо є burst — економимо API calls
  if (baseStats.form4_ratio < 1.5 || baseStats.last30_form4 < 3) {
    return { ...baseStats, direction: 'NEUTRAL', buys: 0, sells: 0, net_value: 0, insiders: [] };
  }

  const form4Filings = (baseStats.form4_recent || []).slice(0, 5);
  const transactions = [];
  for (const f4 of form4Filings) {
    if (!f4.accessionNumber || !f4.primaryDocument) continue;
    const parsed = await parseForm4(baseStats.cik, f4.accessionNumber, f4.primaryDocument);
    if (parsed) transactions.push({ ...parsed, date: f4.date });
    await new Promise(r => setTimeout(r, 400));
  }

  let buys = 0, sells = 0, netValue = 0;
  const insiders = new Set();
  for (const t of transactions) {
    if (t.direction === 'BUY') buys++;
    if (t.direction === 'SELL') sells++;
    netValue += t.netValue || 0;
    insiders.add(t.reporter);
  }

  const direction = netValue < -1000000 ? 'SELL_HEAVY'
    : netValue > 1000000 ? 'BUY_HEAVY'
    : sells > buys * 2 ? 'NET_SELL'
    : buys > sells * 2 ? 'NET_BUY'
    : netValue < 0 ? 'NET_SELL'
    : netValue > 0 ? 'NET_BUY'
    : 'MIXED';

  return {
    ...baseStats,
    direction,
    buys, sells,
    net_value: Math.round(netValue),
    insiders: [...insiders].slice(0, 5)
  };
}
async function runDailyEdgarCheck() {
  console.log('Running daily EDGAR check...');

  // Тягнемо tracked entities з ticker
  const url = SUPABASE_URL + '/rest/v1/tracked_entities?related_ticker=not.is.null&order=importance.desc';
  const tracked = await new Promise((resolve) => {
    https.get(url, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }}, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve([]); } });
    }).on('error', () => resolve([]));
  });

  // Унікальні тикери (компанії, не валюти)
  const tickers = [...new Set(tracked
    .map(t => t.related_ticker)
    .filter(t => t && !/^(USD|EUR|GBP|JPY|HUF|TRY|RUB|UAH|ARS|CNY|INR|BRL|ILS|KRW|CAD|AUD|CHF|GLD|USO|BTC|ETH|TON|BNB|SPY|QQQ|XLF|FXI|SOXX)/i.test(t))
  )];

  console.log('Checking', tickers.length, 'unique tickers');
  let alerts = 0;

  for (const ticker of tickers) {
    const stats = await getEdgarStatsEnhanced(ticker);
    if (!stats) continue;

    // Перший раз — записуємо baseline і пропускаємо
    if (!edgarBaseline[ticker]) {
      edgarBaseline[ticker] = stats;
      continue;
    }

    // Спрацьовує сигнал якщо:
    // 1. form4_ratio >= 2x (insider burst)
    // 2. last30_form4 >= 5 (мінімум активності щоб не на пустоті)
    const isInsiderBurst = stats.form4_ratio >= 2 && stats.last30_form4 >= 5;
    const isStrongSignal = stats.form4_ratio >= 3 && stats.has_8k_burst;

    if (isInsiderBurst) {
      // Знайти tracked entity для контексту
      const entity = tracked.find(t => t.related_ticker === ticker);
      const dirEmoji = stats.direction === 'SELL_HEAVY' ? '🔴' : stats.direction === 'BUY_HEAVY' ? '🟢' : stats.direction === 'NET_SELL' ? '📉' : stats.direction === 'NET_BUY' ? '📈' : '⚖️';
      const detail = ticker + ' ' + dirEmoji + ' ' + (stats.direction || 'NEUTRAL') + ' · ' + stats.form4_ratio + 'x activity (' + stats.last30_form4 + ' form4 last 30d) · buys:' + (stats.buys||0) + '/sells:' + (stats.sells||0) + ' · 8-K:' + stats.last30_8k + ((stats.insiders||[]).length ? ' · ' + stats.insiders.slice(0,2).join(', ') : '');

      console.log('EDGAR alert:', ticker, '|', stats.direction, '|', stats.form4_ratio + 'x', '| F4:', stats.last30_form4, '| B/S:', stats.buys+'/'+stats.sells);

      supabaseInsert('cross_signals', {
        type: 'EDGAR+INSIDER',
        title: ticker,
        detail,
        wiki_title: entity?.wiki_title || ticker,
        crypto_symbol: ticker,
        score: Math.min(100, stats.form4_ratio * 20)
      }, 'title,type');

      alerts++;

      // Action Card
      buildActionCard({
        type: 'EDGAR+INSIDER',
        ticker,
        title: ticker,
        detail,
        direction: stats.direction,
        buys: stats.buys,
        sells: stats.sells,
        net_value: stats.net_value,
        insiders: stats.insiders,
        entity,
        lead_time: 60
      }).catch(e => console.log('Card error:', e.message));

      // Telegram для strong signals
      if (isStrongSignal && TELEGRAM_TOKEN) {
        const dirIcon = stats.direction === 'SELL_HEAVY' ? '🔴 SELL-HEAVY' : stats.direction === 'BUY_HEAVY' ? '🟢 BUY-HEAVY' : (stats.direction || 'MIXED');
        sendTelegram(
          '📊 <b>EDGAR INSIDER BURST: ' + ticker + '</b>\n\n' +
          dirIcon + '\n' +
          '🎯 ' + stats.form4_ratio + 'x activity vs 60-day baseline\n' +
          '📋 Form 4: ' + stats.last30_form4 + ' (30d) vs ' + stats.prior60_form4 + ' (prior 60d)\n' +
          '💰 Buys: ' + (stats.buys||0) + ' / Sells: ' + (stats.sells||0) + ' / Net: USD ' + Math.abs(stats.net_value||0).toLocaleString() + '\n' +
          '📰 8-K: ' + stats.last30_8k + (stats.has_8k_burst ? ' ⚠️ BURST' : '') + '\n' +
          ((stats.insiders||[]).length ? '👥 ' + stats.insiders.slice(0,3).join(', ') + '\n' : '') +
          (entity ? '\n🎯 Linked: ' + entity.wiki_title + '\n' : '') +
          '\n🔗 https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=' + stats.cik
        );
      }
    }

    // Оновлюємо baseline
    edgarBaseline[ticker] = stats;

    // Rate limit — SEC обмежує 10 запитів/сек, тримаємо 1.5с щоб точно
    await new Promise(r => setTimeout(r, 1500));
  }

  console.log('EDGAR daily check done. Alerts:', alerts);
  return { tickers_checked: tickers.length, alerts };
}


// ════════════════════════════════════════
// OUTCOME TRACKER — перевірка карток
// ════════════════════════════════════════

// При approve — фіксуємо entry price
async function captureCardEntry(cardId, ticker) {
  if (!ticker) return null;
  const priceData = await getYahooPrice(ticker);
  if (!priceData?.price) return null;
  
  const body = JSON.stringify({
    entry_price: priceData.price,
    entry_date: new Date().toISOString()
  });
  
  await new Promise((resolve) => {
    const req = https.request({
      hostname: 'ovedzfpptsnxzxioyzkr.supabase.co',
      path: '/rest/v1/action_cards?id=eq.' + cardId,
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        'apikey': SUPABASE_KEY,
        'Authorization': 'Bearer ' + SUPABASE_KEY,
        'Prefer': 'return=minimal'
      }
    }, (r) => { r.on('data', () => {}); r.on('end', resolve); });
    req.on('error', resolve);
    req.write(body); req.end();
  });
  
  return priceData.price;
}

// Парсимо target/stop з тексту в число (USD або %)
function parseTargetValue(text, entryPrice, direction) {
  if (!text || !entryPrice) return null;
  const cleaned = text.toString().replace(/[$,]/g,'').trim();
  
  // Пряма ціна: "920" або "$920"
  const direct = cleaned.match(/^-?\d+(\.\d+)?$/);
  if (direct) {
    const val = parseFloat(cleaned);
    // Якщо < 50% від entry — то це може бути %
    if (Math.abs(val) < entryPrice * 0.3 && Math.abs(val) < 100) {
      // Це %
      return entryPrice * (1 + val/100);
    }
    return val;
  }
  
  // Відсоток: "-5%" або "+10%"
  const pct = cleaned.match(/(-?\d+(\.\d+)?)\s*%/);
  if (pct) {
    return entryPrice * (1 + parseFloat(pct[1])/100);
  }
  
  return null;
}

// Перевіряємо одну картку
async function checkCardOutcome(card) {
  if (!card.entry_price || !card.signal_source) return null;
  
  const ticker = card.signal_source;
  const priceData = await getYahooPrice(ticker);
  if (!priceData?.price) return null;
  
  const currentPrice = priceData.price;
  const entryPrice = parseFloat(card.entry_price);
  const isShort = card.direction === 'SHORT';
  
  // Розраховуємо P&L
  const priceMove = currentPrice - entryPrice;
  const pctMove = (priceMove / entryPrice) * 100;
  const pnl = isShort ? -pctMove : pctMove;
  
  // Парсимо target і stop
  const targetPrice = parseTargetValue(card.target, entryPrice, card.direction);
  const stopPrice = parseTargetValue(card.stop_loss, entryPrice, card.direction);
  
  // Перевіряємо чи досягли
  let outcome = 'STILL_OPEN';
  let outcomeNotes = '';
  
  if (targetPrice !== null) {
    const reachedTarget = isShort ? currentPrice <= targetPrice : currentPrice >= targetPrice;
    if (reachedTarget) {
      outcome = 'WIN';
      outcomeNotes = 'Target ' + targetPrice.toFixed(2) + ' reached at ' + currentPrice.toFixed(2);
    }
  }
  
  if (stopPrice !== null && outcome === 'STILL_OPEN') {
    const hitStop = isShort ? currentPrice >= stopPrice : currentPrice <= stopPrice;
    if (hitStop) {
      outcome = 'LOSS';
      outcomeNotes = 'Stop ' + stopPrice.toFixed(2) + ' hit at ' + currentPrice.toFixed(2);
    }
  }
  
  // Перевіряємо чи timeframe закінчився
  const entryDate = new Date(card.entry_date || card.decision_at);
  const tfDays = parseInt(card.timeframe) || 30;
  const expiresAt = new Date(entryDate.getTime() + tfDays * 86400000);
  const isExpired = Date.now() > expiresAt.getTime();
  
  if (isExpired && outcome === 'STILL_OPEN') {
    outcome = pnl > 0 ? 'WIN_PARTIAL' : pnl < -0.5 ? 'LOSS_PARTIAL' : 'NEUTRAL';
    outcomeNotes = 'Timeframe expired. P&L: ' + pnl.toFixed(2) + '%';
  }
  
  return {
    card_id: card.id,
    ticker,
    entry_price: entryPrice,
    current_price: currentPrice,
    pnl_pct: Math.round(pnl * 100) / 100,
    target_price: targetPrice,
    stop_price: stopPrice,
    outcome,
    outcome_notes: outcomeNotes,
    is_final: outcome !== 'STILL_OPEN'
  };
}

// Daily check всіх approved/watching карток
async function runOutcomeCheck() {
  console.log('Running outcome check on active cards...');
  
  const url = SUPABASE_URL + '/rest/v1/action_cards?status=in.(approved,watching)&outcome=is.null&limit=50';
  const cards = await new Promise((resolve) => {
    https.get(url, { headers: { 'apikey': SUPABASE_KEY, 'Authorization': 'Bearer ' + SUPABASE_KEY }}, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve([]); } });
    }).on('error', () => resolve([]));
  });
  
  console.log('Active cards to check:', cards.length);
  let updates = 0;
  
  for (const card of cards) {
    const result = await checkCardOutcome(card);
    if (!result) continue;
    
    // Якщо це фінальний outcome — записуємо
    if (result.is_final) {
      const body = JSON.stringify({
        outcome: result.outcome,
        outcome_notes: result.outcome_notes + ' (current: $' + result.current_price + ', P&L: ' + result.pnl_pct + '%)',
        outcome_checked_at: new Date().toISOString()
      });
      
      await new Promise((resolve) => {
        const req = https.request({
          hostname: 'ovedzfpptsnxzxioyzkr.supabase.co',
          path: '/rest/v1/action_cards?id=eq.' + card.id,
          method: 'PATCH',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(body),
            'apikey': SUPABASE_KEY,
            'Authorization': 'Bearer ' + SUPABASE_KEY,
            'Prefer': 'return=minimal'
          }
        }, (r) => { r.on('data', () => {}); r.on('end', resolve); });
        req.on('error', resolve);
        req.write(body); req.end();
      });
      
      console.log('Card #' + card.id + ' (' + card.signal_source + ') → ' + result.outcome + ' | P&L: ' + result.pnl_pct + '%');
      updates++;
      
      if (TELEGRAM_TOKEN && (result.outcome === 'WIN' || result.outcome === 'LOSS')) {
        const emoji = result.outcome === 'WIN' ? '✅' : '❌';
        sendTelegram(emoji + ' <b>Card #' + card.id + ' ' + result.outcome + '</b>\n\n' +
          card.direction + ' ' + card.signal_source + '\n' +
          '📍 Entry: $' + result.entry_price + '\n' +
          '🎯 Current: $' + result.current_price + '\n' +
          '📊 P&L: ' + result.pnl_pct + '%\n' +
          result.outcome_notes);
      }
    } else {
      console.log('Card #' + card.id + ' (' + card.signal_source + ') → still open · P&L: ' + result.pnl_pct + '%');
    }
    
    await new Promise(r => setTimeout(r, 500));
  }
  
  console.log('Outcome check done. Updates:', updates);
  return { checked: cards.length, updates };
}

// Розклад: щоденно о 21:00 UTC (ринки US закрилися)
function scheduleOutcomeCheck() {
  const now = new Date();
  const next = new Date(now);
  next.setUTCHours(21, 0, 0, 0);
  if (next <= now) next.setUTCDate(next.getUTCDate() + 1);
  setTimeout(() => {
    runOutcomeCheck();
    setInterval(runOutcomeCheck, 24 * 3600000);
  }, next - now);
  console.log('Outcome check scheduled for', next.toISOString());
}
scheduleOutcomeCheck();
// Розклад: запуск раз на 6 годин (4 рази на день)
function scheduleEdgarCheck() {
  // Перший раз через 5 хвилин (щоб всі компоненти стартували)
  setTimeout(() => {
    runDailyEdgarCheck();
    setInterval(runDailyEdgarCheck, 6 * 3600000);
  }, 5 * 60000);
  console.log('EDGAR daily check scheduled');
}
scheduleEdgarCheck();

connectGlobalUpstream();

server.listen(process.env.PORT || 3000, '0.0.0.0', () => {
  console.log('Resonance proxy on port ' + (process.env.PORT || 3000));
  console.log('Wikis monitored:', ALL_WIKIS.size, '| Tier1:', TIER1.size, '| Tier2:', TIER2.size, '| Tier3:', TIER3.size);
  console.log('TG thresholds — Tier1: 4+ editors | Tier2: 5+ editors | Tier3: 6+ editors');
  if (TELEGRAM_TOKEN) {
    sendTelegram(
      '🟢 <b>RESONANCE v7 online</b>\n\n' +
      '📡 ' + ALL_WIKIS.size + ' Wikipedia мов · real-time\n\n' +
      '<b>Що надсилаю:</b>\n' +
      '⚡ <b>WIKI ALERT</b> — 5+ редакторів / 5хв (tier1), важливий тип або 30+ мов\n' +
      '🔮 <b>LLM Signal</b> — Groq класифікував з confidence >= 0.8\n' +
      '📊 <b>Daily Digest</b> — топ сигнали 2x на добу (6:00 і 18:00 UTC)\n' +
      '🌿 <b>BRANCH</b> — history batch виявив підготовку до події (T-X днів)\n' +
      '🕸 <b>Graph Signal</b> — 2+ пов\'язаних Wikidata вузли активні одночасно\n' +
      '📈 <b>Trends</b> — Wikipedia burst + Google Trends в 2+ країнах\n\n' +
      '<b>Не надсилаю:</b> спорт без macro impact, culture, Wikipedia routine\n\n' +
      '<b>Як читати LLM сигнал:</b>\n' +
      'strength — впевненість Groq (0-1)\n' +
      'pimino — потенціал поширення події (0-1)\n' +
      'assets — конкретні тікери/валюти\n' +
      'direction — LONG/SHORT/STRADDLE/WATCH\n\n' +
      '<b>BRANCH vs FLY:</b>\n' +
      '🌿 BRANCH = подія готується, є T-X сигнал → можна діяти до\n' +
      '🪰 FLY = подія вже сталась, реакція ринку → моментум або запізно\n\n' +
      '⚙️ Tier1 (en/de/fr): 5+ ред · Tier2 (uk/ar): 6+ · Tier3 (hi/he): 7+'
    );
  }
});

// ════════════════════════════════════════
// GITHUB EVENTS — зірочки і форки
// ════════════════════════════════════════

const githubCache = { items: [], fetchedAt: 0 };
const githubStars = {}; // repo -> {stars, prev, delta, fetchedAt}

async function fetchGithubTrending() {
  return new Promise((resolve) => {
    // GitHub public events - no auth needed, 60 req/hour
    const path = '/repos?q=stars:>100&sort=stars&order=desc&per_page=50&type=repositories';
    // Use search API for trending
    const searchPath = '/search/repositories?q=stars:>50+pushed:>' + getRecentDate(1) + '&sort=stars&order=desc&per_page=50';

    https.get({
      hostname: 'api.github.com',
      path: searchPath,
      headers: {
        'User-Agent': 'ResonanceBot/1.0',
        'Accept': 'application/vnd.github.v3+json'
      }
    }, (res) => {
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          const items = (json.items || []).map(r => ({
            name: r.full_name,
            desc: (r.description || '').slice(0, 80),
            stars: r.stargazers_count,
            forks: r.forks_count,
            lang: r.language || '',
            topics: (r.topics || []).slice(0, 3),
            url: r.html_url,
            pushed: r.pushed_at
          }));
          githubCache.items = items;
          githubCache.fetchedAt = Date.now();
          console.log('GitHub updated:', items.length, 'repos, top:', items[0]?.name);
          resolve(items);
        } catch(e) {
          console.log('GitHub parse error:', e.message);
          resolve([]);
        }
      });
    }).on('error', e => { console.log('GitHub fetch error:', e.message); resolve([]); });
  });
}

// Get events to track star velocity
const starVelocity = {}; // repo -> [{t, stars}]

async function fetchGithubEvents() {
  return new Promise((resolve) => {
    https.get({
      hostname: 'api.github.com',
      path: '/events?per_page=100',
      headers: { 'User-Agent': 'ResonanceBot/1.0', 'Accept': 'application/vnd.github.v3+json' }
    }, (res) => {
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const events = JSON.parse(data);
          const now = Date.now();
          const spikes = {};

          events.forEach(ev => {
            if (ev.type === 'WatchEvent' || ev.type === 'ForkEvent') {
              const repo = ev.repo.name;
              if (!spikes[repo]) spikes[repo] = { watches: 0, forks: 0, url: 'https://github.com/' + repo };
              if (ev.type === 'WatchEvent') spikes[repo].watches++;
              if (ev.type === 'ForkEvent') spikes[repo].forks++;
            }
          });

          // Find repos with 3+ stars in this batch
          const hot = Object.entries(spikes)
            .filter(([, v]) => v.watches >= 2 || v.forks >= 2)
            .sort((a, b) => (b[1].watches + b[1].forks) - (a[1].watches + a[1].forks))
            .slice(0, 20)
            .map(([name, v]) => ({ name, ...v, score: v.watches * 2 + v.forks }));

          resolve(hot);
        } catch(e) { resolve([]); }
      });
    }).on('error', () => resolve([]));
  });
}

function getRecentDate(daysAgo) {
  const d = new Date(Date.now() - daysAgo * 86400000);
  return d.toISOString().split('T')[0];
}

// GitHub event stream cache
let githubEventCache = { items: [], fetchedAt: 0 };

async function pollGithub() {
  try {
    const [trending, events] = await Promise.all([
      fetchGithubTrending(),
      fetchGithubEvents()
    ]);
    githubEventCache = { trending, events, fetchedAt: Date.now() };
  } catch(e) {
    console.log('GitHub poll error:', e.message);
  }
}

pollGithub();
setInterval(pollGithub, 120000); // every 2 min (stay under rate limit)


// ════════════════════════════════════════
// BINANCE — об'єми торгів
// ════════════════════════════════════════

let binanceWs = null;
const binanceData = {}; // symbol -> {price, vol1m, vol5m, trades1m, anomaly}
const binanceSubs = ['btcusdt', 'ethusdt', 'bnbusdt', 'solusdt', 'xrpusdt',
                     'adausdt', 'dogeusdt', 'avaxusdt', 'maticusdt', 'linkusdt',
                     'dotusdt', 'uniusdt', 'atomusdt', 'ltcusdt', 'nearusdt'];

function connectBinance() {
  const streams = binanceSubs.map(s => s + '@aggTrade').join('/');
  const WS_URL = 'wss://stream.binance.com:9443/stream?streams=' + streams;

  try {
    // Use https to get initial data, then use polling (WebSocket needs ws module)
    // Fetch 24hr stats via REST as fallback
    fetchBinanceStats();
  } catch(e) {
    console.log('Binance init error:', e.message);
  }
}

let binanceStatsCache = { items: [], fetchedAt: 0 };

async function fetchBinanceStats() {
  return new Promise((resolve) => {
    const symbols = binanceSubs.map(s => s.toUpperCase());
    const query = '?symbols=' + encodeURIComponent(JSON.stringify(symbols));

    https.get({
      hostname: 'api.binance.com',
      path: '/api/v3/ticker/24hr' + query,
      headers: { 'User-Agent': 'ResonanceBot/1.0' }
    }, (res) => {
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          // Binance returns error object if rate limited, not array
          if (!Array.isArray(parsed)) {
            console.log('Binance rate limit or error:', parsed.msg || JSON.stringify(parsed).slice(0,80));
            resolve([]);
            return;
          }
          const tickers = parsed;
          const now = Date.now();

          const items = tickers.map(t => {
            const sym = t.symbol;
            const price = parseFloat(t.lastPrice);
            const change = parseFloat(t.priceChangePercent);
            const vol = parseFloat(t.quoteVolume); // USDT volume
            const trades = parseInt(t.count);

            // Store history for anomaly detection
            if (!binanceData[sym]) binanceData[sym] = { history: [] };
            binanceData[sym].history.push({ vol, price, t: now });
            binanceData[sym].history = binanceData[sym].history.filter(h => now - h.t < 3600000);

            // Calculate volume anomaly
            const hist = binanceData[sym].history;
            const avgVol = hist.length > 1
              ? hist.slice(0, -1).reduce((s, h) => s + h.vol, 0) / (hist.length - 1)
              : vol;
            const volRatio = avgVol > 0 ? vol / avgVol : 1;

            return {
              symbol: sym,
              price,
              change,
              vol: Math.round(vol),
              trades,
              volRatio: +volRatio.toFixed(2),
              isAnomaly: Math.abs(change) >= 3 || volRatio >= 2
            };
          })
          .sort((a, b) => Math.abs(b.change) - Math.abs(a.change));

          binanceStatsCache = { items, fetchedAt: now };
          console.log('Binance updated:', items.length, 'pairs, top mover:', items[0]?.symbol, items[0]?.change + '%');

          // Save anomalies to Supabase
          const anomalies = items.filter(t => t.isAnomaly);
          if (anomalies.length > 0) {
            anomalies.forEach(t => {
              supabaseInsert('binance_snapshots', {
                symbol: t.symbol,
                price: t.price,
                change_pct: t.change,
                volume: t.vol,
                is_anomaly: true
              });
            });
          }
          // Save all snapshots every 10 min (not every minute to save space)
          if (now % 600000 < 180000) {
            items.forEach(t => {
              supabaseInsert('binance_snapshots', {
                symbol: t.symbol,
                price: t.price,
                change_pct: t.change,
                volume: t.vol,
                is_anomaly: t.isAnomaly
              });
            });
          }

          resolve(items);
        } catch(e) {
          console.log('Binance parse error:', e.message, data.slice(0, 100));
          resolve([]);
        }
      });
    }).on('error', e => { console.log('Binance fetch error:', e.message); resolve([]); });
  });
}

connectBinance();
setInterval(fetchBinanceStats, 180000); // every 3 min to avoid rate limits


// ════════════════════════════════════════
// CROSS-SIGNAL DETECTION
// ════════════════════════════════════════

function findCrossSignals() {
  const signals = [];
  const wikiTitles = Object.keys(anomWindow).map(k => k.split(':').slice(1).join(':').toLowerCase());

  // Check if any Binance anomaly matches Wikipedia topic
  if (binanceStatsCache.items) {
    binanceStatsCache.items.filter(t => t.isAnomaly).forEach(ticker => {
      const sym = ticker.symbol.replace('USDT','').toLowerCase();
      const wikiMatch = wikiTitles.find(t => t.includes(sym));
      if (wikiMatch) {
        signals.push({
          type: 'WIKI+CRYPTO',
          title: ticker.symbol + ' & ' + wikiMatch,
          detail: ticker.change + '% · wiki активна',
          color: '#64ffda'
        });
      }
    });
  }

  return signals;
}

// ════════════════════════════════════════
// EDGAR VALIDATION (pre-registered grid analysis)
// Endpoint: /edgar/validate
//
// Pre-registered criteria (RESONANCE_handoff_brief.md):
//   median 60d/90d excess return < SPY - 3pp, p<0.10, n>=30 best cell
// Period: 2024-02-05 to 2024-03-01 (4 weeks)
// Grid: N in {2,3,4} insiders × value in {$500K, $1M, $5M}
//
// Differs from /edgar/backtest: that one is curated 20 events (cherry-picked).
// This is unbiased — fetches ALL Form 4 in period, detects clusters, measures forward returns.
// ════════════════════════════════════════

const SP500_CSV_URL = 'https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv';

function ev_parseCsvLine(line) {
  const out = [];
  let cur = '';
  let inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') inQuote = !inQuote;
    else if (c === ',' && !inQuote) { out.push(cur); cur = ''; }
    else cur += c;
  }
  out.push(cur);
  return out;
}

async function ev_loadSP500CIKs() {
  return new Promise((resolve) => {
    https.get(SP500_CSV_URL, { headers: { 'User-Agent': 'ResonanceBot/1.0' } }, (r) => {
      let raw = ''; r.on('data', d => raw += d);
      r.on('end', () => {
        const lines = raw.split('\n');
        const set = new Set();
        for (let i = 1; i < lines.length; i++) {
          const fields = ev_parseCsvLine(lines[i]);
          if (fields.length < 7) continue;
          const cik = parseInt(fields[6].trim(), 10);
          if (Number.isFinite(cik)) set.add(cik);
        }
        console.log('[validate] loaded S&P 500:', set.size, 'CIKs');
        resolve(set);
      });
    }).on('error', () => resolve(new Set()));
  });
}

async function ev_httpsGetRetry(url, maxAttempts = 4) {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const result = await new Promise((resolve) => {
      const req = https.get(url, { headers: { 'User-Agent': 'ResonanceBot/1.0 abobiy@gmail.com' } }, (r) => {
        let chunks = [];
        r.on('data', c => chunks.push(c));
        r.on('end', () => {
          if (r.statusCode >= 200 && r.statusCode < 300) {
            resolve({ ok: true, body: Buffer.concat(chunks).toString('utf-8') });
          } else if (r.statusCode === 404) {
            resolve({ ok: false, code: 404 });
          } else {
            resolve({ ok: false, code: r.statusCode });
          }
        });
      });
      req.on('error', () => resolve({ ok: false, code: 0 }));
      req.setTimeout(30000, () => { req.destroy(); resolve({ ok: false, code: 0 }); });
    });
    if (result.ok) return result.body;
    if (result.code === 404) return null;
    const wait = 1500 * Math.pow(2, attempt);
    await new Promise(r => setTimeout(r, wait));
  }
  return null;
}

async function ev_fetchDayMetadata(date) {
  const allHits = [];
  let fromOffset = 0;
  while (true) {
    const url = 'https://efts.sec.gov/LATEST/search-index?q=&forms=4&dateRange=custom&startdt='
      + date + '&enddt=' + date + '&from=' + fromOffset;
    const raw = await ev_httpsGetRetry(url);
    if (!raw) break;
    let data;
    try { data = JSON.parse(raw); } catch { break; }
    const hits = data.hits && data.hits.hits ? data.hits.hits : [];
    const total = data.hits && data.hits.total ? data.hits.total.value || 0 : 0;
    allHits.push(...hits);
    if (!hits.length || allHits.length >= total) break;
    fromOffset += hits.length;
    await new Promise(r => setTimeout(r, 200));
  }
  return allHits;
}

async function ev_gatherFilings(startDate, endDate, sp500Ciks) {
  const filings = [];
  let cur = new Date(startDate);
  const end = new Date(endDate);
  while (cur <= end) {
    const dow = cur.getUTCDay();
    if (dow >= 1 && dow <= 5) {
      const dateStr = cur.toISOString().slice(0, 10);
      const t0 = Date.now();
      const hits = await ev_fetchDayMetadata(dateStr);
      let dayMatched = 0;
      for (const h of hits) {
        const src = h._source || {};
        const ciks = (src.ciks || []).map(c => parseInt(c, 10)).filter(Number.isFinite);
        const matched = ciks.find(c => sp500Ciks.has(c));
        if (matched) {
          src._sp500_cik = matched;
          filings.push(src);
          dayMatched++;
        }
      }
      console.log('[validate]', dateStr, '->', hits.length, 'all,', dayMatched, 'S&P500',
        '(' + ((Date.now() - t0) / 1000).toFixed(1) + 's)');
      await new Promise(r => setTimeout(r, 400));
    }
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return filings;
}

// Form 4 XML parsing — own version (different from existing parseForm4 which expects different signature)
async function ev_fetchAndParseForm4(filing) {
  const adsh = filing.adsh;
  const issuerCik = filing._sp500_cik;
  const noDash = adsh.replace(/-/g, '');
  const base = 'https://www.sec.gov/Archives/edgar/data/' + issuerCik + '/' + noDash;
  const idxUrl = base + '/' + adsh + '-index.htm';
  const html = await ev_httpsGetRetry(idxUrl);
  if (!html) return null;

  const xmlMatches = [...html.matchAll(/href="([^"]+\.xml)"/g)].map(m => m[1]);
  let primary = xmlMatches.find(p => !p.toLowerCase().includes('xsl'));
  if (!primary && xmlMatches.length) primary = xmlMatches[0];
  if (!primary) return null;
  const xmlUrl = primary.startsWith('/') ? 'https://www.sec.gov' + primary : primary;
  const xml = await ev_httpsGetRetry(xmlUrl);
  if (!xml) return null;

  const tickerMatch = xml.match(/<issuerTradingSymbol>([^<]+)<\/issuerTradingSymbol>/);
  if (!tickerMatch) return null;
  const ticker = tickerMatch[1].trim();

  // 10b5-1: try direct tag, fallback to footnotes
  let aff10b5 = false;
  const directMatch = xml.match(/<aff10b5One>([^<]+)<\/aff10b5One>/);
  if (directMatch) {
    aff10b5 = directMatch[1].trim().toLowerCase() === 'true' || directMatch[1].trim() === '1';
  }
  if (!aff10b5) {
    const footnotes = xml.match(/<footnote[^>]*>([\s\S]*?)<\/footnote>/g) || [];
    for (const fn of footnotes) {
      if (/10b5-?1/i.test(fn)) { aff10b5 = true; break; }
    }
  }

  // First insider name only (sufficient for cluster detection)
  const ownerBlock = xml.match(/<reportingOwner>[\s\S]*?<\/reportingOwner>/);
  let insiderName = '';
  if (ownerBlock) {
    const nameM = ownerBlock[0].match(/<rptOwnerName>([^<]+)<\/rptOwnerName>/);
    if (nameM) insiderName = nameM[1].trim();
  }

  const transactions = [];
  const txBlocks = xml.match(/<nonDerivativeTransaction>[\s\S]*?<\/nonDerivativeTransaction>/g) || [];
  for (const block of txBlocks) {
    const codeM = block.match(/<transactionCode>([^<]+)<\/transactionCode>/);
    const dateM = block.match(/<transactionDate>\s*<value>([^<]+)<\/value>/);
    const sharesM = block.match(/<transactionShares>\s*<value>([^<]+)<\/value>/);
    const priceM = block.match(/<transactionPricePerShare>\s*<value>([^<]+)<\/value>/);
    const adM = block.match(/<transactionAcquiredDisposedCode>\s*<value>([^<]+)<\/value>/);
    if (!codeM || !dateM) continue;
    const shares = parseFloat(sharesM ? sharesM[1] : '0') || 0;
    const price = parseFloat(priceM ? priceM[1] : '0') || 0;
    transactions.push({
      date: dateM[1].trim(),
      code: codeM[1].trim(),
      shares, price,
      value: shares * price,
      ad: adM ? adM[1].trim() : ''
    });
  }

  return { ticker, insider: insiderName, transactions, aff10b5One: aff10b5 };
}

async function ev_parseAllFilings(filings) {
  const results = [];
  let consecutiveFails = 0;
  for (let i = 0; i < filings.length; i++) {
    const parsed = await ev_fetchAndParseForm4(filings[i]);
    if (parsed) {
      parsed.adsh = filings[i].adsh;
      parsed.file_date = filings[i].file_date;
      parsed.issuer_cik = filings[i]._sp500_cik;
      results.push(parsed);
      consecutiveFails = 0;
    } else {
      consecutiveFails++;
      if (consecutiveFails >= 15) {
        console.log('[validate] 15 consecutive parse fails, sleeping 30s');
        await new Promise(r => setTimeout(r, 30000));
        consecutiveFails = 0;
      }
    }
    if ((i + 1) % 100 === 0) {
      console.log('[validate] parse progress:', (i + 1) + '/' + filings.length, 'parsed=' + results.length);
    }
    await new Promise(r => setTimeout(r, 150));
  }
  return results;
}

function ev_aggregateSells(parsed, excludeFlag) {
  const byTicker = {};
  for (const f of parsed) {
    if (!f.ticker) continue;
    if (excludeFlag && f.aff10b5One) continue;
    const insider = f.insider || 'unknown';
    for (const tx of f.transactions || []) {
      if (tx.code === 'S' && tx.ad === 'D' && tx.value > 0) {
        if (!byTicker[f.ticker]) byTicker[f.ticker] = [];
        byTicker[f.ticker].push({ date: tx.date, insider, value: tx.value });
      }
    }
  }
  return byTicker;
}

function ev_detectClusters(byTicker, nMin, valueMin, windowDays) {
  if (!windowDays) windowDays = 10;
  const clusters = [];
  for (const ticker of Object.keys(byTicker)) {
    const sells = [...byTicker[ticker]].sort((a, b) => a.date.localeCompare(b.date));
    for (let endIdx = 0; endIdx < sells.length; endIdx++) {
      const endDate = new Date(sells[endIdx].date);
      const winStart = new Date(endDate);
      winStart.setDate(winStart.getDate() - windowDays);
      const window = sells.filter(s => {
        const d = new Date(s.date);
        return d >= winStart && d <= endDate;
      });
      const insiders = new Set(window.map(s => s.insider));
      const total = window.reduce((sum, s) => sum + s.value, 0);
      if (insiders.size >= nMin && total >= valueMin) {
        clusters.push({
          ticker,
          alert_date: sells[endIdx].date,
          n_insiders: insiders.size,
          total_value: total,
          sell_count: window.length
        });
        break;
      }
    }
  }
  return clusters;
}

// Yahoo Finance prices via chart endpoint (more reliable than CSV download)
async function ev_fetchYahooHistory(ticker, startMs, endMs) {
  return new Promise((resolve) => {
    const path = '/v8/finance/chart/' + encodeURIComponent(ticker)
      + '?period1=' + Math.floor(startMs / 1000)
      + '&period2=' + Math.floor(endMs / 1000)
      + '&interval=1d';
    https.get({
      hostname: 'query1.finance.yahoo.com', path,
      headers: {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
        'Accept': 'application/json'
      }
    }, (r) => {
      let raw = ''; r.on('data', d => raw += d);
      r.on('end', () => {
        try {
          const data = JSON.parse(raw);
          const result = data.chart && data.chart.result && data.chart.result[0];
          if (!result) { resolve(null); return; }
          const timestamps = result.timestamp || [];
          const closes = (result.indicators && result.indicators.quote && result.indicators.quote[0]
            && result.indicators.quote[0].close) || [];
          const adjcloses = (result.indicators && result.indicators.adjclose && result.indicators.adjclose[0]
            && result.indicators.adjclose[0].adjclose) || closes;
          const out = [];
          for (let i = 0; i < timestamps.length; i++) {
            const close = adjcloses[i] != null ? adjcloses[i] : closes[i];
            if (close == null) continue;
            const date = new Date(timestamps[i] * 1000).toISOString().slice(0, 10);
            out.push({ date, close });
          }
          resolve(out);
        } catch (e) { resolve(null); }
      });
    }).on('error', () => resolve(null));
    setTimeout(() => resolve(null), 15000);
  });
}

function ev_computeReturn(prices, alertDate, windowBd) {
  const idx = prices.findIndex(p => p.date > alertDate);
  if (idx < 0) return null;
  const exitIdx = idx + windowBd;
  if (exitIdx >= prices.length) return null;
  const entry = prices[idx].close;
  const exit = prices[exitIdx].close;
  if (entry <= 0) return null;
  return ((exit - entry) / entry) * 100;
}

async function ev_getReturnsForPairs(pairs, windowBd, spyPrices) {
  const byTicker = {};
  for (const [t, d] of pairs) {
    if (!byTicker[t]) byTicker[t] = [];
    byTicker[t].push(d);
  }
  const allDates = pairs.map(p => p[1]).sort();
  const start = new Date(allDates[0]);
  start.setDate(start.getDate() - 5);
  const end = new Date(allDates[allDates.length - 1]);
  end.setDate(end.getDate() + 280);

  const out = new Map();
  const tickers = Object.keys(byTicker);
  for (let i = 0; i < tickers.length; i++) {
    const ticker = tickers[i];
    const prices = await ev_fetchYahooHistory(ticker, start.getTime(), end.getTime());
    if (!prices) continue;
    for (const d of byTicker[ticker]) {
      const tret = ev_computeReturn(prices, d, windowBd);
      const sret = ev_computeReturn(spyPrices, d, windowBd);
      if (tret !== null && sret !== null) {
        out.set(ticker + '|' + d, { ticker_ret: tret, spy_ret: sret, excess: tret - sret });
      }
    }
    await new Promise(r => setTimeout(r, 120));
    if ((i + 1) % 25 === 0) console.log('[validate] yahoo progress:', (i + 1) + '/' + tickers.length);
  }
  return out;
}

function ev_median(arr) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

function ev_mean(arr) {
  if (!arr.length) return null;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function ev_normCdf(z) {
  const t = 1 / (1 + 0.2316419 * Math.abs(z));
  const d = 0.3989422804 * Math.exp((-z * z) / 2);
  let p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
  if (z > 0) p = 1 - p;
  return p;
}

function ev_mannWhitneyULess(s1, s2) {
  if (s1.length < 5 || s2.length < 5) return null;
  const all = [...s1.map(x => ({ v: x, g: 1 })), ...s2.map(x => ({ v: x, g: 2 }))]
    .sort((a, b) => a.v - b.v);
  let rank = 1;
  for (let i = 0; i < all.length;) {
    let j = i;
    while (j < all.length && all[j].v === all[i].v) j++;
    const avgRank = (rank + (rank + (j - i) - 1)) / 2;
    for (let k = i; k < j; k++) all[k].rank = avgRank;
    rank += j - i;
    i = j;
  }
  const r1 = all.filter(x => x.g === 1).reduce((s, x) => s + x.rank, 0);
  const n1 = s1.length, n2 = s2.length;
  const u1 = r1 - (n1 * (n1 + 1)) / 2;
  const mu = (n1 * n2) / 2;
  const sigma = Math.sqrt((n1 * n2 * (n1 + n2 + 1)) / 12);
  if (sigma === 0) return null;
  return ev_normCdf((u1 - mu) / sigma);
}

function ev_seededRng(seed) {
  let s = seed;
  return () => { s = (s * 1664525 + 1013904223) % 4294967296; return s / 4294967296; };
}

// Main handler — long-running, streams progress to logs
async function handleEdgarValidate(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  const startedAt = new Date().toISOString();
  console.log('[validate] start ' + startedAt);

  try {
    console.log('[validate] [1/5] loading S&P 500');
    const sp500Ciks = await ev_loadSP500CIKs();
    if (sp500Ciks.size === 0) throw new Error('S&P 500 load failed');

    console.log('[validate] [2/5] gathering filings 2024-02-05..2024-03-01');
    const filings = await ev_gatherFilings('2024-02-05', '2024-03-01', sp500Ciks);
    console.log('[validate] total S&P500 filings:', filings.length);

    console.log('[validate] [3/5] parsing XML for ' + filings.length + ' filings');
    const parsed = await ev_parseAllFilings(filings);
    console.log('[validate] parsed:', parsed.length);

    const n10b5 = parsed.filter(p => p.aff10b5One).length;
    console.log('[validate] with 10b5-1 flag:', n10b5,
      '(' + (parsed.length ? (n10b5 / parsed.length * 100).toFixed(1) : '0') + '%)');

    console.log('[validate] [4/5] detecting clusters');
    const grid = [
      { n: 2, v: 500000 }, { n: 2, v: 1000000 }, { n: 2, v: 5000000 },
      { n: 3, v: 500000 }, { n: 3, v: 1000000 }, { n: 3, v: 5000000 },
      { n: 4, v: 1000000 }
    ];
    const scenarios = [
      { label: 'ALL', exclude10b5: false },
      { label: 'EXCL_10B5_1', exclude10b5: true }
    ];

    const allAlertPairs = new Set();
    const cellResults = {};
    for (const sc of scenarios) {
      const sells = ev_aggregateSells(parsed, sc.exclude10b5);
      cellResults[sc.label] = { tickers_with_sells: Object.keys(sells).length, cells: {} };
      for (const g of grid) {
        const clusters = ev_detectClusters(sells, g.n, g.v);
        cellResults[sc.label].cells['N' + g.n + '_$' + (g.v / 1e6) + 'M'] = clusters;
        for (const c of clusters) allAlertPairs.add(c.ticker + '|' + c.alert_date);
      }
    }

    // Random control: 50 random S&P 500 tickers × random dates from period
    const rng = ev_seededRng(42);
    const allTickers = [...new Set(parsed.map(p => p.ticker).filter(Boolean))];
    const allFilingDates = [...new Set(filings.map(f => f.file_date))].sort();
    const randomPairs = [];
    if (allTickers.length && allFilingDates.length) {
      for (let i = 0; i < 50; i++) {
        const t = allTickers[Math.floor(rng() * allTickers.length)];
        const d = allFilingDates[Math.floor(rng() * allFilingDates.length)];
        randomPairs.push([t, d]);
      }
    }
    const randomKeys = randomPairs.map(([t, d]) => t + '|' + d);
    for (const k of randomKeys) allAlertPairs.add(k);

    console.log('[validate] [5/5] fetching forward returns for', allAlertPairs.size, 'pairs');
    const allPairsArr = [...allAlertPairs].map(k => k.split('|'));

    const meta = {
      started_at: startedAt,
      finished_at: null,
      period: '2024-02-05 to 2024-03-01',
      filings_total: filings.length,
      filings_parsed: parsed.length,
      with_10b5_1_flag: n10b5,
      sp500_size: sp500Ciks.size
    };

    // Fetch SPY once for all windows (single price history)
    const allDatesSorted = allPairsArr.map(p => p[1]).sort();
    const start = new Date(allDatesSorted[0]); start.setDate(start.getDate() - 5);
    const endRange = new Date(allDatesSorted[allDatesSorted.length - 1]); endRange.setDate(endRange.getDate() + 280);
    console.log('[validate] fetching SPY...');
    const spyPrices = await ev_fetchYahooHistory('SPY', start.getTime(), endRange.getTime());
    if (!spyPrices) throw new Error('SPY history fetch failed');

    const windows = {};
    // Cache ticker prices to avoid refetch across windows
    const tickerPriceCache = {};
    const uniqueTickers = [...new Set(allPairsArr.map(p => p[0]))];
    console.log('[validate] fetching', uniqueTickers.length, 'ticker histories');
    for (let i = 0; i < uniqueTickers.length; i++) {
      const t = uniqueTickers[i];
      const prices = await ev_fetchYahooHistory(t, start.getTime(), endRange.getTime());
      if (prices) tickerPriceCache[t] = prices;
      await new Promise(r => setTimeout(r, 120));
      if ((i + 1) % 25 === 0) console.log('[validate]   yahoo:', (i + 1) + '/' + uniqueTickers.length);
    }

    for (const windowBd of [30, 60, 90, 180]) {
      const returns = new Map();
      for (const [ticker, alertDate] of allPairsArr) {
        const prices = tickerPriceCache[ticker];
        if (!prices) continue;
        const tret = ev_computeReturn(prices, alertDate, windowBd);
        const sret = ev_computeReturn(spyPrices, alertDate, windowBd);
        if (tret !== null && sret !== null) {
          returns.set(ticker + '|' + alertDate, { ticker_ret: tret, spy_ret: sret, excess: tret - sret });
        }
      }
      const randExcess = randomKeys.map(k => returns.get(k)).filter(Boolean).map(r => r.excess);
      const winResults = {
        random_control: {
          n: randExcess.length,
          median: ev_median(randExcess),
          mean: ev_mean(randExcess)
        },
        scenarios: {}
      };
      for (const sc of scenarios) {
        winResults.scenarios[sc.label] = {};
        for (const cellKey of Object.keys(cellResults[sc.label].cells)) {
          const clusters = cellResults[sc.label].cells[cellKey];
          const excesses = clusters
            .map(c => returns.get(c.ticker + '|' + c.alert_date))
            .filter(Boolean).map(r => r.excess);
          let p = null;
          if (excesses.length >= 5 && randExcess.length >= 5) {
            p = ev_mannWhitneyULess(excesses, randExcess);
          }
          winResults.scenarios[sc.label][cellKey] = {
            n_clusters: clusters.length,
            n_with_returns: excesses.length,
            median_excess: ev_median(excesses),
            mean_excess: ev_mean(excesses),
            p_value_one_tailed_less: p
          };
        }
      }
      windows[windowBd + 'bd'] = winResults;
      console.log('[validate] window', windowBd + 'bd done');
    }

    meta.finished_at = new Date().toISOString();
    const result = { meta, windows, random_control_size: randomPairs.length };
    console.log('[validate] DONE');

    res.statusCode = 200;
    res.end(JSON.stringify(result, null, 2));

    // Persist for later access
    try {
      supabaseInsert('edgar_validation_runs', {
        started_at: startedAt,
        finished_at: meta.finished_at,
        filings_total: meta.filings_total,
        filings_parsed: meta.filings_parsed,
        with_10b5_1_flag: meta.with_10b5_1_flag,
        result_json: result
      });
    } catch (e) { /* ignore — table may not exist yet */ }

  } catch (err) {
    console.log('[validate] ERROR:', err.stack || err.message);
    if (!res.headersSent) {
      res.statusCode = 500;
      res.end(JSON.stringify({ error: err.message }));
    }
  }
}
