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
  const words = title.toLowerCase().split(/\s+/).filter(w => w.length > 3);
  getPolyMarkets().then(markets => {
    const matches = markets.filter(m => {
      const q = (m.question||'').toLowerCase();
      return words.some(w => q.includes(w));
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
    if (editors >= 2 && vol > 100 && TELEGRAM_TOKEN) {
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
          else if (/military|general|admiral|colonel|commander|armed forces/.test(cats))  type = '🎖 ВІЙСЬКОВІ';
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
      });
    }).catch(() => {
      // Fallback — записуємо без деталей
      supabaseInsert('anomalies', {
        title, wiki, lang, type: 'mul',
        edits: hits300, editors: uniq300,
        lang_count: 0, article_type: '', url: wikiUrl,
        score: uniq300 * 3 + hits300, is_trending: false, trend_pct: null
      });
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
      score: (editors300 || editors60) * 3 + (hits300 || hits60),
      is_trending: info.langCount >= 50,
      trend_pct: null
    });

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
