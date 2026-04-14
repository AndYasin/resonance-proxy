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
    saveTrendsToSupabase(trendsCache.items);
    return trendsCache.items;
  });
}

function saveTrendsToSupabase(items) {
  // Групуємо по title — збираємо всі гео де trending
  const byTitle = {};
  items.forEach(item => {
    const t = item.title;
    if (!byTitle[t]) byTitle[t] = { title: t, geos: [], geo_count: 0 };
    byTitle[t].geos.push(item.geo);
    byTitle[t].geo_count++;
  });
  // Пишемо тільки ті що trending в 2+ країнах — це сигнал
  Object.values(byTitle)
    .filter(t => t.geo_count >= 2)
    .sort((a,b) => b.geo_count - a.geo_count)
    .slice(0, 30)
    .forEach(t => {
      supabaseInsert('trends_signals', {
        title: t.title,
        geos: t.geos,
        geo_count: t.geo_count,
        score: t.geo_count * 15
      }, 'title');
    });
  console.log('Trends saved to Supabase:', Object.values(byTitle).filter(t=>t.geo_count>=2).length, 'multi-geo items');
}

// Оновлюємо кожні 15 хв
setInterval(() => { trendsCache.fetchedAt = 0; getTrends(); }, 900000);

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


// ── COMMENT SIGNAL DETECTION ──
function parseComment(comment) {
  if (!comment) return { signal: 0, keywords: [], sentiment: 0 };
  const c = comment.toLowerCase();
  const keywords = [];
  let signal = 0;
  let sentiment = 0;

  // Смерть / трагедія
  if (/\bdied\b|\bdeath\b|\bkilled\b|\bmurdered\b|\bdeceased\b/.test(c)) {
    keywords.push('DEATH'); signal += 30; sentiment = -1;
  }
  // Катастрофа
  if (/\bcrash\b|\bdisaster\b|\bexplosion\b|\battack\b|\bterror\b/.test(c)) {
    keywords.push('DISASTER'); signal += 25; sentiment = -1;
  }
  // Арешт / скандал
  if (/\barrested\b|\bcharged\b|\bconvicted\b|\bscandal\b|\bindicted\b/.test(c)) {
    keywords.push('SCANDAL'); signal += 22; sentiment = -1;
  }
  // Перемога / призначення
  if (/\belected\b|\bwon\b|\bappointed\b|\bnamed\b|\bchosen\b/.test(c)) {
    keywords.push('APPOINTED'); signal += 20; sentiment = 1;
  }
  // Банкрутство / колапс
  if (/\bbankrupt\b|\bcollapsed\b|\bfiled\b|\bliquidat\b/.test(c)) {
    keywords.push('BANKRUPT'); signal += 22; sentiment = -1;
  }
  // Edit war сигнали
  if (/\breverted\b|\bundone\b|\bvandaliz\b/.test(c)) {
    keywords.push('REVERT'); signal -= 5; sentiment = 0;
  }
  // Захист статті
  if (/\bprotected\b|\bsemi-protected\b/.test(c)) {
    keywords.push('PROTECTED'); signal += 10;
  }
  // Нова секція (некролог, біографія)
  if (/\/\* ?(death|personal life|early life|legacy|aftermath) ?\*\//.test(c)) {
    keywords.push('SECTION:'+c.match(/\/\* ?([^*]+) ?\*\//)?.[1]?.trim().toUpperCase());
    signal += 15;
  }

  return { signal, keywords, sentiment };
}

// ── ARTICLE AGE ──
const articleFirstSeen = {}; // title → timestamp першого SSE

function getArticleAgeBonus(title, isNewArticle) {
  const now = Date.now();
  if (isNewArticle) { articleFirstSeen[title] = now; return 40; }
  if (!articleFirstSeen[title]) { articleFirstSeen[title] = now; return 0; }
  const ageMin = (now - articleFirstSeen[title]) / 60000;
  // Стаття з'явилась в нашому вікні моніторингу дуже нещодавно
  if (ageMin < 60)  return 20;
  if (ageMin < 360) return 10;
  return 0;
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

async function checkAnomaly(title, wiki, user, isBot, meta={}) {
  const lang = wiki.replace('wiki', '') || 'en';
  const key = wiki + ':' + title;
  const now = Date.now();

  if (!anomWindow[key]) {
    anomWindow[key] = { ts60:[], users60:new Set(), ts300:[], users300:new Set(), firedMulti:false, firedSingle:false, lastSeen:now, firstSeen:now };
  }
  const w = anomWindow[key];
  w.lastSeen = now;
  const isMinor = meta?.isMinor || false;
  w.ts60.push(now); w.ts300.push(now);
  // Збираємо коментарі і delta bytes
  if (!w.comments) w.comments = [];
  if (!w.deltaBytes) w.deltaBytes = 0;
  if (typeof comment === 'string' && comment) w.comments.push(comment.toLowerCase().slice(0,100));
  if (typeof deltaBytes === 'number') w.deltaBytes += deltaBytes;
  if (w.comments.length > 50) w.comments = w.comments.slice(-50);
  if (isMinor) w.minorCount = (w.minorCount||0)+1;
  else w.nonMinorCount = (w.nonMinorCount||0)+1;
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

  // ── Supabase: записуємо при 2+ редакторах, оновлюємо при кожному новому ──
  if (uniq300 >= 2 && hits300 >= 2) {
    // Пишемо якщо: перший раз АБО новий редактор АБО кожні 2 хв
    const shouldWrite = !w.firedSupabase
      || uniq300 > (w.lastUniq300 || 0)
      || (now - (w.lastSupa || 0)) > 120000;
    if (shouldWrite) {
      w.firedSupabase = true;
      w.lastUniq300 = uniq300;
      w.lastSupa = now;
      const wikiUrl = 'https://' + lang + '.wikipedia.org/wiki/' + encodeURIComponent(title.replace(/ /g,'_'));

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

        const minorPenalty = (w.nonMinorCount||0) > 0 ? 1.0 : 0.4; // якщо всі minor — знижуємо
        const sectionBonus = (meta?.section||'').toLowerCase().match(/death|killed|died|murder/) ? 30 : 0;
        const typeScore = (typeWeights[atype.replace(/[^а-яА-ЯіІїЇєЄa-zA-Z]/g,'')] || 0) * minorPenalty;
        const langScore = Math.min(lc * 0.4, 30);
        const trendScore = trendPct ? Math.min(trendPct / 20, 20) : 0;
        const pvScore = pvRatio ? Math.min((pvRatio - 1) * 3, 15) : 0;
        const e = uniq300;
        const editorScore = e<=2?e*2.5:e<=4?5+(e-2)*8:e<=9?21+(e-4)*20:121+(e-10)*40;
        const actScore = editorScore + hits300 * 0.8;
        const spanMin = (now - (w.firstSeen || now)) / 60000;
        const susScore = Math.min(spanMin / 10, 15);
        const resonanceBonus = trendPct && uniq300 >= 2 ? 25 : 0;
        const burstBonus = e >= 5 && spanMin <= 10 ? 30 : 0;
        // НОВІ метрики
        const commentParsed = parseComment(meta.comment||w.lastComment||'');
        const commentBonus = Math.max(0, commentParsed.signal);
        const deltaBonus = meta.deltaBytes > 5000 ? 20 : meta.deltaBytes < -2000 ? 15 : 0;
        const ageBonus = getArticleAgeBonus(title, meta.isNewArticle||false);
        const score = typeScore + langScore + trendScore + pvScore + actScore + susScore + resonanceBonus + burstBonus + commentBonus + deltaBonus + ageBonus + sectionBonus;
        // Зберігаємо для наступних правок
        if (!w.lastComment && meta.comment) w.lastComment = meta.comment;
        if (commentParsed.keywords.length) w.commentKeywords = commentParsed.keywords;

        supabaseInsert('anomalies', {
          title, wiki, lang,
          type: uniq300 >= 3 ? 'res' : 'mul',
          edits: hits300,
          editors: uniq300,
          lang_count: lc,
          article_type: atype,
          url: wikiUrl,
          score: Math.round(score),
          is_trending: trendPct !== null,
          trend_pct: trendPct,
          comment_keywords: commentParsed.keywords.join(',') || null,
          delta_bytes: meta.deltaBytes || 0,
          is_new_article: meta.isNewArticle || false,
          sentiment: commentParsed.sentiment,
          section: meta?.section || null,
          is_minor: isMinor
        }, 'title,wiki');

        // Prediction markets cross-signal
        checkPredictionSignals(title, wiki, uniq300, Math.round(score));
        // PAI розраховується вручну через Spider → /api/pai

      }).catch(() => {
        supabaseInsert('anomalies', {
          title, wiki, lang, type: 'mul',
          edits: hits300, editors: uniq300,
          lang_count: 0, article_type: '', url: wikiUrl,
          score: uniq300 * 3 + hits300, is_trending: false, trend_pct: null
        }, 'title,wiki');
      });
    }
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
      score: uniq300 * 3 + hits300,
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
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Max-Age', '86400');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
  if (req.method === 'OPTIONS') { res.writeHead(200, {'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Allow-Headers':'*','Access-Control-Max-Age':'86400'}); res.end(); return; }

  // /trending endpoint
  if (req.url === '/trending') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
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
  // ── /markets — VIX, Gold, Oil, DXY, BTC Dominance ──
  if (req.url === '/markets') {
    const cached = getCached('markets');
    if (cached) {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(cached);
      return;
    }

    const symbols = [
      { key: 'vix',  symbol: '%5EVIX',    name: 'VIX',   unit: '' },
      { key: 'gold', symbol: 'GC%3DF',    name: 'Gold',  unit: '$' },
      { key: 'oil',  symbol: 'CL%3DF',    name: 'Oil',   unit: '$' },
      { key: 'dxy',  symbol: 'DX-Y.NYB',  name: 'DXY',   unit: '' },
    ];

    const fetchSymbol = (sym) => new Promise((resolve) => {
      https.get({
        hostname: 'query1.finance.yahoo.com',
        path: '/v8/finance/chart/' + sym.symbol + '?interval=1d&range=5d',
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ResonanceBot/1.0)' }
      }, (r) => {
        let raw = ''; r.on('data', d => raw += d);
        r.on('end', () => {
          try {
            const data = JSON.parse(raw);
            const result = data.chart?.result?.[0];
            if (!result) return resolve(null);
            const closes = result.indicators?.quote?.[0]?.close || [];
            const current = closes.filter(Boolean).pop();
            const prev = closes.filter(Boolean).slice(-2)[0];
            const change = prev ? +((current - prev) / prev * 100).toFixed(2) : 0;
            resolve({ key: sym.key, name: sym.name, value: +current.toFixed(2), change, unit: sym.unit });
          } catch(e) { resolve(null); }
        });
      }).on('error', () => resolve(null));
    });

    // BTC Dominance від CoinGecko
    const fetchBtcDom = () => new Promise((resolve) => {
      https.get({
        hostname: 'api.coingecko.com',
        path: '/api/v3/global',
        headers: { 'User-Agent': 'ResonanceBot/1.0' }
      }, (r) => {
        let raw = ''; r.on('data', d => raw += d);
        r.on('end', () => {
          try {
            const data = JSON.parse(raw);
            const dom = data.data?.market_cap_percentage?.btc;
            resolve(dom ? { key: 'btcdom', name: 'BTC Dom', value: +dom.toFixed(1), change: 0, unit: '%' } : null);
          } catch(e) { resolve(null); }
        });
      }).on('error', () => resolve(null));
    });

    Promise.all([...symbols.map(fetchSymbol), fetchBtcDom()]).then(results => {
      const items = results.filter(Boolean);
      const result = JSON.stringify({ items, fetchedAt: Date.now() });
      setCache('markets', result);
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(result);
    });
    return;
  }

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
    res.end(JSON.stringify({
      items: predCache.items,
      fetchedAt: predCache.fetchedAt,
      count: predCache.items.length,
      bySource: {
        metaculus: predCache.items.filter(i=>i.source==='metaculus').length,
        manifold: predCache.items.filter(i=>i.source==='manifold').length,
        predictit: predCache.items.filter(i=>i.source==='predictit').length,
      }
    }));
    return;
  }

  // /sec endpoint — SEC EDGAR filings
  if (req.url.startsWith('/sec')) {
    const params = new URLSearchParams(req.url.split('?')[1]||'');
    const forms = params.get('forms') || 'S-1,S-1/A,8-K,SC 13D,425,DEFM14A';
    // Скидаємо кеш якщо запит з браузера (force refresh)
    if (params.get('refresh') === '1') secCache.fetchedAt = 0;
    fetchSecFilings(forms).then(items => {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify({ items, fetchedAt: secCache.fetchedAt, count: items.length }));
    }).catch(() => {
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify({ items: [], count: 0 }));
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
                data.title && !data.title.includes(':')) {
              const deltaBytes = (data.length?.new||0) - (data.length?.old||0);
              const comment = data.comment || data.parsedcomment || '';
              const isNewArticle = data.type === 'new' || (data.revision?.old === 0);
              const isMinor = !!data.minor;
              const section = comment.match(/\/\* ?([^*]+?) ?\*\//)?.[1] || '';
              const meta = { deltaBytes, comment, isNewArticle, isMinor, section };
              checkAnomaly(data.title, data.wiki, data.user, data.bot, meta);
              const msg = 'data: ' + JSON.stringify({
                title: data.title, wiki: data.wiki,
                user: data.user, bot: data.bot,
                type: data.type, timestamp: data.timestamp,
                tier: getTier(data.wiki),
                deltaBytes, comment: comment.slice(0,100),
                isNewArticle
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
// PREDICTION MARKETS — Рівень 1
// Metaculus + Manifold + PredictIt
// ════════════════════════════════════════

let predCache = { items: [], fetchedAt: 0 };

// ── POLYMARKET (замість Metaculus який закрив API) ──
async function fetchPolymarketDirect() {
  return new Promise((resolve) => {
    https.get({
      hostname: 'gamma-api.polymarket.com',
      path: '/markets?closed=false&limit=100&order=volumeNum&ascending=false',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://polymarket.com',
        'Referer': 'https://polymarket.com/'
      }
    }, (res) => {
      let raw = ''; res.on('data', d => raw += d);
      res.on('end', () => {
        // CF повертає HTML якщо блокує
        if (raw.startsWith('<!DOCTYPE') || raw.startsWith('<html')) {
          console.log('Polymarket CF blocked, using cache');
          resolve(polyCache.items.slice(0,50).map(m => ({
            source: 'polymarket',
            id: 'pm_' + (m.id||''),
            title: m.question || '',
            probability: (() => { try { return parseFloat(JSON.parse(m.outcomePrices||'[]')[0]||0); } catch(e) { return null; } })(),
            volume: Math.round((m.volumeNum||0)/1000),
            url: 'https://polymarket.com/event/' + (m.slug||''),
            categories: '',
            activity: m.volumeNum || 0,
            closeTime: m.endDateIso || null
          })).filter(q => q.title));
          return;
        }
        try {
          const data = JSON.parse(raw);
          const items = (Array.isArray(data) ? data : []).map(m => {
            let prob = null;
            try { prob = parseFloat(JSON.parse(m.outcomePrices||'[]')[0]||0); } catch(e) {}
            return {
              source: 'polymarket',
              id: 'pm_' + (m.id||''),
              title: m.question || '',
              probability: prob,
              volume: Math.round((m.volumeNum||0)/1000),
              url: 'https://polymarket.com/event/' + (m.slug||''),
              categories: '',
              activity: m.volumeNum || 0,
              closeTime: m.endDateIso || null
            };
          }).filter(q => q.title);
          if (items.length) polyCache = { items: data, fetchedAt: Date.now() };
          console.log('Polymarket direct:', items.length, 'markets');
          resolve(items);
        } catch(e) { console.log('Polymarket parse error:', e.message); resolve([]); }
      });
    }).on('error', e => { console.log('Polymarket direct error:', e.message); resolve([]); });
  });
}

// ── MANIFOLD ──
async function fetchManifold() {
  return new Promise((resolve) => {
    const makeReq = (options) => {
      const req = https.get(options, (res) => {
        if (res.statusCode === 301 || res.statusCode === 302) {
          const loc = res.headers.location || '';
          res.resume();
          if (loc.startsWith('http')) {
            const u = new URL(loc);
            makeReq({ hostname: u.hostname, path: u.pathname + u.search, headers: options.headers });
          }
          return;
        }
        let raw = ''; res.on('data', d => raw += d);
        res.on('end', () => {
          try {
            const data = JSON.parse(raw);
            if (!Array.isArray(data)) { console.log('Manifold not array:', typeof data); resolve([]); return; }
            const items = data.map(q => ({
              source: 'manifold',
              id: 'mf_' + q.id,
              title: q.question || '',
              probability: q.probability || null,
              volume: Math.round(q.volume || 0),
              url: q.url || 'https://manifold.markets/' + q.id,
              categories: (q.tags || []).join(','),
              activity: q.volume || 0,
              closeTime: q.closeTime ? new Date(q.closeTime).toISOString() : null
            })).filter(q => q.title);
            console.log('Manifold:', items.length, 'markets');
            resolve(items);
          } catch(e) { console.log('Manifold parse error:', e.message, raw.slice(0,100)); resolve([]); }
        });
      });
      req.on('error', e => { console.log('Manifold error:', e.message); resolve([]); });
      req.setTimeout(8000, () => { req.destroy(); resolve([]); });
    };
    makeReq({
      hostname: 'api.manifold.markets',
      path: '/v0/markets?limit=50&sort=last-bet-time',
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ResonanceBot/1.0)', 'Accept': 'application/json' }
    });
  });
}

// ── PREDICTIT ──
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
          const items = (data.markets || []).map(m => {
            // Беремо контракт з найвищим обсягом
            const contracts = m.contracts || [];
            const top = contracts.sort((a,b) => (b.volume||0)-(a.volume||0))[0];
            const prob = top ? (top.lastTradePrice || top.bestYesPrice || null) : null;
            return {
              source: 'predictit',
              id: 'pi_' + m.id,
              title: m.name || '',
              probability: prob,
              volume: contracts.reduce((s,c) => s+(c.volume||0), 0),
              url: m.url || 'https://www.predictit.org/markets/detail/' + m.id,
              categories: '',
              activity: m.dateEndKnown ? 1 : 0,
              closeTime: m.timeStamp || null
            };
          }).filter(q => q.title);
          console.log('PredictIt:', items.length, 'markets');
          resolve(items);
        } catch(e) { console.log('PredictIt error:', e.message); resolve([]); }
      });
    }).on('error', e => { console.log('PredictIt fetch error:', e.message); resolve([]); });
  });
}

// ── АГРЕГАТОР ──
async function fetchAllPredictions() {
  const now = Date.now();
  if (now - predCache.fetchedAt < 600000 && predCache.items.length) {
    return predCache.items;
  }

  const [metaculus, manifold, predictit] = await Promise.all([
    fetchPolymarketDirect(),
    fetchManifold(),
    fetchPredictIt()
  ]);

  const all = [...metaculus, ...manifold, ...predictit];
  predCache = { items: all, fetchedAt: now };
  console.log('Predictions total:', all.length);
  return all;
}

// ── KEYWORD MATCH ──
function matchPrediction(wikiTitle, predictions) {
  const words = wikiTitle.toLowerCase()
    .split(/[\s,.-]+/)
    .filter(w => w.length > 3);

  return predictions
    .filter(p => {
      const t = p.title.toLowerCase();
      const matchCount = words.filter(w => t.includes(w)).length;
      // Мінімум 2 слова або 1 довге (> 6 символів)
      return matchCount >= 2 || words.some(w => w.length > 6 && t.includes(w));
    })
    .map(p => {
      const t = p.title.toLowerCase();
      const matchCount = words.filter(w => t.includes(w)).length;
      const correlation = Math.min(matchCount / words.length, 1);
      return { ...p, correlation };
    })
    .sort((a,b) => b.correlation - a.correlation)
    .slice(0, 3);
}

// ── CROSS SIGNAL GENERATOR ──
async function checkPredictionSignals(title, wiki, editors, score) {
  if (editors < 2) return; // тільки для підтверджених аномалій

  try {
    const predictions = await fetchAllPredictions();
    const matches = matchPrediction(title, predictions);

    for (const match of matches) {
      const prob = match.probability !== null ? Math.round(match.probability * 100) : null;
      const detail = [
        prob !== null ? 'YES:' + prob + '%' : '',
        match.volume > 0 ? 'vol:' + (match.volume > 1000 ? Math.round(match.volume/1000)+'K' : match.volume) : '',
        match.source.toUpperCase(),
        'corr:' + Math.round(match.correlation*100) + '%'
      ].filter(Boolean).join(' · ');

      const crossScore = Math.round(score * match.correlation * (prob ? prob/100 : 0.5));

      console.log('Prediction match:', title, '->', match.title.slice(0,60), '| prob:', prob, '| score:', crossScore);

      supabaseInsert('cross_signals', {
        type: 'WIKI+PREDICT',
        title: title,
        detail: detail + ' · ' + match.title.slice(0, 80),
        wiki_title: title,
        crypto_symbol: null,
        score: crossScore,
        source_url: match.url
      });

      // Telegram якщо сильний сигнал
      if (crossScore > 100 && editors >= 3 && TELEGRAM_TOKEN) {
        const emoji = prob > 70 ? '🟢' : prob > 40 ? '🟡' : '🔴';
        sendTelegram(
          emoji + ' <b>Prediction Signal: ' + title + '</b>\n\n' +
          '📊 ' + match.source.toUpperCase() + ': ' + match.title.slice(0,80) + '\n' +
          (prob !== null ? '💰 Ймовірність: <b>' + prob + '%</b>\n' : '') +
          '📈 Обсяг: ' + match.volume + ' · Cross score: ' + crossScore + '\n' +
          '🔗 <a href="' + match.url + '">відкрити ринок</a>'
        );
      }
    }
  } catch(e) {
    console.log('Prediction signal error:', e.message);
  }
}

// Оновлюємо prediction cache кожні 10 хв
fetchAllPredictions();
setInterval(fetchAllPredictions, 600000);


// ════════════════════════════════════════
// KEYWORD DETECTION + NEW SCORING
// ════════════════════════════════════════

const KEYWORD_GROUPS = {
  CRISIS:      { words: ['bankruptcy','chapter 11','bankrupt','insolvent','collapsed','fraud','arrested','indicted','charged','convicted','coup','overthrow','martial law','state of emergency','shooting','explosion','disaster','crash','killed'], score: 60 },
  MILESTONE:   { words: ['trillion','billion dollar','record high','all-time high','most valuable','milestone','historic','unprecedented','largest ever','first ever','ipo','went public','listed on nasdaq','listed on nyse'], score: 50 },
  CORPORATE:   { words: ['acquisition','acquired','merger','takeover','spinoff','spin-off','joint venture','stake','buyout','privatization','delisted','stepped down','resigned','appointed','fired','ousted','ceo change'], score: 40 },
  GEOPOLITICAL:{ words: ['invaded','invasion','sanctions','ceasefire','annexed','annexation','declared war','military operation','airstrike','coup','referendum','independence'], score: 35 },
  CRYPTO:      { words: ['exploit','hack','rug pull','token launch','listing','delisted','sec charges','ponzi','exit scam','flash loan'], score: 25 },
  REWRITE:     { words: ['rewrite','major revision','complete overhaul','restructure','rewrote'], score: 30 },
  CURRENT:     { words: ['{{current}}','currentevent','this article documents','breaking'], score: 45 },
  PROTECTION:  { words: ['protected','semi-protected','full protection','edit war','editwar'], score: 20 },
};

function detectKeywords(comments) {
  const text = comments.join(' ').toLowerCase();
  const found = [];
  let bonusScore = 0;
  
  for (const [group, cfg] of Object.entries(KEYWORD_GROUPS)) {
    if (cfg.words.some(w => text.includes(w))) {
      found.push(group);
      bonusScore += cfg.score;
    }
  }
  return { keywords: found, bonusScore };
}

function calcNewScore({ editors, langCount, trendPct, deltaBytes, keywords, bonusScore, isNew, pvRatio, predictionMatch }) {
  // ШАР 1: якість сигналу (редактори)
  const editorMult = editors >= 10 ? 3.0 : editors >= 5 ? 2.0 : editors >= 3 ? 1.5 : editors >= 2 ? 1.0 : 0.5;
  
  // ШАР 2: зміст коментарів
  const contentScore = bonusScore || 0;
  
  // ШАР 3: контекст (мови)
  const langMult = langCount >= 50 ? 2.5 : langCount >= 20 ? 1.8 : langCount >= 10 ? 1.3 : langCount >= 5 ? 1.2 : langCount >= 2 ? 0.8 : 0.4;
  
  // Базовий score
  let score = (editors * 3 + contentScore) * editorMult * langMult;
  
  // Підсилювачі
  if (trendPct >= 100) score *= 1.5;
  else if (trendPct >= 50) score *= 1.3;
  
  if (pvRatio >= 5) score *= 1.3;
  else if (pvRatio >= 2) score *= 1.1;
  
  if (predictionMatch) score *= 1.3;
  
  if (deltaBytes > 5000) score *= 1.2;
  else if (deltaBytes < -2000) score *= 0.8;
  
  if (isNew) score *= 1.8;
  
  return Math.round(score);
}

// Додати в index.js Railway — SEC EDGAR моніторинг

// ════════════════════════════════════════
// SEC EDGAR — S-1 / 8-K / 13D моніторинг
// ════════════════════════════════════════

let secCache = { items: [], fetchedAt: 0 };

const EIGHT_K_ITEMS = {
  '1.01': { label: 'M&A угода',      score: 80, emoji: '💼' },
  '1.02': { label: 'M&A завершення', score: 70, emoji: '💼' },
  '1.03': { label: 'Банкрутство',    score: 90, emoji: '💥' },
  '2.02': { label: 'Результати',     score: 50, emoji: '📊' },
  '2.04': { label: 'Дефолт',         score: 85, emoji: '🔴' },
  '5.01': { label: 'Зміна власника', score: 75, emoji: '🎯' },
  '5.02': { label: 'Зміна CEO/CFO',  score: 70, emoji: '👤' },
  '7.01': { label: 'Прес-реліз',     score: 30, emoji: '📢' },
  '8.01': { label: 'Інше',           score: 20, emoji: '📄' },
};

const SEC_FORMS = {
  'S-1':   { label: 'IPO',        emoji: '🚀', score: 80 },
  'S-1/A': { label: 'IPO амend',  emoji: '🚀', score: 40 },
  '8-K':   { label: 'Подія',      emoji: '⚡', score: 60 },
  'SC 13D':{ label: 'Акціонер',   emoji: '🎯', score: 50 },
  'SC 13G':{ label: 'Акціонер',   emoji: '🎯', score: 30 },
  '425':   { label: 'M&A',        emoji: '💼', score: 70 },
  'DEFM14A':{ label: 'Merger',    emoji: '💼', score: 75 },
};

async function fetchSecFilings(forms) {
  const now = Date.now();
  const cacheKey = forms || 'default';
  if (now - secCache.fetchedAt < 300000 && secCache.items.length && secCache.key === cacheKey) return secCache.items;

  const today = new Date().toISOString().slice(0, 10);
  const yesterday = new Date(now - 86400000).toISOString().slice(0, 10);
  const formList = forms || Object.keys(SEC_FORMS).join(',');

  return new Promise((resolve) => {
    const path = '/LATEST/search-index?forms=' +
      encodeURIComponent(formList) +
      '&dateRange=custom&startdt=' + yesterday +
      '&enddt=' + today +
      '&_source=file_date,display_names,period_ending,file_num,root_forms,biz_states,items&from=0&size=100';

    https.get({
      hostname: 'efts.sec.gov',
      path,
      headers: { 'User-Agent': 'ResonanceBot/1.0 contact@resonance.app', 'Accept': 'application/json' }
    }, (res) => {
      let raw = ''; res.on('data', d => raw += d);
      res.on('end', () => {
        try {
          const data = JSON.parse(raw);
          const seen = new Set();
          const items = [];

          for (const hit of (data.hits?.hits || [])) {
            const s = hit._source;
            const nameRaw = s.display_names?.[0] || '';
            const company = nameRaw.split('(')[0].trim();
            const tickerM = nameRaw.match(/\(([A-Z0-9]{1,5})\)/);
            const ticker = tickerM ? tickerM[1] : '';
            const form = s.root_forms?.[0] || '';
            const key = company + ':' + form;

            if (seen.has(key)) continue;
            seen.add(key);

            let meta = SEC_FORMS[form] || { label: form, emoji: '📄', score: 20 };
            let itemTypes = s.items || [];

            // Для 8-K — беремо найважливіший item
            if (form === '8-K' && itemTypes.length) {
              const best = itemTypes
                .map(it => EIGHT_K_ITEMS[it] || { label: it, score: 0, emoji: '📄' })
                .sort((a, b) => b.score - a.score)[0];
              if (best.score > meta.score || best.score > 20) {
                meta = { ...meta, label: best.label, emoji: best.emoji, score: Math.max(meta.score, best.score) };
              }
              // Фільтруємо нудні 8-K
              const maxScore = itemTypes.length ? Math.max(...itemTypes.map(it => (EIGHT_K_ITEMS[it]||{score:0}).score)) : 0;
              if (maxScore < 40) { seen.delete(key); continue; }
            }

            items.push({
              company: company.slice(0, 60),
              ticker,
              form,
              label: meta.label,
              emoji: meta.emoji,
              score: meta.score,
              state: s.biz_states?.[0] || '',
              date: s.file_date,
              url: 'https://www.sec.gov/cgi-bin/browse-edgar?company=' +
                   encodeURIComponent(company.replace(/[,.]/g,'').trim()) +
                   '&CIK=&type=' + encodeURIComponent(form) +
                   '&dateb=&owner=include&count=10&search_text=&action=getcompany',
              itemTypes
            });
          }

          // Сортуємо за score (важливість форми)
          items.sort((a, b) => b.score - a.score);
          secCache = { items, fetchedAt: now, key: cacheKey };
          console.log('SEC updated:', items.length, 'filings, forms:', [...new Set(items.map(i => i.form))].join(','));
          resolve(items);
        } catch(e) {
          console.log('SEC parse error:', e.message);
          resolve([]);
        }
      });
    }).on('error', e => { console.log('SEC fetch error:', e.message); resolve([]); });
  });
}

// Cross-signal: SEC filing + Wikipedia burst
async function checkSecWikiSignal(secItem) {
  if (!secItem.company) return;
  // Беремо тільки значимі слова - мінімум 5 символів, не стоп-слова
  const stopWords = new Set(['inc','corp','ltd','llc','the','and','for','with','from','that','this','have','will','been','were','they','their','your','what','when','where','which','who','how','its','our','more','also','than','into','over','after','some','such','only','each','most','then','other','these','those','would','could','should','about','there','between','through','because','within','without','during','before','after','since','while','group','holdings','company','partners','capital','management','services','solutions','systems','technologies','therapeutics','biosciences','health']);
  
  const words = secItem.company.toLowerCase()
    .replace(/[.,()]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length >= 5 && !stopWords.has(w));
  
  if (words.length === 0) return; // нема значимих слів
  
  // Шукаємо в anomWindow
  for (const [key, w] of Object.entries(anomWindow)) {
    const wikiTitle = key.split(':').slice(1).join(':').toLowerCase();
    // Мінімум 1 значиме слово має точно матчитись (не підрядок)
    const wikiWords = wikiTitle.split(/\s+/);
    const matchCount = words.filter(word => wikiWords.includes(word)).length;
    if (matchCount === 0) continue; // тільки точний збіг слів
    if (w.ts300.length < 2) continue;

    console.log('SEC+WIKI signal:', secItem.company, '->', wikiTitle, '| form:', secItem.form);
    supabaseInsert('cross_signals', {
      type: 'SEC+WIKI',
      title: secItem.company,
      detail: secItem.emoji + ' ' + secItem.form + ' · ' + secItem.label + ' · wiki: ' + wikiTitle,
      wiki_title: wikiTitle,
      crypto_symbol: null,
      score: secItem.score * 2,
      source_url: secItem.url
    });

    if (TELEGRAM_TOKEN) {
      sendTelegram(
        '📋 <b>SEC+WIKI Signal: ' + secItem.company + '</b>\n\n' +
        secItem.emoji + ' Form <b>' + secItem.form + '</b> (' + secItem.label + ')\n' +
        '📖 Wikipedia активна: ' + wikiTitle + '\n' +
        '🔗 <a href="' + secItem.url + '">SEC filing</a>'
      );
    }
  }
}

// Оновлюємо SEC кожні 15 хв і перевіряємо cross-signals
async function pollSec() {
  try {
    const items = await fetchSecFilings('S-1,S-1/A,8-K,SC 13D,425,DEFM14A');
    // Перевіряємо cross-signals з активними Wikipedia аномаліями
    for (const item of items.filter(i => i.score >= 60)) {
      await checkSecWikiSignal(item);
    }
  } catch(e) {
    console.log('SEC poll error:', e.message);
  }
}

pollSec();
setInterval(pollSec, 900000); // кожні 15 хв


// ════════════════════════════════════════
// GROQ LLM — семантична класифікація
// Wikipedia коментарів
// ════════════════════════════════════════

const GROQ_API_KEY = process.env.GROQ_API_KEY || '';
const GROQ_MODEL = 'llama-3.3-70b-versatile';

// Кеш щоб не класифікувати однакові коментарі двічі
const groqCache = new Map();

async function classifyWithGroq(comments, title, wiki) {
  if (!GROQ_API_KEY) return null;
  if (!comments?.length) return null;

  // Беремо тільки унікальні непорожні коментарі
  const unique = [...new Set(comments.filter(c => c && c.length > 5))].slice(0, 5);
  if (!unique.length) return null;

  const cacheKey = unique.join('|');
  if (groqCache.has(cacheKey)) return groqCache.get(cacheKey);

  const prompt = `You are a financial signal detector analyzing Wikipedia edit comments.

Article: "${title}" (${wiki})
Recent edit comments:
${unique.map((c, i) => `${i+1}. "${c}"`).join('\n')}

Respond ONLY with valid JSON, no markdown, no explanation:
{
  "event_type": "IPO|CRISIS|MILESTONE|DEATH|CORPORATE|GEOPOLITICAL|CRYPTO|NOISE",
  "signal_strength": 0.0,
  "affected_assets": [],
  "direction": "LONG|SHORT|STRADDLE|WATCH|NONE",
  "pimino_score": 0.0,
  "keywords": [],
  "reasoning": "one sentence max"
}

Rules:
- signal_strength: 0.0-1.0 (how financially significant)
- pimino_score: 0.0-1.0 (viral amplification potential)
- affected_assets: stock tickers, sectors, or currencies
- direction: market direction implied by the event
- NOISE if edits are routine/maintenance`;

  return new Promise((resolve) => {
    const body = JSON.stringify({
      model: GROQ_MODEL,
      max_tokens: 250,
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
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.error) {
            console.log('Groq API error:', json.error.message?.slice(0,80));
            resolve(null); return;
          }
          const text = json.choices?.[0]?.message?.content || '{}';
          const clean = text.replace(/```json|```/g, '').trim();
          const result = JSON.parse(clean);

          // Кешуємо на 30 хвилин
          groqCache.set(cacheKey, result);
          setTimeout(() => groqCache.delete(cacheKey), 1800000);

          console.log('Groq classified:', title,
            '| type:', result.event_type,
            '| strength:', result.signal_strength,
            '| pimino:', result.pimino_score,
            '| assets:', result.affected_assets?.join(',') || 'none'
          );
          resolve(result);
        } catch(e) {
          console.log('Groq parse error:', e.message, data.slice(0,100));
          resolve(null);
        }
      });
    });
    req.on('error', e => { console.log('Groq req error:', e.message); resolve(null); });
    req.setTimeout(8000, () => { req.destroy(); resolve(null); });
    req.write(body);
    req.end();
  });
}

// Groq usage stats
let groqStats = { calls: 0, hits: 0, errors: 0, resetAt: Date.now() };
setInterval(() => {
  if (groqStats.calls > 0) {
    console.log('Groq stats (last hour):',
      'calls:', groqStats.calls,
      '| cache hits:', groqStats.hits,
      '| errors:', groqStats.errors
    );
  }
  groqStats = { calls: 0, hits: 0, errors: 0, resetAt: Date.now() };
}, 3600000);

connectGlobalUpstream();

server.listen(process.env.PORT || 3000, '0.0.0.0', () => {
  console.log('Resonance proxy on port ' + (process.env.PORT || 3000));
  console.log('Wikis monitored:', ALL_WIKIS.size, '| Tier1:', TIER1.size, '| Tier2:', TIER2.size, '| Tier3:', TIER3.size);
  console.log('TG thresholds — Tier1: 4+ editors | Tier2: 5+ editors | Tier3: 6+ editors');
  if (TELEGRAM_TOKEN) {
    sendTelegram(
      '🟢 <b>Resonance v6 запущено</b>\n\n' +
      '📡 Wikipedia + GitHub + Binance\n' +
      '📡 Мов: ' + ALL_WIKIS.size + ' (en/de/fr/es/ru/ja/zh + ar/fa/ta/hi + 19 інших)\n\n' +
      '⚙️ Пороги Telegram:\n' +
      '🔵 Tier 1 (en/de/fr/es...): 4+ редактори / 5 хв\n' +
      '🟡 Tier 2 (uk/ar/fa/tr...): 5+ редактори / 5 хв\n' +
      '🔴 Tier 3 (ta/hi/he...): 6+ редактори / 5 хв\n\n' +
      '📌 Тільки важливі типи: смерть, політик, бізнес, геополітика, глобальні статті (30+ мов)'
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
// PAGEVIEW POLLER — кожні 10 хв через Vercel edge
// Пише pageview_spikes в Supabase → всі браузери бачать однаково
// ════════════════════════════════════════

const VERCEL_PV = 'https://resonance-dashboard-7a1u.vercel.app/api/pageviews';

// ════════════════════════════════════════
// PAI — Піміно Amplification Index
// Оцінює потенціал поширення події через Claude
// ════════════════════════════════════════

const paiCache = {}; // title → {pai, details, ts}

async function calcPAI(title, articleType, langCount, editors) {
  const cacheKey = title;
  const now = Date.now();

  // Кешуємо на 24 год
  if (paiCache[cacheKey] && now - paiCache[cacheKey].ts < 86400000) {
    return paiCache[cacheKey];
  }

  // Тільки для важливих подій щоб не витрачати API
  const importantTypes = ['СМЕРТЬ','ПОЛІТИК','ГЕОПОЛІТИКА','ВІЙСЬКОВІ','БІЗНЕС'];
  if (!importantTypes.includes(articleType) && langCount < 20) return null;

  const prompt = `Analyze this Wikipedia event for viral/resonance potential.
Event: "${title}"
Type: ${articleType || 'unknown'}
Language versions: ${langCount}
Simultaneous editors: ${editors}

Rate these 4 factors from 0.0 to 1.0:
1. identification: Can people identify with the victim/subject? (0=no, 1=universal)
2. concrete_villain: Is there a specific named perpetrator/cause? (0=anonymous system, 1=named person)
3. amplifier: Is there an existing movement/media ready to amplify? (0=none, 1=large ready audience)
4. evidence: Is there visual/video evidence or clear documentation? (0=none, 1=viral video)

Also provide:
- amplification_type: "person" | "movement" | "symbol" | "system" | "none"
- resonance_prediction: "viral" | "regional" | "local" | "none"
- brief_reason: one sentence why

Respond ONLY with valid JSON, no markdown:
{"identification":0.0,"concrete_villain":0.0,"amplifier":0.0,"evidence":0.0,"amplification_type":"none","resonance_prediction":"none","brief_reason":"..."}`;

  return new Promise((resolve) => {
    const body = JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 300,
      messages: [{ role: 'user', content: prompt }]
    });

    const req = https.request({
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
        'anthropic-version': '2023-06-01',
        'x-api-key': process.env.ANTHROPIC_API_KEY || ''
      }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          const text = (json.content || []).filter(b => b.type === 'text').map(b => b.text).join('').trim();
          const details = JSON.parse(text);
          const pai = details.identification * details.concrete_villain * details.amplifier * details.evidence;
          const result = { pai: Math.round(pai * 100) / 100, details, ts: Date.now() };
          paiCache[cacheKey] = result;
          console.log('PAI:', title, '→', result.pai, details.resonance_prediction);
          resolve(result);
        } catch(e) {
          console.log('PAI parse error:', e.message);
          resolve(null);
        }
      });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(15000, () => { req.destroy(); resolve(null); });
    req.write(body);
    req.end();
  });
}

async function pollPageviews() {
  if (!trendingCache.items.length) return;

  // Беремо топ-30 trending + активні edit аномалії
  const trendTitles = trendingCache.items.slice(0, 20).map(t => t.article);
  const anomTitles  = Object.keys(anomWindow)
    .map(k => k.split(':').slice(1).join(':'))
    .filter(Boolean)
    .slice(0, 10);

  // Унікальні, без дублів
  const allTitles = [...new Set([...trendTitles, ...anomTitles])];
  if (!allTitles.length) return;

  const encoded = allTitles.map(t => encodeURIComponent(t.replace(/ /g,'_'))).join(',');
  const url = VERCEL_PV + '?titles=' + encoded + '&lang=en&mode=hourly';

  try {
    const r = await fetch(url);
    if (!r.ok) { console.log('PV poll error:', r.status); return; }
    const data = await r.json();
    if (!data.batch) return;

    const now = Date.now();
    let inserted = 0;

    for (const [rawTitle, pv] of Object.entries(data.batch)) {
      if (!pv || !pv.items || pv.items.length < 3) continue;
      const { ratio, trend, items } = pv;
      if (ratio < 2) continue; // тільки справжні спайки

      const title = rawTitle.replace(/_/g, ' ');
      const lastViews = items[items.length - 1]?.v || 0;

      // Серіалізуємо останні 24 год (погодинно) для спарклайну
      const sparkline = items.slice(-24).map(i => i.v);

      supabaseInsert('pageview_spikes', {
        title,
        lang: 'en',
        ratio: Math.round(ratio * 10) / 10,
        trend_pct: trend || 0,
        last_views: lastViews,
        sparkline: JSON.stringify(sparkline),
        url: 'https://en.wikipedia.org/wiki/' + encodeURIComponent(title.replace(/ /g,'_')),
        is_trending: trendingCache.items.some(t => t.article === title)
      }, 'title,lang');
      inserted++;

      // Якщо дуже різкий спайк — cross_signal (upsert по title+type)
      if (ratio >= 5) {
        supabaseInsert('cross_signals', {
          type: 'WIKI+PAGEVIEW',
          title,
          detail: '×' + ratio.toFixed(1) + ' від норми · ' + lastViews.toLocaleString() + ' переглядів',
          wiki_title: title,
          crypto_symbol: null,
          score: Math.round(ratio * 8)  // ratio 11 → 88, ratio 20 → 160
        }, 'title,type');
      }
    }

    if (inserted > 0) console.log('PV poll: inserted', inserted, 'spikes');
  } catch(e) {
    console.log('PV poll fetch error:', e.message);
  }
}

// Додаємо /pv endpoint щоб дашборд читав поточні спайки
// (замість локального pvPinned — тепер з Supabase)

// Старт + інтервал кожні 10 хв
setTimeout(pollPageviews, 30000); // перший запуск через 30 сек після старту
setInterval(pollPageviews, 600000);
