const http = require('http');
const https = require('https');

// ── GDELT AUTO-SIGNAL ──
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
          res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
          res.end(JSON.stringify({ items: articles, avgTone: 0, query: q, fetchedAt: Date.now() }));
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
      { name: 'AP News', url: 'https://rsshub.app/apnews/topics/world-news' }
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

    Promise.all(feeds.map(fetchFeed)).then(results => {
      let all = results.flat();
      // Filter by query if provided
      if (q) {
        const words = q.split(/\s+/).filter(w=>w.length>3);
        all = all.filter(it => {
          const t = it.title.toLowerCase();
          return words.some(w => t.includes(w));
        });
      }
      // Sort by freshness (keep order as proxy of recency)
      all = all.slice(0, 20);
      res.writeHead(200, {'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
      res.end(JSON.stringify({ items: all, query: q, fetchedAt: Date.now() }));
    });
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

  // /ping endpoint — keepalive
  if (req.url === '/ping') {
    res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
    res.end('ok');
    return;
  }

  // /ddg endpoint — DuckDuckGo news proxy
  if (req.url.startsWith('/ddg?')) {
    const q = new URL('http://localhost' + req.url).searchParams.get('q') || '';
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
          res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
          res.end(JSON.stringify({ items }));
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
          res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
          res.end(JSON.stringify({ items }));
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
connectGlobalUpstream();

server.listen(process.env.PORT || 3000, '0.0.0.0', () => {
  console.log('Resonance proxy on port ' + (process.env.PORT || 3000));
  console.log('Wikis monitored:', ALL_WIKIS.size, '| Tier1:', TIER1.size, '| Tier2:', TIER2.size, '| Tier3:', TIER3.size);
  console.log('TG thresholds — Tier1: 4+ editors | Tier2: 5+ editors | Tier3: 6+ editors');
  if (TELEGRAM_TOKEN) {
    sendTelegram(
      '🟢 <b>Resonance v5 запущено</b>\n\n' +
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
