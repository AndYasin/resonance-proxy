const http = require('http');
const https = require('https');

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
  if (t === 1) return 4;
  if (t === 2) return 5;
  return 6;
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
  if (user && !isBot) { w.users60.add(user); w.users300.add(user); }
  w.ts60  = w.ts60.filter(t => now - t < 60000);
  w.ts300 = w.ts300.filter(t => now - t < 300000);
  if (w.ts60.length === 0) w.users60 = new Set();
  if (w.ts300.length === 0) { w.users300 = new Set(); w.firedMulti = false; w.firedSingle = false; }

  const hits60  = w.ts60.length;
  const uniq60  = w.users60.size;
  const hits300 = w.ts300.length;
  const uniq300 = w.users300.size;

  const tgThreshold    = getTgThreshold(wiki);
  const spikeThreshold = getSpikeThreshold(wiki);
  const alertKey = key + ':' + Math.floor(now / 300000);

  // ── TELEGRAM: N+ unique editors in 5 min (tier-based) ──
  if (uniq300 >= tgThreshold && !w.firedMulti && !sentAlerts[alertKey + ':tg']) {
    w.firedMulti = true;
    sentAlerts[alertKey + ':tg'] = true;

    const [info, pvData] = await Promise.all([
      getWikiInfo(title, lang),
      getViewsRatio(lang, title)
    ]);
    const { type, langCount } = info;

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

  // ── TELEGRAM: single spike (tier-based, only deaths/geopolitics/global) ──
  if (hits60 >= spikeThreshold && uniq60 <= 1 && !w.firedSingle && !sentAlerts[alertKey + ':single']) {
    w.firedSingle = true;
    sentAlerts[alertKey + ':single'] = true;
    const info = await getWikiInfo(title, lang);
    // Single spikes only for very important content
    if (info.type.includes('СМЕРТЬ') || info.type.includes('ГЕОПОЛІТИКА') || info.langCount >= 50) {
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

  // SSE stream
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  function connectUpstream() {
    const req2 = https.get({
      hostname: 'stream.wikimedia.org',
      path: '/v2/stream/recentchange',
      headers: { 'Accept': 'text/event-stream', 'User-Agent': 'ResonanceProxy/1.0' }
    }, (upstream) => {
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
                try {
                  res.write('data: ' + JSON.stringify({
                    title: data.title, wiki: data.wiki,
                    user: data.user, bot: data.bot,
                    type: data.type, timestamp: data.timestamp,
                    tier: getTier(data.wiki)
                  }) + '\n\n');
                } catch(e) {}
              }
            } catch(e) {}
          });
        } catch(e) {}
      });
      upstream.on('end', () => setTimeout(connectUpstream, 2000));
      upstream.on('error', () => setTimeout(connectUpstream, 2000));
    });
    req2.on('error', () => setTimeout(connectUpstream, 2000));
  }

  connectUpstream();
  req.on('close', () => res.end());
});

server.listen(process.env.PORT || 3000, '0.0.0.0', () => {
  console.log('Resonance proxy on port ' + (process.env.PORT || 3000));
  console.log('Wikis monitored:', ALL_WIKIS.size, '| Tier1:', TIER1.size, '| Tier2:', TIER2.size, '| Tier3:', TIER3.size);
  console.log('TG thresholds — Tier1: 4+ editors | Tier2: 5+ editors | Tier3: 6+ editors');
  if (TELEGRAM_TOKEN) {
    sendTelegram(
      '🟢 <b>Resonance v4 запущено</b>\n\n' +
      '📡 Мов: ' + ALL_WIKIS.size + ' (en/de/fr/es/ru/ja/zh + ar/fa/ta/hi + 19 інших)\n\n' +
      '⚙️ Пороги Telegram:\n' +
      '🔵 Tier 1 (en/de/fr/es...): 4+ редактори / 5 хв\n' +
      '🟡 Tier 2 (uk/ar/fa/tr...): 5+ редактори / 5 хв\n' +
      '🔴 Tier 3 (ta/hi/he...): 6+ редактори / 5 хв\n\n' +
      '📌 Тільки важливі типи: смерть, політик, бізнес, геополітика, глобальні статті (30+ мов)'
    );
  }
});
