const http = require('http');
const https = require('https');

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const allowedWikis = ['enwiki','ukwiki','dewiki','frwiki','ruwiki','eswiki','jawiki','plwiki'];
const anomWindow = {};
const sentAlerts = {};

// ── PAGEVIEWS CACHE ──
// Stores {today, avg7, ratio, fetchedAt} per article
const pvcache = {};

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
      try { const r = JSON.parse(d); if (!r.ok) console.log('TG error:', r.description); }
      catch(e) {}
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
          const json = JSON.parse(data);
          const page = Object.values(json.query?.pages || {})[0];
          if (!page) return resolve({ type: 'стаття', langCount: 1 });
          const cats = (page.categories || []).map(c => c.title.toLowerCase()).join(' ');
          const langCount = (page.langlinks || []).length + 1;
          let type = 'стаття';
          if (/deaths in 20|died 20/.test(cats))                                 type = '💀 СМЕРТЬ';
          else if (/politician|president|minister|senator|parliament|governor/.test(cats)) type = '🏛 ПОЛІТИК';
          else if (/businessperson|ceo|billionaire|executive|entrepreneur/.test(cats))     type = '💼 БІЗНЕС';
          else if (/sportsperson|athlete|footballer|tennis|basketball|olympic/.test(cats)) type = '⚽ СПОРТ';
          else if (/actor|musician|singer|director|comedian|rapper/.test(cats))            type = '🎭 КУЛЬТУРА';
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
function getPageviews(lang, title, days) {
  return new Promise((resolve) => {
    const now = new Date();
    const start = new Date(now - days * 86400000);
    const fmt = d => d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
    const path = '/api/rest_v1/metrics/pageviews/per-article/' +
      lang + '.wikipedia/all-access/all-agents/' +
      encodeURIComponent(title.replace(/ /g,'_')) +
      '/daily/' + fmt(start) + '/' + fmt(now);
    https.get({
      hostname: 'wikimedia.org', path,
      headers: { 'User-Agent': 'ResonanceBot/1.0' }
    }, (res) => {
      let data = ''; res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const items = JSON.parse(data).items || [];
          resolve(items.map(i => ({ date: i.timestamp.slice(0,8), views: i.views })));
        } catch(e) { resolve([]); }
      });
    }).on('error', () => resolve([]));
  });
}

async function getViewsRatio(lang, title) {
  const cacheKey = lang + ':' + title;
  const now = Date.now();
  if (pvcache[cacheKey] && now - pvcache[cacheKey].fetchedAt < 3600000) {
    return pvcache[cacheKey];
  }
  try {
    const items = await getPageviews(lang, title, 8);
    if (items.length < 2) return null;
    const today = items[items.length - 1].views;
    const prev = items.slice(0, -1).map(i => i.views);
    const avg7 = Math.round(prev.reduce((s,v) => s+v, 0) / prev.length);
    const ratio = avg7 > 0 ? +(today / avg7).toFixed(1) : 0;
    const result = { today, avg7, ratio, fetchedAt: now };
    pvcache[cacheKey] = result;
    return result;
  } catch(e) { return null; }
}

// ── TOP VIEWED trending articles ──
// Fetches top-1000 for en.wikipedia and computes delta vs yesterday
let trendingCache = { items: [], fetchedAt: 0 };

async function fetchTrending() {
  const now = new Date();
  // Use yesterday and day-before-yesterday (today's data not ready until end of day)
  const yesterday = new Date(now - 86400000);
  const dayBefore = new Date(now - 172800000);
  const fmt = d => ({
    year: d.getFullYear(),
    month: String(d.getMonth()+1).padStart(2,'0'),
    day: String(d.getDate()).padStart(2,'0')
  });
  const td = fmt(yesterday);   // "today" = yesterday
  const yd = fmt(dayBefore);   // "yesterday" = day before

  function getTop(d) {
    return new Promise((resolve) => {
      // Try both 'all-days' and specific date
      const path = '/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/' +
        d.year + '/' + d.month + '/' + d.day;
      console.log('Fetching top:', path);
      https.get({
        hostname: 'wikimedia.org', path,
        headers: { 'User-Agent': 'ResonanceBot/1.0' }
      }, (res) => {
        let data = ''; res.on('data', c => data += c);
        res.on('end', () => {
          try {
            const items = JSON.parse(data).items?.[0]?.articles || [];
            resolve(items);
          } catch(e) { resolve([]); }
        });
      }).on('error', (e) => { console.log('getTop error:', e.message); resolve([]); });
    });
  }

  try {
    console.log('Fetching trending for', td.year+'-'+td.month+'-'+td.day, 'vs', yd.year+'-'+yd.month+'-'+yd.day);
    const [todayTop, yestTop] = await Promise.all([getTop(td), getTop(yd)]);
    console.log('Got', todayTop.length, 'today articles,', yestTop.length, 'yesterday articles');
    const yestMap = {};
    yestTop.forEach(a => { yestMap[a.article] = a.views; });

    const trending = todayTop
      .filter(a => !['Main_Page','Special:','Wikipedia:'].some(x => a.article.startsWith(x)))
      .map(a => {
        const prev = yestMap[a.article] || 0;
        const delta = prev > 0 ? +((a.views - prev) / prev * 100).toFixed(0) : 0;
        return { article: a.article.replace(/_/g,' '), views: a.views, prev, delta, rank: a.rank };
      })
      .filter(a => a.views > 1000)
      .sort((a,b) => b.delta - a.delta)
      .slice(0, 50);

    trendingCache = { items: trending, fetchedAt: Date.now() };
    console.log('Trending updated:', trending.length, 'articles, top:', trending[0]?.article);
  } catch(e) {
    console.log('Trending fetch error:', e.message);
  }
}

// Fetch trending on start and every 30 min
fetchTrending();
setInterval(fetchTrending, 1800000);

// ── ANOMALY CHECK ──
async function checkAnomaly(title, wiki, user, isBot) {
  const lang = wiki.replace('wiki', '') || 'en';
  const key = wiki + ':' + title;
  const now = Date.now();

  if (!anomWindow[key]) {
    anomWindow[key] = {
      ts60: [], users60: new Set(),
      ts300: [], users300: new Set(),
      firedTelegram: false, firedSingle: false,
      lastReset: now
    };
  }

  const w = anomWindow[key];
  w.ts60.push(now); w.ts300.push(now);
  if (user && !isBot) { w.users60.add(user); w.users300.add(user); }
  w.ts60  = w.ts60.filter(t => now - t < 60000);
  w.ts300 = w.ts300.filter(t => now - t < 300000);
  if (w.ts60.length === 0) w.users60 = new Set();
  if (w.ts300.length === 0) {
    w.users300 = new Set();
    w.firedTelegram = false; w.firedSingle = false;
  }

  const hits60  = w.ts60.length;
  const uniq60  = w.users60.size;
  const hits300 = w.ts300.length;
  const uniq300 = w.users300.size;
  const alertKey = key + ':' + Math.floor(now / 300000);

  // Multi-editor Telegram alert: 4+ unique in 5 min
  if (uniq300 >= 4 && !w.firedTelegram && !sentAlerts[alertKey + ':tg']) {
    w.firedTelegram = true;
    sentAlerts[alertKey + ':tg'] = true;

    const [info, pvData] = await Promise.all([
      getWikiInfo(title, lang),
      getViewsRatio(lang, title)
    ]);
    const { type, langCount } = info;

    const isImportant =
      type.includes('СМЕРТЬ') || type.includes('ПОЛІТИК') || type.includes('БІЗНЕС') ||
      type.includes('ГЕОПОЛІТИКА') || type.includes('ВІЙСЬКОВІ') ||
      (type.includes('ПЕРСОНА') && langCount >= 5) ||
      (type.includes('СПОРТ') && langCount >= 10) ||
      (type.includes('КУЛЬТУРА') && langCount >= 10) ||
      (type.includes('ФУТБОЛ') && langCount >= 15) ||
      (type.includes('НАУКА') && langCount >= 5) ||
      langCount >= 20;

    if (isImportant) {
      const wikiUrl = 'https://' + lang + '.wikipedia.org/wiki/' +
        encodeURIComponent(title.replace(/ /g,'_'));
      const langLabel = langCount >= 50 ? '🌍 глобальна (' + langCount + ' мов)'
        : langCount >= 20 ? '🌐 міжнар. (' + langCount + ' мов)'
        : langCount >= 10 ? '📍 регіон. (' + langCount + ' мов)'
        : '📌 локальна (' + langCount + ' мов)';

      const timeWindow = Math.round((w.ts300[w.ts300.length-1] - w.ts300[0]) / 1000);
      const windowStr = timeWindow < 60 ? timeWindow + ' сек'
        : Math.round(timeWindow/60) + ' хв ' + (timeWindow%60) + ' сек';

      // Pageviews line
      let pvLine = '';
      if (pvData && pvData.ratio >= 2) {
        pvLine = '\n📈 Переглядів сьогодні: ' + pvData.today.toLocaleString() +
          ' (у ' + pvData.ratio + 'x більше звичайного)';
      } else if (pvData) {
        pvLine = '\n👁 Переглядів сьогодні: ' + pvData.today.toLocaleString();
      }

      const msg =
        '⚡ <b>RESONANCE ALERT</b>\n\n' +
        '<b>' + title + '</b>\n' +
        type + ' · ' + langLabel + '\n\n' +
        '👥 ' + uniq300 + ' незалежних редактори\n' +
        '📝 ' + hits300 + ' правок за ' + windowStr +
        pvLine + '\n\n' +
        '<a href="' + wikiUrl + '">Відкрити статтю →</a>';

      sendTelegram(msg);
      console.log('TG SENT:', title, '|', type, '|', langCount, 'langs |', uniq300, 'editors');
    }
  }

  // Single spike: 6+ edits, death/geopolitics/global
  if (hits60 >= 6 && uniq60 <= 1 && !w.firedSingle && !sentAlerts[alertKey + ':single']) {
    w.firedSingle = true;
    sentAlerts[alertKey + ':single'] = true;
    const info = await getWikiInfo(title, lang);
    if (info.type.includes('СМЕРТЬ') || info.type.includes('ГЕОПОЛІТИКА') || info.langCount >= 30) {
      const pvData = await getViewsRatio(lang, title);
      const wikiUrl = 'https://' + lang + '.wikipedia.org/wiki/' +
        encodeURIComponent(title.replace(/ /g,'_'));
      let pvLine = pvData ? '\n📈 ' + pvData.today.toLocaleString() + ' переглядів (x' + pvData.ratio + ')' : '';
      sendTelegram(
        '🔴 <b>SPIKE</b>\n\n<b>' + title + '</b>\n' +
        info.type + ' · ' + info.langCount + ' мов\n' +
        '⚡ ' + hits60 + ' правок / 60 сек' + pvLine + '\n\n' +
        '<a href="' + wikiUrl + '">Відкрити →</a>'
      );
    }
  }

  if (now - w.lastReset > 600000) delete anomWindow[key];
}

// Cleanups
setInterval(() => {
  const keys = Object.keys(sentAlerts);
  if (keys.length > 1000) keys.slice(0,500).forEach(k => delete sentAlerts[k]);
}, 900000);
setInterval(() => {
  const now = Date.now();
  Object.keys(anomWindow).forEach(k => {
    if (now - anomWindow[k].lastReset > 600000) delete anomWindow[k];
  });
  Object.keys(pvcache).forEach(k => {
    if (now - pvcache[k].fetchedAt > 7200000) delete pvcache[k];
  });
}, 300000);

// ── HTTP SERVER ──
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');

  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

  // Trending API endpoint
  if (req.url === '/trending') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      items: trendingCache.items,
      fetchedAt: trendingCache.fetchedAt,
      count: trendingCache.items.length
    }));
    return;
  }

  // Main SSE stream
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
                  allowedWikis.includes(data.wiki) &&
                  data.title && !data.title.includes(':')) {
                checkAnomaly(data.title, data.wiki, data.user, data.bot);
                try {
                  res.write('data: ' + JSON.stringify({
                    title: data.title, wiki: data.wiki,
                    user: data.user, bot: data.bot,
                    type: data.type, timestamp: data.timestamp
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
  console.log('Telegram: ' + (TELEGRAM_TOKEN ? 'ON' : 'OFF'));
  console.log('Endpoints: / (SSE stream), /trending (JSON)');
  if (TELEGRAM_TOKEN) {
    sendTelegram(
      '🟢 <b>Resonance v3 запущено</b>\n' +
      'Wikipedia stream + Pageviews + Trending\n\n' +
      '⚙️ Поріг: 4+ редактори за 5 хв\n' +
      '📈 Pageviews ratio в алертах\n' +
      '🔥 /trending endpoint активний'
    );
  }
});
