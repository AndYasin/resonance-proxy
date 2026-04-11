const http = require('http');
const https = require('https');

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const allowedWikis = ['enwiki','ukwiki','dewiki','frwiki','ruwiki','eswiki','jawiki','plwiki'];

// Anomaly tracking
// Short window (60s) for dashboard stream
// Long window (300s) for Telegram alerts
const anomWindow = {};
const sentAlerts = {};

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
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body)
    }
  }, (res) => {
    let d = '';
    res.on('data', c => d += c);
    res.on('end', () => {
      try {
        const r = JSON.parse(d);
        if (!r.ok) console.log('Telegram error:', r.description);
        else console.log('Telegram sent OK');
      } catch(e) {}
    });
  });
  req.on('error', (e) => console.log('Telegram req error:', e.message));
  req.write(body);
  req.end();
}

async function getWikiInfo(title, lang) {
  return new Promise((resolve) => {
    const path = '/w/api.php?action=query&prop=categories|langlinks&titles=' +
      encodeURIComponent(title) +
      '&cllimit=30&clshow=!hidden&lllimit=500&format=json';

    const req = https.get({
      hostname: lang + '.wikipedia.org',
      path: path,
      headers: { 'User-Agent': 'ResonanceBot/1.0' }
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          const pages = json.query && json.query.pages ? json.query.pages : {};
          const page = Object.values(pages)[0];
          if (!page) return resolve({ type: 'стаття', langCount: 1 });

          const cats = page.categories
            ? page.categories.map(c => c.title.toLowerCase()).join(' ')
            : '';
          const langCount = page.langlinks ? page.langlinks.length + 1 : 1;

          let type = 'стаття';
          if (cats.indexOf('deaths in 20') !== -1 || cats.indexOf('died 20') !== -1) {
            type = '💀 СМЕРТЬ';
          } else if (/politician|president|minister|senator|congress|parliament|governor|mayor/.test(cats)) {
            type = '🏛 ПОЛІТИК';
          } else if (/businessperson|ceo|billionaire|executive|entrepreneur|founded/.test(cats)) {
            type = '💼 БІЗНЕС';
          } else if (/sportsperson|athlete|footballer|tennis|basketball|olympic|cricket|rugby/.test(cats)) {
            type = '⚽ СПОРТ';
          } else if (/actor|musician|singer|director|comedian|rapper|entertainer/.test(cats)) {
            type = '🎭 КУЛЬТУРА';
          } else if (/military|general|admiral|colonel|commander|armed forces/.test(cats)) {
            type = '🎖 ВІЙСЬКОВІ';
          } else if (/scientist|professor|physicist|biologist|chemist|mathematician/.test(cats)) {
            type = '🔬 НАУКА';
          } else if (/strait|canal|waterway|conflict|crisis|war|military operation|international waters/.test(cats)) {
            type = '🌏 ГЕОПОЛІТИКА';
          } else if (/football club|association football|league|championship|tournament/.test(cats)) {
            type = '🏆 ФУТБОЛ';
          } else if (cats.indexOf('living people') !== -1) {
            type = '👤 ПЕРСОНА';
          }

          resolve({ type, langCount });
        } catch(e) {
          resolve({ type: 'стаття', langCount: 1 });
        }
      });
    });

    req.on('error', () => resolve({ type: 'стаття', langCount: 1 }));
    req.setTimeout(6000, () => {
      req.destroy();
      resolve({ type: 'стаття', langCount: 1 });
    });
  });
}

async function checkAnomaly(title, wiki, user, isBot) {
  const lang = wiki.replace('wiki', '') || 'en';
  const key = wiki + ':' + title;
  const now = Date.now();

  if (!anomWindow[key]) {
    anomWindow[key] = {
      // Short window 60s — for dashboard
      ts60: [],
      users60: new Set(),
      // Long window 300s — for Telegram (5 min)
      ts300: [],
      users300: new Set(),
      firedTelegram: false,
      firedSingle: false,
      lastReset: now
    };
  }

  const w = anomWindow[key];

  // Update both windows
  w.ts60.push(now);
  w.ts300.push(now);
  if (user && !isBot) {
    w.users60.add(user);
    w.users300.add(user);
  }

  // Clean old entries
  w.ts60   = w.ts60.filter(t => now - t < 60000);
  w.ts300  = w.ts300.filter(t => now - t < 300000);

  // Remove users that are no longer in window (approximate — reset if empty)
  if (w.ts60.length === 0) w.users60 = new Set();
  if (w.ts300.length === 0) {
    w.users300 = new Set();
    w.firedTelegram = false;
    w.firedSingle = false;
  }

  const hits60   = w.ts60.length;
  const uniq60   = w.users60.size;
  const hits300  = w.ts300.length;
  const uniq300  = w.users300.size;

  const alertKey = key + ':' + Math.floor(now / 300000); // one alert per 5-min bucket

  // ── TELEGRAM ALERT: 4+ unique editors within 5 minutes ──
  if (uniq300 >= 4 && !w.firedTelegram && !sentAlerts[alertKey + ':tg']) {
    w.firedTelegram = true;
    sentAlerts[alertKey + ':tg'] = true;

    const info = await getWikiInfo(title, lang);
    const langCount = info.langCount;
    const type = info.type;

    const isImportant =
      type.includes('СМЕРТЬ') ||
      type.includes('ПОЛІТИК') ||
      type.includes('БІЗНЕС') ||
      type.includes('ГЕОПОЛІТИКА') ||
      type.includes('ВІЙСЬКОВІ') ||
      (type.includes('ПЕРСОНА') && langCount >= 5) ||
      (type.includes('СПОРТ') && langCount >= 10) ||
      (type.includes('КУЛЬТУРА') && langCount >= 10) ||
      (type.includes('ФУТБОЛ') && langCount >= 15) ||
      (type.includes('НАУКА') && langCount >= 5) ||
      langCount >= 20;

    if (isImportant) {
      const wikiUrl = 'https://' + lang + '.wikipedia.org/wiki/' +
        encodeURIComponent(title.replace(/ /g, '_'));

      const langLabel = langCount >= 50
        ? '🌍 глобальна (' + langCount + ' мов)'
        : langCount >= 20
          ? '🌐 міжнар. (' + langCount + ' мов)'
          : langCount >= 10
            ? '📍 регіон. (' + langCount + ' мов)'
            : '📌 локальна (' + langCount + ' мов)';

      const timeWindow = Math.round((w.ts300[w.ts300.length-1] - w.ts300[0]) / 1000);
      const windowStr = timeWindow < 60
        ? timeWindow + ' сек'
        : Math.round(timeWindow / 60) + ' хв ' + (timeWindow % 60) + ' сек';

      const msg =
        '⚡ <b>RESONANCE ALERT</b>\n\n' +
        '<b>' + title + '</b>\n' +
        type + ' · ' + langLabel + '\n\n' +
        '👥 ' + uniq300 + ' незалежних редактори\n' +
        '📝 ' + hits300 + ' правок за ' + windowStr + '\n' +
        '🌐 ' + lang + '.wikipedia\n\n' +
        '<a href="' + wikiUrl + '">Відкрити статтю →</a>';

      sendTelegram(msg);
      console.log('TELEGRAM SENT:', title, '| type:', type, '| langs:', langCount, '| editors:', uniq300, '| window:', windowStr);
    }
  }

  // ── TELEGRAM ALERT: death or global spike from single editor ──
  if (hits60 >= 6 && uniq60 <= 1 && !w.firedSingle && !sentAlerts[alertKey + ':single']) {
    w.firedSingle = true;
    sentAlerts[alertKey + ':single'] = true;

    const info = await getWikiInfo(title, lang);
    const langCount = info.langCount;
    const type = info.type;

    if (type.includes('СМЕРТЬ') || type.includes('ГЕОПОЛІТИКА') || langCount >= 30) {
      const wikiUrl = 'https://' + lang + '.wikipedia.org/wiki/' +
        encodeURIComponent(title.replace(/ /g, '_'));

      const msg =
        '🔴 <b>RESONANCE — SPIKE</b>\n\n' +
        '<b>' + title + '</b>\n' +
        type + ' · ' + langCount + ' мов\n' +
        '⚡ ' + hits60 + ' правок за 60 сек · один редактор\n' +
        lang + '.wikipedia\n\n' +
        '<a href="' + wikiUrl + '">Відкрити →</a>';

      sendTelegram(msg);
      console.log('SPIKE SENT:', title, type, langCount);
    }
  }

  // Cleanup old windows after 10 min
  if (now - w.lastReset > 600000) {
    delete anomWindow[key];
  }
}

// Cleanup sentAlerts every 15 min
setInterval(() => {
  const keys = Object.keys(sentAlerts);
  if (keys.length > 1000) {
    keys.slice(0, 500).forEach(k => delete sentAlerts[k]);
    console.log('Cleaned', 500, 'old alert keys');
  }
}, 900000);

// Cleanup anomWindow every 5 min
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;
  Object.keys(anomWindow).forEach(key => {
    if (now - anomWindow[key].lastReset > 600000) {
      delete anomWindow[key];
      cleaned++;
    }
  });
  if (cleaned > 0) console.log('Cleaned', cleaned, 'anomaly windows');
}, 300000);

// ── HTTP SERVER ──
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

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
      headers: {
        'Accept': 'text/event-stream',
        'User-Agent': 'ResonanceProxy/1.0'
      }
    }, (upstream) => {
      upstream.on('data', chunk => {
        try {
          const lines = chunk.toString().split('\n');
          lines.forEach(line => {
            if (line.startsWith('data: ')) {
              try {
                const data = JSON.parse(line.slice(6));
                if ((data.type === 'edit' || data.type === 'new') &&
                    allowedWikis.includes(data.wiki) &&
                    data.title &&
                    !data.title.includes(':')) {

                  // Async anomaly check (Telegram)
                  checkAnomaly(data.title, data.wiki, data.user, data.bot);

                  // Forward to dashboard
                  try {
                    res.write('data: ' + JSON.stringify({
                      title: data.title,
                      wiki: data.wiki,
                      user: data.user,
                      bot: data.bot,
                      type: data.type,
                      timestamp: data.timestamp
                    }) + '\n\n');
                  } catch(e) {}
                }
              } catch(e) {}
            }
          });
        } catch(e) {}
      });

      upstream.on('end', () => {
        console.log('Upstream ended, reconnecting...');
        setTimeout(connectUpstream, 2000);
      });
      upstream.on('error', (e) => {
        console.log('Upstream error:', e.message);
        setTimeout(connectUpstream, 2000);
      });
    });

    req2.on('error', (e) => {
      console.log('Request error:', e.message);
      setTimeout(connectUpstream, 2000);
    });
  }

  connectUpstream();
  req.on('close', () => {
    console.log('Client disconnected');
    res.end();
  });
});

server.listen(process.env.PORT || 3000, '0.0.0.0', () => {
  const port = process.env.PORT || 3000;
  console.log('Resonance proxy running on port ' + port);
  console.log('Telegram alerts: ' + (TELEGRAM_TOKEN ? 'ENABLED' : 'DISABLED'));
  console.log('Alert threshold: 4+ unique editors within 5 min');

  if (TELEGRAM_TOKEN) {
    sendTelegram(
      '🟢 <b>Resonance запущено</b>\n' +
      'Моніторинг Wikipedia активний\n\n' +
      '⚙️ Налаштування:\n' +
      '👥 Поріг: 4+ редактори за 5 хв\n' +
      '🌍 Або: глобальна стаття (20+ мов)\n' +
      '💀 Або: смерть/геополітика будь-який spike'
    );
  }
});
