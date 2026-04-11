const http = require('http');
const https = require('https');

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const allowedWikis = ['enwiki','ukwiki','dewiki','frwiki','ruwiki','eswiki','jawiki','plwiki'];

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
          } else if (/businessperson|ceo|billionaire|executive|entrepreneur/.test(cats)) {
            type = '💼 БІЗНЕС';
          } else if (/sportsperson|athlete|footballer|tennis|basketball|olympic/.test(cats)) {
            type = '⚽ СПОРТ';
          } else if (/actor|musician|singer|director|comedian|rapper/.test(cats)) {
            type = '🎭 КУЛЬТУРА';
          } else if (/military|general|admiral|colonel|commander/.test(cats)) {
            type = '🎖 ВІЙСЬКОВІ';
          } else if (/scientist|professor|physicist|biologist|chemist/.test(cats)) {
            type = '🔬 НАУКА';
          } else if (cats.indexOf('living people') !== -1) {
            type = '👤 ПЕРСОНА';
          } else if (/football club|association football|league|championship/.test(cats)) {
            type = '🏆 ФУТБОЛ';
          }

          resolve({ type, langCount });
        } catch(e) {
          resolve({ type: 'стаття', langCount: 1 });
        }
      });
    });

    req.on('error', () => resolve({ type: 'стаття', langCount: 1 }));
    req.setTimeout(5000, () => {
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
    anomWindow[key] = { ts: [], users: new Set(), firedMulti: false, firedSingle: false };
  }

  anomWindow[key].ts.push(now);
  if (user && !isBot) anomWindow[key].users.add(user);
  anomWindow[key].ts = anomWindow[key].ts.filter(t => now - t < 60000);

  const hits = anomWindow[key].ts.length;
  const uniq = anomWindow[key].users.size;
  const alertKey = key + ':' + Math.floor(now / 60000);

  // Multi-editor: 2+ unique editors, 2+ edits
  if (uniq >= 2 && hits >= 2 && !anomWindow[key].firedMulti && !sentAlerts[alertKey + ':multi']) {
    anomWindow[key].firedMulti = true;
    sentAlerts[alertKey + ':multi'] = true;

    const info = await getWikiInfo(title, lang);
    const langCount = info.langCount;
    const type = info.type;

    // Send for any classified type OR 10+ languages
    const isImportant = type !== 'стаття' || langCount >= 10;

    if (isImportant) {
      const wikiUrl = 'https://' + lang + '.wikipedia.org/wiki/' +
        encodeURIComponent(title.replace(/ /g, '_'));

      const langLabel = langCount >= 50
        ? '🌍 глобальна (' + langCount + ' мов)'
        : langCount >= 20
          ? '🌐 міжнар. (' + langCount + ' мов)'
          : langCount >= 10
            ? '📍 регіональна (' + langCount + ' мов)'
            : '📌 локальна (' + langCount + ' мов)';

      const msg =
        '⚡ <b>RESONANCE — МУЛЬТИ-РЕДАКТОР</b>\n\n' +
        '<b>' + title + '</b>\n' +
        type + ' · ' + langLabel + '\n' +
        '👥 ' + uniq + ' редактори · ' + hits + ' правок / 60 сек\n' +
        lang + '.wikipedia\n\n' +
        '<a href="' + wikiUrl + '">Відкрити статтю →</a>';

      sendTelegram(msg);
      console.log('Alert sent:', title, type, langCount + ' langs');
    }
  }

  // Single spike: 5+ edits from one editor
  if (hits >= 5 && uniq <= 1 && !anomWindow[key].firedSingle && !sentAlerts[alertKey + ':single']) {
    anomWindow[key].firedSingle = true;
    sentAlerts[alertKey + ':single'] = true;

    const info = await getWikiInfo(title, lang);
    const langCount = info.langCount;
    const type = info.type;

    if (type.includes('СМЕРТЬ') || langCount >= 20) {
      const wikiUrl = 'https://' + lang + '.wikipedia.org/wiki/' +
        encodeURIComponent(title.replace(/ /g, '_'));

      const msg =
        '🔴 <b>RESONANCE — SPIKE</b>\n\n' +
        '<b>' + title + '</b>\n' +
        type + ' · ' + langCount + ' мов\n' +
        hits + ' правок · один редактор\n' +
        lang + '.wikipedia\n\n' +
        '<a href="' + wikiUrl + '">Відкрити →</a>';

      sendTelegram(msg);
    }
  }

  // Cleanup old windows after 3 min
  setTimeout(() => {
    if (anomWindow[key]) {
      anomWindow[key].ts = anomWindow[key].ts.filter(t => Date.now() - t < 60000);
      if (anomWindow[key].ts.length === 0) delete anomWindow[key];
    }
  }, 180000);
}

// Cleanup sentAlerts every 10 min
setInterval(() => {
  const keys = Object.keys(sentAlerts);
  if (keys.length > 500) keys.slice(0, 250).forEach(k => delete sentAlerts[k]);
}, 600000);

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
      headers: { 'Accept': 'text/event-stream', 'User-Agent': 'ResonanceProxy/1.0' }
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

                  checkAnomaly(data.title, data.wiki, data.user, data.bot);

                  res.write('data: ' + JSON.stringify({
                    title: data.title,
                    wiki: data.wiki,
                    user: data.user,
                    bot: data.bot,
                    type: data.type,
                    timestamp: data.timestamp
                  }) + '\n\n');
                }
              } catch(e) {}
            }
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
  if (TELEGRAM_TOKEN) {
    sendTelegram('🟢 <b>Resonance запущено</b>\nМоніторинг Wikipedia активний\nПоріг: 2+ редактори або 10+ мов');
    console.log('Telegram enabled');
  }
});
