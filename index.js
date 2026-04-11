const http = require('http');
const https = require('https');

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

  const allowedWikis = ['enwiki','ukwiki','dewiki','frwiki','ruwiki','eswiki','jawiki','plwiki'];

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
                  res.write(`data: ${JSON.stringify({
                    title: data.title,
                    wiki: data.wiki,
                    user: data.user,
                    bot: data.bot,
                    type: data.type,
                    timestamp: data.timestamp
                  })}\n\n`);
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
  console.log(`Resonance proxy on port ${process.env.PORT || 3000}`);
});
