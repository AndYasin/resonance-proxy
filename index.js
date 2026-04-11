const http = require('http');
const https = require('https');

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

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
        try { res.write(chunk); } catch(e) { upstream.destroy(); }
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
