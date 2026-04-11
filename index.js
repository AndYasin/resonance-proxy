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

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const options = {
    hostname: 'stream.wikimedia.org',
    path: '/v2/stream/recentchange',
    headers: { 'Accept': 'text/event-stream' }
  };

  const upstream = https.get(options, (upstream_res) => {
    upstream_res.on('data', (chunk) => {
      try { res.write(chunk); } catch(e) {}
    });
    upstream_res.on('end', () => {
      res.end();
      setTimeout(() => server.emit('reconnect'), 1000);
    });
  });

  upstream.on('error', () => {
    try { res.end(); } catch(e) {}
  });

  req.on('close', () => upstream.destroy());
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`Resonance proxy running on port ${PORT}`);
});
