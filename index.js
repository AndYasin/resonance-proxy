const http = require('http');
const EventSource = require('eventsource');

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  const es = new EventSource(
    'https://stream.wikimedia.org/v2/stream/recentchange'
  );

  es.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data);
      if (data.type === 'edit' || data.type === 'new') {
        res.write(`data: ${JSON.stringify(data)}\n\n`);
      }
    } catch(err) {}
  };

  req.on('close', () => es.close());
});

server.listen(process.env.PORT || 3000);
console.log('Resonance proxy running on port', process.env.PORT || 3000);
