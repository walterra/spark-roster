// node --test relay.test.cjs
// The container-side relay must make a dead upstream visible to Pi. A bare
// pipe() leaves the client waiting forever when the upstream dies mid-body.
const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { startRelay } = require('./runner.cjs');

// macOS caps Unix socket paths near 104 bytes, so avoid os.tmpdir().
function upstreamOnSocket(handler) {
  const dir = fs.mkdtempSync('/tmp/sr');
  const sock = path.join(dir, 'u.sock');
  return new Promise(resolve => {
    const server = http.createServer(handler).listen(sock, () => resolve({ server, sock, dir }));
  });
}

function request(port, guardMs = 3000) {
  return new Promise(resolve => {
    const t0 = Date.now();
    let settled = false;
    let bytes = 0;
    let req;
    const timer = setTimeout(() => done({ kind: 'hang' }), guardMs);
    function done(outcome) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      req.destroy();
      resolve({ ...outcome, bytes, ms: Date.now() - t0 });
    }
    req = http.request({ host: '127.0.0.1', port, method: 'POST', path: '/v1/chat/completions' }, res => {
      res.on('data', d => { bytes += d.length; });
      res.on('end', () => done({ kind: 'end', status: res.statusCode }));
      res.on('error', e => done({ kind: 'error', code: e.code, status: res.statusCode }));
      res.on('close', () => { if (!res.complete) done({ kind: 'error', code: 'closed_early', status: res.statusCode }); });
    });
    req.on('error', e => done({ kind: 'error', code: e.code }));
    req.end('{}');
  });
}

async function withRelay(handler, fn) {
  const up = await upstreamOnSocket(handler);
  let relay;
  try {
    relay = await startRelay(up.sock, 0);
    await fn(relay.address().port);
  } finally {
    // Always release the upstream, even when setup itself fails, or a red run hangs.
    if (relay) { relay.closeAllConnections(); relay.close(); }
    up.server.closeAllConnections();
    up.server.close();
    fs.rmSync(up.dir, { recursive: true, force: true });
  }
}

test('upstream that dies mid-body is an error for the client, not a hang or a clean end', async () => {
  await withRelay((req, res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write('data: {}\n\n');
    setTimeout(() => res.socket.destroy(), 50);
  }, async port => {
    const r = await request(port);
    assert.strictEqual(r.kind, 'error', JSON.stringify(r));
    assert.ok(r.ms < 2000, 'failure took ' + r.ms + 'ms');
  });
});

test('a complete response still ends cleanly', async () => {
  await withRelay((req, res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.end('data: {}\n\ndata: [DONE]\n\n');
  }, async port => {
    const r = await request(port);
    assert.strictEqual(r.kind, 'end', JSON.stringify(r));
    assert.ok(r.bytes > 0);
  });
});

test('an unreachable upstream is a 502', async () => {
  const relay = await startRelay('/tmp/sr-missing/none.sock', 0);
  try {
    const r = await request(relay.address().port);
    assert.strictEqual(r.status, 502, JSON.stringify(r));
  } finally {
    relay.closeAllConnections();
    relay.close();
  }
});
