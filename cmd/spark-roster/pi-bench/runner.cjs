const http = require('node:http');
const {spawn} = require('node:child_process');
const fs = require('node:fs');

async function agent(model, prompt, window = 32768, maxTokens = 4096, attachment, timeoutMs) {
  const relay = http.createServer((req, res) => {
    const upstream = http.request({socketPath:'/relay/inference.sock', path:req.url, method:req.method,
      headers:{'content-type':'application/json'}}, response => {
      res.writeHead(response.statusCode, {'content-type':response.headers['content-type'] || 'application/json'});
      response.pipe(res);
    });
    upstream.on('error', () => { if (!res.headersSent) res.writeHead(502); res.end(); });
    res.on('close', () => upstream.destroy());
    req.pipe(upstream);
  });
  await new Promise((resolve, reject) => relay.on('error', reject).listen(18765, '127.0.0.1', resolve));
  fs.mkdirSync('/tmp/pi', {mode:0o700});
  if (timeoutMs) {
    const timeout = Number(timeoutMs);
    fs.writeFileSync('/tmp/pi/settings.json', JSON.stringify({httpIdleTimeoutMs:timeout, retry:{enabled:false, provider:{timeoutMs:timeout,maxRetries:0}}}));
  }
  fs.writeFileSync('/tmp/pi/models.json', JSON.stringify({providers:{'bench-local':{
    baseUrl:'http://127.0.0.1:18765/v1', api:'openai-completions', apiKey:'local',
    models:[{id:model, name:model, reasoning:false, input:['text'], contextWindow:Number(window), maxTokens:Number(maxTokens)}]
  }}}));
  const child = spawn('/opt/bench/node_modules/.bin/pi', [
    '-p','--mode','json','--model',`bench-local/${model}`,'--no-session','--no-extensions','--no-mcp',
    '--no-skills','--no-context-files','--no-prompt-templates','--no-themes','--no-approve','--offline',
    '--tools','read,bash,edit,write','--', ...(attachment && attachment !== '-' ? [`@${attachment}`] : []), prompt
  ], {cwd:'/work', stdio:['ignore','inherit','inherit'], env:{...process.env, HOME:'/tmp', PI_CODING_AGENT_DIR:'/tmp/pi', PI_OFFLINE:'1', NO_COLOR:'1', OTEL_SDK_DISABLED:'true'}});
  child.on('close', code => {relay.closeAllConnections(); relay.close(); process.exitCode = code ?? 1;});
  child.on('error', () => {relay.close(); process.exitCode = 1;});
}

async function grade() {
  const input = JSON.parse(process.argv[3]);
  // stdout is the result channel, not the submission's diagnostic stream.
  const emitResult = process.stdout.write.bind(process.stdout);
  process.stdout.write = process.stderr.write.bind(process.stderr);
  const implementation = require('/work/solution.cjs');
  const results=[];
  for (const args of input.cases) {
    const before = input.immutable && JSON.stringify(args);
    results.push(await implementation(...args));
    if (input.immutable && before !== JSON.stringify(args)) throw Error('input mutated');
  }
  emitResult(JSON.stringify(results)+'\n');
}

if (process.argv[2] === 'agent') agent(process.argv[3], process.argv[4], process.argv[5], process.argv[6], process.argv[7], process.argv[8]).catch(() => {process.exitCode=1;});
else if (process.argv[2] === 'grade') grade().catch(err => {console.error(err.message); process.exitCode=1;});
else process.exitCode=2;
