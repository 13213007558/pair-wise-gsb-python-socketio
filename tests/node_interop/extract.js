const cacache = require('/Users/apple/nodejs/lib/node_modules/npm/node_modules/cacache');
const { execSync } = require('child_process');
const path = require('path');
const fs = require('fs');

const CACHE = '/Users/apple/.npm/_cacache';
const pkgs = {
  'msgpackr': 'make-fetch-happen:request-cache:https://registry.npmjs.org/msgpackr/-/msgpackr-1.11.12.tgz',
  'socket.io-adapter': 'make-fetch-happen:request-cache:https://registry.npmjs.org/socket.io-adapter/-/socket.io-adapter-2.5.7.tgz',
  'socket.io-parser': 'make-fetch-happen:request-cache:https://registry.npmjs.org/socket.io-parser/-/socket.io-parser-4.2.6.tgz',
  '@socket.io/component-emitter': 'make-fetch-happen:request-cache:https://registry.npmjs.org/@socket.io/component-emitter/-/component-emitter-3.1.2.tgz',
  'debug': 'make-fetch-happen:request-cache:https://registry.npmjs.org/debug/-/debug-4.4.1.tgz',
  'ws': 'make-fetch-happen:request-cache:https://registry.npmjs.org/ws/-/ws-8.20.1.tgz',
  'ms': 'make-fetch-happen:request-cache:https://registry.npmjs.org/ms/-/ms-2.1.3.tgz',
};

(async () => {
  for (const [name, key] of Object.entries(pkgs)) {
    const dest = path.join('node_modules', name);
    fs.rmSync(dest, { recursive: true, force: true });
    fs.mkdirSync(dest, { recursive: true });
    const data = await cacache.get(CACHE, key);
    const tmp = path.join('node_modules', '.tmp-' + name.replace(/\//g, '_') + '.tgz');
    fs.writeFileSync(tmp, data.data);
    execSync('tar -xzf ' + tmp + ' -C ' + dest + ' --strip-components=1');
    fs.rmSync(tmp);
    console.log('extracted', name);
  }
})().catch(e => { console.error(e); process.exit(1); });
