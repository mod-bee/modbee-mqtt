// An application deciding modbee's MQTT logins, over the Application API.
//
// modbee-mqtt serve, with:
//
//   authentication:
//     username: platform          # the application's own login: never asked
//     password: ...
//     authority: {}               # every other login is asked of this program
//   api:
//     grpc: ["unix:/run/modbee/api.sock"]
//
// asks this program about each login it has not decided recently, ONCE, and
// keeps the answer for the time it is given (ttlSeconds). What a login may do
// - publish, subscribe, a narrowed wide subscription, levels it may never
// touch - is in the answer, so no MQTT message ever waits on this program.
//
// Run as many copies as you like: questions are dealt round them, and one a
// copy did not answer before it stopped is asked of another.
//
//   npm install
//   MODBEE_API=unix:/run/modbee/api.sock node authority.js
//
// The LOGINS table stands in for a real application's database.

'use strict';

const grpc = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const os = require('os');
const path = require('path');

const PROTO = process.env.MODBEE_PROTO || path.join(__dirname, '../../../proto/modbee/app/v1/app.proto');
const TARGET = process.env.MODBEE_API || 'unix:/run/modbee/api.sock';
const TOKEN = process.env.MODBEE_API_TOKEN || '';     // api.token-file's contents, if it has one

const definition = loader.loadSync(PROTO, { longs: String, defaults: true, oneofs: true });
const v1 = grpc.loadPackageDefinition(definition).modbee.app.v1;
const credentials = grpc.credentials.createInsecure();

function metadata() {
  const m = new grpc.Metadata();
  if (TOKEN) m.set('authorization', `Bearer ${TOKEN}`);
  return m;
}

// --- The application's own rules ------------------------------------------------

// One tenant's namespace and its Sparkplug group: the shapes a platform gives.
const LOGINS = {
  // A device's own login: its own topics only.
  'acme/pump-07': { password: 's3cret', scope: {
    publish: ['acme/pump-07/#'],
    subscribe: ['acme/pump-07/#'],
  } },
  // A shared login for the namespace: everything in it but the devices'
  // reserved levels; and a Sparkplug host that may subscribe spBv1.0/# and
  // receives only its own group through it.
  'acme/host': { password: 'h0st', scope: {
    publish: ['acme/#', 'spBv1.0/acme/#', 'spBv1.0/STATE/acme-host'],
    subscribe: ['acme/#', 'spBv1.0/acme/#', 'spBv1.0/STATE/+'],
    subscribeWide: ['spBv1.0/#'],
    deny: ['acme/+/$ota/#'],
    tag: Buffer.from('tenant=acme'),
  } },
};

function decide(question) {
  const user = question.username || '';
  const password = question.password ? Buffer.from(question.password).toString() : '';
  const login = LOGINS[user];
  if (!login || login.password !== password) {
    return { id: question.id, allow: false, refusal: 'REFUSAL_BAD_CREDENTIALS', ttlSeconds: 30 };
  }
  return { id: question.id, allow: true, ttlSeconds: 300, scope: login.scope };
}

// --- Authority.Serve: answer each question as it comes ----------------------------

function serve() {
  const authority = new v1.Authority(TARGET, credentials);
  const call = authority.Serve(metadata());
  call.write({ open: { instance: `${os.hostname()}:${process.pid}` } });
  call.on('data', (down) => {
    if (!down.connect) return;
    const decision = decide(down.connect);
    console.log(`asked about "${down.connect.username}" (client ${down.connect.clientId}): ` +
                (decision.allow ? 'allowed' : 'refused'));
    call.write({ decision });
  });
  // modbee restarted, or the socket went: come back. Meanwhile modbee keeps
  // serving the logins it was told about.
  const again = () => setTimeout(serve, 1000);
  call.on('error', (err) => {
    if (err.code !== grpc.status.CANCELLED) console.error(`authority stream: ${err.message}`);
  });
  call.on('end', again);
  call.on('error', again);
}

new v1.System(TARGET, credentials).Hello({ apiVersion: '1.0', application: 'example-authority' }, metadata(),
  (err, reply) => {
    if (err) {
      console.error(`modbee at ${TARGET}: ${err.message}`);
      process.exit(1);
    }
    console.log(`${reply.serverVersion} (API ${reply.apiVersion}, ${reply.features.join(', ')}) at ${TARGET}`);
    serve();
  });
