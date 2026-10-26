// One member of a consumer group: an application reading every device's
// messages over the Application API, as a platform's ingest does.
//
// Run as many copies as you like with the same group: they share the
// messages, one device's to one copy, in order. A copy that stops - even
// killed - loses nothing: what it had not acknowledged goes to the others,
// marked `redelivered`, and with nobody connected the group keeps collecting
// (`replay` on what waited).
//
//   npm install
//   MODBEE_API=unix:/run/modbee/api.sock MODBEE_GROUP=platform node consumer.js
//
// Each message is printed as a line of JSON, then acknowledged: an
// acknowledgement says "handled", so it goes after the work, never before.

'use strict';

const grpc = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const path = require('path');

const PROTO = process.env.MODBEE_PROTO || path.join(__dirname, '../../../proto/modbee/app/v1/app.proto');
const TARGET = process.env.MODBEE_API || 'unix:/run/modbee/api.sock';
const TOKEN = process.env.MODBEE_API_TOKEN || '';
const GROUP = process.env.MODBEE_GROUP || 'platform';
const FILTERS = (process.env.MODBEE_FILTERS || '#').split(',');

const definition = loader.loadSync(PROTO, { longs: String, defaults: true, oneofs: true });
const v1 = grpc.loadPackageDefinition(definition).modbee.app.v1;

function metadata() {
  const m = new grpc.Metadata();
  if (TOKEN) m.set('authorization', `Bearer ${TOKEN}`);
  return m;
}

function consume() {
  const messages = new v1.Messages(TARGET, grpc.credentials.createInsecure());
  const call = messages.Consume(metadata());
  call.write({ open: { group: GROUP, filters: FILTERS, maxInFlight: 100 } });
  call.on('data', (down) => {
    if (down.opened) {
      console.error(`member of "${GROUP}" (${down.opened.members} connected, ${down.opened.queued} waiting)`);
    } else if (down.delivery) {
      const d = down.delivery;
      console.log(JSON.stringify({
        topic: d.topic,
        payload: Buffer.from(d.payload).toString(),
        client: d.publisher.clientId,
        login: d.publisher.login,
        time: Number(d.brokerTimeMs),
        replay: d.replay,
        redelivered: d.redelivered,
      }));
      call.write({ ack: { ids: [d.id] } });
    } else if (down.event) {
      console.log(JSON.stringify({ event: down.event.kind, client: down.event.clientId }));
      call.write({ ack: { ids: [down.event.id] } });
    }
  });
  const again = () => setTimeout(consume, 1000);
  call.on('error', (err) => {
    if (err.code !== grpc.status.CANCELLED) console.error(`consume: ${err.message}`);
  });
  call.on('end', again);
  call.on('error', again);
}

consume();
