// THE ECLIPSE SPARKPLUG TCK, HOST APPLICATION PROFILE, AGAINST host.ts.
//
// Everything goes through modbee's Application API - the host's MQTT
// connection to the kit's broker and this runner's own (the kit is driven
// over control topics: SPARKPLUG_TCK/TEST_CONTROL starts a test, RESULT
// carries its report, CONSOLE_PROMPT asks for what a person would do, and
// CONSOLE_REPLY answers). No MQTT library anywhere.
//
// The test list, parameters and grading follow modbee-iot-platform's
// docker/sparkplug/tck/driver.mjs, which runs the same profile against the
// platform's own host; what each test wants of the host was read out of the
// kit's v3.0.0 source.
//
//   docker run -d --name modbee-tck -p 127.0.0.1:11883:1883 modbee-sparkplug-tck   (tck/Dockerfile)
//   MODBEE_API=unix:/run/modbee/api.sock npm run tck
//
// Exit code: 0 every test run passed, 1 one did not, 2 the run could not start.
//
// A pass means "passes the TCK Host Application profile". It does NOT mean
// "Sparkplug Compatible", a trademark with an Eclipse Foundation process.

import { SparkplugDataType, type SparkplugValue } from '@modbee/sparkplug';
import { Api, type Delivery } from './api.js';
import { SparkplugHost, type DeviceView, type MetricView } from './host.js';

const API = process.env.MODBEE_API || 'unix:/run/modbee/api.sock';
const TOKEN = process.env.MODBEE_API_TOKEN || '';
const TCK_HOST = process.env.TCK_BROKER_HOST || '127.0.0.1';
const TCK_PORT = Number(process.env.TCK_BROKER_PORT || 11883);
const HOST_ID = process.env.TCK_HOST_ID || 'modbee-tck-host';
const CLIENT_ID = `modbee-host-${HOST_ID}`;
const GROUP_ID = 'SparkplugTCK';
const EDGE_NODE_ID = 'TCKEdgeNode';
const DEVICE_ID = 'TCKDevice';
const REORDER_MS = 5000;
const ONLY = process.env.TCK_ONLY || '';
const TEST_TIMEOUT_MS = Number(process.env.TCK_TEST_TIMEOUT_MS || 60000);

const TOPIC = {
  control: 'SPARKPLUG_TCK/TEST_CONTROL',
  result: 'SPARKPLUG_TCK/RESULT',
  log: 'SPARKPLUG_TCK/LOG',
  prompt: 'SPARKPLUG_TCK/CONSOLE_PROMPT',
  reply: 'SPARKPLUG_TCK/CONSOLE_REPLY',
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const started = Date.now();
const say = (line: string) => console.log(`  [${((Date.now() - started) / 1000).toFixed(1).padStart(6)}s] ${line}`);

type HostStep = 'connect' | 'disconnect' | 'online';

interface HostTest {
  name: string;
  params: string[];
  /** What the host does once the kit has armed the test. */
  host: HostStep;
  skip?: string;
}

const TESTS: HostTest[] = [
  // Starts with the host away; it connects inside the test's window - the kit
  // judges the CONNECT packet itself.
  { name: 'SessionEstablishmentTest', params: [HOST_ID], host: 'connect' },
  // The host up, its client id given; it says goodbye and disconnects.
  { name: 'SessionTerminationTest', params: [HOST_ID, CLIENT_ID], host: 'disconnect' },
  // FOUR parameters, though the kit asks for three: it checks for three and
  // reads a fourth.
  { name: 'EdgeSessionTerminationTest', params: [HOST_ID, GROUP_ID, EDGE_NODE_ID, DEVICE_ID], host: 'online' },
  { name: 'MessageOrderingTest', params: [HOST_ID, GROUP_ID, EDGE_NODE_ID, DEVICE_ID, String(REORDER_MS)], host: 'online' },
  { name: 'SendCommandTest', params: [HOST_ID, GROUP_ID, EDGE_NODE_ID, DEVICE_ID], host: 'online' },
  {
    name: 'MultipleBrokerTest', params: [HOST_ID], host: 'online',
    skip: 'needs a second broker for the host to fail over to (item 6: client broker failover)',
  },
];

// --- The host's answers to the kit's questions --------------------------------
//
// EdgeSessionTerminationTest asks a person to confirm six things the host did
// that no packet shows. Each is answered from the host's own model - and only
// PASS when the model shows it.

function allStale(metrics: Map<string, MetricView>, at: number | undefined): boolean {
  if (metrics.size === 0 || at === undefined) return false;
  for (const m of metrics.values()) if (!m.stale || m.staleAt !== at) return false;
  return true;
}

type Check = () => { pass: boolean; why: string };

// Which of the six a prompt is, as a check of the host's model.
function question(host: SparkplugHost, prompt: string, devicesOfflineSeen: { n: number }): Check {
  const second = /Sparkplug Devices associated with the Edge Node as offline/.test(prompt) && devicesOfflineSeen.n++ > 0;
  return () => answer(host, prompt, second);
}

function answer(host: SparkplugHost, prompt: string, second: boolean): { pass: boolean; why: string } {
  const node = host.nodes.get(`${GROUP_ID}/${EDGE_NODE_ID}`);
  const device: DeviceView | undefined = node?.devices.get(DEVICE_ID);
  if (!node) return { pass: false, why: 'the host has no such edge node' };
  if (/DDEATH/.test(prompt) && /as offline/.test(prompt)) {
    return {
      pass: !!device && !device.online && device.offlineAt !== undefined,
      why: `device ${DEVICE_ID} online=${device?.online} offlineAt=${device?.offlineAt} (the DDEATH's timestamp)`,
    };
  }
  if (/DDEATH/.test(prompt) && /STALE/.test(prompt)) {
    return {
      pass: !!device && allStale(device.metrics, device.offlineAt),
      why: `${device?.metrics.size} DBIRTH metrics stale at ${device?.offlineAt}`,
    };
  }
  const recent = (t?: number) => t !== undefined && Math.abs(Date.now() - t) < 30000;
  if (/NDEATH/.test(prompt) && /mark the Edge Node as offline/.test(prompt)) {
    return { pass: !node.online && recent(node.offlineAt), why: `node online=${node.online} offlineAt=${node.offlineAt} (host time)` };
  }
  if (/NDEATH/.test(prompt) && /NBIRTH as STALE/.test(prompt)) {
    return { pass: allStale(node.metrics, node.offlineAt), why: `${node.metrics.size} NBIRTH metrics stale at ${node.offlineAt}` };
  }
  if (/Sparkplug Devices associated with the Edge Node as offline/.test(prompt)) {
    // Asked twice, word for word: the kit's second asks for the devices'
    // metrics (a copy-paste in its source).
    const devices = [...node.devices.values()];
    if (!second) {
      return {
        pass: devices.length > 0 && devices.every((d) => !d.online && d.offlineAt === node.offlineAt),
        why: `${devices.length} device(s) offline at ${node.offlineAt}`,
      };
    }
    return {
      pass: devices.length > 0 && devices.every((d) => allStale(d.metrics, node.offlineAt)),
      why: `the devices' metrics stale at ${node.offlineAt}`,
    };
  }
  return { pass: false, why: 'a prompt this runner does not know' };
}

/** A metric from a birth to write to: a plain scalar, not a control. */
function writable(metrics: Map<string, MetricView>): [string, SparkplugDataType, SparkplugValue] | null {
  const prefer = [SparkplugDataType.Int32, SparkplugDataType.Boolean, SparkplugDataType.Double];
  for (const type of prefer) {
    for (const [name, m] of metrics) {
      if (m.datatype !== type || /Control\/|^bdSeq$/.test(name)) continue;
      const value: SparkplugValue = type === SparkplugDataType.Boolean ? { kind: 'bool', value: true }
        : type === SparkplugDataType.Double ? { kind: 'float', value: 42.5 } : { kind: 'int', value: 42 };
      return [name, type, value];
    }
  }
  return null;
}

// SendCommandTest asks for four commands, one at a time.
async function command(host: SparkplugHost, prompt: string): Promise<void> {
  const node = host.nodes.get(`${GROUP_ID}/${EDGE_NODE_ID}`);
  if (/Send an edge rebirth/.test(prompt)) {
    await host.rebirth(GROUP_ID, EDGE_NODE_ID);
  } else if (/Send an edge command/.test(prompt)) {
    const target = node && writable(node.metrics);
    if (target) await host.writeNodeMetric(GROUP_ID, EDGE_NODE_ID, ...target);
    else say('no NBIRTH metric to write');
  } else if (/Send a device rebirth/.test(prompt)) {
    await host.deviceRebirth(GROUP_ID, EDGE_NODE_ID, DEVICE_ID);
  } else if (/Send a device command/.test(prompt)) {
    const device = node?.devices.get(DEVICE_ID);
    const target = device && writable(device.metrics);
    if (target) await host.writeDeviceMetric(GROUP_ID, EDGE_NODE_ID, DEVICE_ID, ...target);
    else say('no DBIRTH metric to write');
  }
}

// --- The run ---------------------------------------------------------------------

interface Report {
  test: string;
  status: string;
  passed: number;
  failed: number;
  notRun: number;
  overall: string;
  failures: string[];
  unexercised: string[];
  note?: string;
}

async function main(): Promise<number> {
  const api = new Api(API, TOKEN);
  const servers = [{ host: TCK_HOST, port: TCK_PORT }];

  // What a run before this one left on the server.
  await api.remove('tck-host');
  await api.remove('tck-control');

  // This runner's own connection to the kit.
  const inbox: Delivery[] = [];
  let onDelivery: (d: Delivery) => void = () => {};
  await api.consume('tck-control', [TOPIC.result, TOPIC.log, TOPIC.prompt], (d) => {
    inbox.push(d);
    onDelivery(d);
  });
  await api.put({ id: 'tck-control', servers, clientId: 'modbee-tck-runner', cleanStart: true, protocolVersion: 5 });
  const controlUp = await new Promise<boolean>((resolve) => {
    const stop = api.watch(['tck-control'], (info) => {
      if (info.state === 'CONNECTION_CONNECTED') { stop(); resolve(true); }
    });
    setTimeout(() => { stop(); resolve(false); }, 10000);
  });
  if (!controlUp) {
    console.error(`the kit's broker at ${TCK_HOST}:${TCK_PORT} could not be reached`);
    return 2;
  }
  const control = (text: string, topic = TOPIC.control) =>
    api.publish('tck-control', { topic, payload: Buffer.from(text), qos: 1, retain: false });

  const host = new SparkplugHost({
    api, hostId: HOST_ID, connection: 'tck-host', servers, clientId: CLIENT_ID, reorderTimeoutMs: REORDER_MS,
    log: (line) => say(`host: ${line}`),
  });
  await host.start();

  const reports: Report[] = [];
  for (const t of TESTS) {
    if (ONLY && t.name !== ONLY) continue;
    if (t.skip) {
      reports.push({ test: t.name, status: 'SKIP', passed: 0, failed: 0, notRun: 0, overall: '', failures: [], unexercised: [], note: t.skip });
      continue;
    }
    console.log(`\n${t.name}`);
    if (t.host === 'connect' && host.isOnline) await host.stop();
    if (t.host !== 'connect' && !host.isOnline) await host.start();

    const before = inbox.length;
    const devicesOfflineSeen = { n: 0 };
    let ended = false;
    onDelivery = (d) => {
      const text = Buffer.from(d.payload).toString();
      if (d.topic === TOPIC.result) ended = true;
      if (d.topic === TOPIC.prompt) {
        say(`kit asks: ${text}`);
        if (t.name === 'EdgeSessionTerminationTest') {
          // The prompt and the death it is about come on two connections:
          // the host is given a moment to have seen it, as a person reading
          // a screen would.
          const check = question(host, text, devicesOfflineSeen);
          (async () => {
            let a = check();
            for (const until = Date.now() + 3000; !a.pass && Date.now() < until; a = check()) await sleep(50);
            say(`  the host's model: ${a.why} -> ${a.pass ? 'PASS' : 'FAIL'}`);
            await control(a.pass ? 'PASS' : 'FAIL', TOPIC.reply);
          })().catch(() => {});
        } else if (t.name === 'SendCommandTest') {
          command(host, text).catch((e) => say(`command failed: ${e.message}`));
        }
      } else if (d.topic === TOPIC.log && /not enough parameters|error starting test/i.test(text)) {
        say(`kit log: ${text}`);
      }
    };
    await control(`NEW_TEST host ${t.name} ${t.params.join(' ')}`);
    await sleep(1500);
    if (t.host === 'connect') await host.start();
    if (t.host === 'disconnect') {
      await sleep(1000);
      await host.stop();
    }

    const deadline = Date.now() + TEST_TIMEOUT_MS;
    while (!ended && Date.now() < deadline) await sleep(250);
    if (!ended) {
      say('no result yet: END_TEST');
      await control('END_TEST');
      const grace = Date.now() + 5000;
      while (!ended && Date.now() < grace) await sleep(250);
    }
    await sleep(1000);
    onDelivery = () => {};

    const fresh = inbox.slice(before).map((d) => ({ topic: d.topic, text: Buffer.from(d.payload).toString() }));
    // One message: "<id>: PASS; <id>: FAIL <why>; ...; OVERALL: PASS".
    const assertions = fresh.filter((m) => m.topic === TOPIC.result)
      .flatMap((m) => m.text.split(';')).map((a) => a.trim()).filter(Boolean);
    const overall = assertions.find((a) => /^OVERALL:/i.test(a)) ?? '';
    const graded = assertions.filter((a) => !/^OVERALL:/i.test(a));
    const failed = graded.filter((a) => /:\s*FAIL\b/i.test(a));
    const passed = graded.filter((a) => /:\s*PASS\b/i.test(a));
    const notRun = graded.filter((a) => /NOT EXECUTED/i.test(a));
    const refused = fresh.find((m) => m.topic === TOPIC.log && /not enough parameters|error starting test/i.test(m.text));
    // NOT EXECUTED is not a pass, and neither is silence.
    const status = refused ? 'REFUSED'
      : assertions.length === 0 ? 'NO RESULT'
      : failed.length || /^OVERALL:\s*FAIL/i.test(overall) ? 'FAIL'
      : /^OVERALL:\s*NOT EXECUTED/i.test(overall) || passed.length === 0 ? 'NOT EXERCISED'
      : 'PASS';
    reports.push({
      test: t.name, status, passed: passed.length, failed: failed.length, notRun: notRun.length, overall,
      failures: [...failed, ...graded.filter((a) => /MAYBE/i.test(a))], unexercised: notRun, note: refused?.text,
    });
    say(`${t.name}: ${status} (${passed.length} passed, ${failed.length} failed, ${notRun.length} not executed)`);
  }

  if (host.isOnline) await host.stop();
  host.close();
  await api.remove('tck-control');

  console.log('\n─── Eclipse Sparkplug TCK v3.0.0 · Host Application profile, through the Application API ───');
  console.log(`    kit broker ${TCK_HOST}:${TCK_PORT}, host id "${HOST_ID}", API ${API}\n`);
  for (const r of reports) {
    const counts = r.status === 'SKIP' ? '' : `  (${r.passed} passed, ${r.failed} failed${r.notRun ? `, ${r.notRun} not executed` : ''})`;
    console.log(`  ${r.status.padEnd(13)} ${r.test}${counts}`);
    if (r.note) console.log(`                ${r.note}`);
    if (r.overall) console.log(`                ${r.overall}`);
    for (const f of r.failures.slice(0, 8)) console.log(`                ${f.slice(0, 200)}`);
    for (const f of r.unexercised) console.log(`                ${f.slice(0, 200)}`);
  }
  const ran = reports.filter((r) => r.status !== 'SKIP');
  const good = ran.filter((r) => r.status === 'PASS');
  console.log(`\n  ${good.length} of ${ran.length} run passed, ${reports.length - ran.length} skipped; `
    + `${ran.reduce((n, r) => n + r.passed, 0)} assertions passed, ${ran.reduce((n, r) => n + r.failed, 0)} failed.`);
  console.log('  A pass means "passes the TCK Host Application profile", not "Sparkplug Compatible".\n');
  return good.length === ran.length ? 0 : 1;
}

main().then((code) => process.exit(code), (e) => { console.error(e); process.exit(2); });
