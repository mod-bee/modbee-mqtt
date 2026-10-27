// A Sparkplug B 3.0 Host Application on a broker it does not own, with no
// MQTT library: modbee-mqtt serve holds the MQTT connection (Connections), hands
// over what arrives on it (Messages.Consume), and publishes for it
// (Messages.Publish, answered once the remote broker has acknowledged).
//
// The codec, the topic grammar and the STATE timestamp rule are the
// platform's own (@modbee/sparkplug, read-only); what is here is the host's
// behaviour:
//
//   CONNECT    a Will on spBv1.0/STATE/<host id>, {"online":false} with the
//              time of THIS connect (a fresh one each time: RECONNECT_WHEN_ASKED
//              hands every reconnect back to us), QoS 1, retained; clean start
//   SUBSCRIBE  spBv1.0/# and spBv1.0/STATE/<host id>, before the birth (the
//              server subscribes a connection's filters as it connects)
//   BIRTH      {"online":true} with the Will's timestamp, QoS 1, retained -
//              and again whenever an offline STATE for this host appears
//   GOODBYE    {"online":false} published before an intentional disconnect:
//              a DISCONNECT makes the broker discard the Will
//   SEQ        per edge node; a gap waits `reorderTimeoutMs` for the missing
//              message, then asks for ONE rebirth (Node Control/Rebirth)
//   DEATH      DDEATH: the device offline and its metrics stale at the
//              DDEATH's timestamp; NDEATH (its bdSeq matching the birth): the
//              node, its metrics, its devices and theirs, at the host's time

import {
  NODE_CONTROL_REBIRTH,
  DEVICE_CONTROL_REBIRTH,
  SparkplugDataType,
  SparkplugHostState,
  SparkplugMessageType,
  buildStateTopic,
  encodePayload,
  encodeStatePayload,
  parseSparkplugTopic,
  parseStatePayload,
  tryDecodePayload,
  type SparkplugMetric,
  type SparkplugPayload,
  type SparkplugValue,
} from '@modbee/sparkplug';
import type { Api, ConnectionInfo, Delivery, Message } from './api.js';

export interface MetricView {
  datatype?: SparkplugDataType;
  value: SparkplugValue;
  stale: boolean;
  staleAt?: number;
}

export interface DeviceView {
  online: boolean;
  offlineAt?: number;
  metrics: Map<string, MetricView>;
}

export interface NodeView {
  groupId: string;
  edgeNodeId: string;
  online: boolean;
  offlineAt?: number;
  bdSeq?: bigint;
  lastSeq?: number;
  metrics: Map<string, MetricView>;
  aliases: Map<string, { name: string; deviceId?: string }>;
  devices: Map<string, DeviceView>;
  /** Messages that arrived ahead of a gap, by seq, until it fills or the wait ends. */
  held: Map<number, () => void>;
  reorderTimer?: NodeJS.Timeout;
  rebirthAsked: boolean;
}

export interface HostOptions {
  api: Api;
  hostId: string;
  /** The managed connection's id on the server. */
  connection: string;
  servers: { host: string; port: number }[];
  clientId: string;
  reorderTimeoutMs: number;
  log?: (line: string) => void;
}

const NAMESPACE_FILTER = 'spBv1.0/#';

export class SparkplugHost {
  readonly nodes = new Map<string, NodeView>();
  private readonly state: SparkplugHostState;
  private readonly stateTopic: string;
  private online = false;
  private birthed = false;
  private stopWatching?: () => void;
  private stopConsuming?: () => void;
  private connected?: () => void;

  constructor(private readonly o: HostOptions) {
    this.state = new SparkplugHostState(o.hostId);
    this.stateTopic = buildStateTopic(o.hostId);
  }

  private log(line: string): void {
    this.o.log?.(line);
  }

  get isOnline(): boolean {
    return this.online;
  }

  /** Connects, subscribes and publishes the birth; resolves once the birth is acknowledged. */
  async start(): Promise<void> {
    if (!this.stopConsuming) {
      // The stream first: its filters are the connection's subscriptions, in
      // place before the CONNECT so they go out ahead of the birth.
      this.stopConsuming = await this.o.api.consume(this.o.connection, [NAMESPACE_FILTER, this.stateTopic],
        (d) => this.onMessage(d));
    }
    const up = new Promise<void>((resolve) => { this.connected = resolve; });
    this.birthed = false;
    await this.o.api.put({
      id: this.o.connection,
      servers: this.o.servers,
      clientId: this.o.clientId,
      protocolVersion: 5,
      cleanStart: true,
      sessionExpirySeconds: 0,
      will: this.will(),
      reconnect: 'RECONNECT_WHEN_ASKED',
    });
    this.stopWatching?.();
    this.stopWatching = this.o.api.watch([this.o.connection], (info) => this.onState(info));
    await up;
  }

  /** The goodbye a DISCONNECT would otherwise swallow, then the disconnect. */
  async stop(): Promise<void> {
    this.stopWatching?.();
    this.stopWatching = undefined;
    if (this.online && this.state.timestamp !== null) {
      const reply = await this.o.api.publish(this.o.connection, {
        topic: this.stateTopic,
        payload: encodeStatePayload({ online: false, timestamp: this.state.timestamp }),
        qos: 1,
        retain: true,
      });
      this.log(`STATE offline published (${reply.outcome})`);
    }
    this.online = false;
    await this.o.api.remove(this.o.connection);
    this.log('disconnected');
  }

  close(): void {
    this.stopWatching?.();
    this.stopConsuming?.();
    for (const node of this.nodes.values()) clearTimeout(node.reorderTimer);
  }

  /** A Will carrying the time of the connect it goes with. */
  private will(): Message {
    return {
      topic: this.stateTopic,
      payload: encodeStatePayload(this.state.willPayload(Date.now())),
      qos: 1,
      retain: true,
    };
  }

  private onState(info: ConnectionInfo): void {
    if (info.state === 'CONNECTION_CONNECTED' && !this.birthed) {
      this.birthed = true;
      this.publishBirth().then(() => {
        this.online = true;
        this.connected?.();
      });
    } else if (info.state === 'CONNECTION_WAITING') {
      // Dropped: back with a Will of this moment.
      this.online = false;
      this.birthed = false;
      this.log('connection lost: reconnecting with a new Will');
      this.o.api.reconnect(this.o.connection, this.will()).catch(() => {});
    }
  }

  private async publishBirth(): Promise<void> {
    const reply = await this.o.api.publish(this.o.connection, {
      topic: this.stateTopic,
      payload: encodeStatePayload(this.state.birthPayload()),
      qos: 1,
      retain: true,
    });
    this.log(`STATE online published, timestamp ${this.state.timestamp} (${reply.outcome})`);
  }

  // --- What arrives ---

  private onMessage(d: Delivery): void {
    if (d.topic === this.stateTopic) {
      // Somebody says this host is offline while it is up - the broker, or a
      // stale retained message: say otherwise. Our own older ones are ignored.
      const s = parseStatePayload(d.payload);
      // From the moment the birth is sent: the offline can overtake its PUBACK.
      if (s && !s.online && this.birthed && this.state.timestamp !== null && s.timestamp >= this.state.timestamp) {
        this.log('an offline STATE for this host: publishing the birth again');
        this.publishBirth().catch(() => {});
      }
      return;
    }
    const topic = parseSparkplugTopic(d.topic);
    if (!topic || topic.messageType === SparkplugMessageType.STATE) return;
    if (topic.messageType === SparkplugMessageType.NCMD || topic.messageType === SparkplugMessageType.DCMD) return;
    const decoded = tryDecodePayload(d.payload);
    if (!decoded.ok) {
      this.log(`${d.topic}: not a Sparkplug B payload (${decoded.reason})`);
      return;
    }
    const p = decoded.payload;
    const node = this.nodeOf(topic.groupId, topic.edgeNodeId);
    switch (topic.messageType) {
      case SparkplugMessageType.NBIRTH:
        return this.onNodeBirth(node, p);
      case SparkplugMessageType.NDEATH:
        return this.onNodeDeath(node, p);
      default:
        return this.inSequence(node, p, topic.messageType, () => this.apply(node, topic.messageType, topic.deviceId, p));
    }
  }

  private nodeOf(groupId: string, edgeNodeId: string): NodeView {
    const key = `${groupId}/${edgeNodeId}`;
    let node = this.nodes.get(key);
    if (!node) {
      node = {
        groupId, edgeNodeId, online: false, metrics: new Map(), aliases: new Map(), devices: new Map(),
        held: new Map(), rebirthAsked: false,
      };
      this.nodes.set(key, node);
    }
    return node;
  }

  private onNodeBirth(node: NodeView, p: SparkplugPayload): void {
    clearTimeout(node.reorderTimer);
    node.reorderTimer = undefined;
    node.held.clear();
    node.rebirthAsked = false;
    node.online = true;
    node.offlineAt = undefined;
    node.lastSeq = p.seq === undefined ? 0 : Number(p.seq);
    node.metrics.clear();
    node.aliases.clear();
    node.devices.clear();
    for (const m of p.metrics) {
      if (m.name === 'bdSeq' && m.value.kind === 'long') node.bdSeq = m.value.value;
      this.record(node, node.metrics, m, undefined);
    }
    this.log(`NBIRTH ${node.groupId}/${node.edgeNodeId}: ${p.metrics.length} metrics, bdSeq ${node.bdSeq}`);
  }

  private onNodeDeath(node: NodeView, p: SparkplugPayload): void {
    const bdSeq = p.metrics.find((m) => m.name === 'bdSeq');
    if (bdSeq && bdSeq.value.kind === 'long' && node.bdSeq !== undefined && bdSeq.value.value !== node.bdSeq) {
      // A death from a session already replaced.
      this.log(`NDEATH ${node.groupId}/${node.edgeNodeId} for bdSeq ${bdSeq.value.value}: not this session, ignored`);
      return;
    }
    const now = Date.now();
    clearTimeout(node.reorderTimer);
    node.reorderTimer = undefined;
    node.held.clear();
    node.online = false;
    node.offlineAt = now;
    markStale(node.metrics, now);
    for (const device of node.devices.values()) {
      device.online = false;
      device.offlineAt = now;
      markStale(device.metrics, now);
    }
    this.log(`NDEATH ${node.groupId}/${node.edgeNodeId}: the node, ${node.devices.size} device(s) and their metrics stale at ${now}`);
  }

  /**
   * In seq order. One ahead of a gap waits, with what follows it, for
   * `reorderTimeoutMs`; the gap filling in time releases them, and not
   * filling asks the node for ONE rebirth.
   */
  private inSequence(node: NodeView, p: SparkplugPayload, type: SparkplugMessageType, handle: () => void): void {
    if (!node.online || node.lastSeq === undefined || p.seq === undefined) {
      // Data from a node whose birth this host never saw.
      if (!node.rebirthAsked) {
        this.log(`${type} from ${node.groupId}/${node.edgeNodeId} with no birth seen: asking for a rebirth`);
        this.rebirth(node.groupId, node.edgeNodeId).catch(() => {});
      }
      return;
    }
    const seq = Number(p.seq);
    const expected = (node.lastSeq + 1) % 256;
    const ahead = (seq - expected + 256) % 256;
    if (ahead === 0) {
      node.lastSeq = seq;
      handle();
      // What was waiting behind it, in order.
      let next = (seq + 1) % 256;
      while (node.held.has(next)) {
        const waiting = node.held.get(next)!;
        node.held.delete(next);
        node.lastSeq = next;
        waiting();
        next = (next + 1) % 256;
      }
      if (node.held.size === 0 && node.reorderTimer) {
        clearTimeout(node.reorderTimer);
        node.reorderTimer = undefined;
        this.log(`${node.groupId}/${node.edgeNodeId}: seq ${seq} arrived late, in time - no rebirth`);
      }
      return;
    }
    if (ahead >= 128) return;   // behind: a repeat
    node.held.set(seq, handle);
    if (node.reorderTimer || node.rebirthAsked) return;
    this.log(`${node.groupId}/${node.edgeNodeId}: seq ${seq} while ${expected} is missing - waiting ${this.o.reorderTimeoutMs} ms`);
    node.reorderTimer = setTimeout(() => {
      node.reorderTimer = undefined;
      node.held.clear();
      this.log(`${node.groupId}/${node.edgeNodeId}: seq ${expected} never came - asking for a rebirth`);
      this.rebirth(node.groupId, node.edgeNodeId).catch(() => {});
    }, this.o.reorderTimeoutMs);
  }

  private apply(node: NodeView, type: SparkplugMessageType, deviceId: string | undefined, p: SparkplugPayload): void {
    if (type === SparkplugMessageType.DBIRTH && deviceId) {
      const device: DeviceView = { online: true, metrics: new Map() };
      node.devices.set(deviceId, device);
      for (const m of p.metrics) this.record(node, device.metrics, m, deviceId);
      this.log(`DBIRTH ${node.groupId}/${node.edgeNodeId}/${deviceId}: ${p.metrics.length} metrics`);
    } else if (type === SparkplugMessageType.DDEATH && deviceId) {
      const device = node.devices.get(deviceId);
      if (!device) return;
      const at = p.timestamp !== undefined ? Number(p.timestamp) : Date.now();
      device.online = false;
      device.offlineAt = at;
      markStale(device.metrics, at);
      this.log(`DDEATH ${node.groupId}/${node.edgeNodeId}/${deviceId}: offline, ${device.metrics.size} metrics stale at ${at} (its timestamp)`);
    } else if (type === SparkplugMessageType.NDATA) {
      for (const m of p.metrics) this.update(node, node.metrics, m);
    } else if (type === SparkplugMessageType.DDATA && deviceId) {
      const device = node.devices.get(deviceId);
      if (device) for (const m of p.metrics) this.update(node, device.metrics, m);
    }
  }

  private record(node: NodeView, into: Map<string, MetricView>, m: SparkplugMetric, deviceId?: string): void {
    if (!m.name) return;
    into.set(m.name, { datatype: m.datatype, value: m.value, stale: false });
    if (m.alias !== undefined) node.aliases.set(m.alias.toString(), { name: m.name, deviceId });
  }

  private update(node: NodeView, into: Map<string, MetricView>, m: SparkplugMetric): void {
    const name = m.name ?? (m.alias !== undefined ? node.aliases.get(m.alias.toString())?.name : undefined);
    const known = name ? into.get(name) : undefined;
    if (known) {
      known.value = m.value;
      known.stale = false;
      known.staleAt = undefined;
    }
  }

  // --- Commands: QoS 0, not retained, a timestamp and no seq ---

  private async command(topic: string, metrics: SparkplugMetric[]): Promise<void> {
    const payload = encodePayload({ timestamp: BigInt(Date.now()), metrics });
    const reply = await this.o.api.publish(this.o.connection, { topic, payload, qos: 0, retain: false });
    this.log(`${topic}: ${metrics.map((m) => m.name).join(', ')} (${reply.outcome})`);
  }

  async rebirth(groupId: string, edgeNodeId: string): Promise<void> {
    const node = this.nodeOf(groupId, edgeNodeId);
    node.rebirthAsked = true;
    await this.command(`spBv1.0/${groupId}/NCMD/${edgeNodeId}`, [
      { name: NODE_CONTROL_REBIRTH, datatype: SparkplugDataType.Boolean, value: { kind: 'bool', value: true } },
    ]);
  }

  async writeNodeMetric(groupId: string, edgeNodeId: string, name: string, datatype: SparkplugDataType,
                        value: SparkplugValue): Promise<void> {
    await this.command(`spBv1.0/${groupId}/NCMD/${edgeNodeId}`, [{ name, datatype, value }]);
  }

  /** Sparkplug has no device rebirth; Device Control/Rebirth is the convention. */
  async deviceRebirth(groupId: string, edgeNodeId: string, deviceId: string): Promise<void> {
    await this.command(`spBv1.0/${groupId}/DCMD/${edgeNodeId}/${deviceId}`, [
      { name: DEVICE_CONTROL_REBIRTH, datatype: SparkplugDataType.Boolean, value: { kind: 'bool', value: true } },
    ]);
  }

  async writeDeviceMetric(groupId: string, edgeNodeId: string, deviceId: string, name: string,
                          datatype: SparkplugDataType, value: SparkplugValue): Promise<void> {
    await this.command(`spBv1.0/${groupId}/DCMD/${edgeNodeId}/${deviceId}`, [{ name, datatype, value }]);
  }
}

function markStale(metrics: Map<string, MetricView>, at: number): void {
  for (const m of metrics.values()) {
    m.stale = true;
    m.staleAt = at;
  }
}
