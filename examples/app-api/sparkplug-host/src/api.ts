// The Application API, as this application uses it: the server's managed
// connections (Connections), their messages (Messages.Consume, Publish), and
// nothing else. No MQTT library - the server holds the MQTT connection.

import * as grpc from '@grpc/grpc-js';
import * as loader from '@grpc/proto-loader';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
const PROTO = process.env.MODBEE_PROTO || path.join(here, '../../../../proto/modbee/app/v1/app.proto');

const definition = loader.loadSync(PROTO, { longs: String, enums: String, defaults: true, oneofs: true });
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const v1 = (grpc.loadPackageDefinition(definition) as any).modbee.app.v1;

export interface Message {
  topic: string;
  payload: Uint8Array;
  qos: number;
  retain: boolean;
}

export interface Delivery extends Message {
  id: string;
  connection: string;
}

export interface ConnectionSpec {
  id: string;
  servers: { host: string; port: number }[];
  clientId: string;
  username?: string;
  password?: Uint8Array;
  protocolVersion?: number;
  keepaliveSeconds?: number;
  cleanStart?: boolean;
  sessionExpirySeconds?: number;
  will?: Message;
  reconnect?: 'RECONNECT_AUTOMATIC' | 'RECONNECT_WHEN_ASKED';
}

export interface ConnectionInfo {
  spec: { id: string };
  state: string;
  lastError: string;
}

export interface PublishReply {
  outcome: string;
  reasonCode: number;
}

export class Api {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly connections: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly messages: any;

  constructor(readonly target: string, private readonly token = '') {
    const credentials = grpc.credentials.createInsecure();
    this.connections = new v1.Connections(target, credentials);
    this.messages = new v1.Messages(target, credentials);
  }

  private metadata(): grpc.Metadata {
    const m = new grpc.Metadata();
    if (this.token) m.set('authorization', `Bearer ${this.token}`);
    return m;
  }

  private unary<T>(service: any, method: string, request: object): Promise<T> {
    return new Promise((resolve, reject) => {
      service[method](request, this.metadata(), (error: grpc.ServiceError | null, reply: T) =>
        error ? reject(error) : resolve(reply));
    });
  }

  put(spec: ConnectionSpec): Promise<ConnectionInfo> {
    return this.unary(this.connections, 'Put', spec);
  }

  async remove(id: string): Promise<void> {
    await this.unary(this.connections, 'Delete', { id }).catch((e: grpc.ServiceError) => {
      if (e.code !== grpc.status.NOT_FOUND) throw e;
    });
  }

  reconnect(id: string, will?: Message): Promise<ConnectionInfo> {
    return this.unary(this.connections, 'Reconnect', will ? { id, will } : { id });
  }

  /** Every change of state of `ids`, starting with each one's state now. */
  watch(ids: string[], changed: (info: ConnectionInfo) => void): () => void {
    const call = this.connections.Watch({ ids }, this.metadata());
    call.on('data', changed);
    call.on('error', () => {});
    return () => call.cancel();
  }

  /** On `connection`; answered once the remote broker has acknowledged a QoS 1/2 message. */
  publish(connection: string, message: Message): Promise<PublishReply> {
    return this.unary(this.messages, 'Publish', {
      connection,
      message,
      confirm: message.qos > 0 ? 'CONFIRM_DELIVERED' : 'CONFIRM_ACCEPTED',
    });
  }

  /**
   * A stream of its own (group "") on `connection`: its filters become the
   * connection's subscriptions. Each delivery is acknowledged once `handle`
   * has returned. Resolves once the stream is open - its filters in place.
   */
  consume(connection: string, filters: string[], handle: (d: Delivery) => void): Promise<() => void> {
    const call = this.messages.Consume(this.metadata());
    call.write({ open: { group: '', connection, filters, maxInFlight: 1000 } });
    return new Promise((resolve, reject) => {
      call.on('data', (down: any) => {
        if (down.opened) {
          resolve(() => call.cancel());
          return;
        }
        if (!down.delivery) return;
        const d = down.delivery;
        handle({ id: d.id, topic: d.topic, payload: d.payload, qos: d.qos, retain: d.retain, connection: d.connection });
        call.write({ ack: { ids: [d.id] } });
      });
      call.on('error', (e: grpc.ServiceError) => {
        if (e.code !== grpc.status.CANCELLED) reject(e);
      });
    });
  }
}
