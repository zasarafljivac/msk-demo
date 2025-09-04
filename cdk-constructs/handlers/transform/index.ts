import { metricScope, Unit } from 'aws-embedded-metrics';
import { generateAuthToken } from 'aws-msk-iam-sasl-signer-js';
import type { Producer } from 'kafkajs';
import { Kafka, logLevel, Partitioners } from 'kafkajs';
import { parse as uuidParse, stringify as uuidStringify } from 'uuid';

import { log } from '../write/helpers';

type UuidVariants = string | Buffer | Uint8Array;

const bufferTopic = process.env.BUFFER_TOPIC!;
const bootstrap = process.env.BOOTSTRAP_BROKERS_SASL_IAM!;
const region = process.env.AWS_REGION!;

type EntityName =
  | 'orders_ops'
  | 'shipments_ops'
  | 'shipment_events_ops'
  | 'invoices_ops'
  | 'invoice_items_ops';

export type ControlMode = 'GREEN' | 'YELLOW' | 'RED';

export interface Envelope {
  entity?: EntityName;
  op?: 'upsert' | 'delete';
  data?: Record<string, unknown>;
  control?: {
    mode: ControlMode;
    version: number | string;
    updatedAt: string;
    alarm: string;
    rawState: string;
  };
}

type MutableData = Record<string, unknown> & {
  updated_ts?: string;
  is_deleted?: boolean;
  currency?: string;
  order_id?: UuidVariants;
  shipment_id?: UuidVariants;
  invoice_id?: UuidVariants;
  event_id?: string;
};

type NormalizedEnvelope = Omit<Envelope, 'data'> & {
  data: MutableData;
};

interface KafkaRecordMin {
  value: string;
}
interface KafkaEventMin {
  records?: Record<string, KafkaRecordMin[]>;
}

let producer: Producer | null = null;

async function getProducer(): Promise<Producer> {
  if (producer) {
    return producer;
  }
  const kafka = new Kafka({
    clientId: 'transform-lambda',
    brokers: bootstrap
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
    ssl: true,
    sasl: {
      mechanism: 'oauthbearer',
      oauthBearerProvider: async () => {
        const { token } = await generateAuthToken({ region });
        return { value: token };
      },
    },
    logLevel: logLevel.NOTHING,
  });
  producer = kafka.producer({
    allowAutoTopicCreation: false,
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await producer.connect();
  return producer;
}

const ENTITIES = new Set<EntityName>([
  'orders_ops',
  'shipments_ops',
  'shipment_events_ops',
  'invoices_ops',
  'invoice_items_ops',
]);

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null;

const isEntity = (v: unknown): v is EntityName =>
  typeof v === 'string' && ENTITIES.has(v as EntityName);

const isOp = (v: unknown): v is 'upsert' | 'delete' => v === 'upsert' || v === 'delete';

function asUuidString(v: unknown): string | undefined {
  if (typeof v === 'string') {
    return v;
  }
  if (Buffer.isBuffer(v) || v instanceof Uint8Array) {
    const u8 = Buffer.isBuffer(v) ? new Uint8Array(v) : v;
    if (u8.length === 16) {
      return uuidStringify(u8);
    }
    return Buffer.from(u8).toString('hex');
  }
  return undefined;
}

function sanitizeIds(data: MutableData): void {
  if (data.order_id != null) {
    const s = asUuidString(data.order_id);
    if (s) {
      data.order_id = s;
    }
  }
  if (data.shipment_id != null) {
    const s = asUuidString(data.shipment_id);
    if (s) {
      data.shipment_id = s;
    }
  }
  if (data.invoice_id != null) {
    const s = asUuidString(data.invoice_id);
    if (s) {
      data.invoice_id = s;
    }
  }
}

function normalize(envelope: Envelope): NormalizedEnvelope {
  const data: MutableData = { ...(envelope.data ?? {}) };
  const now = new Date().toISOString();
  data.updated_ts ??= now;
  if (envelope.op) {
    data.is_deleted = envelope.op === 'delete' ? true : !!data.is_deleted;
  }
  if ((envelope.entity === 'orders_ops' || envelope.entity === 'invoices_ops') && !data.currency) {
    data.currency = 'USD';
  }
  sanitizeIds(data);
  return { ...envelope, data };
}

function toEnvelope(obj: unknown): NormalizedEnvelope | null {
  if (!isRecord(obj)) {
    return null;
  }

  if ('control' in obj && isRecord(obj.control)) {
    const control = obj.control as NormalizedEnvelope['control'];
    return { control, data: {} };
  }

  const entity = (obj as { entity?: unknown }).entity;
  const op = (obj as { op?: unknown }).op;
  const data = (obj as { data?: unknown }).data;

  if (isEntity(entity) && isOp(op) && isRecord(data)) {
    return normalize({ entity, op, data });
  }

  if ('order_id' in obj) {
    const isDel = Boolean((obj as { is_deleted?: unknown }).is_deleted);
    return normalize({
      entity: 'orders_ops',
      op: isDel ? 'delete' : 'upsert',
      data: { ...obj },
    });
  }

  return null;
}

function inferPartitionKey(env: NormalizedEnvelope): Buffer | undefined {
  if (env.control) {
    return Buffer.from('db-mode');
  }
  const d = env.data;
  switch (env.entity) {
    case 'orders_ops':
      return toKey(d.order_id);
    case 'shipments_ops':
      return toKey(d.shipment_id);
    case 'shipment_events_ops':
      return toKey(d.shipment_id);
    case 'invoices_ops':
      return toKey(d.invoice_id);
    case 'invoice_items_ops':
      return toKey(d.invoice_id);
    default:
      return undefined;
  }
}
function toKey(input?: unknown): Buffer | undefined {
  if (typeof input === 'string') {
    try {
      return Buffer.from(uuidParse(input));
    } catch {
      return Buffer.from(input, 'utf8');
    }
  }
  if (Buffer.isBuffer(input)) {
    return input;
  }
  if (input instanceof Uint8Array) {
    return Buffer.from(input);
  }
  return undefined;
}

export const handler = metricScope((metrics) => async (event: KafkaEventMin) => {
  const producer = await getProducer();
  const msgs: { key: Buffer; value: Buffer }[] = [];

  metrics.setNamespace('MSKDemo');
  metrics.setDimensions({
    Stage: 'source',
    Topic: process.env.SOURCE_TOPIC ?? 'msk-demo-source',
  });
  const count = Object.values(event.records ?? {}).reduce((n, arr) => n + (arr?.length ?? 0), 0);
  metrics.putMetric('TPS', count, Unit.Count);

  const batches = Object.values(event.records ?? {});
  for (const arr of batches) {
    for (const rec of arr) {
      try {
        const raw = Buffer.from(rec.value, 'base64').toString('utf8');
        const parsed: unknown = JSON.parse(raw);
        const env = toEnvelope(parsed);
        if (!env) {
          log('warn', 'transform.drop.no-env', env);
          continue;
        }

        const norm = normalize(env);
        const key = inferPartitionKey(norm);
        if (!key) {
          log('warn', 'transform.drop.no-key', { entity: norm.entity });
          continue;
        }

        const { ...valueObj } = norm;
        msgs.push({ key, value: Buffer.from(JSON.stringify(valueObj)) });
      } catch (e) {
        log('debug', 'transform.malformed', { err: String(e) });
      }
    }
  }

  if (msgs.length) {
    await producer.send({ topic: bufferTopic, messages: msgs });
  }
  return { statusCode: 200, batchItemFailures: [] as { itemIdentifier: string }[] };
});
