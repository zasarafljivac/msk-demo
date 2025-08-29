import { Kafka, logLevel, Partitioners } from 'kafkajs';
import type { Producer } from 'kafkajs';
import { generateAuthToken } from 'aws-msk-iam-sasl-signer-js';
import { log } from '../write/helpers';

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

export type Envelope = {
  entity: EntityName;
  op?: 'upsert' | 'delete';
  data?: Record<string, any>;
  key?: string;
  control?: {
    mode: ControlMode;
    version: number;
    updatedAt: string;
    alarm: string;
    rawState: string;
  };
};

let producer: Producer | null = null;
async function getProducer(): Promise<Producer> {
  if (producer) return producer;
  const kafka = new Kafka({
    clientId: 'transform-lambda',
    brokers: bootstrap.split(',').map(s => s.trim()).filter(Boolean),
    ssl: true,
    sasl: {
      mechanism: 'oauthbearer',
      oauthBearerProvider: async () => {
        const { token } = await generateAuthToken({ region });
        return { value: token };
      }
    },
    logLevel: logLevel.NOTHING
  });
  producer = kafka.producer({ 
    allowAutoTopicCreation: false,
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await producer.connect();
  return producer;
}

const nowIsoMs = () => new Date().toISOString();

function inferKey(entity: EntityName, d: Record<string, any>): string | undefined {
  switch (entity) {
    case 'orders_ops': return d.order_id;
    case 'shipments_ops': return d.shipment_id;
    case 'shipment_events_ops': return d.shipment_id;
    case 'invoices_ops': return d.invoice_id;
    case 'invoice_items_ops': return d.invoice_id;
    default: return undefined;
  }
}

function normalize(env: Envelope): Envelope {
  const data = { ...env.data };

  data.updated_ts ??= nowIsoMs();
  data.is_deleted = env.op === 'delete' ? true : !!data.is_deleted;

  if ((env.entity === 'orders_ops' || env.entity === 'invoices_ops') && !data.currency) {
    data.currency = 'USD';
  }

  return {
    entity: env.entity,
    op: env.op,
    data: data,
    key: env.key ?? inferKey(env.entity!, data),
  };
}

function toEnvelope(obj: any): Envelope | null {
  if (obj && obj.entity && obj.op && obj.data) {
    return normalize(obj as Envelope);
  }
  if (obj && obj.order_id) {
    return normalize({
      entity: 'orders_ops',
      op: obj.is_deleted ? 'delete' as const : 'upsert',
      data: obj,
      key: obj.order_id
    });
  }
  return null;
}

export const handler = async (event: any) => {
  producer = await getProducer();
  console.log(JSON.stringify(event));

  const out: { key?: Buffer; value: Buffer }[] = [];
  for (const arr of Object.values(event.records || {})) {
    for (const rec of arr as any[]) {
      const raw = Buffer.from(rec.value, 'base64').toString('utf8');
      try {
        const obj = JSON.parse(raw);
        const env = toEnvelope(obj);
        if (!env) continue;

        const n = normalize(env);
        if (!n.key) continue;

        out.push({
          key: Buffer.from(String(n.key)),
          value: Buffer.from(JSON.stringify(n))
        });
      } catch(err) {
        // ignore malformed records
        log('debug', 'kafka.transform.malformed', err)
      }
    }
  }

  if (out.length) {
    await producer.send({ topic: bufferTopic, messages: out });
  }

  return { statusCode: 200, batchItemFailures: [] as Array<{ itemIdentifier: string }> };
};
