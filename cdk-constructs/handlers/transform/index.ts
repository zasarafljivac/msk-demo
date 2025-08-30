import { Kafka, logLevel, Partitioners } from "kafkajs";
import type { Producer } from "kafkajs";
import { generateAuthToken } from "aws-msk-iam-sasl-signer-js";
import { parse as uuidParse } from "uuid";
import { log } from "../write/helpers";

type UuidVariants = string | Buffer | Uint8Array;

const bufferTopic = process.env.BUFFER_TOPIC!;
const bootstrap = process.env.BOOTSTRAP_BROKERS_SASL_IAM!;
const region = process.env.AWS_REGION!;

type EntityName =
  | "orders_ops"
  | "shipments_ops"
  | "shipment_events_ops"
  | "invoices_ops"
  | "invoice_items_ops";

export type ControlMode = "GREEN" | "YELLOW" | "RED";

export type Envelope = {
  entity: EntityName;
  op?: "upsert" | "delete";
  data?: Record<string, any>;
  key?: string | Uint8Array | Buffer;
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
    clientId: "transform-lambda",
    brokers: bootstrap
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
    ssl: true,
    sasl: {
      mechanism: "oauthbearer",
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

function toKeyBuffer(input?: UuidVariants): Buffer | undefined {
  if (input == null) return undefined;
  if (Buffer.isBuffer(input)) return input;
  if (input instanceof Uint8Array) return Buffer.from(input);
  const s = String(input).trim();
  try {
    return Buffer.from(uuidParse(s));
  } catch {
    return Buffer.from(s, "utf8");
  }
}

function inferPartitionKey(
  entity: EntityName,
  data: Record<string, any>,
): Buffer | undefined {
  switch (entity) {
    case "orders_ops":
      return toKeyBuffer(data.order_id);
    case "shipments_ops":
      return toKeyBuffer(data.shipment_id);
    case "shipment_events_ops":
      return toKeyBuffer(data.shipment_id);
    case "invoices_ops":
      return toKeyBuffer(data.invoice_id);
    case "invoice_items_ops":
      return toKeyBuffer(data.invoice_id);
    default:
      return undefined;
  }
}

function normalize(envelope: Envelope): Envelope {
  const data = { ...envelope.data };
  const now = new Date().toISOString();

  data.updated_ts ??= now;
  data.is_deleted = envelope.op === "delete" ? true : !!data.is_deleted;

  if (
    (envelope.entity === "orders_ops" || envelope.entity === "invoices_ops") &&
    !data.currency
  ) {
    data.currency = "USD";
  }

  const key =
    toKeyBuffer(envelope.key) ?? inferPartitionKey(envelope.entity, data);
  return { ...envelope, data, key };
}

function toEnvelope(obj: any): Envelope | null {
  if (obj && obj.entity && obj.op && obj.data) {
    return normalize(obj as Envelope);
  }
  if (obj && obj.order_id) {
    return normalize({
      entity: "orders_ops",
      op: obj.is_deleted ? ("delete" as const) : "upsert",
      data: obj,
      key: obj.order_id,
    });
  }
  return null;
}

export const handler = async (event: any) => {
  const producer = await getProducer();
  const msgs: { key?: Buffer; value: Buffer }[] = [];

  for (const arr of Object.values(event.records || {})) {
    for (const rec of arr as any[]) {
      try {
        const raw = Buffer.from(rec.value, "base64").toString("utf8");
        const envelope = toEnvelope(JSON.parse(raw));
        if (!envelope) continue;
        const normalized = normalize(envelope);
        const key = normalized.key as Buffer | undefined;
        if (!key) {
          log("warn", "transform.drop.no-key", { entity: normalized.entity });
          continue;
        }
        const { key: _unusedKey, ...valueObj } = normalized;
        msgs.push({
          key,
          value: Buffer.from(JSON.stringify(valueObj)),
        });
      } catch (e) {
        log("debug", "transform.malformed", { err: String(e) });
      }
    }
  }

  if (msgs.length) await producer.send({ topic: bufferTopic, messages: msgs });
  return {
    statusCode: 200,
    batchItemFailures: [] as Array<{ itemIdentifier: string }>,
  };
};
