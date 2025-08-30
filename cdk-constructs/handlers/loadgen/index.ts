import { Kafka, Partitioners } from "kafkajs";
import { generateAuthToken } from "aws-msk-iam-sasl-signer-js";
import type { Producer } from "kafkajs";
import type { Envelope as XEnvelope } from "../transform";
import { ids } from "./ids";

type Envelope = Omit<XEnvelope, "key" | "data"> & {
  key?: Buffer;
  data?: Record<string, any>;
};

type MixKey = "orders" | "shipments" | "events" | "invoices";
type MixWeights = Record<MixKey, number>;

type OrderRef = { order_id: Buffer; partner_id: string };
type ShipmentRef = { shipment_id: Buffer; order_id: Buffer };
type InvoiceRef = { invoice_id: Buffer; order_id: Buffer; partner_id: string };

type LoadgenEvent = {
  topic?: string;
  rate?: number;
  seconds?: number;
  mix?: string; // "orders:0.45,shipments:0.2,events:0.25,invoices:0.1"
  deletePct?: number; // 0..1
};

const BOOTSTRAP = (process.env.BOOTSTRAP_BROKERS_SASL_IAM || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const DEFAULT_TOPIC = process.env.SOURCE_TOPIC || "msk-demo-source";
const REGION = process.env.AWS_REGION || "us-east-1";

let producer: Producer | null = null;
async function getProducer(): Promise<Producer> {
  if (producer) return producer;
  const kafka = new Kafka({
    clientId: "loadgen-lambda",
    brokers: BOOTSTRAP,
    ssl: true,
    sasl: {
      mechanism: "oauthbearer",
      oauthBearerProvider: async () => {
        const { token } = await generateAuthToken({ region: REGION });
        return { value: token };
      },
    },
  });
  producer = kafka.producer({
    allowAutoTopicCreation: false,
    idempotent: false,
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await producer.connect();
  return producer;
}

const rndInt = (min: number, max: number): number =>
  Math.floor(Math.random() * (max - min + 1)) + min;

const rndPick = <T>(arr: T[]): T => arr[rndInt(0, arr.length - 1)];

const nowIsoMs = (): string => new Date().toISOString();

const cap = <T>(arr: T[], n: number = 2000): void => {
  if (arr.length > n) arr.splice(0, arr.length - n);
};

const STATUSES_ORDER = ["NEW", "PACK", "SHIP", "DONE"] as const;
const STATUSES_SHIPMENT = [
  "CREATED",
  "PICKED_UP",
  "IN_TRANSIT",
  "DELIVERED",
  "EXCEPTION",
] as const;
const CARRIERS = ["UPS", "FEDEX", "DHL", "USPS", "GLS", "DPD", "TNT"] as const;
const LOCATIONS = [
  "JFK",
  "LAX",
  "ORD",
  "AMS",
  "FRA",
  "CDG",
  "LHR",
  "MAD",
  "DXB",
  "HKG",
] as const;

const recentOrders: OrderRef[] = [];
const recentShipments: ShipmentRef[] = [];
const recentInvoices: InvoiceRef[] = [];

function parseMix(s?: string): MixWeights {
  const def: MixWeights = {
    orders: 0.45,
    shipments: 0.2,
    events: 0.25,
    invoices: 0.1,
  };
  if (!s) return def;

  const parts = Object.fromEntries(
    s.split(",").map((p: string) => {
      const [k, v] = p.split(":").map((x: string) => x.trim());
      return [k, Math.max(0, Number(v) || 0)];
    }),
  ) as Partial<Record<MixKey, number>>;

  const total =
    (parts.orders ?? 0) +
      (parts.shipments ?? 0) +
      (parts.events ?? 0) +
      (parts.invoices ?? 0) || 1;

  return {
    orders: (parts.orders ?? 0) / total,
    shipments: (parts.shipments ?? 0) / total,
    events: (parts.events ?? 0) / total,
    invoices: (parts.invoices ?? 0) / total,
  };
}

function planBatch(n: number, weights: MixWeights): Record<MixKey, number> {
  const counts: Record<MixKey, number> = {
    orders: 0,
    shipments: 0,
    events: 0,
    invoices: 0,
  };
  for (let i = 0; i < n; i++) {
    const r = Math.random();
    if (r < weights.orders) counts.orders++;
    else if (r < weights.orders + weights.shipments) counts.shipments++;
    else if (r < weights.orders + weights.shipments + weights.events)
      counts.events++;
    else counts.invoices++;
  }
  return counts;
}

function buildOrder(upsert = true): Envelope {
  const order_id_bin = ids.newBinaryId();
  const order_id = ids.binaryIdToString(order_id_bin);
  const partner_id = `p-${rndInt(1, 1000)}`;

  const payload = {
    order_id,
    partner_id,
    status: rndPick([...STATUSES_ORDER]),
    total_amount: rndInt(10, 2000),
    currency: "USD",
    updated_ts: nowIsoMs(),
    produced_ts: nowIsoMs(),
    is_deleted: !upsert,
  };

  if (upsert) {
    recentOrders.push({ order_id: order_id_bin, partner_id });
    cap(recentOrders);
  }
  return {
    entity: "orders_ops",
    op: upsert ? "upsert" : "delete",
    data: payload,
    key: order_id_bin,
  };
}

function buildShipment(upsert = true): Envelope {
  let order = rndPick(recentOrders);
  if (!order) {
    const o = buildOrder(true).data as { order_id: string; partner_id: string };
    const obin = ids.stringToBinaryId(o.order_id);
    order = { order_id: obin, partner_id: o.partner_id };
    recentOrders.push(order);
  }

  const shipment_id_bin = ids.newBinaryId();
  const shipment_id = ids.binaryIdToString(shipment_id_bin);

  const payload = {
    shipment_id,
    order_id: ids.binaryIdToString(order.order_id),
    carrier_code: rndPick([...CARRIERS]),
    status: rndPick([...STATUSES_SHIPMENT]),
    tracking_no: `TRK${rndInt(100000000, 999999999)}`,
    updated_ts: nowIsoMs(),
    produced_ts: nowIsoMs(),
    is_deleted: !upsert,
  };

  if (upsert) {
    recentShipments.push({
      shipment_id: shipment_id_bin,
      order_id: order.order_id,
    });
    cap(recentShipments);
  }
  return {
    entity: "shipments_ops",
    op: upsert ? "upsert" : "delete",
    data: payload,
    key: shipment_id_bin,
  };
}

function buildShipmentEvent(): Envelope {
  let rs = rndPick(recentShipments);
  if (!rs) {
    const newShipment = buildShipment(true);
    const d = newShipment.data as { shipment_id: string; order_id: string };
    rs = {
      shipment_id: ids.stringToBinaryId(d.shipment_id),
      order_id: ids.stringToBinaryId(d.order_id),
    };
    recentShipments.push(rs);
  }

  const event_time = nowIsoMs();
  const event_type = rndPick([...STATUSES_SHIPMENT]);
  const location_code = rndPick([...LOCATIONS]);

  const event_id = ids.makeEventId({
    shipment_id: rs.shipment_id,
    event_type,
    event_time,
    location_code,
  });

  const payload = {
    event_id,
    shipment_id: ids.binaryIdToString(rs.shipment_id),
    event_type,
    status: rndPick(["OK", "WARN", "INFO", "DELAY", "CUSTOMS"]),
    event_time,
    location_code,
    details_json: JSON.stringify({
      note: "auto",
      rnd: Math.random().toFixed(6),
    }),
    updated_ts: nowIsoMs(),
    produced_ts: nowIsoMs(),
    is_deleted: false,
  };

  return {
    entity: "shipment_events_ops",
    op: "upsert",
    data: payload,
    key: rs.shipment_id,
  };
}

function buildInvoice(upsert = true): Envelope {
  let order = rndPick(recentOrders);
  if (!order) {
    const o = buildOrder(true).data as { order_id: string; partner_id: string };
    order = {
      order_id: ids.stringToBinaryId(o.order_id),
      partner_id: o.partner_id,
    };
    recentOrders.push(order);
  }

  const invoice_id_bin = ids.newBinaryId();
  const invoice_id = ids.binaryIdToString(invoice_id_bin);
  const order_id = ids.binaryIdToString(order.order_id);
  const status = rndPick(["ISSUED", "PAID", "VOID"] as const);

  const payload = {
    invoice_id,
    order_id,
    partner_id: order.partner_id,
    status,
    total_amount: 0,
    currency: "USD",
    issued_ts: nowIsoMs(),
    due_ts: status === "PAID" ? nowIsoMs() : null,
    paid_ts: status === "PAID" ? nowIsoMs() : null,
    updated_ts: nowIsoMs(),
    produced_ts: nowIsoMs(),
    is_deleted: !upsert,
  };

  if (upsert) {
    recentInvoices.push({
      invoice_id: invoice_id_bin,
      order_id: order.order_id,
      partner_id: order.partner_id,
    });
    cap(recentInvoices);
  }

  return {
    entity: "invoices_ops",
    op: upsert ? "upsert" : "delete",
    data: payload,
    key: invoice_id_bin,
  };
}

function buildInvoiceItems(
  invoice_id_bin: Buffer,
  lines: number = rndInt(1, 5),
): { msgs: Envelope[]; total: number } {
  const invoice_id = ids.binaryIdToString(invoice_id_bin);
  const msgs: Envelope[] = [];
  let total = 0;

  for (let i = 1; i <= lines; i++) {
    const qty = rndInt(1, 5);
    const unit = rndInt(5, 300);
    const line = qty * unit;
    total += line;

    msgs.push({
      entity: "invoice_items_ops",
      op: "upsert",
      key: invoice_id_bin,
      data: {
        invoice_id,
        line_no: i,
        sku: `sku-${rndInt(100, 999)}`,
        description: `Item ${i}`,
        quantity: qty,
        unit_price: unit,
        line_amount: line,
        updated_ts: nowIsoMs(),
        produced_ts: nowIsoMs(),
        is_deleted: false,
      },
    });
  }

  return { msgs, total };
}

function makeSecondBatch(
  rate: number,
  mix: MixWeights,
  deletePct: number,
): Envelope[] {
  const envelopes: Envelope[] = [];
  const counts = planBatch(rate, mix);

  for (let i = 0; i < counts.orders; i++) {
    const del = Math.random() < deletePct && recentOrders.length > 0;
    const picked = del ? rndPick(recentOrders) : null;

    envelopes.push(
      del
        ? {
            entity: "orders_ops",
            op: "delete",
            key: picked!.order_id,
            data: {
              order_id: picked!.order_id,
              partner_id: picked!.partner_id,
              status: "DONE",
              total_amount: 0,
              currency: "USD",
              updated_ts: nowIsoMs(),
              produced_ts: nowIsoMs(),
              is_deleted: true,
            },
          }
        : buildOrder(true),
    );
  }

  for (let i = 0; i < counts.shipments; i++) {
    const del = Math.random() < deletePct && recentShipments.length > 0;
    const picked = del ? rndPick(recentShipments) : null;
    envelopes.push(
      del
        ? {
            entity: "shipments_ops",
            op: "delete",
            key: picked!.shipment_id,
            data: {
              shipment_id: ids.binaryIdToString(picked!.shipment_id),
              order_id: ids.binaryIdToString(picked!.order_id),
              carrier_code: rndPick([...CARRIERS]),
              status: "EXCEPTION",
              tracking_no: null,
              updated_ts: nowIsoMs(),
              produced_ts: nowIsoMs(),
              is_deleted: true,
            },
          }
        : buildShipment(true),
    );
  }

  for (let i = 0; i < counts.events; i++) {
    envelopes.push(buildShipmentEvent());
  }

  for (let i = 0; i < counts.invoices; i++) {
    const del = Math.random() < deletePct && recentInvoices.length > 0;

    if (del) {
      const inv = rndPick(recentInvoices);
      envelopes.push({
        entity: "invoices_ops",
        op: "delete",
        key: inv.invoice_id,
        data: {
          invoice_id: ids.binaryIdToString(inv.invoice_id),
          order_id: ids.binaryIdToString(inv.order_id),
          partner_id: inv.partner_id,
          status: "VOID",
          total_amount: 0,
          currency: "USD",
          issued_ts: nowIsoMs(),
          due_ts: null,
          paid_ts: null,
          updated_ts: nowIsoMs(),
          produced_ts: nowIsoMs(),
          is_deleted: true,
        },
      });
      const lines = rndInt(1, 3);
      for (let ln = 1; ln <= lines; ln++) {
        envelopes.push({
          entity: "invoice_items_ops",
          op: "delete",
          key: inv.invoice_id,
          data: {
            invoice_id: ids.binaryIdToString(inv.invoice_id),
            line_no: ln,
            sku: `sku-${100 + ln}`,
            description: `Item ${ln}`,
            quantity: 0,
            unit_price: 0,
            line_amount: 0,
            updated_ts: nowIsoMs(),
            produced_ts: nowIsoMs(),
            is_deleted: true,
          },
        });
      }
    } else {
      const header = buildInvoice(true);
      const invoiceIdBin = header.key as Buffer;
      const { msgs: lineMsgs, total } = buildInvoiceItems(invoiceIdBin);
      (header.data as { total_amount: number }).total_amount = total;
      envelopes.push(header, ...lineMsgs);
    }
  }

  return envelopes;
}

export async function handler(
  event: LoadgenEvent = {} as LoadgenEvent,
): Promise<{
  topic: string;
  rate: number;
  seconds: number;
  sent: number;
  durationMs: number;
}> {
  const topic = event.topic || DEFAULT_TOPIC;
  const rate = Number.isFinite(event.rate as number) ? Number(event.rate) : 50;
  const seconds = Number.isFinite(event.seconds as number)
    ? Number(event.seconds)
    : 30;
  const mix = parseMix(event.mix || process.env.MIX);
  const deletePct =
    (event.deletePct ?? Number(process.env.DELETE_PCT) ?? 0.02) * 1;

  if (!BOOTSTRAP.length) throw new Error("Missing BOOTSTRAP_BROKERS_SASL_IAM");

  const prod = await getProducer();
  const started = Date.now();
  let sent = 0;

  for (let sec = 0; sec < seconds; sec++) {
    const envelopes = makeSecondBatch(rate, mix, deletePct);
    if (envelopes.length) {
      await prod.send({
        topic,
        messages: envelopes.map(({ key, ...rest }) => ({
          key,
          value: Buffer.from(JSON.stringify(rest)),
        })),
      });
      sent += envelopes.length;
    }
    await new Promise<void>((r) => setTimeout(r, 1000));
  }

  return { topic, rate, seconds, sent, durationMs: Date.now() - started };
}
