import * as mysql from "mysql2/promise";
import { log } from "./helpers";
import { SQL } from "./sql";
import { parse as uuidParse } from "uuid";

export const asBin16 = (input: unknown): Buffer => {
  if (Buffer.isBuffer(input))
    return input.length === 16 ? input : Buffer.from(input);
  if (input instanceof Uint8Array) return Buffer.from(input);
  if (typeof input === "string") return Buffer.from(uuidParse(input));
  throw new Error("asBin16: unsupported id type");
};

type EntityName =
  | "orders_ops"
  | "shipments_ops"
  | "shipment_events_ops"
  | "invoices_ops"
  | "invoice_items_ops";

type UpsertHandler = (
  pool: mysql.Pool,
  row: any,
  effectiveTs: string,
) => Promise<unknown>;
type DeleteHandler = UpsertHandler;

export const stopTime = () => {
  const s = Date.now();
  return () => Date.now() - s;
};

async function exec(
  pool: mysql.Pool,
  label: string,
  sql: string,
  params: any[],
) {
  const stop = stopTime();
  const [res] = await pool.query(sql, params);
  const ms = stop();
  if (ms > 1000) log("warn", "sql.exec.slow", { label, ms });
  else log("debug", "sql.exec.ok", { label, ms });
  return res;
}

async function softDelete(
  pool: mysql.Pool,
  table: EntityName,
  whereClause: string,
  params: any[],
  updated_ts: string,
) {
  const sql = `UPDATE ${table}
               SET is_deleted=1, updated_ts=GREATEST(updated_ts, ?)
               WHERE ${whereClause}`;
  return exec(pool, `${table}.softDelete`, sql, [updated_ts, ...params]);
}

export const ENTITY_HANDLERS: Record<
  EntityName,
  { upsert: UpsertHandler; remove: DeleteHandler }
> = {
  orders_ops: {
    remove: (pool, row, effectiveTs) =>
      softDelete(
        pool,
        "orders_ops",
        "order_id=?",
        [asBin16(row.order_id)],
        effectiveTs,
      ),
    upsert: (pool, row, effectiveTs) =>
      exec(pool, "orders_upsert", SQL.orders_upsert, [
        asBin16(row.order_id),
        row.partner_id,
        row.status,
        row.total_amount ?? 0,
        row.currency || "USD",
        effectiveTs,
        !!row.is_deleted,
      ]),
  },

  shipments_ops: {
    remove: (pool, row, effectiveTs) =>
      softDelete(
        pool,
        "shipments_ops",
        "shipment_id=?",
        [asBin16(row.shipment_id)],
        effectiveTs,
      ),
    upsert: (pool, row, effectiveTs) =>
      exec(pool, "shipments_upsert", SQL.shipments_upsert, [
        asBin16(row.shipment_id),
        asBin16(row.order_id),
        row.carrier_code,
        row.status,
        row.tracking_no ?? null,
        effectiveTs,
        !!row.is_deleted,
      ]),
  },

  shipment_events_ops: {
    remove: (pool, row, effectiveTs) =>
      softDelete(
        pool,
        "shipment_events_ops",
        "event_id=?",
        [row.event_id],
        effectiveTs,
      ),
    upsert: (pool, row, effectiveTs) => {
      const detailsJson =
        typeof row.details_json === "string"
          ? row.details_json
          : JSON.stringify(row.details_json ?? null);
      return exec(pool, "shipment_events_upsert", SQL.shipment_events_upsert, [
        row.event_id,
        asBin16(row.shipment_id),
        row.event_type,
        row.status ?? null,
        row.event_time,
        row.location_code ?? null,
        detailsJson,
        effectiveTs,
        !!row.is_deleted,
      ]);
    },
  },

  invoices_ops: {
    remove: (pool, row, effectiveTs) =>
      softDelete(
        pool,
        "invoices_ops",
        "invoice_id=?",
        [asBin16(row.invoice_id)],
        effectiveTs,
      ),
    upsert: (pool, row, effectiveTs) =>
      exec(pool, "invoices_upsert", SQL.invoices_upsert, [
        asBin16(row.invoice_id),
        asBin16(row.order_id),
        row.partner_id,
        row.status,
        row.total_amount ?? 0,
        row.currency || "USD",
        row.issued_ts ?? null,
        row.due_ts ?? null,
        row.paid_ts ?? null,
        effectiveTs,
        !!row.is_deleted,
      ]),
  },

  invoice_items_ops: {
    remove: (pool, row, effectiveTs) =>
      softDelete(
        pool,
        "invoice_items_ops",
        "invoice_id=? AND line_no=?",
        [asBin16(row.invoice_id), row.line_no],
        effectiveTs,
      ),

    upsert: (pool, row, effectiveTs) =>
      exec(pool, "invoice_items_upsert", SQL.invoice_items_upsert, [
        asBin16(row.invoice_id),
        row.line_no,
        row.sku,
        row.description ?? null,
        row.quantity ?? 0,
        row.unit_price ?? 0,
        row.line_amount ?? 0,
        effectiveTs,
        !!row.is_deleted,
      ]),
  },
};
