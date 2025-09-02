import type * as mysql from 'mysql2/promise';
import { parse as uuidParse } from 'uuid';

import { log } from './helpers';
import { SQL } from './sql';

export interface IdempotentInsertResult {
  response: mysql.ResultSetHeader;
  inserted: number;
  entity: string;
}

type UuidInput = Buffer | Uint8Array | string;

export const asBin16 = (input: unknown): Buffer => {
  if (Buffer.isBuffer(input)) {
    return input.length === 16 ? input : Buffer.from(input);
  }
  if (input instanceof Uint8Array) {
    return Buffer.from(input);
  }
  if (typeof input === 'string') {
    return Buffer.from(uuidParse(input));
  }
  throw new Error('asBin16: unsupported id type');
};

type EntityName =
  | 'orders_ops'
  | 'shipments_ops'
  | 'shipment_events_ops'
  | 'invoices_ops'
  | 'invoice_items_ops';

interface OrdersRow {
  order_id: UuidInput;
  partner_id: string;
  status: string;
  total_amount?: number;
  currency?: string;
  is_deleted?: boolean;
}

interface ShipmentsRow {
  shipment_id: UuidInput;
  order_id: UuidInput;
  carrier_code: string;
  status: string;
  tracking_no?: string | null;
  is_deleted?: boolean;
}

interface ShipmentEventsRow {
  event_id: string;
  shipment_id: UuidInput;
  event_type: string;
  status?: string | null;
  event_time: string;
  location_code?: string | null;
  details_json?: unknown;
  is_deleted?: boolean;
}

interface InvoicesRow {
  invoice_id: UuidInput;
  order_id: UuidInput;
  partner_id: string;
  status: string;
  total_amount?: number;
  currency?: string;
  issued_ts?: string | null;
  due_ts?: string | null;
  paid_ts?: string | null;
  is_deleted?: boolean;
}

interface InvoiceItemsRow {
  invoice_id: UuidInput;
  line_no: number;
  sku: string;
  description?: string | null;
  quantity?: number;
  unit_price?: number;
  line_amount?: number;
  is_deleted?: boolean;
}

type UpsertHandler = (
  pool: mysql.Pool,
  row: unknown,
  effectiveTs: string,
) => Promise<IdempotentInsertResult>;
type DeleteHandler = UpsertHandler;

export const stopTime = () => {
  const s = Date.now();
  return () => Date.now() - s;
};

function getInfoString(x: unknown): string {
  if (x && typeof x === 'object' && 'info' in x) {
    const v = (x as Record<string, unknown>).info;
    if (typeof v === 'string') {
      return v;
    }
  }
  return '';
}

export function insertedFromUpsert(rows: mysql.ResultSetHeader): number {
  const info = getInfoString(rows);
  // Multi-row upsert: "Records: X  Duplicates: Y  Warnings: Z"
  const m = /Records:\s+(\d+)\s+Duplicates:\s+(\d+)/.exec(info);
  if (m) {
    const records = Number(m[1]);
    const duplicates = Number(m[2]);
    return Math.max(0, records - duplicates);
  }
  switch (rows.affectedRows) {
    case 0:
      return 0;
    case 1:
      return 1;
    case 2:
      return 0;
    default:
      return 0;
  }
}

async function exec(
  pool: mysql.Pool,
  label: string,
  sql: string,
  params: unknown[],
): Promise<IdempotentInsertResult> {
  const stop = stopTime();
  const [response] = await pool.query<mysql.ResultSetHeader>(sql, params);
  const inserted = insertedFromUpsert(response);
  const ms = stop();
  if (ms > 1000) {
    log('warn', 'sql.exec.slow', { label, ms });
  } else {
    log('debug', 'sql.exec.ok', { label, ms });
  }
  return { response, inserted, entity: label.split('.')[0] };
}

async function softDelete(
  pool: mysql.Pool,
  table: EntityName,
  whereClause: string,
  params: unknown[],
  updated_ts: string,
) {
  const sql = `UPDATE ${table}
    SET is_deleted=1, updated_ts=GREATEST(updated_ts, ?)
    WHERE ${whereClause}`;
  return exec(pool, `${table}.softDelete`, sql, [updated_ts, ...params]);
}

export const ENTITY_HANDLERS: Record<EntityName, { upsert: UpsertHandler; remove: DeleteHandler }> =
  {
    orders_ops: {
      remove: (pool, row, effectiveTs) => {
        const r = row as OrdersRow;
        return softDelete(pool, 'orders_ops', 'order_id=?', [asBin16(r.order_id)], effectiveTs);
      },
      upsert: (pool, row, effectiveTs) => {
        const r = row as OrdersRow;
        return exec(pool, 'orders_upsert', SQL.orders_upsert, [
          asBin16(r.order_id),
          r.partner_id,
          r.status,
          r.total_amount ?? 0,
          r.currency ?? 'USD',
          effectiveTs,
          !!r.is_deleted,
        ]);
      },
    },

    shipments_ops: {
      remove: (pool, row, effectiveTs) => {
        const r = row as ShipmentsRow;
        return softDelete(
          pool,
          'shipments_ops',
          'shipment_id=?',
          [asBin16(r.shipment_id)],
          effectiveTs,
        );
      },
      upsert: (pool, row, effectiveTs) => {
        const r = row as ShipmentsRow;
        return exec(pool, 'shipments_upsert', SQL.shipments_upsert, [
          asBin16(r.shipment_id),
          asBin16(r.order_id),
          r.carrier_code,
          r.status,
          r.tracking_no ?? null,
          effectiveTs,
          !!r.is_deleted,
        ]);
      },
    },

    shipment_events_ops: {
      remove: (pool, row, effectiveTs) => {
        const r = row as ShipmentEventsRow;
        return softDelete(pool, 'shipment_events_ops', 'event_id=?', [r.event_id], effectiveTs);
      },
      upsert: (pool, row, effectiveTs) => {
        const r = row as ShipmentEventsRow;
        const detailsJson =
          typeof r.details_json === 'string'
            ? r.details_json
            : JSON.stringify(r.details_json ?? null);
        return exec(pool, 'shipment_events_upsert', SQL.shipment_events_upsert, [
          r.event_id,
          asBin16(r.shipment_id),
          r.event_type,
          r.status ?? null,
          r.event_time,
          r.location_code ?? null,
          detailsJson,
          effectiveTs,
          !!r.is_deleted,
        ]);
      },
    },

    invoices_ops: {
      remove: (pool, row, effectiveTs) => {
        const r = row as InvoicesRow;
        return softDelete(
          pool,
          'invoices_ops',
          'invoice_id=?',
          [asBin16(r.invoice_id)],
          effectiveTs,
        );
      },
      upsert: (pool, row, effectiveTs) => {
        const r = row as InvoicesRow;
        return exec(pool, 'invoices_upsert', SQL.invoices_upsert, [
          asBin16(r.invoice_id),
          asBin16(r.order_id),
          r.partner_id,
          r.status,
          r.total_amount ?? 0,
          r.currency ?? 'USD',
          r.issued_ts ?? null,
          r.due_ts ?? null,
          r.paid_ts ?? null,
          effectiveTs,
          !!r.is_deleted,
        ]);
      },
    },

    invoice_items_ops: {
      remove: (pool, row, effectiveTs) => {
        const r = row as InvoiceItemsRow;
        return softDelete(
          pool,
          'invoice_items_ops',
          'invoice_id=? AND line_no=?',
          [asBin16(r.invoice_id), r.line_no],
          effectiveTs,
        );
      },
      upsert: (pool, row, effectiveTs) => {
        const r = row as InvoiceItemsRow;
        return exec(pool, 'invoice_items_upsert', SQL.invoice_items_upsert, [
          asBin16(r.invoice_id),
          r.line_no,
          r.sku,
          r.description ?? null,
          r.quantity ?? 0,
          r.unit_price ?? 0,
          r.line_amount ?? 0,
          effectiveTs,
          !!r.is_deleted,
        ]);
      },
    },
  };
