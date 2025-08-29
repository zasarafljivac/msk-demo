import mysql from 'mysql2/promise';
import { SQL } from './sql';
import pMap from 'p-map';
import pRetry from 'p-retry';
import { ControlMode, Envelope } from '../transform';

export type EntityName =
  | 'orders_ops'
  | 'shipments_ops'
  | 'shipment_events_ops'
  | 'invoices_ops'
  | 'invoice_items_ops';

export type CachedSecret = { 
  dbClusterIdentifier: string;
  password: string 
  dbname: string 
  engine: string 
  port: number 
  host: string 
  username: string 
} | null;

export type EnvType = {
  SAFETY_MS: number, 
  CHUNK_CONCURRENCY: number, 
  CHUNK_SIZE: number, 
  CHUNK_PARALLELISM: number, 
  LOG_LEVEL: string,
  RDS_PROXY_ENDPOINT: string,
  TOKEN: string,
  PARAM_NAME: string,
  DEFAULT_MODE: ControlMode,
};

type ProcessResult = {
  ok: number;
  failed: number;
  retried: number;
  errors: Array<{ index: number; entity: string; reason: string }>;
};

let SAFETY_MS = 0;
let CHUNK_CONCURRENCY = 0; 
let CHUNK_PARALLELISM = 0;
let LOG_LEVEL = 'info';
let RDS_PROXY_ENDPOINT = '';
let TOKEN = '';
let PARAM_NAME = '/msk-demo/db-mode';
let DEFAULT_MODE: ControlMode = 'GREEN';


const DEFAULTS = {
  maxRetries: 3,
  exponentialFactor: 2, 
  backoffMs: 200,
  maxBackoffMs: 5000,
};

export const setVars = (input: EnvType) => {
  SAFETY_MS = input.SAFETY_MS;
  CHUNK_CONCURRENCY = input.CHUNK_CONCURRENCY;
  CHUNK_PARALLELISM = input.CHUNK_PARALLELISM;
  LOG_LEVEL = input.LOG_LEVEL;
  RDS_PROXY_ENDPOINT = input.RDS_PROXY_ENDPOINT;
  TOKEN = input.TOKEN;
  PARAM_NAME = input.PARAM_NAME;
  DEFAULT_MODE = input.DEFAULT_MODE;
}

const nowIsoMs = () => new Date().toISOString();
const stopTime = () => { const s = Date.now(); return () => Date.now() - s; };

const SecretId = process.env.DB_SECRET_ARN!;
let cachedSecret: CachedSecret = null;


const levels: Record<string, number> = { error: 0, warn: 1, info: 2, debug: 3, trace: 4 };
const enabled = (lvl: keyof typeof levels) => levels[lvl] <= (levels[LOG_LEVEL] ?? 2);

export const log = (lvl: keyof typeof levels, msg: string, extra?: any) => {
  if (!enabled(lvl)) return;
  const line = `[${new Date().toISOString()}] ${lvl.toUpperCase()} ${msg}`;
  extra ? console.log(line, extra) : console.log(line);
};

type UpsertHandler = (pool: mysql.Pool, row: any, effectiveTs: string) => Promise<unknown>;
type DeleteHandler = UpsertHandler;

const ENTITY_HANDLERS: Record<EntityName, { upsert: UpsertHandler; remove: DeleteHandler }> = {
  orders_ops: {
    remove: (pool, row, effectiveTs) =>
      softDelete(pool, 'orders_ops', 'order_id=?', [row.order_id], effectiveTs),
    upsert: (pool, row, effectiveTs) =>
      exec(pool, 'orders_upsert', SQL.orders_upsert, [
        row.order_id, row.partner_id, row.status, row.total_amount ?? 0, row.currency || 'USD',
        effectiveTs, !!row.is_deleted,
      ]),
  },
  shipments_ops: {
    remove: (pool, row, effectiveTs) =>
      softDelete(pool, 'shipments_ops', 'shipment_id=?', [row.shipment_id], effectiveTs),
    upsert: (pool, row, effectiveTs) =>
      exec(pool, 'shipments_upsert', SQL.shipments_upsert, [
        row.shipment_id, row.order_id, row.carrier_code, row.status, row.tracking_no ?? null,
        effectiveTs, !!row.is_deleted,
      ]),
  },
  shipment_events_ops: {
    remove: (pool, row, effectiveTs) =>
      softDelete(pool, 'shipment_events_ops', 'event_id=?', [row.event_id], effectiveTs),
    upsert: (pool, row, effectiveTs) => {
      const detailsJson = typeof row.details_json === 'string'
        ? row.details_json
        : JSON.stringify(row.details_json ?? null);
      return exec(pool, 'shipment_events_upsert', SQL.shipment_events_upsert, [
        row.event_id, row.shipment_id, row.event_type, row.status ?? null, row.event_time,
        row.location_code ?? null, detailsJson, effectiveTs, !!row.is_deleted,
      ]);
    },
  },
  invoices_ops: {
    remove: (pool, row, effectiveTs) =>
      softDelete(pool, 'invoices_ops', 'invoice_id=?', [row.invoice_id], effectiveTs),
    upsert: (pool, row, effectiveTs) =>
      exec(pool, 'invoices_upsert', SQL.invoices_upsert, [
        row.invoice_id, row.order_id, row.partner_id, row.status, row.total_amount ?? 0,
        row.currency || 'USD', row.issued_ts ?? null, row.due_ts ?? null, row.paid_ts ?? null,
        effectiveTs, !!row.is_deleted,
      ]),
  },
  invoice_items_ops: {
    remove: (pool, row, effectiveTs) =>
      softDelete(pool, 'invoice_items_ops', 'invoice_id=? AND line_no=?', [row.invoice_id, row.line_no], effectiveTs),
    upsert: (pool, row, effectiveTs) =>
      exec(pool, 'invoice_items_upsert', SQL.invoice_items_upsert, [
        row.invoice_id, row.line_no, row.sku, row.description ?? null, row.quantity ?? 0,
        row.unit_price ?? 0, row.line_amount ?? 0, effectiveTs, !!row.is_deleted,
      ]),
  },
};

export async function exec(pool: mysql.Pool, label: string, sql: string, params: any[]) {
  const stop = stopTime();
  const [res] = await pool.query(sql, params);
  const ms = stop();
  if (ms > 1000) log('warn', 'sql.exec.slow', { label, ms });
  else log('debug', 'sql.exec.ok', { label, ms });
  return res;
}

export async function softDelete(
  pool: mysql.Pool,
  table: EntityName,
  whereClause: string,
  params: any[],
  updated_ts: string
): Promise<unknown> {
  const sql = `UPDATE ${table} SET is_deleted=1, updated_ts=GREATEST(updated_ts, ?) WHERE ${whereClause}`;
  return exec(pool, `${table}.softDelete`, sql, [updated_ts, ...params]);
}

export function toEnvelope(obj: any): Envelope | null {
  if (obj && obj.entity && obj.op && obj.data) return obj as Envelope;
  if (obj && obj.order_id) {
    return { entity: 'orders_ops', op: obj.is_deleted ? 'delete' : 'upsert', data: obj, key: obj.order_id };
  }
  return null;
}

export function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export async function getPool(existingPool: mysql.Pool | null): Promise<mysql.Pool> {
  if (existingPool) return existingPool;

  let stop = stopTime();
  log('info', 'secrets.get.start');
  cachedSecret = await getDbSecret(SecretId);
  log('info', 'secrets.get.ok', { ms: stop() });

  const connectionLimit =
    Math.min(32, Math.max(2, CHUNK_PARALLELISM * Math.min(Math.max(CHUNK_CONCURRENCY, 1), 4)));

  stop = stopTime();
  const pool = mysql.createPool({
    host: RDS_PROXY_ENDPOINT,
    user: cachedSecret!.username,
    password: cachedSecret!.password,
    database: 'ops',
    waitForConnections: true,
    connectionLimit: 2, // per lambda instance which makes it 5x2 = 10
    queueLimit: 0,
    enableKeepAlive: true,
    ssl: { rejectUnauthorized: true, minVersion: 'TLSv1.2' },
    connectTimeout: 5000,
    connectAttributes: {
      program_name: 'writer-lambda',
      component: 'writer',
    },
  });
  log('info', 'pool.create.ok', { ms: stop(), connectionLimit });

  stop = stopTime();
  const connection = await pool.getConnection();
  log('info', 'pool.getConnection.ok', { ms: stop() });
  stop = stopTime();
  await connection.ping();
  log('info', 'db.ping.ok', { ms: stop() });
  connection.release();

  return pool;
}

async function processOne(pool: mysql.Pool, envelope: Envelope) {
  const row = envelope.data ?? {};
  const effectiveTs = typeof row.updated_ts === 'string' ? row.updated_ts : nowIsoMs();

  const handler = ENTITY_HANDLERS[envelope.entity];
  if (!handler) {
    log('warn', 'record.unknown', { entity: envelope.entity });
    return;
  }
  return envelope.op === 'delete'
    ? handler.remove(pool, row, effectiveTs)
    : handler.upsert(pool, row, effectiveTs);
}

export async function processChunk(
  pool: mysql.Pool,
  items: Envelope[],
  config = DEFAULTS
): Promise<ProcessResult> {
  const result: ProcessResult = { ok: 0, failed: 0, retried: 0, errors: [] };

  const retryOptions = {
    retries: config.maxRetries,
    factor: config.exponentialFactor,
    minTimeout: config.backoffMs,
    maxTimeout: config.maxBackoffMs,
    randomize: true,
    shouldRetry: ({ error }: any) => isTransient(error),
    onFailedAttempt: () => { result.retried++; },
  } as const;

  await pMap(
    items,
    async (item, index) => {
      try {
        await pRetry(() => processOne(pool, item), retryOptions);
        result.ok++;
      } catch (err: any) {
        result.failed++;
        await sendToDLQ({ item, error: serializeErr(err) });
        result.errors.push({ index, entity: String((item as any).entity), reason: shortErr(err) });
      }
    },
    { concurrency: CHUNK_CONCURRENCY }
  );

  return result;
}

const isTransient = (err: any): boolean => {
  const code = err?.code ?? err?.errno ?? err?.sqlState;
  const msg = (err?.message ?? '').toLowerCase();
  return (
    code === 'ER_LOCK_DEADLOCK' ||
    code === 1213 ||               
    code === 1205 ||
    code === 'PROTOCOL_CONNECTION_LOST' ||
    code === 'ECONNRESET' ||
    msg.includes('deadlock') ||
    msg.includes('lock wait timeout') ||
    msg.includes('timeout exceeded') ||
    msg.includes('connection lost')
  );
}

const shortErr = (e: any) => `${e?.code ?? ''} ${e?.message ?? e}`;

const serializeErr = (e: any) => ({
  code: e?.code,
  errno: e?.errno,
  sqlState: e?.sqlState,
  message: e?.message,
  stack: e?.stack,
});

async function sendToDLQ(payload: any) {
  // TODO: need to decide on destination
  log('warn', 'dlq.sent', { payload });
}

export async function getDbSecret(SecretId: string) {
  if (cachedSecret) return cachedSecret;
  const url = `http://localhost:2773/secretsmanager/get?secretId=${encodeURIComponent(SecretId)}&versionStage=AWSCURRENT`;
  const res = await fetch(url, { headers: { 'X-Aws-Parameters-Secrets-Token': TOKEN } });
  if (!res.ok) throw new Error(`Extension error ${res.status} ${await res.text()}`);
  const body = await res.json() as any;
  const value = body.SecretString
    ? JSON.parse(body.SecretString)
    : JSON.parse(Buffer.from(body.SecretBinary, 'base64').toString('utf8'));
  cachedSecret = value; 
  return value;
}

export async function getParam(name: string): Promise<string | null> {
  const url = `http://localhost:2773/systemsmanager/parameters/get?name=${encodeURIComponent(name)}&withDecryption=false`;
  const res = await fetch(url, { headers: { 'X-Aws-Parameters-Secrets-Token': TOKEN } });
  if (!res.ok) throw new Error(`Extension error ${res.status} ${await res.text()}`);
  const body = await res.json() as any;
  return body?.Parameter?.Value ?? null;
}

export async function seedModeFromSsm(): Promise<ControlMode> {
  const value = await getParam(PARAM_NAME);
  log('info', 'write.param.value', value);
  const mode: ControlMode =
    value === 'RED' || value === 'YELLOW' || value === 'GREEN'
      ? (value as ControlMode)
      : (DEFAULT_MODE as ControlMode);
  return mode;
}
