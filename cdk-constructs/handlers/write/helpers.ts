import mysql from 'mysql2/promise';
import pMap from 'p-map';
import pRetry from 'p-retry';

import { ENTITY_HANDLERS, stopTime } from './sql-upsert-handlers';
import { bumpRecentActivity, writeDlqSent } from './ws';

type ControlMode = 'GREEN' | 'YELLOW' | 'RED';

interface DlqPayload {
  entity?: string;
  error: unknown;
}

type EntityName =
  | 'orders_ops'
  | 'shipments_ops'
  | 'shipment_events_ops'
  | 'invoices_ops'
  | 'invoice_items_ops';

interface Envelope {
  entity: EntityName;
  op?: 'upsert' | 'delete';
  data?: Record<string, unknown>;
  control?: {
    mode: ControlMode;
    version: number;
    updatedAt: string;
    alarm: string;
    rawState: string;
  };
}

interface SecretsGetResponse {
  SecretString?: string;
  SecretBinary?: string;
}

interface RawSecret {
  dbClusterIdentifier?: unknown;
  password?: unknown;
  dbname?: unknown;
  engine?: unknown;
  port?: unknown;
  host?: unknown;
  username?: unknown;
}

export type CachedSecret = {
  dbClusterIdentifier: string;
  password: string;
  dbname: string;
  engine: string;
  port: number;
  host: string;
  username: string;
} | null;

export interface EnvType {
  CHUNK_CONCURRENCY: number;
  CHUNK_SIZE: number;
  CHUNK_PARALLELISM: number;
  LOG_LEVEL: string;
  RDS_PROXY_ENDPOINT: string;
  TOKEN: string;
  PARAM_NAME: string;
  DEFAULT_MODE: ControlMode;
}

interface ProcessResult {
  ok: number;
  failed: number;
  retried: number;
  errors: { index: number; entity: string; reason: string }[];
}

interface SqlError {
  code?: string | number;
  errno?: number;
  sqlState?: string;
  message?: string;
  stack?: string;
}

interface SsmParamResponse {
  Parameter?: { Value?: unknown };
}

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
  CHUNK_CONCURRENCY = input.CHUNK_CONCURRENCY;
  CHUNK_PARALLELISM = input.CHUNK_PARALLELISM;
  LOG_LEVEL = input.LOG_LEVEL;
  RDS_PROXY_ENDPOINT = input.RDS_PROXY_ENDPOINT;
  TOKEN = input.TOKEN;
  PARAM_NAME = input.PARAM_NAME;
  DEFAULT_MODE = input.DEFAULT_MODE;
};

export function isoMs(input?: string | number | Date | null): string {
  if (!input) {
    return new Date().toISOString();
  }
  if (typeof input === 'string') {
    const s = input.trim();
    if (s === '') {
      return new Date().toISOString();
    }
    if (/^\d+$/.test(s)) {
      return new Date(Number(s)).toISOString();
    }
  }
  return new Date(input).toISOString();
}

const SecretId = process.env.DB_SECRET_ARN ?? '';
let cachedSecret: CachedSecret = null;

const levels: Record<string, number> = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 3,
  trace: 4,
};
const enabled = (lvl: keyof typeof levels) => levels[lvl] <= (levels[LOG_LEVEL] ?? 2);

export const log = (lvl: keyof typeof levels, msg: string, extra?: unknown) => {
  if (!enabled(lvl)) {
    return;
  }
  const line = `[${isoMs()}] ${String(lvl).toUpperCase()} ${msg}`;
  if (extra !== undefined) {
    console.log(line, extra);
  } else {
    console.log(line);
  }
};

const isRecord = (input: unknown): input is Record<string, unknown> =>
  typeof input === 'object' && input !== null;

export function toEnvelope(obj: unknown): Envelope | null {
  if (!isRecord(obj)) {
    return null;
  }

  if ('control' in obj && isRecord(obj.control)) {
    const c = (obj as { control: Envelope['control'] }).control;
    return { entity: 'orders_ops', op: 'upsert', data: {}, control: c };
  }

  if ('entity' in obj && 'data' in obj) {
    const { entity, op, data } = obj as { entity: unknown; op?: unknown; data?: unknown };
    if (
      typeof entity === 'string' &&
      [
        'orders_ops',
        'shipments_ops',
        'shipment_events_ops',
        'invoices_ops',
        'invoice_items_ops',
      ].includes(entity) &&
      isRecord(data)
    ) {
      const isDel = Boolean(data.is_deleted);
      const finalOp: 'upsert' | 'delete' =
        op === 'delete' || (op !== 'upsert' && isDel) ? 'delete' : 'upsert';
      return { entity: entity as EntityName, op: finalOp, data: data };
    }
  }

  if ('order_id' in obj) {
    const data = obj;
    const isDel = Boolean((data as { is_deleted?: unknown }).is_deleted);
    return { entity: 'orders_ops', op: isDel ? 'delete' : 'upsert', data };
  }

  return null;
}

export function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

export async function getPool(existingPool: mysql.Pool | null): Promise<mysql.Pool> {
  if (existingPool) {
    return existingPool;
  }

  let stop = stopTime();
  log('info', 'secrets.get.start');
  cachedSecret = await getDbSecret(SecretId);
  log('info', 'secrets.get.ok', { ms: stop() });

  const connectionLimit = Math.min(
    32,
    Math.max(2, CHUNK_PARALLELISM * Math.min(Math.max(CHUNK_CONCURRENCY, 1), 4)),
  );

  stop = stopTime();
  const pool = mysql.createPool({
    host: RDS_PROXY_ENDPOINT,
    user: cachedSecret?.username ?? '',
    password: cachedSecret?.password ?? '',
    database: 'ops',
    waitForConnections: true,
    connectionLimit: 2,
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
  const updated = isRecord(row) ? row.updated_ts : undefined;
  const effectiveTs =
    typeof updated === 'string' || updated instanceof Date ? isoMs(updated) : isoMs();

  const handler = ENTITY_HANDLERS[envelope.entity];
  if (!handler) {
    log('warn', 'record.unknown', { entity: envelope.entity });
    return;
  }

  if (envelope.op === 'delete') {
    return handler.remove(pool, row, effectiveTs);
  }
  return handler.upsert(pool, row, effectiveTs);
}

export async function processChunk(
  pool: mysql.Pool,
  items: Envelope[],
  config = DEFAULTS,
): Promise<ProcessResult> {
  const result: ProcessResult = { ok: 0, failed: 0, retried: 0, errors: [] };

  const retryOptions = {
    retries: config.maxRetries,
    factor: config.exponentialFactor,
    minTimeout: config.backoffMs,
    maxTimeout: config.maxBackoffMs,
    randomize: true,
    shouldRetry: ({ error }: { error: unknown }) => isTransient(error),
    onFailedAttempt: () => {
      result.retried++;
    },
  } as const;

  await pMap(
    items,
    async (item, index) => {
      try {
        await pRetry(() => processOne(pool, item), retryOptions);
        result.ok++;
      } catch (err) {
        const e = err as SqlError;
        result.failed++;
        await sendToDLQ({ entity: item.entity, error: serializeErr(e) });
        result.errors.push({
          index,
          entity: String(item.entity),
          reason: shortErr(e),
        });
      }
    },
    { concurrency: CHUNK_CONCURRENCY },
  );
  await bumpRecentActivity(result);
  return result;
}

const isTransient = (err: unknown): boolean => {
  const e = err as SqlError | undefined;
  const code = e?.code;
  const errno = e?.errno;
  const state = e?.sqlState;
  const msg = (e?.message ?? '').toLowerCase();
  return (
    code === 'ER_LOCK_DEADLOCK' ||
    errno === 1213 ||
    errno === 1205 ||
    code === 'PROTOCOL_CONNECTION_LOST' ||
    code === 'ECONNRESET' ||
    msg.includes('deadlock') ||
    msg.includes('lock wait timeout') ||
    msg.includes('timeout exceeded') ||
    msg.includes('connection lost') ||
    state === '40001'
  );
};

const shortErr = (e: unknown): string => {
  const x = e as SqlError | undefined;
  const c = x?.code ?? '';
  const m = x?.message ?? String(e);
  return `${String(c)} ${m}`.trim();
};

const serializeErr = (e: unknown) => {
  const x = e as SqlError | undefined;
  return {
    code: x?.code,
    errno: x?.errno,
    sqlState: x?.sqlState,
    message: x?.message,
    stack: x?.stack,
  };
};

async function sendToDLQ(payload: DlqPayload): Promise<void> {
  log('warn', 'write.dlq.sent', payload);
  const entity = typeof payload?.entity === 'string' ? payload.entity : 'unknown';
  const reason = shortErr(payload.error) ?? 'no details';
  await writeDlqSent(entity, reason);
}

const asString = (v: unknown, def = ''): string =>
  typeof v === 'string' ? v : v == null ? def : '';

const asNumber = (v: unknown, def = 0): number => {
  if (typeof v === 'number' && Number.isFinite(v)) {
    return v;
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
};

function coerceCachedSecret(u: unknown): CachedSecret {
  if (!isRecord(u)) {
    throw new Error('Secret JSON must be an object');
  }
  const r = u as RawSecret;
  const username = asString(r.username);
  const password = asString(r.password);
  if (!username || !password) {
    throw new Error('Secret missing username/password');
  }

  return {
    dbClusterIdentifier: asString(r.dbClusterIdentifier),
    password,
    dbname: asString(r.dbname),
    engine: asString(r.engine),
    port: asNumber(r.port, 3306),
    host: asString(r.host),
    username,
  };
}

export async function getDbSecret(SecretId: string) {
  if (cachedSecret) {
    return cachedSecret;
  }

  const url = `http://localhost:2773/secretsmanager/get?secretId=${encodeURIComponent(
    SecretId,
  )}&versionStage=AWSCURRENT`;

  const res = await fetch(url, {
    headers: { 'X-Aws-Parameters-Secrets-Token': TOKEN },
  });
  if (!res.ok) {
    throw new Error(`Extension error ${res.status} ${await res.text()}`);
  }

  const bodyUnknown: unknown = await res.json();
  const body = bodyUnknown as SecretsGetResponse;

  const jsonStr =
    body.SecretString ?? Buffer.from(body.SecretBinary ?? '', 'base64').toString('utf8');

  const parsedUnknown: unknown = JSON.parse(jsonStr);
  const value: CachedSecret = coerceCachedSecret(parsedUnknown);

  cachedSecret = value;
  return value;
}

const isString = (v: unknown): v is string => typeof v === 'string';

export async function getParam(name: string): Promise<string | null> {
  const url = `http://localhost:2773/systemsmanager/parameters/get?name=${encodeURIComponent(
    name,
  )}&withDecryption=false`;

  const res = await fetch(url, {
    headers: { 'X-Aws-Parameters-Secrets-Token': TOKEN },
  });
  if (!res.ok) {
    throw new Error(`Extension error ${res.status} ${await res.text()}`);
  }

  const bodyUnknown: unknown = await res.json();
  const body = bodyUnknown as SsmParamResponse;

  const v = body.Parameter?.Value;
  return isString(v) ? v : null;
}

export async function seedModeFromSsm(): Promise<ControlMode> {
  const value = await getParam(PARAM_NAME);
  log('info', 'write.param.value', value);
  const mode: ControlMode =
    value === 'RED' || value === 'YELLOW' || value === 'GREEN'
      ? (value as ControlMode)
      : DEFAULT_MODE;
  return mode;
}
