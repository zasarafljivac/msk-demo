import type { Pool } from 'mysql2/promise';
import PQueue from 'p-queue';

import { chunk, getPool, log, processChunk, seedModeFromSsm, setVars, toEnvelope } from './helpers';
import { broadcastMessage } from './ws';

type EntityName =
  | 'orders_ops'
  | 'shipments_ops'
  | 'shipment_events_ops'
  | 'invoices_ops'
  | 'invoice_items_ops';

type ControlMode = 'GREEN' | 'YELLOW' | 'RED';

interface LambdaContext {
  getRemainingTimeInMillis?: () => number;
}

interface KafkaRecord {
  value: string;
}
interface KafkaEvent {
  records?: Record<string, KafkaRecord[]>;
}

interface EnvelopeControl {
  mode: ControlMode;
  version: number;
  updatedAt: string;
  alarm: string;
  rawState: string;
}

interface Envelope {
  entity: EntityName;
  op?: 'upsert' | 'delete';
  data?: Record<string, unknown>;
  key?: string | Uint8Array | Buffer;
  control?: EnvelopeControl;
  kind?: 'CONTROL' | 'DATA';
}

let pool: Pool | null = null;
let currentMode: ControlMode | null = null;

const PARAM_NAME = process.env.DB_CONTROL_PARAM ?? '/msk-demo/db-mode';
const DEFAULT_MODE = (process.env.DEFAULT_MODE as ControlMode | undefined) ?? 'GREEN';

const LOG_LEVEL = (process.env.LOG_LEVEL ?? 'info').toLowerCase();
const SAFETY_MS = Number.parseInt(process.env.SAFETY_MS ?? '2500', 10);
let CHUNK_PARALLELISM = Number.parseInt(process.env.GROUPS ?? '1', 10);
let CHUNK_SIZE = Number.parseInt(process.env.CHUNK_SIZE ?? '30', 10);
let CHUNK_CONCURRENCY = Number.parseInt(process.env.CHUNK_CONCURRENCY ?? '2', 10);
const RDS_PROXY_ENDPOINT = process.env.RDS_PROXY_ENDPOINT ?? '';
const TOKEN = process.env.AWS_SESSION_TOKEN ?? '';

const WS_ENDPOINT = process.env.WS_ENDPOINT!;
const CONNECTIONS_TABLE = process.env.CONNECTIONS_TABLE!;

setVars({
  LOG_LEVEL,
  CHUNK_PARALLELISM,
  CHUNK_SIZE,
  CHUNK_CONCURRENCY,
  RDS_PROXY_ENDPOINT,
  TOKEN,
  PARAM_NAME,
  DEFAULT_MODE,
});

const safeJsonParse = (s: string): unknown => {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
};

export const handler = async (event: KafkaEvent, context: LambdaContext) => {
  currentMode = currentMode ?? (await seedModeFromSsm());

  const remainingTime =
    typeof context.getRemainingTimeInMillis === 'function'
      ? context.getRemainingTimeInMillis()
      : 30000;

  const budgetMs = Math.max(0, remainingTime - SAFETY_MS);
  const deadline = Date.now() + budgetMs;

  log('info', 'invoke.start', {
    budgetMs,
    groups: Object.keys(event.records ?? {}).length,
  });
  await broadcastMessage(
    {
      type: 'invoke.start',
      ts: new Date().toISOString(),
      data: { budgetMs, groups: Object.keys(event.records ?? {}).length },
    },
    CONNECTIONS_TABLE,
    WS_ENDPOINT,
  );

  pool = await getPool(pool);

  const allRecords = Object.values(event.records ?? {}).flat() ?? [];
  const envelopes: Envelope[] = allRecords
    .map((rec) => Buffer.from(rec.value, 'base64').toString('utf8'))
    .map(safeJsonParse)
    .map((o) => toEnvelope(o as Record<string, unknown>))
    .filter(Boolean) as Envelope[];

  for (const env of envelopes) {
    const hasControl = env.kind === 'CONTROL' || !!env.control;
    if (!hasControl || !env.control) {
      continue;
    }

    currentMode = env.control.mode;

    switch (currentMode) {
      case 'RED':
        CHUNK_PARALLELISM = 1;
        CHUNK_CONCURRENCY = 1;
        CHUNK_SIZE = 10;
        break;
      case 'YELLOW':
        CHUNK_PARALLELISM = 2;
        CHUNK_CONCURRENCY = 3;
        CHUNK_SIZE = 20;
        break;
      case 'GREEN':
      default:
        CHUNK_PARALLELISM = 4;
        CHUNK_CONCURRENCY = 8;
        CHUNK_SIZE = 30;
        break;
    }

    setVars({
      LOG_LEVEL,
      CHUNK_PARALLELISM,
      CHUNK_SIZE,
      CHUNK_CONCURRENCY,
      RDS_PROXY_ENDPOINT,
      TOKEN,
      PARAM_NAME,
      DEFAULT_MODE,
    });

    log('info', 'db.health.apply', {
      mode: currentMode,
      newSettings: {
        parallelism: CHUNK_PARALLELISM,
        concurrency: CHUNK_CONCURRENCY,
        chunkSize: CHUNK_SIZE,
      },
      ts: new Date().toISOString(),
    });

    await broadcastMessage(
      {
        type: 'control.update',
        ts: new Date().toISOString(),
        data: { mode: currentMode },
      },
      CONNECTIONS_TABLE,
      WS_ENDPOINT,
    );
  }

  if (!currentMode) {
    log('warn', 'control.not.ready', { skipped: envelopes.length });
    await broadcastMessage(
      {
        type: 'control.not.ready',
        ts: new Date().toISOString(),
        data: { skipped: envelopes.length },
      },
      CONNECTIONS_TABLE,
      WS_ENDPOINT,
    );
    return { statusCode: 200 };
  }

  const dataEnvelopes = envelopes.filter((e) => !e.control);
  const chunks = chunk(dataEnvelopes, CHUNK_SIZE);

  log('info', 'invoke.plan', {
    total: dataEnvelopes.length,
    chunkSize: CHUNK_SIZE,
    chunks: chunks.length,
    groups: CHUNK_PARALLELISM,
    perChunk: CHUNK_CONCURRENCY,
  });
  await broadcastMessage(
    {
      type: 'invoke.plan',
      ts: new Date().toISOString(),
      data: {
        total: dataEnvelopes.length,
        chunkSize: CHUNK_SIZE,
        chunks: chunks.length,
        groups: CHUNK_PARALLELISM,
        perChunk: CHUNK_CONCURRENCY,
      },
    },
    CONNECTIONS_TABLE,
    WS_ENDPOINT,
  );

  const chunksQueue = new PQueue({ concurrency: CHUNK_PARALLELISM });

  let scheduledChunks = 0;
  for (const ch of chunks) {
    if (Date.now() >= deadline) {
      log('warn', 'budget.exhausted', {
        scheduledChunks,
        remainingMs: context.getRemainingTimeInMillis?.(),
      });
      break;
    }
    scheduledChunks++;
    void chunksQueue.add(() => processChunk(pool!, ch));
  }

  const msLeft = Math.max(0, deadline - Date.now());
  try {
    await Promise.race([
      chunksQueue.onIdle(),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error('TIME_BUDGET_EARLY_EXIT')), msLeft),
      ),
    ]);
    log('info', 'chunks.processed', { scheduledChunks });
  } catch (err) {
    const e = err as { message?: string } | undefined;
    log('warn', 'early.exit.failfast', {
      scheduledChunks,
      err: e?.message ?? 'unknown',
    });
    throw err;
  }

  log('info', 'invoke.done', {
    scheduledChunks,
    tookMs: remainingTime - (context.getRemainingTimeInMillis?.() ?? 0),
  });

  return { statusCode: 200 };
};
