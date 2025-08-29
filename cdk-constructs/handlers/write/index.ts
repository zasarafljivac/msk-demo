import { chunk, getPool, log, processChunk, seedModeFromSsm, toEnvelope } from "./helpers";
import { setVars } from "./helpers";
import PQueue from 'p-queue';
import { Pool } from 'mysql2/promise';
import { Envelope, ControlMode } from "../transform";


let pool: Pool | null = null;
let currentMode: ControlMode | null = null;

const PARAM_NAME = process.env.DB_CONTROL_PARAM || '/msk-demo/db-mode';
const DEFAULT_MODE = (process.env.DEFAULT_MODE as ControlMode) || 'GREEN';

const LOG_LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase();
const SAFETY_MS = parseInt(process.env.SAFETY_MS ?? '2500', 10); // leave ~2.5s before Lambda timeout
let CHUNK_PARALLELISM = parseInt(process.env.GROUPS ?? '1', 10); // default 1 parallel chunk (no prallelis, increase to process chunks in parallel)
let CHUNK_SIZE = parseInt(process.env.CHUNK_SIZE ?? '30', 10); // default 30 records per chunk
let CHUNK_CONCURRENCY = parseInt(process.env.CHUNK_CONCURRENCY ?? '2', 10); // default 2 at once makes inner parallelism per chunk
const RDS_PROXY_ENDPOINT = process.env.RDS_PROXY_ENDPOINT!;
const TOKEN = process.env.AWS_SESSION_TOKEN!;

setVars({
  LOG_LEVEL,
  SAFETY_MS,
  CHUNK_PARALLELISM,
  CHUNK_SIZE,
  CHUNK_CONCURRENCY,
  RDS_PROXY_ENDPOINT,
  TOKEN,
  PARAM_NAME,
  DEFAULT_MODE,
});

export const handler = async (event: any, context: any) => {

  if (!currentMode) {
    currentMode = await seedModeFromSsm();
  }

  const remaingTimeMs = typeof context?.getRemainingTimeInMillis === 'function'
    ? context.getRemainingTimeInMillis()
    : 30000;
  const budgetMs = Math.max(0, remaingTimeMs - SAFETY_MS);
  const deadline = Date.now() + budgetMs;

  log('info', 'invoke.start', { budgetMs, groups: Object.keys(event.records || {}).length });
  pool = await getPool(pool);

  const allRecords = Object.values(event.records || {}).flat() as any[];
  const envelopes: Envelope[] = allRecords
    .map((rec) => { try { return JSON.parse(Buffer.from(rec.value, 'base64').toString('utf8')); } catch { return null; } })
    .map(toEnvelope)
    .filter(Boolean) as Envelope[];

  for (const env of envelopes) {
    if ((env as any).kind === 'CONTROL' || (env as any).control) {
      currentMode = (env as any).control.mode;
      log('info', 'control.update', env.control);
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
        SAFETY_MS,
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
        ts: new Date().toISOString()
      });
    }
  }

  if (!currentMode) {
    log('warn', 'control.not.ready', { skipped: envelopes.length });
    return { statusCode: 200 };
  }

  const dataEnvelopes = envelopes.filter(e => !('control' in e));

  const chunks = chunk(dataEnvelopes, CHUNK_SIZE);
  log('info', 'window.plan', {
    total: dataEnvelopes.length,
    chunkSize: CHUNK_SIZE,
    chunks: chunks.length,
    groups: CHUNK_PARALLELISM,
    perChunk: CHUNK_CONCURRENCY,
  });

  const chunksQueue = new PQueue({ concurrency: CHUNK_PARALLELISM });

  let scheduledChunks = 0;
  for (const ch of chunks) {
    if (Date.now() >= deadline) {
      log('warn', 'budget.exhausted', { scheduledChunks, remainingMs: context?.getRemainingTimeInMillis?.() });
      break;
    }
    scheduledChunks++;
    chunksQueue.add(() => processChunk(pool!, ch));
  }

  const msLeft = Math.max(0, deadline - Date.now());
  try {
    await Promise.race([
      chunksQueue.onIdle(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('TIME_BUDGET_EARLY_EXIT')), msLeft)),
    ]);
  } catch (err: any) {
    log('warn', 'early.exit.failfast', { scheduledChunks, err: err?.message });
    throw err;
  }

  log('info', 'invoke.done', { scheduledChunks, tookMs: (remaingTimeMs - (context?.getRemainingTimeInMillis?.() ?? 0)) });
  return { statusCode: 200 };
};
