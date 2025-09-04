import { ApiGatewayManagementApi, GoneException } from '@aws-sdk/client-apigatewaymanagementapi';
import {
  DeleteItemCommand,
  DynamoDBClient,
  QueryCommand,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb';
import PQueue from 'p-queue';

import { getParam, log } from './helpers';

interface ChunkResult {
  ok: number;
  failed: number;
  retried: number;
  rows?: number;
}

type WsMsg =
  | {
      type: 'dashboard.live.activity';
      ts: string;
      data: { ok: number; failed: number; retried: number };
    }
  | { type: 'invoke.start'; ts: string; data: { budgetMs: number; groups: number } }
  | { type: 'control.update'; ts: string; data: { mode: 'GREEN' | 'YELLOW' | 'RED' } }
  | { type: 'control.not.ready'; ts: string; data: { skipped: number } }
  | {
      type: 'invoke.plan';
      ts: string;
      data: { total: number; chunkSize: number; chunks: number; groups: number; perChunk: number };
    }
  | {
      type: 'chunk.result';
      ts: string;
      data: {
        ok: number;
        failed: number;
        retried: number;
        sampleErrors?: { index: number; entity: string; reason: string }[];
      };
    }
  | {
      type: 'dashboard.live.snapshot';
      ts: string;
      data: {
        shipments: number;
        orders: number;
        shipmentEvents: number;
        invoices: number;
        invoiceItems: number;
        totalRecords: number;
      };
    }
  | { type: 'write.dlq.sent'; ts: string; data: { entity: string; reason: string } }
  | { type: 'invoke.done'; ts: string; data: { scheduledChunks: number; tookMs: number } }
  | { type: 'heartbeat'; ts: string };

interface WsBatch {
  type: 'dashboard.batch';
  ts: string;
  data: WsMsg[];
}

let tableName: string;
let wsEndpoint: string;

export async function loadConfig(tableParam: string, endpointParam: string) {
  if (!tableName) {
    const name = await getParam(tableParam);
    tableName = name ?? 'msk-demo-app';
  }
  if (!wsEndpoint) {
    const endpoint = await getParam(endpointParam);
    if (!endpoint) {
      throw new Error('WebSocket endpoint not configured');
    }
    wsEndpoint = endpoint.replace(/^wss:/, 'https:'); // APIG Mgmt API uses https
  }
}

const WS_CONCURRENCY = Number(process.env.WS_CONCURRENCY ?? 40); // parallel posts
const WS_RATE_CAP = Number(process.env.WS_RATE_CAP ?? 400); // posts/sec cap
const WS_MAX_RETRY = Number(process.env.WS_MAX_RETRY ?? 3);

const WS_COALESCE_MIN_MS = Number(process.env.WS_COALESCE_MIN_MS ?? 2000);
const WS_COALESCE_MAX_MS = Number(process.env.WS_COALESCE_MAX_MS ?? 3000);

let apigw: ApiGatewayManagementApi | null = null;
function getApi(endpoint: string) {
  apigw = apigw ?? new ApiGatewayManagementApi({ endpoint });
  return apigw;
}

let ddb = new DynamoDBClient();

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function postSafe(
  api: ApiGatewayManagementApi,
  connectionId: string,
  payload: unknown,
): Promise<'ok' | 'gone' | 'fail'> {
  const Data = typeof payload === 'string' ? payload : JSON.stringify(payload);
  let delay = 80 + Math.random() * 40; // jitter
  for (let attempt = 0; attempt < WS_MAX_RETRY; attempt++) {
    try {
      await api.postToConnection({ ConnectionId: connectionId, Data });
      return 'ok';
    } catch (e: unknown) {
      const err = e as {
        $metadata?: { httpStatusCode?: number };
        code?: string;
        statusCode?: number;
      };
      const statusCode = err?.$metadata?.httpStatusCode ?? err?.statusCode;
      const code = err?.code ?? '';

      if (e instanceof GoneException || statusCode === 410) {
        return 'gone';
      }

      if (
        statusCode === 429 ||
        ['EBUSY', 'EAI_AGAIN', 'ENOTFOUND', 'ETIMEDOUT', 'ECONNRESET'].includes(code)
      ) {
        await sleep(delay);
        delay = Math.min(delay * 2, 1000);
        continue;
      }
      log('warn', 'ws.post.error.nonretry', { statusCode, code });
      return 'fail';
    }
  }
  return 'fail';
}

async function broadcastMessage(message: WsMsg | WsBatch, tableParam: string, wsParam: string) {
  await loadConfig(tableParam, wsParam);
  const api = getApi(wsEndpoint);
  ddb = ddb ?? new DynamoDBClient();

  // fetch all connection ids
  const command = new QueryCommand({
    TableName: tableName!,
    KeyConditionExpression: 'pk = :pk',
    ExpressionAttributeValues: { ':pk': { S: 'CONN' } },
    ProjectionExpression: 'pk, sk',
  });
  const doc = await ddb.send(command);
  const items = doc.Items ?? [];
  const ids = Array.from(
    new Set(items.map((it) => it.sk?.S?.split('#')[1]).filter(Boolean) as string[]),
  );

  if (!ids.length) {
    return;
  }

  const q = new PQueue({ concurrency: WS_CONCURRENCY, interval: 1000, intervalCap: WS_RATE_CAP });

  await q.addAll(
    ids.map((id) => async () => {
      const res = await postSafe(api, id, message);
      if (res === 'gone') {
        // prune dead connection
        try {
          await ddb.send(
            new DeleteItemCommand({
              TableName: tableName!,
              Key: { pk: { S: 'CONN' }, sk: { S: `CONN#${id}` } },
            }),
          );
        } catch {
          /* ignore */
        }
      }
    }),
  );
  await q.onIdle();
}

const pending = new Map<WsMsg['type'], WsMsg>();
let timer: NodeJS.Timeout | null = null;
let lastSend = 0;

function jitterDelay() {
  const span = Math.max(0, WS_COALESCE_MAX_MS - WS_COALESCE_MIN_MS);
  return WS_COALESCE_MIN_MS + Math.floor(Math.random() * span);
}

function mergePending(msg: WsMsg) {
  if (msg.type === 'chunk.result') {
    const cur = pending.get('chunk.result') as (WsMsg & { type: 'chunk.result' }) | undefined;
    if (!cur) {
      pending.set('chunk.result', msg);
      return;
    }
    const merged: WsMsg & { type: 'chunk.result' } = {
      type: 'chunk.result',
      ts: msg.ts, // latest timestamp
      data: {
        ok: (cur.data.ok ?? 0) + (msg.data.ok ?? 0),
        failed: (cur.data.failed ?? 0) + (msg.data.failed ?? 0),
        retried: (cur.data.retried ?? 0) + (msg.data.retried ?? 0),
        sampleErrors: [...(cur.data.sampleErrors ?? []), ...(msg.data.sampleErrors ?? [])].slice(
          0,
          5,
        ),
      },
    };
    pending.set('chunk.result', merged);
    return;
  }

  pending.set(msg.type, msg);
}

export function enqueueWs(msg: WsMsg, tableParam?: string, wsParam?: string) {
  mergePending(msg);
  if (timer) {
    return;
  }

  const now = Date.now();
  const due = Math.max(lastSend + WS_COALESCE_MIN_MS, now + jitterDelay());
  const delay = Math.max(0, due - now);

  const fireAndForget = (p: Promise<unknown>) => {
    void p.catch((e) => log('warn', 'ws.flushPending.error', e));
  };

  timer = setTimeout(() => {
    timer = null;
    if (pending.size) {
      fireAndForget(flushPending(tableParam, wsParam));
    }
  }, delay);
}

export async function flushPending(tableParam?: string, wsParam?: string) {
  if (tableParam && wsParam) {
    await loadConfig(tableParam, wsParam);
  } else if (!tableName || !wsEndpoint) {
    return;
  }

  if (!pending.size) {
    return;
  }
  const frames = Array.from(pending.values());
  pending.clear();
  lastSend = Date.now();

  const batch: WsBatch = { type: 'dashboard.batch', ts: new Date().toISOString(), data: frames };
  await broadcastMessage(batch, tableName!, wsEndpoint!);
}

export function hasPending(): boolean {
  return pending.size > 0;
}

export async function bumpRecentActivity(input: ChunkResult): Promise<void> {
  input.rows = input.ok + input.failed + input.retried;
  ddb = ddb ?? new DynamoDBClient();
  const now = new Date().toISOString();

  const command = new UpdateItemCommand({
    TableName: tableName!,
    Key: { pk: { S: 'DASHBOARD' }, sk: { S: 'STATS' } },
    UpdateExpression:
      'SET lastUpdated = :now ADD ok :o, failed :f, retried :r, chunks :one, records :records',
    ExpressionAttributeValues: {
      ':o': { N: (input.ok ?? 0).toString() },
      ':f': { N: (input.failed ?? 0).toString() },
      ':r': { N: (input.retried ?? 0).toString() },
      ':records': { N: (input.rows ?? 0).toString() },
      ':one': { N: '1' },
      ':now': { S: now },
    },
    ReturnValues: 'UPDATED_NEW',
  });

  const result = await ddb.send(command);
  const data = result.Attributes
    ? {
        ok: Number(result.Attributes.ok.N),
        failed: Number(result.Attributes.failed.N),
        retried: Number(result.Attributes.retried.N),
      }
    : { ok: input.ok, failed: input.failed, retried: input.retried };

  enqueueWs({ type: 'dashboard.live.activity', ts: now, data });
}

export async function writeSnapshot(t: {
  orders: number;
  shipments: number;
  shipmentEvents: number;
  invoices: number;
  invoiceItems: number;
  total: number;
}) {
  ddb = ddb ?? new DynamoDBClient();
  const now = new Date().toISOString();

  const result = await ddb.send(
    new UpdateItemCommand({
      TableName: tableName,
      Key: { pk: { S: 'DASHBOARD' }, sk: { S: 'SNAPSHOT' } },
      UpdateExpression:
        'SET snapshotTs = :now ADD orders :o, shipments :s, shipmentEvents :se, invoices :i, invoiceItems :ii, totalRecords :t',
      ExpressionAttributeValues: {
        ':o': { N: t.orders.toString() },
        ':s': { N: t.shipments.toString() },
        ':se': { N: t.shipmentEvents.toString() },
        ':i': { N: t.invoices.toString() },
        ':ii': { N: t.invoiceItems.toString() },
        ':t': { N: t.total.toString() },
        ':now': { S: now },
      },
      ReturnValues: 'ALL_NEW',
    }),
  );

  const data = result.Attributes
    ? {
        shipments: Number(result.Attributes.shipments.N),
        orders: Number(result.Attributes.orders.N),
        shipmentEvents: Number(result.Attributes.shipmentEvents.N),
        invoices: Number(result.Attributes.invoices.N),
        invoiceItems: Number(result.Attributes.invoiceItems.N),
        totalRecords: Number(result.Attributes.totalRecords.N),
      }
    : {
        shipments: 0,
        orders: 0,
        shipmentEvents: 0,
        invoices: 0,
        invoiceItems: 0,
        totalRecords: 0,
      };

  enqueueWs({ type: 'dashboard.live.snapshot', ts: now, data });
}

export async function writeDlqSent(entity: string, reason: string): Promise<void> {
  ddb = ddb ?? new DynamoDBClient();
  const now = new Date().toISOString();
  log('warn', 'write.dlq.sent', { entity, reason });

  await ddb.send(
    new UpdateItemCommand({
      TableName: tableName,
      Key: { pk: { S: 'DASHBOARD' }, sk: { S: 'SNAPSHOT' } },
      UpdateExpression: 'SET snapshotTs = :now ADD dlq :one',
      ExpressionAttributeValues: {
        ':one': { N: '1' },
        ':now': { S: now },
      },
      ReturnValues: 'ALL_NEW',
    }),
  );

  enqueueWs({ type: 'write.dlq.sent', ts: now, data: { entity, reason } });
}
