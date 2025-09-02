import { ApiGatewayManagementApi } from '@aws-sdk/client-apigatewaymanagementapi';
import { DynamoDBClient, QueryCommand, UpdateItemCommand } from '@aws-sdk/client-dynamodb';

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
    wsEndpoint = endpoint.replace(/^wss:/, 'https:');
  }
}

let ddb = new DynamoDBClient();

export async function broadcastMessage(
  message: WsMsg,
  tableParam: string,
  wsParam: string,
): Promise<void> {
  await loadConfig(tableParam, wsParam);

  const apigw = new ApiGatewayManagementApi({ endpoint: wsEndpoint });
  ddb = ddb ?? new DynamoDBClient();

  const command = new QueryCommand({
    TableName: tableName!,
    KeyConditionExpression: 'pk = :pk',
    ExpressionAttributeValues: { ':pk': { S: 'CONN' } },
    ProjectionExpression: 'pk, sk',
  });

  const doc = await ddb.send(command);
  const items = doc.Items ?? [];
  await Promise.all(
    items.map(async ({ sk }) => {
      const connectionId = sk?.S?.split('#')[1];
      if (!connectionId) {
        return;
      }
      try {
        await apigw.postToConnection({
          ConnectionId: connectionId,
          Data: JSON.stringify(message),
        });
      } catch (err: unknown) {
        log('warn', 'write.handler.broadcastMessage', err);
      }
    }),
  );
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
    : input;
  await broadcastMessage({ type: 'dashboard.live.activity', ts: now, data }, tableName, wsEndpoint);
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
      UpdateExpression: 'SET snapshotTs = :now ADD orders :o, shipments :s, shipmentEvents :se, invoices :i, invoiceItems :ii, totalRecords :t',
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
  await broadcastMessage({ type: 'dashboard.live.snapshot', ts: now, data }, tableName, wsEndpoint);
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

  await broadcastMessage(
    {
      type: 'write.dlq.sent',
      ts: new Date().toISOString(),
      data: { entity, reason },
    },
    tableName,
    wsEndpoint,
  );
}

export async function sendHeartbeat(): Promise<void> {
  const now = new Date().toISOString();
  await broadcastMessage({ type: 'heartbeat', ts: now }, tableName, wsEndpoint);
}
