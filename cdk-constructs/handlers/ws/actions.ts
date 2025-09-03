import { ApiGatewayManagementApi } from '@aws-sdk/client-apigatewaymanagementapi';
import { DynamoDBClient, GetItemCommand } from '@aws-sdk/client-dynamodb';
import type { APIGatewayProxyEvent } from 'aws-lambda';

import { log } from '../write/helpers';

const ddb = new DynamoDBClient();
const tableName = process.env.TABLE!;

function apigwClientFromEvent(domainName: string, stage: string) {
  return new ApiGatewayManagementApi({ endpoint: `https://${domainName}/${stage}` });
}
const enc = new TextEncoder();

async function sendToConnection(
  apigw: ApiGatewayManagementApi,
  connectionId: string,
  msg: unknown,
) {
  try {
    await apigw.postToConnection({
      ConnectionId: connectionId,
      Data: enc.encode(JSON.stringify(msg)),
    });
  } catch (err: unknown) {
    log('error', 'ws.actions.sendToConnection', err);
  }
}

export async function handler(event: APIGatewayProxyEvent) {
  const { domainName, stage, connectionId } = event.requestContext;
  if (!domainName || !stage) {
    return { statusCode: 400, body: 'no domainName or stage' };
  }
  if (!connectionId) {
    return { statusCode: 400, body: 'no connectionId' };
  }
  const apigw = apigwClientFromEvent(domainName, stage);
  const now = new Date().toISOString();

  const [stats, snap] = await Promise.all([
    ddb.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { pk: { S: 'DASHBOARD' }, sk: { S: 'STATS' } },
      }),
    ),
    ddb.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { pk: { S: 'DASHBOARD' }, sk: { S: 'SNAPSHOT' } },
      }),
    ),
  ]);

  const activityData = stats.Item
    ? {
        ok: Number(stats.Item.ok?.N ?? '0'),
        failed: Number(stats.Item.failed?.N ?? '0'),
        retried: Number(stats.Item.retried?.N ?? '0'),
      }
    : { ok: 0, failed: 0, retried: 0 };

  await sendToConnection(apigw, connectionId, {
    type: 'dashboard.live.activity',
    ts: stats.Item?.lastUpdated?.S ?? now,
    data: activityData,
  });

  const snapshotData = snap.Item
    ? {
        shipments: Number(snap.Item.shipments?.N ?? '0'),
        orders: Number(snap.Item.orders?.N ?? '0'),
        shipmentEvents: Number(snap.Item.shipmentEvents?.N ?? '0'),
        invoices: Number(snap.Item.invoices?.N ?? '0'),
        invoiceItems: Number(snap.Item.invoiceItems?.N ?? '0'),
        totalRecords: Number(snap.Item.totalRecords?.N ?? '0'),
      }
    : {
        shipments: 0,
        orders: 0,
        shipmentEvents: 0,
        invoices: 0,
        invoiceItems: 0,
        totalRecords: 0,
      };

  await sendToConnection(apigw, connectionId, {
    type: 'dashboard.live.snapshot',
    ts: snap.Item?.snapshotTs?.S ?? now,
    data: snapshotData,
  });

  return { statusCode: 200, body: 'refreshed' };
}
