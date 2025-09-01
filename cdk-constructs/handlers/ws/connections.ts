import { DeleteItemCommand, DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import type { APIGatewayProxyEvent } from 'aws-lambda';

const ddb = new DynamoDBClient({});

export const handler = async (event: APIGatewayProxyEvent) => {
  const table = process.env.TABLE;
  const ctx = event.requestContext || {};
  if (!table) {
    return { statusCode: 500, body: 'No table configured' };
  }
  if (!ctx.connectionId || !ctx.eventType) {
    return {
      statusCode: 400,
      body: 'No connectionId or eventType',
    };
  }
  const connectionId = ctx.connectionId;
  const eventType = ctx.eventType;
  if (!table || !connectionId || !eventType) {
    return { statusCode: 400 };
  }

  if (eventType === 'CONNECT') {
    await ddb.send(
      new PutItemCommand({
        TableName: table,
        Item: {
          pk: { S: 'CONN' },
          sk: { S: 'CONN#' + connectionId },
          connectedAt: { S: new Date().toISOString() },
        },
      }),
    );
  } else if (eventType === 'DISCONNECT') {
    await ddb.send(
      new DeleteItemCommand({
        TableName: table,
        Key: {
          pk: { S: 'CONN' },
          sk: { S: 'CONN#' + connectionId },
        },
      }),
    );
  }
  return { statusCode: 200, body: 'OK' };
};
