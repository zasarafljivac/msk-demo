import { lookup as dnsLookup } from 'node:dns/promises';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as path from 'node:path';

import mysql from 'mysql2/promise';

import { getDbSecret } from '../write/helpers';

const RDS_PROXY_ENDPOINT = process.env.RDS_PROXY_ENDPOINT!;
const DB_SECRET_ARN = process.env.DB_SECRET_ARN!;
const CONNECT_TIMEOUT_MS = 5000;
const TOTAL_WAIT_MS = 30000;
const PROBE_INTERVAL_MS = 5000;

function t0() {
  const s = Date.now();
  return () => Date.now() - s;
}

async function tcpProbe(host: string, port: number, timeoutMs = 4000): Promise<boolean> {
  return await new Promise((resolve) => {
    const s = net.createConnection({ host, port });
    const done = (ok: boolean) => {
      try {
        s.destroy();
      } finally {
        resolve(ok);
      }
    };
    s.once('connect', () => done(true));
    s.once('error', () => done(false));
    s.setTimeout(timeoutMs, () => done(false));
  });
}

async function getSecretAndConnect() {
  const secret = await getDbSecret(DB_SECRET_ARN);
  const connection = await mysql.createConnection({
    host: RDS_PROXY_ENDPOINT,
    user: secret!.username,
    password: secret?.password,
    ssl: {
      rejectUnauthorized: true,
      minVersion: 'TLSv1.2',
    },
    connectTimeout: CONNECT_TIMEOUT_MS,
    multipleStatements: true,
  });
  return connection;
}

async function connectToDb() {
  const stop = t0();
  const dns = await dnsLookup(RDS_PROXY_ENDPOINT);
  console.log(`[schema] DNS ok ${RDS_PROXY_ENDPOINT} -> ${dns.address}`);
  const tcp = await tcpProbe(RDS_PROXY_ENDPOINT, 3306, 4000);
  if (!tcp) {
    throw new Error('tcp probe failed');
  }
  const connection = await getSecretAndConnect();
  await connection.ping();
  await connection.end();
  console.log(`[schema] DB ping ok after ${stop()}ms`);
  return;
}

async function waitForDbReady() {
  const end = Date.now() + TOTAL_WAIT_MS;

  while (Date.now() < end) {
    try {
      await connectToDb();
    } catch (e: unknown) {
      console.log(`[schema] DB not ready yet`, e);
      await new Promise((r) => setTimeout(r, PROBE_INTERVAL_MS));
    }
  }
  throw new Error('DB not ready within timeout');
}
function loadSchemaSql(): string {
  const p = path.join(__dirname, 'schema.sql');
  const sql = fs.readFileSync(p, 'utf8');
  if (!sql?.trim()) {
    throw new Error('schema.sql is empty');
  }
  return sql;
}

export const handler = async () => {
  console.log('[schema] start');
  await waitForDbReady();
  const sql = loadSchemaSql();
  try {
    console.log('[schema] applying schema.sql...');
    const start = Date.now();
    const connection = await getSecretAndConnect();
    await connection.query(sql);
    console.log(`[schema] done in ${Date.now() - start}ms`);
    await connection.end();
  } catch (err: unknown) {
    console.log(`Connection error`, err);
  }

  return { PhysicalResourceId: 'SchemaRunnerOnce', Data: { Applied: true } };
};
