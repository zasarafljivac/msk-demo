import * as fs from "node:fs";
import * as path from "node:path";
import * as net from "node:net";
import { lookup as dnsLookup } from "node:dns/promises";
import mysql from "mysql2/promise";
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from "@aws-sdk/client-secrets-manager";

const RDS_PROXY_ENDPOINT = process.env.RDS_PROXY_ENDPOINT!;
const DB_SECRET_ARN = process.env.DB_SECRET_ARN!;
const CONNECT_TIMEOUT_MS = 5000;
const TOTAL_WAIT_MS = 30000;
const PROBE_INTERVAL_MS = 5000;

const client = new SecretsManagerClient({});
const command = new GetSecretValueCommand({ SecretId: DB_SECRET_ARN });

function t0() {
  const s = Date.now();
  return () => Date.now() - s;
}

async function tcpProbe(
  host: string,
  port: number,
  timeoutMs = 4000,
): Promise<boolean> {
  return await new Promise((resolve) => {
    const s = net.createConnection({ host, port });
    const done = (ok: boolean) => {
      try {
        s.destroy();
      } catch {}
      resolve(ok);
    };
    s.once("connect", () => done(true));
    s.once("error", () => done(false));
    s.setTimeout(timeoutMs, () => done(false));
  });
}

async function waitForDbReady() {
  const stop = t0();
  const end = Date.now() + TOTAL_WAIT_MS;

  while (Date.now() < end) {
    try {
      const dns = await dnsLookup(RDS_PROXY_ENDPOINT);
      console.log(`[schema] DNS ok ${RDS_PROXY_ENDPOINT} -> ${dns.address}`);
      const tcp = await tcpProbe(RDS_PROXY_ENDPOINT, 3306, 4000);
      if (!tcp) throw new Error("tcp probe failed");

      const { username, password } = await getCreds();
      const conn = await mysql.createConnection({
        host: RDS_PROXY_ENDPOINT,
        user: username,
        password,
        ssl: {
          rejectUnauthorized: true,
          minVersion: "TLSv1.2",
        },
        connectTimeout: CONNECT_TIMEOUT_MS,
        multipleStatements: true,
      });
      await conn.ping();
      await conn.end();
      console.log(`[schema] DB ping ok after ${stop()}ms`);
      return;
    } catch (e: any) {
      console.log(`[schema] DB not ready yet: ${e?.message || e}`);
      await new Promise((r) => setTimeout(r, PROBE_INTERVAL_MS));
    }
  }
  throw new Error("DB not ready within timeout");
}

async function getCreds() {
  const sec = await client.send(command);
  const s = JSON.parse(sec.SecretString || "{}");
  return { username: s.username as string, password: s.password as string };
}

function loadSchemaSql(): string {
  const p = path.join(__dirname, "schema.sql");
  const sql = fs.readFileSync(p, "utf8");
  if (!sql || !sql.trim()) throw new Error("schema.sql is empty");
  return sql;
}

export const handler = async () => {
  console.log("[schema] start");
  await waitForDbReady();

  const { username, password } = await getCreds();
  const sql = loadSchemaSql();

  const conn = await mysql.createConnection({
    host: RDS_PROXY_ENDPOINT,
    user: username,
    password,
    ssl: {
      rejectUnauthorized: true,
      minVersion: "TLSv1.2",
    },
    connectTimeout: CONNECT_TIMEOUT_MS,
    multipleStatements: true,
  });

  try {
    console.log("[schema] applying schema.sql...");
    const start = Date.now();
    await conn.query(sql);
    console.log(`[schema] done in ${Date.now() - start}ms`);
  } finally {
    await conn.end();
  }

  return { PhysicalResourceId: "SchemaRunnerOnce", Data: { Applied: true } };
};
