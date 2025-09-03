import {
  CloudWatchClient,
  type CloudWatchClientConfig,
  GetMetricDataCommand,
  type GetMetricDataCommandInput,
  type GetMetricDataCommandOutput,
  type MetricDataQuery,
} from '@aws-sdk/client-cloudwatch';

const cw: CloudWatchClient = new CloudWatchClient({
  region: process.env.AWS_REGION,
} satisfies CloudWatchClientConfig);

interface APIGEvent {
  rawPath: string;
  queryStringParameters?: Record<string, string | undefined>;
}
interface APIGResp {
  statusCode: number;
  headers?: Record<string, string>;
  body: string;
}

const CORS = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' } as const;

function errMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function kafkaSearchExpr(opts: { cluster: string; topic?: string; period?: number }) {
  const { cluster, topic, period = 60 } = opts;

  const dims = topic
    ? '{AWS/Kafka,Cluster Name,Broker ID,Topic}'
    : '{AWS/Kafka,Cluster Name,Broker ID}';

  const filters = ['MetricName="MessagesInPerSec"', `Cluster\\ Name="${cluster}"`];
  if (topic) {
    filters.push(`Topic="${topic}"`);
  }

  return {
    searchId: topic ? 'mtopic' : 'mbroker',
    expr: `SEARCH('${dims} ${filters.join(' AND ')}', 'Sum', ${period})`,
  };
}

function parseRange(s = '15m') {
  const m = /^(\d+)([smhd])$/.exec(s);
  const n = m ? parseInt(m[1], 10) : 15;
  const unit = m ? m[2] : 'm';
  const ms = unit === 's' ? n * 1e3 : unit === 'm' ? n * 6e4 : unit === 'h' ? n * 36e5 : n * 864e5;
  const end = new Date();
  const start = new Date(end.getTime() - ms);
  const period = Math.max(60, Math.floor(ms / 60 / 1000) * 60); // ≥60s
  return { start, end, period };
}

function toSeries(res?: { Timestamps?: Date[]; Values?: number[] }) {
  const ts = res?.Timestamps ?? [];
  const vs = res?.Values ?? [];
  const out = ts.map((t, i) => ({ ts: t.toISOString(), value: vs[i] ?? 0 }));
  out.sort((a, b) => +new Date(a.ts) - +new Date(b.ts));
  return out;
}

export async function getMetricData(
  queries: MetricDataQuery[],
  start: Date,
  end: Date,
): Promise<Map<string, { Timestamps?: Date[]; Values?: number[] }>> {
  const input: GetMetricDataCommandInput = {
    MetricDataQueries: queries,
    StartTime: start,
    EndTime: end,
  };
  const out: GetMetricDataCommandOutput = await cw.send(new GetMetricDataCommand(input));

  const map = new Map<string, { Timestamps?: Date[]; Values?: number[] }>();
  for (const r of out.MetricDataResults ?? []) {
    if (r.Id) {
      map.set(r.Id, { Timestamps: r.Timestamps, Values: r.Values });
    }
  }
  return map;
}

async function throughput(qs: Record<string, string | undefined>) {
  const cluster = qs.cluster ?? process.env.CLUSTER!;
  const rawTopic = qs.rawTopic ?? process.env.RAW_TOPIC!;
  const bufferTopic = qs.bufferTopic ?? process.env.BUFFER_TOPIC!;
  const { start, end } = parseRange(qs.range);

  const qRawTopic = kafkaSearchExpr({ cluster, topic: rawTopic });
  const qBufTopic = kafkaSearchExpr({ cluster, topic: bufferTopic });

  let queries: MetricDataQuery[] = [
    { Id: qRawTopic.searchId, Expression: qRawTopic.expr },
    { Id: 'raw', Expression: 'SUM(METRICS())' },
    { Id: qBufTopic.searchId, Expression: qBufTopic.expr },
    { Id: 'buffer', Expression: 'SUM(METRICS())' },
  ];

  try {
    const map = await getMetricData(queries, start, end);
    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify({
        raw: toSeries(map.get('raw')),
        buffer: toSeries(map.get('buffer')),
      }),
    };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    const isInvalidSearch = msg.includes('Invalid SEARCH parameter');
    if (!isInvalidSearch) {
      return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: msg }) };
    }

    const qRaw = kafkaSearchExpr({ cluster });
    // const qBuf = kafkaSearchExpr({ cluster });
    queries = [
      { Id: qRaw.searchId, Expression: qRaw.expr },

      { Id: 'all', Expression: 'SUM(METRICS())' },
    ];
    const map = await getMetricData(queries, start, end);
    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify({
        raw: toSeries(map.get('all')),
        buffer: toSeries(map.get('all')),
        note: 'Per-topic metrics not enabled; showing cluster-wide MessagesInPerSec.',
      }),
    };
  }
}

async function lag(qs: Record<string, string | undefined>) {
  const cluster = qs.cluster ?? process.env.CLUSTER!;
  const topic = qs.topic ?? process.env.RAW_TOPIC!;
  const group = qs.consumerGroup ?? process.env.CONSUMER_GROUP!;
  const { start, end } = parseRange(qs.range);

  const queries: MetricDataQuery[] = [
    {
      Id: 'mbroker',
      Expression:
        `SEARCH('{AWS/Kafka,Cluster Name,Consumer Group,Topic,Partition} MetricName="SumOffsetLag" ` +
        `AND Cluster\\ Name="${cluster}" AND Consumer\\ Group="${group}" AND Topic="${topic}"', 'Sum', 60)`,
    },
    { Id: 'broker', Expression: 'SUM(METRICS())' },
    {
      Id: 'lambda',
      MetricStat: {
        Metric: {
          Namespace: 'AWS/Lambda',
          MetricName: 'OffsetLag',
          Dimensions: [{ Name: 'FunctionName', Value: process.env.CONSUMER_FN ?? '' }],
        },
        Period: 60,
        Stat: 'Maximum',
      },
    },
  ];
  const res = await getMetricData(queries, start, end);
  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({
      broker: toSeries(res.get('broker') ?? {}),
      lambda: toSeries(res.get('lambda') ?? {}),
    }),
  };
}

async function lambdaQuality(qs: Record<string, string | undefined>) {
  const fn = qs.function ?? process.env.CONSUMER_FN!;
  const { start, end } = parseRange(qs.range);
  const queries: MetricDataQuery[] = [
    {
      Id: 'invocations',
      MetricStat: {
        Metric: {
          Namespace: 'AWS/Lambda',
          MetricName: 'Invocations',
          Dimensions: [{ Name: 'FunctionName', Value: fn }],
        },
        Period: 60,
        Stat: 'Sum',
      },
    },
    {
      Id: 'errors',
      MetricStat: {
        Metric: {
          Namespace: 'AWS/Lambda',
          MetricName: 'Errors',
          Dimensions: [{ Name: 'FunctionName', Value: fn }],
        },
        Period: 60,
        Stat: 'Sum',
      },
    },
    {
      Id: 'durationP95',
      MetricStat: {
        Metric: {
          Namespace: 'AWS/Lambda',
          MetricName: 'Duration',
          Dimensions: [{ Name: 'FunctionName', Value: fn }],
        },
        Period: 60,
        Stat: 'p95',
      },
    },
  ];
  const res = await getMetricData(queries, start, end);
  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({
      functionName: fn,
      invocations: toSeries(res.get('invocations') ?? {}),
      errors: toSeries(res.get('errors') ?? {}),
      durationP95: toSeries(res.get('durationP95') ?? {}),
    }),
  };
}

export async function handler(event: APIGEvent): Promise<APIGResp> {
  try {
    const path = event.rawPath || '';
    const qs = event.queryStringParameters ?? {};
    if (path.endsWith('/throughput')) {
      return throughput(qs);
    }
    if (path.endsWith('/lag')) {
      return lag(qs);
    }
    if (path.endsWith('/lambda-quality')) {
      return lambdaQuality(qs);
    }
    return { statusCode: 404, headers: CORS, body: JSON.stringify({ error: 'not found' }) };
  } catch (e: unknown) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: errMessage(e) }) };
  }
}
