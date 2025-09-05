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

function pickStageDefaults(qs: Record<string, string | undefined>) {
  const stage = (qs.stage ?? qs.target ?? 'write').toLowerCase();
  const isTransform = stage === 'transform' || stage === 'xform';

  const fn =
    qs.function ?? (isTransform ? process.env.TRANSFORM_FN : process.env.CONSUMER_FN) ?? '';

  const consumerGroup =
    qs.consumerGroup ??
    (isTransform ? process.env.TRANSFORM_CONSUMER_GROUP : process.env.CONSUMER_GROUP) ??
    '';

  const topic = qs.topic ?? (isTransform ? process.env.RAW_TOPIC : process.env.BUFFER_TOPIC) ?? '';

  return { stage, isTransform, fn, consumerGroup, topic };
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

async function lag(qs: Record<string, string | undefined>) {
  const cluster = qs.cluster ?? process.env.CLUSTER!;
  const { fn, consumerGroup, topic } = pickStageDefaults(qs);
  const { start, end } = parseRange(qs.range);

  const queries: MetricDataQuery[] = [
    {
      Id: 'broker',
      MetricStat: {
        Metric: {
          Namespace: 'AWS/Kafka',
          MetricName: 'SumOffsetLag',
          Dimensions: [
            { Name: 'Cluster Name', Value: cluster },
            { Name: 'Consumer Group', Value: consumerGroup },
            { Name: 'Topic', Value: topic },
          ],
        },
        Period: 60,
        Stat: 'Maximum',
      },
    },
    {
      Id: 'lambda',
      MetricStat: {
        Metric: {
          Namespace: 'AWS/Lambda',
          MetricName: 'OffsetLag',
          Dimensions: [{ Name: 'FunctionName', Value: fn }],
        },
        Period: 60,
        Stat: 'Maximum',
      },
    },
  ];

  const res = await getMetricData(queries, start, end);
  let broker = toSeries(res.get('broker') ?? {});
  if (broker.length === 0) {
    const alt: MetricDataQuery[] = [
      {
        Id: 'broker',
        MetricStat: {
          Metric: {
            Namespace: 'AWS/Kafka',
            MetricName: 'SumOffsetLag',
            Dimensions: [
              { Name: 'Cluster Name', Value: cluster },
              { Name: 'Consumer Group', Value: consumerGroup },
              // Topic omitted
            ],
          },
          Period: 60,
          Stat: 'Maximum',
        },
      },
    ];
    const altRes = await getMetricData(alt, start, end);
    broker = toSeries(altRes.get('broker') ?? {});
  }

  return {
    statusCode: 200,
    headers: CORS,
    body: JSON.stringify({
      broker,
      lambda: toSeries(res.get('lambda') ?? {}),
      used: { topic, consumerGroup, functionName: fn },
    }),
  };
}

async function lambdaQuality(qs: Record<string, string | undefined>) {
  const { fn } = pickStageDefaults(qs);
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
