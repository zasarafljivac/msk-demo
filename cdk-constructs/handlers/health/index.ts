import { generateAuthToken } from 'aws-msk-iam-sasl-signer-js';
import type { Producer } from 'kafkajs';
import { Kafka, logLevel, Partitioners } from 'kafkajs';

type ControlMode = 'GREEN' | 'YELLOW' | 'RED';

interface CloudWatchAlarmEvent {
  detail?: {
    state?: { value?: string | null } | null;
    alarmName?: string | null;
  } | null;
}

interface ControlEnvelope {
  control: {
    mode: ControlMode;
    version: number;
    updatedAt: string;
    alarm: string;
    rawState: string;
  };
}

const region = process.env.AWS_REGION!;
const bootstrapBrokers = process.env.BOOTSTRAP_BROKERS_SASL_IAM!;
const controlTopic = process.env.CONTROL_TOPIC ?? 'msk-demo-db-health';

let producer: Producer | null = null;

async function getProducer(
  clientId: string,
  brokersCsv: string,
  awsRegion: string,
): Promise<Producer> {
  const kafka = new Kafka({
    clientId,
    brokers: brokersCsv
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
    ssl: true,
    sasl: {
      mechanism: 'oauthbearer',
      oauthBearerProvider: async () => {
        const { token } = await generateAuthToken({ region: awsRegion });
        return { value: token };
      },
    },
    logLevel: logLevel.NOTHING,
  });
  const p = kafka.producer({
    allowAutoTopicCreation: false,
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await p.connect();
  return p;
}

function log(level: 'info' | 'warn' | 'error' | 'debug', msg: string, extra?: unknown): void {
  const line = `[${new Date().toISOString()}] ${level.toUpperCase()} ${msg}`;
  if (extra !== undefined) {
    console.log(line, extra);
  } else {
    console.log(line);
  }
}

export const handler = async (event: CloudWatchAlarmEvent): Promise<void> => {
  producer = producer ?? (await getProducer('alarm-publisher', bootstrapBrokers, region));

  const state = event.detail?.state?.value ?? 'UNKNOWN';
  const alarmName = event.detail?.alarmName ?? 'unknown-alarm';

  let mode: ControlMode = 'GREEN';
  if (state === 'ALARM') {
    mode = 'RED';
  } else if (state === 'INSUFFICIENT_DATA') {
    mode = 'YELLOW';
  }

  const controlEnvelope: ControlEnvelope = {
    control: {
      mode,
      version: Date.now(),
      updatedAt: new Date().toISOString(),
      alarm: alarmName,
      rawState: state,
    },
  };

  await producer.send({
    topic: controlTopic,
    messages: [
      {
        key: 'db-mode',
        value: JSON.stringify(controlEnvelope),
      },
    ],
  });

  log('info', 'db.health.update', {
    mode,
    reason: alarmName,
    state,
    ts: new Date().toISOString(),
  });
};
