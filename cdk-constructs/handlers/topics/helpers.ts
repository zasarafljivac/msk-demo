import { DescribeClusterV2Command, KafkaClient } from '@aws-sdk/client-kafka';
import type { Admin } from 'kafkajs';
import { ConfigResourceTypes } from 'kafkajs';

export async function waitForMskActive(
  timeoutMs: number,
  region: string,
  clusterArn: string,
): Promise<void> {
  const client = new KafkaClient({ region });
  const start = Date.now();
  for (;;) {
    const out = await client.send(new DescribeClusterV2Command({ ClusterArn: clusterArn }));
    if (out.ClusterInfo?.State === 'ACTIVE') {
      return;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error('MSK cluster not ACTIVE in time');
    }
    await new Promise<void>((r) => setTimeout(r, 5000));
  }
}

interface ConfigEntryLite {
  name: string;
  value: string | null;
}

function isEntryLike(obj: unknown): obj is { name: unknown; value?: unknown } {
  return !!obj && typeof obj === 'object' && 'name' in obj;
}

function coerceEntries(entriesUnknown: unknown): ConfigEntryLite[] {
  const out: ConfigEntryLite[] = [];
  if (!Array.isArray(entriesUnknown)) {
    return out;
  }
  for (const item of entriesUnknown) {
    if (isEntryLike(item) && typeof (item as { name: unknown }).name === 'string') {
      const name = (item as { name: string }).name;
      const v = (item as { value?: unknown }).value;
      const value = v == null ? null : JSON.stringify(v);
      out.push({ name, value });
    }
  }
  return out;
}

type AdminWithIncremental = Admin & {
  incrementalAlterConfigs?: (args: {
    resources: {
      type: number;
      name: string;
      configs: { name: string; value: string }[];
    }[];
    validateOnly?: boolean;
  }) => Promise<void>;
};

export async function compactTopic(admin: Admin, topic: string): Promise<void> {
  const described = await admin.describeConfigs({
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
        configNames: ['cleanup.policy', 'min.cleanable.dirty.ratio', 'segment.ms'],
      },
    ],
    includeSynonyms: true,
  });

  const resources = described.resources ?? [];
  const first = resources[0];
  const entries = coerceEntries(first?.configEntries ?? []);
  const cfg: Record<string, string | null> = {};
  for (const e of entries) {
    cfg[e.name] = e.value;
  }

  const cleanupPolicy = cfg['cleanup.policy'];
  const needsCleanupPolicy = cleanupPolicy == null || !String(cleanupPolicy).includes('compact');
  const needsDirtyRatio = cfg['min.cleanable.dirty.ratio'] !== '0.01';
  const needsSegmentMs = cfg['segment.ms'] !== '600000';

  if (!needsCleanupPolicy && !needsDirtyRatio && !needsSegmentMs) {
    return;
  }

  const desired = [
    needsCleanupPolicy ? { name: 'cleanup.policy', value: 'compact' } : null,
    needsDirtyRatio ? { name: 'min.cleanable.dirty.ratio', value: '0.01' } : null,
    needsSegmentMs ? { name: 'segment.ms', value: '600000' } : null,
  ].filter((x): x is { name: string; value: string } => x != null);

  const a = admin as AdminWithIncremental;
  if (typeof a.incrementalAlterConfigs === 'function') {
    await a.incrementalAlterConfigs({
      resources: [{ type: ConfigResourceTypes.TOPIC, name: topic, configs: desired }],
      validateOnly: false,
    });
    return;
  }

  await admin.alterConfigs({
    resources: [{ type: ConfigResourceTypes.TOPIC, name: topic, configEntries: desired }],
    validateOnly: false,
  });
}
