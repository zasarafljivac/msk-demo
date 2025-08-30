import { ConfigResourceTypes, Admin } from "kafkajs";
import { DescribeClusterV2Command, KafkaClient } from "@aws-sdk/client-kafka";

export async function waitForMskActive(
  timeoutMs: number,
  region: string,
  clusterArn: string,
) {
  const client = new KafkaClient({ region });
  const start = Date.now();
  while (true) {
    const out = await client.send(
      new DescribeClusterV2Command({ ClusterArn: clusterArn }),
    );
    if (out.ClusterInfo?.State === "ACTIVE") return;
    if (Date.now() - start > timeoutMs)
      throw new Error("MSK cluster not ACTIVE in time");
    await new Promise((r) => setTimeout(r, 5000));
  }
}

export async function compactTopic(admin: Admin, topic: string) {
  const describedConfigResult = await admin.describeConfigs({
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
        configNames: [
          "cleanup.policy",
          "min.cleanable.dirty.ratio",
          "segment.ms",
        ],
      },
    ],
    includeSynonyms: true,
  });

  const entries = describedConfigResult?.resources?.[0]?.configEntries ?? [];
  const toMap = (arr: any[]) =>
    Object.fromEntries(arr.map((e: any) => [e.name, e.value]));
  const cfg = toMap(entries);

  const needsCleanupPolicy =
    !cfg["cleanup.policy"] ||
    !String(cfg["cleanup.policy"]).includes("compact");
  const needsDirtyRatio = cfg["min.cleanable.dirty.ratio"] !== "0.01";
  const needsSegmentMs = cfg["segment.ms"] !== "600000";

  if (!needsCleanupPolicy && !needsDirtyRatio && !needsSegmentMs) return;

  const desired = [
    needsCleanupPolicy && { name: "cleanup.policy", value: "compact" },
    needsDirtyRatio && { name: "min.cleanable.dirty.ratio", value: "0.01" },
    needsSegmentMs && { name: "segment.ms", value: "600000" },
  ].filter(Boolean) as Array<{ name: string; value: string }>;

  const hasIncremental =
    typeof (admin as any).incrementalAlterConfigs === "function";

  // because legacy drama ¯\_(ツ)_/¯
  if (hasIncremental) {
    await (admin as any).incrementalAlterConfigs({
      resources: [
        {
          type: ConfigResourceTypes.TOPIC,
          name: topic,
          configs: desired.map((d) => ({ name: d.name, value: d.value })),
        },
      ],
      validateOnly: false,
    });
    return;
  }

  await admin.alterConfigs({
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
        configEntries: desired,
      },
    ],
    validateOnly: false,
  });
}
