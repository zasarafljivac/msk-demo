import { Producer } from "kafkajs";
import { Envelope, ControlMode } from "../transform";
import { log } from "../write/helpers";
import { getProducer } from "./helpers";

const region = process.env.AWS_REGION!;
const bootstrapBrokers = process.env.BOOTSTRAP_BROKERS_SASL_IAM!;
const controlTopic = process.env.CONTROL_TOPIC || `msk-demo-db-health`;

let producer: Producer | null = null;

export const handler = async (event: any) => {
  if (!producer) {
    producer = await getProducer("alarm-publisher", bootstrapBrokers, region);
  }

  const detail = event.detail || {};
  const state = detail.state?.value || "UNKNOWN";
  const alarmName = detail.alarmName || "unknown-alarm";

  // Map ALARM/OK/INSUFFICIENT_DATA to control modes
  let mode: ControlMode = "GREEN";
  if (state === "ALARM") mode = "RED";
  else if (state === "INSUFFICIENT_DATA") mode = "YELLOW";

  const controlEnvelope: Envelope = {
    entity: undefined as any,
    op: undefined as any,
    data: {},
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
        key: "db-mode", // compaction key
        value: JSON.stringify(controlEnvelope),
      },
    ],
  });

  log("info", "db.health.update", {
    mode,
    reason: alarmName,
    state,
    ts: new Date().toISOString(),
  });
};
