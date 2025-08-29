import { Kafka as KafkaJS } from 'kafkajs';
import { Kafka as KafkaApi, GetBootstrapBrokersCommand } from "@aws-sdk/client-kafka";
import { generateAuthToken } from 'aws-msk-iam-sasl-signer-js';
import { log } from '../write/helpers';
import { compactTopic, waitForMskActive } from './helpers';

const region = process.env.AWS_REGION!;
const clusterArn = process.env.CLUSTER_ARN!;
const topics = JSON.parse(process.env.TOPICS ?? '[]') as string[];
const controlTopic = process.env.CONTROL_TOPIC ?? 'msk-demo-db-health';

export const handler = async (event:any) => {
  log('info', 'kafka.admin.create.waiting');
  await waitForMskActive(300000, region, clusterArn);
  const mk = new KafkaApi({});
  const out = await mk.send(new GetBootstrapBrokersCommand({ ClusterArn: clusterArn }));
  const bootstrap = out.BootstrapBrokerStringSaslIam || '';
  log('info', 'kafka.admin.create.login');
  const kafka = new KafkaJS({
    clientId: 'topic-maker=buffer-source-health',
    brokers: bootstrap.split(','),
    ssl: true,
    sasl: {
      mechanism: 'oauthbearer',
      oauthBearerProvider: async () => {
        const { token } = await generateAuthToken({
          region,
        });
        return { value: token };
      }
    },  
  });
  try {
    const admin = kafka.admin();
    await admin.connect();

    const existingTopics = await admin.listTopics();
    const missingDataTopics = topics.filter(t => !existingTopics.includes(t));

    if (missingDataTopics.length > 0) {
      log('info', 'kafka.admin.create.sourceBuffer', { missingDataTopics });
      await admin.createTopics({
        topics: missingDataTopics.map(t => ({
          topic: t,
          numPartitions: 5,
          replicationFactor: 3,
        })),
        waitForLeaders: true,
      });
    } else {
      log('info', 'kafka.admin.update.sourceBuffer', { controlTopic });

    }

    const missingControl = !existingTopics.includes(controlTopic);

    if (missingControl) {
      log('info', 'kafka.admin.create.controlTopic', { controlTopic });
      await admin.createTopics({
        topics: [
          {
            topic: controlTopic,
            numPartitions: 1,
            replicationFactor: 3,
            configEntries: [
              { name: 'cleanup.policy', value: 'compact' },
              { name: 'min.cleanable.dirty.ratio', value: '0.01' },
              { name: 'segment.ms', value: '600000' },
            ],
          },
        ],
        waitForLeaders: true,
      });
    } else {
      log('info', 'kafka.admin.compact.controlTopic', { controlTopic });
      await compactTopic(admin, controlTopic);
    }
    await admin.disconnect();
  } catch (error) {
    log('debug', 'kafka.admin.create.topics', error);
  }

  return {
    PhysicalResourceId: 'TopicMakerSourceBuffer',
    Data: { BootstrapBrokersSaslIam: bootstrap }
  };
};
