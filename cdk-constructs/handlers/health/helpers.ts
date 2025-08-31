import { generateAuthToken } from 'aws-msk-iam-sasl-signer-js';
import type { Producer } from 'kafkajs';
import { Kafka, Partitioners } from 'kafkajs';

export async function getProducer(
  clientId: string,
  bootstrapBrokers: string,
  region: string,
): Promise<Producer> {
  const kafka = new Kafka({
    clientId,
    brokers: bootstrapBrokers.split(','),
    ssl: true,
    sasl: {
      mechanism: 'oauthbearer',
      oauthBearerProvider: async () => {
        const { token } = await generateAuthToken({ region });
        return { value: token };
      },
    },
  });

  const producer = kafka.producer({
    allowAutoTopicCreation: false,
    idempotent: true,
    createPartitioner: Partitioners.DefaultPartitioner,
  });

  await producer.connect();
  return producer;
}
