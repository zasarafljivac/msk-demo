import { Kafka, Partitioners, Producer } from "kafkajs";
import { generateAuthToken } from 'aws-msk-iam-sasl-signer-js';

export async function getProducer(clientId: string, bootstrapBrokers: string, region: string): Promise<Producer> {
  let producer: Producer;
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

  producer = kafka.producer({
    allowAutoTopicCreation: false,
    idempotent: true,
    createPartitioner: Partitioners.DefaultPartitioner,
  });

  await producer.connect();
  return producer;
}