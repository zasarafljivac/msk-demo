import type { IFunction } from 'aws-cdk-lib/aws-lambda';
import { CfnEventSourceMapping } from 'aws-cdk-lib/aws-lambda';
import { Construct } from 'constructs';

export interface EventSourcesConstructProps {
  mskArn: string;
  sourceTopic: string;
  bufferTopic: string;
  controlTopic: string;
  transformFn: IFunction;
  writeFn: IFunction;
  healthFn: IFunction;
}
export class EventSourcesConstruct extends Construct {
  constructor(scope: Construct, id: string, props: EventSourcesConstructProps) {
    super(scope, id);

    const esm1 = new CfnEventSourceMapping(this, 'EsmSource', {
      functionName: props.transformFn.functionArn,
      eventSourceArn: props.mskArn,
      topics: [props.sourceTopic],
      batchSize: 100,
      maximumBatchingWindowInSeconds: 1,
      startingPosition: 'LATEST',
      amazonManagedKafkaEventSourceConfig: {
        consumerGroupId: 'transform-source-consumer-group',
      },
    });

    const esm2 = new CfnEventSourceMapping(this, 'EsmBuffer', {
      functionName: props.writeFn.functionArn,
      eventSourceArn: props.mskArn,
      topics: [props.bufferTopic],
      batchSize: 100,
      maximumBatchingWindowInSeconds: 1,
      startingPosition: 'LATEST',
      amazonManagedKafkaEventSourceConfig: {
        consumerGroupId: 'write-buffer-consumer-group',
      },
    });

    new CfnEventSourceMapping(this, 'EsmDbHealth', {
      functionName: props.writeFn.functionArn,
      eventSourceArn: props.mskArn,
      topics: [props.controlTopic],
      batchSize: 10,
      maximumBatchingWindowInSeconds: 1,
      startingPosition: 'LATEST',
    });

    esm2.addDependency(esm1);
  }
}
