import type { StackProps } from 'aws-cdk-lib';
import { CfnOutput, Stack } from 'aws-cdk-lib';
import type { Construct } from 'constructs';

import { LambdaConstruct } from '../cdk-constructs/lambda-construct';
import { MetricsApi } from '../cdk-constructs/metrics-api-construct';
import type { DatabaseStack } from './database-stack';
import type { MskStack } from './msk-stack';
import type { NetworkStack } from './network-stack';

export class ComputeStack extends Stack {
  readonly lambdas: LambdaConstruct;
  constructor(
    scope: Construct,
    id: string,
    props: StackProps & {
      networkStack: NetworkStack;
      mskStack: MskStack;
      databaseStack: DatabaseStack;
      bootstrapBrokersSaslIam: string;
      BUFFER_TOPIC: string;
      SOURCE_TOPIC: string;
      CONTROL_TOPIC: string;
      CONNECTIONS_TABLE: string;
      WS_ENDPOINT: string;
    },
  ) {
    super(scope, id, props);

    this.lambdas = new LambdaConstruct(this, 'LambdaSet', {
      vpc: props.networkStack.net.vpc,
      lambdasSg: props.networkStack.net.lambdasSg,
      clusterArn: props.mskStack.msk.cluster.attrArn,
      clusterIdentifier: props.databaseStack.db.cluster.clusterIdentifier,
      rdsProxyName: props.databaseStack.db.proxy.dbProxyName,
      dbSecret: props.databaseStack.db.secret,
      rdsProxyEndpoint: props.databaseStack.db.proxy.endpoint,
      bootstrapBrokersSaslIam: props.bootstrapBrokersSaslIam,
      BUFFER_TOPIC: props.BUFFER_TOPIC,
      SOURCE_TOPIC: props.SOURCE_TOPIC,
      CONTROL_TOPIC: props.CONTROL_TOPIC,
      CONNECTIONS_TABLE: props.CONNECTIONS_TABLE,
      WS_ENDPOINT: props.WS_ENDPOINT,
    });

    const metrics = new MetricsApi(this, 'Metrics', {
      envVars: {
        CLUSTER: props.mskStack.msk.cluster.clusterName,
        RAW_TOPIC: props.SOURCE_TOPIC,
        BUFFER_TOPIC: props.BUFFER_TOPIC,
        CONSUMER_GROUP: 'write-consumer-group',
        CONSUMER_FN: this.lambdas.writeFn.functionName,
        WS_API_ID: props.WS_ENDPOINT.split('/')[2].split('.')[0],
        WS_STAGE: 'dev',
        SCHED_GROUP: 'default',
      },
      corsOrigin: '*',
    });

    new CfnOutput(this, 'MetricsApiUrl', {
      value: metrics.url,
    });

    new CfnOutput(this, 'LoadGeneratorFunction', {
      value: this.lambdas.loadGenFn.functionName,
    });
  }
}
