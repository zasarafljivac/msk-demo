import type { StackProps } from 'aws-cdk-lib';
import { CfnOutput, Stack } from 'aws-cdk-lib';
import type { Construct } from 'constructs';

import { LambdaConstruct } from '../cdk-constructs/lambda-construct';
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
    });

    new CfnOutput(this, 'LoadGeneratorFunction', {
      value: this.lambdas.loadGenFn.functionName,
    });
  }
}
