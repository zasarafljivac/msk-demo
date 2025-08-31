import * as cdk from 'aws-cdk-lib';
import type { ISecurityGroup, IVpc } from 'aws-cdk-lib/aws-ec2';

import { ComputeStack } from './cdk-stacks/compute-stack';
import { DatabaseStack } from './cdk-stacks/database-stack';
import { IntegrationStack } from './cdk-stacks/integration-stack';
import { MskStack } from './cdk-stacks/msk-stack';
import { NetworkStack } from './cdk-stacks/network-stack';

export interface DatabaseStackProps extends cdk.StackProps {
  vpc: IVpc;
  dbSg: ISecurityGroup;
  proxySg: ISecurityGroup;
  bastionSg: ISecurityGroup;
}

const BUFFER_TOPIC = 'msk-demo-buffer';
const SOURCE_TOPIC = 'msk-demo-source';
const CONTROL_TOPIC = 'msk-demo-db-health';

const app = new cdk.App();
const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const networkStack = new NetworkStack(app, 'MskDemo-Network', {
  env,
});

const dbProps: DatabaseStackProps = {
  vpc: networkStack.net.vpc,
  bastionSg: networkStack.net.bastionSg,
  proxySg: networkStack.net.proxySg,
  dbSg: networkStack.net.dbSg,
};

const mskStack = new MskStack(app, 'MskDemo-Msk', {
  env,
  networkStack,
});

const databaseStack = new DatabaseStack(app, 'MskDemo-Database', {
  env,
  ...dbProps,
});
const computeStack = new ComputeStack(app, 'MskDemo-Compute', {
  env,
  networkStack,
  mskStack,
  databaseStack,
  bootstrapBrokersSaslIam: mskStack.msk.bootstrapBrokersSaslIam,
  BUFFER_TOPIC,
  SOURCE_TOPIC,
  CONTROL_TOPIC,
});

new IntegrationStack(app, 'MskDemo-Integration', {
  env,
  mskStack,
  computeStack,
  SOURCE_TOPIC,
  BUFFER_TOPIC,
  CONTROL_TOPIC,
});
