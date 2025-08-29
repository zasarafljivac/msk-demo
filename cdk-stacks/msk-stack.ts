import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { MskClusterConstruct } from '../cdk-constructs/msk-cluster-construct';
import { NetworkStack } from './network-stack';

export class MskStack extends Stack {
  readonly msk: MskClusterConstruct;
  constructor(scope: Construct, id: string, props: StackProps & { networkStack: NetworkStack }) {
    super(scope, id, props);
    this.msk = new MskClusterConstruct(this, 'MSK', {
      vpc: props.networkStack.net.vpc,
      mskSg: props.networkStack.net.mskSg,
    });
  }
}
