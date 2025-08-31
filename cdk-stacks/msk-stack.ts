import type { StackProps } from 'aws-cdk-lib';
import { Stack } from 'aws-cdk-lib';
import type { Construct } from 'constructs';

import { MskClusterConstruct } from '../cdk-constructs/msk-cluster-construct';
import type { NetworkStack } from './network-stack';

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
