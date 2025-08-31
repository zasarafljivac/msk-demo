import type { StackProps } from 'aws-cdk-lib';
import { Stack } from 'aws-cdk-lib';
import type { Construct } from 'constructs';

import { NetworkConstruct } from '../cdk-constructs/network-construct';

export class NetworkStack extends Stack {
  readonly net: NetworkConstruct;
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    this.net = new NetworkConstruct(this, 'NetworkConstruct', { maxAzs: 3 });
  }
}
