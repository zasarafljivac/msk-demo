import { CfnOutput, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { AuroraProxyConstruct } from '../cdk-constructs/aurora-proxy-construct';
import { BastionConstruct } from '../cdk-constructs/bastion-construct';
import { DatabaseStackProps } from '../app';

export class DatabaseStack extends Stack {
  readonly db: AuroraProxyConstruct;
  readonly bastion: BastionConstruct;
  constructor(scope: Construct, id: string, props: StackProps & DatabaseStackProps) {
    super(scope, id, props);
    this.db = new AuroraProxyConstruct(this, 'AuroraWithProxy', {
      vpc: props.vpc,
      dbSg: props.dbSg,
      proxySg: props.proxySg,
      dbName: 'ops',
    });

    this.bastion = new BastionConstruct(this, 'BastionHost', {
      vpc: props.vpc,
      bastionSg: props.bastionSg,
      dbSecret: this.db.secret,
      installKafkaTools: true,
      installMariadbClient: true,
    });

    new CfnOutput(this, 'BastionInstanceId', { 
      value: this.bastion.instance.instanceId 
    });

    new CfnOutput(this, 'BastionSsmStartSession', {
      value: `aws ssm start-session --target ${this.bastion.instance.instanceId} --region ${this.region}`
    });

    new CfnOutput(this, 'RdsProxyEndpoint', { 
      value: this.db.proxy.endpoint 
    });

    new CfnOutput(this, 'DbSecretArn', { 
      value: this.db.secret.secretArn 
    });

    new CfnOutput(this, 'RdsProxyName', { 
      value: this.db.proxy.dbProxyName 
    });
  }
}
