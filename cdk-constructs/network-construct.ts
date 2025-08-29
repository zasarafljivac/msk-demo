import { Construct } from 'constructs';
import { Vpc, SecurityGroup, Port, Peer } from 'aws-cdk-lib/aws-ec2';

export interface NetworkConstructProps {
  maxAzs?: number;
}
export class NetworkConstruct extends Construct {
  readonly vpc: Vpc;
  readonly mskSg: SecurityGroup;
  readonly lambdasSg: SecurityGroup;
  readonly dbSg: SecurityGroup;
  readonly proxySg: SecurityGroup;
  readonly bastionSg: SecurityGroup;

  constructor(scope: Construct, id: string, props: NetworkConstructProps = {}) {
    super(scope, id);

    this.vpc = new Vpc(this, 'Vpc', { maxAzs: props.maxAzs ?? 3, natGateways: 1 });

    this.mskSg = new SecurityGroup(this, 'MskSg', { vpc: this.vpc, allowAllOutbound: true });
    this.lambdasSg = new SecurityGroup(this, 'LambdasSg', { vpc: this.vpc, allowAllOutbound: false });
    this.dbSg = new SecurityGroup(this, 'DbSg', { vpc: this.vpc, allowAllOutbound: true });
    this.proxySg = new SecurityGroup(this, 'ProxySg', { vpc: this.vpc, allowAllOutbound: true });
    this.bastionSg = new SecurityGroup(this, 'BastionSg', { vpc: this.vpc, allowAllOutbound: true });

    this.dbSg.addIngressRule(this.proxySg, Port.tcp(3306), 'Proxy to DB 3306');
    this.proxySg.addIngressRule(this.lambdasSg, Port.tcp(3306), 'Lambda to Proxy 3306');
    this.mskSg.addIngressRule(this.lambdasSg, Port.tcp(9098), 'Kafka broker (IAM/TLS)');
    this.mskSg.addIngressRule(this.mskSg, Port.tcp(9098), 'MSK self');
    this.mskSg.addIngressRule(this.bastionSg, Port.tcp(9098), 'Bastion to MSK');
    this.proxySg.addIngressRule(this.bastionSg, Port.tcp(3306), 'Bastion to RDS Proxy');

    this.lambdasSg.addEgressRule(this.mskSg, Port.tcp(9098), 'To MSK');
    this.lambdasSg.addEgressRule(this.proxySg, Port.tcp(3306), 'To RDS Proxy');
    this.lambdasSg.addEgressRule(Peer.ipv4(this.vpc.vpcCidrBlock), Port.udp(53), 'DNS UDP');
    this.lambdasSg.addEgressRule(Peer.ipv4(this.vpc.vpcCidrBlock), Port.tcp(53), 'DNS TCP');
    this.lambdasSg.addEgressRule(Peer.anyIpv4(), Port.tcp(443), 'HTTPS');
  }
}