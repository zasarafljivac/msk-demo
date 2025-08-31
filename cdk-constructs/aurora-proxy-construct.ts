import { Duration, RemovalPolicy } from 'aws-cdk-lib';
import type { ISecurityGroup, IVpc } from 'aws-cdk-lib/aws-ec2';
import {
  InstanceClass,
  InstanceSize,
  InstanceType,
  SecurityGroup,
  SubnetType,
} from 'aws-cdk-lib/aws-ec2';
import { ManagedPolicy, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import {
  AuroraMysqlEngineVersion,
  ClientPasswordAuthType,
  ClusterInstance,
  Credentials,
  DatabaseCluster,
  DatabaseClusterEngine,
  DatabaseProxy,
  DatabaseSecret,
  PerformanceInsightRetention,
  ProxyTarget,
  SessionPinningFilter,
} from 'aws-cdk-lib/aws-rds';
import { Construct } from 'constructs';

export interface AuroraProxyConstructProps {
  vpc: IVpc;
  dbSg: ISecurityGroup;
  proxySg: ISecurityGroup;
  dbName?: string;
}
export class AuroraProxyConstruct extends Construct {
  readonly secret: DatabaseSecret;
  readonly cluster: DatabaseCluster;
  readonly proxy: DatabaseProxy;

  constructor(scope: Construct, id: string, props: AuroraProxyConstructProps) {
    super(scope, id);

    const dbSg = SecurityGroup.fromSecurityGroupId(
      this,
      'DbSgImported',
      props.dbSg.securityGroupId,
      {
        mutable: true,
      },
    );
    const proxySg = SecurityGroup.fromSecurityGroupId(
      this,
      'ProxySg',
      props.proxySg.securityGroupId,
      {
        mutable: false,
      },
    );

    this.secret = new DatabaseSecret(this, 'DbSecret', {
      username: 'opsadmin',
    });

    this.cluster = new DatabaseCluster(this, 'Aurora', {
      engine: DatabaseClusterEngine.auroraMysql({
        version: AuroraMysqlEngineVersion.VER_3_10_0,
      }),
      credentials: Credentials.fromSecret(this.secret),
      defaultDatabaseName: props.dbName ?? 'ops',
      vpc: props.vpc,
      securityGroups: [dbSg],
      vpcSubnets: { subnetType: SubnetType.PRIVATE_WITH_EGRESS },
      writer: ClusterInstance.provisioned('Writer', {
        instanceType: InstanceType.of(InstanceClass.T4G, InstanceSize.MEDIUM),
        enablePerformanceInsights: true,
      }),
      storageEncrypted: true,
      performanceInsightRetention: PerformanceInsightRetention.DEFAULT,
      removalPolicy: RemovalPolicy.DESTROY,
      deletionProtection: false,
      enableClusterLevelEnhancedMonitoring: true,
      enablePerformanceInsights: true,
      monitoringInterval: Duration.seconds(60),
      monitoringRole: new Role(this, 'MonitoringRole', {
        assumedBy: new ServicePrincipal('monitoring.rds.amazonaws.com'),
        managedPolicies: [
          ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonRDSEnhancedMonitoringRole'),
        ],
      }),
    });

    this.proxy = new DatabaseProxy(this, 'Proxy', {
      proxyTarget: ProxyTarget.fromCluster(this.cluster),
      secrets: [this.secret],
      vpc: props.vpc,
      securityGroups: [proxySg],
      vpcSubnets: { subnetType: SubnetType.PRIVATE_WITH_EGRESS },
      requireTLS: true,
      borrowTimeout: Duration.seconds(120),
      maxConnectionsPercent: 30,
      maxIdleConnectionsPercent: 0,
      clientPasswordAuthType: ClientPasswordAuthType.MYSQL_NATIVE_PASSWORD,
      sessionPinningFilters: [SessionPinningFilter.EXCLUDE_VARIABLE_SETS],
    });
  }
}
