import { Construct } from "constructs";
import { CfnCluster } from "aws-cdk-lib/aws-msk";
import { LogGroup, RetentionDays } from "aws-cdk-lib/aws-logs";
import { CfnOutput, RemovalPolicy, Stack } from "aws-cdk-lib";
import { IVpc, ISecurityGroup } from "aws-cdk-lib/aws-ec2";
import * as cr from "aws-cdk-lib/custom-resources";

export interface MskClusterConstructProps {
  vpc: IVpc;
  mskSg: ISecurityGroup;
}
export class MskClusterConstruct extends Construct {
  readonly logGroup: LogGroup;
  readonly cluster: CfnCluster;
  readonly bootstrapBrokersSaslIam: string;

  constructor(scope: Construct, id: string, props: MskClusterConstructProps) {
    super(scope, id);

    this.logGroup = new LogGroup(this, "MskLogs", {
      retention: RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    this.cluster = new CfnCluster(this, "Msk", {
      clusterName: `${Stack.of(this).stackName}-demo`,
      kafkaVersion: "3.8.x",
      numberOfBrokerNodes: 3,
      brokerNodeGroupInfo: {
        clientSubnets: props.vpc.privateSubnets.map((s) => s.subnetId),
        securityGroups: [props.mskSg.securityGroupId],
        instanceType: "kafka.t3.small",
        storageInfo: { ebsStorageInfo: { volumeSize: 100 } },
      },
      clientAuthentication: {
        sasl: {
          iam: {
            enabled: true,
          },
        },
      },
      encryptionInfo: {
        encryptionInTransit: {
          clientBroker: "TLS",
          inCluster: true,
        },
      },
      loggingInfo: {
        brokerLogs: {
          cloudWatchLogs: {
            enabled: true,
            logGroup: this.logGroup.logGroupName,
          },
        },
      },
    });

    const getBrokers = new cr.AwsCustomResource(this, "GetBootstrapBrokers", {
      onUpdate: {
        service: "Kafka",
        action: "getBootstrapBrokers",
        parameters: { ClusterArn: this.cluster.attrArn },
        physicalResourceId: cr.PhysicalResourceId.of(
          `GetBootstrapBrokers-${id}`,
        ),
      },
      policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
        resources: [this.cluster.attrArn],
      }),
    });

    this.bootstrapBrokersSaslIam = getBrokers.getResponseField(
      "BootstrapBrokerStringSaslIam",
    );

    new CfnOutput(this, "BootstrapBrokersSaslIam", {
      value: this.bootstrapBrokersSaslIam,
    });
  }
}
