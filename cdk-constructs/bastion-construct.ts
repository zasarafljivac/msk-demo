import { Construct } from "constructs";
import {
  Instance,
  InstanceClass,
  InstanceSize,
  InstanceType,
  IVpc,
  ISecurityGroup,
  MachineImage,
  SubnetType,
  CloudFormationInit,
  InitCommand,
  InitPackage,
  InitFile,
} from "aws-cdk-lib/aws-ec2";
import {
  Role,
  ManagedPolicy,
  ServicePrincipal,
  PolicyStatement,
} from "aws-cdk-lib/aws-iam";
import { ISecret } from "aws-cdk-lib/aws-secretsmanager";
import { Duration } from "aws-cdk-lib";

export interface BastionConstructProps {
  vpc: IVpc;
  bastionSg: ISecurityGroup;
  subnetsType?: SubnetType;
  dbSecret?: ISecret;
  installKafkaTools?: boolean;
  installMariadbClient?: boolean;
}

export class BastionConstruct extends Construct {
  readonly role: Role;
  readonly instance: Instance;

  constructor(scope: Construct, id: string, props: BastionConstructProps) {
    super(scope, id);

    this.role = new Role(this, "BastionRole", {
      assumedBy: new ServicePrincipal("ec2.amazonaws.com"),
      description: "Role for SSM-managed bastion",
    });
    this.role.addManagedPolicy(
      ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMManagedInstanceCore"),
    );

    if (props.dbSecret) {
      this.role.addToPolicy(
        new PolicyStatement({
          actions: ["secretsmanager:GetSecretValue"],
          resources: [props.dbSecret.secretArn],
        }),
      );
    }

    this.instance = new Instance(this, "Bastion", {
      vpc: props.vpc,
      vpcSubnets: {
        subnetType: props.subnetsType ?? SubnetType.PRIVATE_WITH_EGRESS,
      },
      securityGroup: props.bastionSg,
      role: this.role,
      instanceType: InstanceType.of(InstanceClass.T3, InstanceSize.MICRO),
      machineImage: MachineImage.latestAmazonLinux2023(),
      detailedMonitoring: false,
      ssmSessionPermissions: true,
    });

    const init = CloudFormationInit.fromElements(
      InitCommand.shellCommand("set -euxo pipefail"),
      InitPackage.yum("jq"),
      InitPackage.yum("tar"),
      InitPackage.yum("gzip"),
      InitPackage.yum("curl"),

      InitPackage.yum("mariadb"),
      InitPackage.yum("java-17-amazon-corretto-headless"),

      InitCommand.shellCommand("mkdir -p /opt/kafka"),
      InitCommand.shellCommand(
        "KVER=3.8.1 SCALA=2.13; " +
          "test -f /opt/kafka/bin/kafka-topics.sh " +
          '|| (curl -fSL -o /tmp/kafka.tgz "https://archive.apache.org/dist/kafka/${KVER}/kafka_${SCALA}-${KVER}.tgz" ' +
          "&& tar -xzf /tmp/kafka.tgz -C /opt/kafka --strip-components=1 && rm -f /tmp/kafka.tgz)",
      ),

      InitFile.fromString(
        "/opt/kafka/client.properties",
        [
          "security.protocol=SASL_SSL",
          "sasl.mechanism=AWS_MSK_IAM",
          "sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;",
          "sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler",
          "client.dns.lookup=use_all_dns_ips",
          "",
        ].join("\n"),
        { mode: "000644" },
      ),

      InitFile.fromString(
        "/etc/profile.d/kafka.sh",
        'export PATH=/opt/kafka/bin:$PATH\nexport KAFKA_HEAP_OPTS="-Xms128m -Xmx512m"\n',
        { mode: "000644" },
      ),
    );

    this.instance.applyCloudFormationInit(init, {
      printLog: true,
      embedFingerprint: true,
      timeout: Duration.minutes(15),
    });
  }
}
