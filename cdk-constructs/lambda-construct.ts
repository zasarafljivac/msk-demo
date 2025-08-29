import { Construct } from 'constructs';
import { IVpc, ISecurityGroup } from 'aws-cdk-lib/aws-ec2';
import { ISecret } from 'aws-cdk-lib/aws-secretsmanager';
import { NodejsFunction, OutputFormat } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Runtime, Function as Fn, IFunction, LayerVersion } from 'aws-cdk-lib/aws-lambda';
import { Duration, RemovalPolicy, Stack } from 'aws-cdk-lib';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import * as path from 'path';
import * as iam from 'aws-cdk-lib/aws-iam';
import { cwd } from 'process';
import { Rule } from 'aws-cdk-lib/aws-events';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';
import { DbHealthAlarms } from './db-health-alarms-contruct';
import * as ssm from 'aws-cdk-lib/aws-ssm';

export interface LambdaConstructProps {
  vpc: IVpc;
  lambdasSg: ISecurityGroup;
  clusterArn: string;
  clusterIdentifier: string;
  dbSecret: ISecret;
  rdsProxyEndpoint: string;
  rdsProxyName: string;
  bootstrapBrokersSaslIam?: string;
  SOURCE_TOPIC: string;
  BUFFER_TOPIC: string;
  CONTROL_TOPIC: string;
}
export class LambdaConstruct extends Construct {
  readonly createSchemaFn: IFunction;
  readonly createTopicFn: IFunction;
  readonly loadGenFn: IFunction;
  readonly transformFn: IFunction;
  readonly writeFn: IFunction;
  readonly healthFn: IFunction;


  constructor(scope: Construct, id: string, props: LambdaConstructProps) {
    super(scope, id);

    const mkLog = (name: string) =>
      new LogGroup(this, `${name}LogGroup`, {
        retention: RetentionDays.ONE_WEEK,
        removalPolicy: RemovalPolicy.DESTROY,
      });


    const controlParam = new ssm.StringParameter(this, 'DbHealthModeParam', {
      parameterName: '/msk-demo/db-mode',
      stringValue: 'GREEN',
    });


    const TOPICS = JSON.stringify([props.SOURCE_TOPIC, props.BUFFER_TOPIC]);
    const CONTROL_TOPIC = props.CONTROL_TOPIC;

    const schemaEntry = path.join(__dirname, './handlers/schema/index.ts'); 
    const schemaFileRelFromEntryDir = './schema.sql';
    this.createSchemaFn = new NodejsFunction(this, 'CreateSchemaFunction', {
      runtime: Runtime.NODEJS_22_X,
      vpc: props.vpc,
      securityGroups: [props.lambdasSg],
      memorySize: 512,
      bundling: {
        target: 'node22',
        format: OutputFormat.CJS,
        minify: true,
        sourceMap: false,
        commandHooks: {
          beforeBundling() { return []; },
          beforeInstall()  { return []; },
          afterBundling(inputDir: string, outputDir: string) {
            const hostRel = path
              .relative(cwd(), path.join(path.dirname(schemaEntry), schemaFileRelFromEntryDir))
              .replace(/\\/g, '/');
            const srcInContainer = `${inputDir}/${hostRel}`;
            return [
              `cp "${srcInContainer}" "${outputDir}/schema.sql"`,
            ];
          },
        },
      },
      entry: schemaEntry,
      handler: 'handler',
      timeout: Duration.minutes(1),
      logGroup: mkLog('SchemaRunner'),
      environment: {
        RDS_PROXY_ENDPOINT: props.rdsProxyEndpoint,
        DB_SECRET_ARN: props.dbSecret.secretArn,
      },
    });
    props.dbSecret.grantRead(this.createSchemaFn);

    this.createTopicFn = new NodejsFunction(this, 'CreateTopicFunction', {
      runtime: Runtime.NODEJS_22_X,
      vpc: props.vpc,
      securityGroups: [props.lambdasSg],
      memorySize: 512,
      bundling: {
        target: 'node22',
        format: OutputFormat.CJS,
        minify: true,
        sourceMap: false,
        nodeModules: [
          '@aws-sdk/client-kafka',
          'aws-msk-iam-sasl-signer-js',
          'kafkajs',
        ]
      },
      entry: path.join(__dirname, './handlers/topics/index.ts'),
      handler: 'handler',
      timeout: Duration.seconds(60),
      logGroup: mkLog('TopicMaker'),
      environment: {
        CLUSTER_ARN: props.clusterArn,
        TOPICS,
        CONTROL_TOPIC,
      },
    });
    this.attachMskPolicies(this.createTopicFn);

    this.loadGenFn = new NodejsFunction(this, 'LoadGeneratorFunction', {
      runtime: Runtime.NODEJS_22_X,
      vpc: props.vpc,
      securityGroups: [props.lambdasSg],
      memorySize: 1024,
      bundling: {
        target: 'node22',
        format: OutputFormat.CJS,
        minify: true,
        sourceMap: false,
        nodeModules: [
          '@aws-sdk/client-kafka',
          'aws-msk-iam-sasl-signer-js',
          'kafkajs',
        ]
      },
      entry: path.join(__dirname, './handlers/loadgen/index.ts'),
      handler: 'handler',
      timeout: Duration.seconds(300),
      logGroup: mkLog('LoadGeneratorFunction'),
      environment: {
        CLUSTER_ARN: props.clusterArn,
        BOOTSTRAP_BROKERS_SASL_IAM: props.bootstrapBrokersSaslIam ?? '',
        TOPICS,
        SOURCE_TOPIC: props.SOURCE_TOPIC,
      },
    });

    this.loadGenFn.role?.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
    );
    this.loadGenFn.role?.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole')
    );
    this.attachMskPolicies(this.loadGenFn);

    this.transformFn = new NodejsFunction(this, 'TransformFunction', {
      runtime: Runtime.NODEJS_22_X,
      vpc: props.vpc,
      securityGroups: [props.lambdasSg],
      memorySize: 512,
      bundling: {
        target: 'node22',
        format: OutputFormat.CJS,
        minify: true,
        sourceMap: false,
        nodeModules: [
          '@aws-sdk/client-kafka',
          'aws-msk-iam-sasl-signer-js',
          'kafkajs',
        ]
      },
      entry: path.join(__dirname, './handlers/transform/index.ts'),
      handler: 'handler',
      timeout: Duration.seconds(60),
      logGroup: mkLog('TransformFunction'),
      environment: {
        BUFFER_TOPIC: props.BUFFER_TOPIC,
        BOOTSTRAP_BROKERS_SASL_IAM: props.bootstrapBrokersSaslIam ?? '',
      },
    });
    this.attachMskPolicies(this.transformFn);

    this.healthFn = new NodejsFunction(this, 'HealtAlarmFunction', {
      runtime: Runtime.NODEJS_22_X,
      vpc: props.vpc,
      securityGroups: [props.lambdasSg],
      memorySize: 512,
      bundling: {
        target: 'node22',
        format: OutputFormat.CJS,
        minify: true,
        sourceMap: false,
        nodeModules: [
          '@aws-sdk/client-kafka',
          'aws-msk-iam-sasl-signer-js',
          'kafkajs',
        ]
      },
      entry: path.join(__dirname, './handlers/health/index.ts'),
      handler: 'handler',
      reservedConcurrentExecutions: 1,
      timeout: Duration.seconds(10),
      logGroup: mkLog('HealthFunction'),
      environment: {
        CONTROL_TOPIC: props.CONTROL_TOPIC,
        BOOTSTRAP_BROKERS_SASL_IAM: props.bootstrapBrokersSaslIam ?? '',
      },
    });
    this.attachMskPolicies(this.healthFn);

    new Rule(this, 'DbHealthRule', {
      eventPattern: {
        source: ['aws.cloudwatch'],
        detailType: ['CloudWatch Alarm State Change'],
        detail: {
          state: { value: ['ALARM', 'OK', 'INSUFFICIENT_DATA'] }
        }
      },
      targets: [new LambdaFunction(this.healthFn)]
    });
  
    new DbHealthAlarms(this, 'DbHealthAlarms', {
      proxyName: props.rdsProxyName,
      clusterId: props.clusterIdentifier,
      alarmLambda: this.healthFn,
    });

    const layer = LayerVersion.fromLayerVersionArn(this, 'SecretLayer', 'arn:aws:lambda:us-east-1:177933569100:layer:AWS-Parameters-and-Secrets-Lambda-Extension:18');

    this.writeFn = new NodejsFunction(this, 'WriteFunction', {
      runtime: Runtime.NODEJS_22_X,
      vpc: props.vpc,
      securityGroups: [props.lambdasSg],
      memorySize: 1024,
      bundling: {
        target: 'node22',
        format: OutputFormat.CJS,
        minify: true,
        sourceMap: false,
      },
      entry: path.join(__dirname, './handlers/write/index.ts'),
      handler: 'handler',
      timeout: Duration.seconds(10),
      reservedConcurrentExecutions: 5,
      logGroup: mkLog('WriteFunction'),
      environment: {
        RDS_PROXY_ENDPOINT: props.rdsProxyEndpoint,
        DB_SECRET_ARN: props.dbSecret.secretArn,
        INSERT_CONCURRENCY: '2',
        INSERT_RATE_CAP: '10',
        BOOTSTRAP_BROKERS_SASL_IAM: props.bootstrapBrokersSaslIam ?? '',
        DB_NAME: 'ops',
        PARAMETERS_SECRETS_EXTENSION_CACHE_ENABLED: 'TRUE',
        SECRETS_MANAGER_TTL: '300',
        PARAMETERS_SECRETS_EXTENSION_CACHE_SIZE: '1000',
        DB_CONTROL_PARAM: '/msk-demo/db-mode',
        DEFAULT_MODE: 'GREEN'
      },
      layers: [
        layer
      ]
    });
 
    props.dbSecret.grantRead(this.writeFn);
    this.attachMskPolicies(this.writeFn);
    this.writeFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['secretsmanager:GetSecretValue'],
      resources: [props.dbSecret.secretArn],
    }));
    this.writeFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['kms:Decrypt'],
      resources: ['*'], 
    }));
    this.writeFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['ssm:GetParameter'],
      resources: [controlParam.parameterArn],
    }));
  }

  // WARNING: TOO BROAD!!! make it narrower for real use case. 
  private attachMskPolicies(fn: IFunction) {
    fn.addToRolePolicy(new iam.PolicyStatement({
      // https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control-use-cases.html
      actions: [
        'kafka-cluster:*'        
      ], 
      resources: ['*']
    }));
    fn.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'kafka:GetBootstrapBrokers',
        'kafka:DescribeCluster',
        'kafka:DescribeClusterV2',
        'kafka:ListNodes'
      ],
      resources: ['*']
    }));
    fn.role?.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaMSKExecutionRole')
    );
  }

}


