import * as cdk from 'aws-cdk-lib';
import * as apigwv2 from 'aws-cdk-lib/aws-apigatewayv2';
import * as integrations from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import * as lambdaNode from 'aws-cdk-lib/aws-lambda-nodejs';
import { Construct } from 'constructs';
import * as path from 'path';

export class MetricsApi extends Construct {
  public readonly url: string;

  constructor(
    scope: Construct,
    id: string,
    props: {
      envVars?: Record<string, string>;
      corsOrigin?: string;
    },
  ) {
    super(scope, id);

    const metricsApiFnLogGroup = new cdk.aws_logs.LogGroup(this, 'MetricsApiFnLogGroup', {
      retention: cdk.aws_logs.RetentionDays.ONE_DAY,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const fn = new lambdaNode.NodejsFunction(this, 'MetricsApiFunction', {
      runtime: Runtime.NODEJS_22_X,
      memorySize: 512,
      bundling: {
        target: 'node22',
        format: lambdaNode.OutputFormat.CJS,
        minify: true,
        sourceMap: false,
        nodeModules: ['@aws-sdk/client-kafka', 'aws-msk-iam-sasl-signer-js', 'kafkajs'],
      },
      entry: path.join(__dirname, './handlers/metrics/index.ts'),
      handler: 'handler',
      timeout: cdk.Duration.seconds(30),
      logGroup: metricsApiFnLogGroup,
      environment: props.envVars,
    });

    fn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['cloudwatch:GetMetricData', 'cloudwatch:ListMetrics'],
        resources: ['*'],
      }),
    );

    fn.role?.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );

    const api = new apigwv2.HttpApi(this, 'MetricsHttpApi', {
      corsPreflight: {
        allowHeaders: ['*'],
        allowMethods: [apigwv2.CorsHttpMethod.GET],
        allowOrigins: [props.corsOrigin ?? '*'],
      },
    });

    const integ = new integrations.HttpLambdaIntegration('MetricsInteg', fn);

    api.addRoutes({
      path: '/metrics/throughput',
      methods: [apigwv2.HttpMethod.GET],
      integration: integ,
    });
    api.addRoutes({ path: '/metrics/lag', methods: [apigwv2.HttpMethod.GET], integration: integ });
    api.addRoutes({
      path: '/metrics/lambda-quality',
      methods: [apigwv2.HttpMethod.GET],
      integration: integ,
    });
    api.addRoutes({ path: '/metrics/ws', methods: [apigwv2.HttpMethod.GET], integration: integ });
    api.addRoutes({
      path: '/metrics/scheduler',
      methods: [apigwv2.HttpMethod.GET],
      integration: integ,
    });

    this.url = api.apiEndpoint;
    new cdk.CfnOutput(this, 'MetricsBaseUrl', { value: `${this.url}/metrics` });
  }
}
