import path from 'node:path';

import * as cdk from 'aws-cdk-lib';
import * as apigatewayv2 from 'aws-cdk-lib/aws-apigatewayv2';
import * as integrations from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Function, Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction, OutputFormat } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup } from 'aws-cdk-lib/aws-logs';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import type { Construct } from 'constructs';

import type { WebSocketStackProps } from '../app';

export class WebSocketStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: WebSocketStackProps) {
    super(scope, id, props);

    const connectionsTable = new dynamodb.Table(this, 'ConnectionsTable', {
      tableName: 'msk-demo-app',
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const connectionHandlerLogGroup = new LogGroup(this, 'ConnectionHandlerLogGroup', {
      retention: cdk.aws_logs.RetentionDays.ONE_DAY,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const connectionHandler = new NodejsFunction(this, 'ConnectionHandler', {
      runtime: Runtime.NODEJS_22_X,
      memorySize: 512,
      bundling: {
        target: 'node22',
        format: OutputFormat.CJS,
        minify: true,
        sourceMap: false,
      },
      entry: path.join(__dirname, '../cdk-constructs/handlers/ws/connections.ts'),
      handler: 'handler',
      timeout: cdk.Duration.seconds(10),
      environment: {
        TABLE: connectionsTable.tableName,
      },
      logGroup: connectionHandlerLogGroup,
    });

    connectionsTable.grantReadWriteData(connectionHandler);
    connectionHandler.role?.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );

    const actionHandlerLogGroup = new LogGroup(this, 'ActionsHandlerLogGroup', {
      retention: cdk.aws_logs.RetentionDays.ONE_DAY,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const actionsHandler = new NodejsFunction(this, 'ActionsHandler', {
      runtime: Runtime.NODEJS_22_X,
      memorySize: 512,
      bundling: {
        target: 'node22',
        format: OutputFormat.CJS,
        minify: true,
        sourceMap: false,
      },
      entry: path.join(__dirname, '../cdk-constructs/handlers/ws/actions.ts'),
      handler: 'handler',
      timeout: cdk.Duration.seconds(10),
      environment: {
        TABLE: connectionsTable.tableName,
      },
      logGroup: actionHandlerLogGroup,
    });

    connectionsTable.grantReadWriteData(actionsHandler);
    actionsHandler.role?.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
    );

    const webSocketApi = new apigatewayv2.WebSocketApi(this, 'WebSocketApi', {
      connectRouteOptions: {
        integration: new integrations.WebSocketLambdaIntegration(
          'ConnectIntegration',
          connectionHandler,
        ),
      },
      disconnectRouteOptions: {
        integration: new integrations.WebSocketLambdaIntegration(
          'DisconnectIntegration',
          connectionHandler,
        ),
      },
    });

    new apigatewayv2.WebSocketStage(this, 'WebSocketStage', {
      webSocketApi,
      stageName: 'dev',
      autoDeploy: true,
    });

    webSocketApi.addRoute('stats.refresh', {
      integration: new integrations.WebSocketLambdaIntegration(
        'ActionsIntegration',
        actionsHandler,
      ),
    });

    const kafkaConsumerLambda = Function.fromFunctionAttributes(this, 'ImportedKafkaConsumer', {
      functionArn: props.writeFnArn,
      sameEnvironment: true,
      role: iam.Role.fromRoleArn(this, 'ImportedKafkaConsumerRole', props.writeFnRoleArn, {
        mutable: true,
      }),
    });

    const tableParam = new ssm.StringParameter(this, 'ConnectionsTableParam', {
      parameterName: `/msk-demo/connections-table`,
      stringValue: connectionsTable.tableName,
    });

    const wsEndpointParam = new ssm.StringParameter(this, 'WsEndpointParam', {
      parameterName: `/msk-demo/ws-endpoint`,
      stringValue: `${webSocketApi.apiEndpoint}/dev`,
    });

    connectionsTable.grantReadData(kafkaConsumerLambda);
    tableParam.grantRead(kafkaConsumerLambda);
    wsEndpointParam.grantRead(kafkaConsumerLambda);

    const getParamsPolicy = new iam.PolicyStatement({
      actions: ['ssm:GetParameter', 'ssm:GetParameters'],
      resources: [tableParam.parameterArn, wsEndpointParam.parameterArn],
    });

    const getDynamoPolicy = new iam.PolicyStatement({
      actions: [
        'dynamodb:Scan',
        'dynamodb:Query',
        'dynamodb:UpdateItem',
        'dynamodb:GetItem',
        'dynamodb:DeleteItem',
        'dynamodb:PutItem',
      ],
      resources: [connectionsTable.tableArn],
    });

    const getManagedConnectPolicy = new iam.PolicyStatement({
      actions: ['execute-api:ManageConnections'],
      resources: [
        webSocketApi.arnForExecuteApiV2(),
        `${webSocketApi.arnForExecuteApiV2()}/*/POST/@connections/*`,
      ],
    });

    kafkaConsumerLambda.role?.addToPrincipalPolicy(getParamsPolicy);
    kafkaConsumerLambda.role?.addToPrincipalPolicy(getDynamoPolicy);
    kafkaConsumerLambda.role?.addToPrincipalPolicy(getManagedConnectPolicy);

    actionsHandler.role?.addToPrincipalPolicy(getManagedConnectPolicy);

    new cdk.CfnOutput(this, 'WebSocketUrl', {
      value: webSocketApi.apiEndpoint,
    });

    new cdk.CfnOutput(this, 'ConnectionsTableParamOutput', {
      value: tableParam.stringValue,
    });

    new cdk.CfnOutput(this, 'WsEndpointParamOutput', {
      value: `${webSocketApi.apiEndpoint}/dev`,
    });
  }
}
