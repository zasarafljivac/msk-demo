import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as lambda from "aws-cdk-lib/aws-lambda";

export interface DbHealthAlarmsProps {
  proxyName: string;
  clusterId: string;
  alarmLambda: lambda.IFunction;
}

export class DbHealthAlarms extends Construct {
  constructor(scope: Construct, id: string, props: DbHealthAlarmsProps) {
    super(scope, id);

    const clientConnections = new cloudwatch.Metric({
      namespace: "AWS/RDS",
      metricName: "ClientConnections",
      dimensionsMap: { DBProxyName: props.proxyName },
      statistic: "Maximum",
      period: cdk.Duration.minutes(1),
    });

    const dbConnections = new cloudwatch.Metric({
      namespace: "AWS/RDS",
      metricName: "DatabaseConnections",
      dimensionsMap: { DBProxyName: props.proxyName },
      statistic: "Maximum",
      period: cdk.Duration.minutes(1),
    });

    const pinnedConnections = new cloudwatch.Metric({
      namespace: "AWS/RDS",
      metricName: "DatabaseConnectionsCurrentlySessionPinned",
      dimensionsMap: { DBProxyName: props.proxyName },
      statistic: "Maximum",
      period: cdk.Duration.minutes(1),
    });

    const cpuUtil = new cloudwatch.Metric({
      namespace: "AWS/RDS",
      metricName: "CPUUtilization",
      dimensionsMap: { DBClusterIdentifier: props.clusterId },
      statistic: "Average",
      period: cdk.Duration.minutes(1),
    });

    const alarms: cloudwatch.Alarm[] = [
      new cloudwatch.Alarm(this, "ProxyClientConnectionsHigh", {
        metric: clientConnections,
        threshold: 50, // DEMO!!! production shouild be higher
        evaluationPeriods: 1,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        alarmDescription: "High number of client connections to RDS Proxy",
      }),

      new cloudwatch.Alarm(this, "ProxyDatabaseConnectionsHigh", {
        metric: dbConnections,
        threshold: 20, // DEMO!!! production shouild be higher
        evaluationPeriods: 1,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        alarmDescription: "High number of DB connections from RDS Proxy",
      }),

      new cloudwatch.Alarm(this, "ProxySessionPinnedHigh", {
        metric: pinnedConnections,
        threshold: 5, // DEMO!!! production shouild be higher
        evaluationPeriods: 1,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        alarmDescription: "Many session-pinned DB connections",
      }),

      new cloudwatch.Alarm(this, "AuroraHighCpu", {
        metric: cpuUtil,
        threshold: 40, // DEMO!!! production shouild be higher
        evaluationPeriods: 1,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        alarmDescription: "Aurora CPU utilization > 40%",
      }),
    ];

    const rule = new events.Rule(this, "DbHealthRule", {
      eventPattern: {
        source: ["aws.cloudwatch"],
        detailType: ["CloudWatch Alarm State Change"],
        detail: {
          state: { value: ["ALARM", "OK", "INSUFFICIENT_DATA"] },
          alarmName: alarms.map((a) => a.alarmName),
        },
      },
    });

    rule.addTarget(new targets.LambdaFunction(props.alarmLambda));
  }
}
