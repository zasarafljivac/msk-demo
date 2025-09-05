import * as cdk from 'aws-cdk-lib';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import type * as lambda from 'aws-cdk-lib/aws-lambda';
import * as scheduler from 'aws-cdk-lib/aws-scheduler';
import { Construct } from 'constructs';

interface EBConstructProps extends cdk.StackProps {
  loadGenFn: lambda.IFunction;
}

export class EventBridgeConstruct extends Construct {
  constructor(scope: Construct, id: string, props: EBConstructProps) {
    super(scope, id);

    const bus = new events.EventBus(this, 'LoadGenBurstBus');

    const rule = new events.Rule(this, 'LoadGenBurstRule', {
      eventBus: bus,
      eventPattern: { source: ['burst.scheduler'] },
    });

    rule.addTarget(
      new targets.LambdaFunction(props.loadGenFn, {
        retryAttempts: 0,
        maxEventAge: cdk.Duration.minutes(1),
      }),
    );

    const schedulerRole = new iam.Role(this, 'SchedulerPutEventsRole', {
      assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com'),
    });
    schedulerRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['events:PutEvents'],
        resources: [bus.eventBusArn],
      }),
    );

    const makeBurstSchedule = (
      idSuffix: string,
      detailType: string,
      rate: number,
      seconds: number,
      state: 'ENABLED' | 'DISABLED',
    ) =>
      new scheduler.CfnSchedule(this, `LoadGenBurst-${idSuffix}`, {
        flexibleTimeWindow: { mode: 'OFF' },
        scheduleExpressionTimezone: 'UTC',
        scheduleExpression: 'cron(30 * * * ? *)',
        state, // ENABLED or DISABLED
        target: {
          arn: 'arn:aws:scheduler:::aws-sdk:eventbridge:putEvents',
          roleArn: schedulerRole.roleArn,
          input: JSON.stringify({
            Entries: [
              {
                Source: 'burst.scheduler',
                DetailType: detailType,
                EventBusName: bus.eventBusName,
                Detail: JSON.stringify({ rate, seconds }),
              },
            ],
          }),
        },
        description: `Emit burst event (${rate}/s for ${seconds}s) at :30 every hour`,
      });

    makeBurstSchedule('100rps', 'hourly-burst-100', 100, 60, 'DISABLED');
    makeBurstSchedule('400rps', 'hourly-burst-400', 400, 60, 'ENABLED');
  }
}
