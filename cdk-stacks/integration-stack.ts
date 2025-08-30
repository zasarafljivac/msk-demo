import { Stack, StackProps, CustomResource } from "aws-cdk-lib";
import { Construct } from "constructs";
import { Provider } from "aws-cdk-lib/custom-resources";
import { EventSourcesConstruct } from "../cdk-constructs/event-sources-construct";
import { ComputeStack } from "./compute-stack";
import { MskStack } from "./msk-stack";

export class IntegrationStack extends Stack {
  constructor(
    scope: Construct,
    id: string,
    props: StackProps & {
      mskStack: MskStack;
      computeStack: ComputeStack;
      BUFFER_TOPIC: string;
      SOURCE_TOPIC: string;
      CONTROL_TOPIC: string;
    },
  ) {
    super(scope, id, props);

    const provider = new Provider(this, "CreateTopicProvider", {
      onEventHandler: props.computeStack.lambdas.createTopicFn,
    });
    const topicCreateCustomResource = new CustomResource(
      this,
      "CreateTopicsFetchBootstrap",
      {
        serviceToken: provider.serviceToken,
      },
    );

    const esms = new EventSourcesConstruct(this, "EsmsSourceBuffer", {
      mskArn: props.mskStack.msk.cluster.attrArn,
      sourceTopic: props.SOURCE_TOPIC,
      bufferTopic: props.BUFFER_TOPIC,
      controlTopic: props.CONTROL_TOPIC,
      transformFn: props.computeStack.lambdas.transformFn,
      writeFn: props.computeStack.lambdas.writeFn,
      healthFn: props.computeStack.lambdas.healthFn,
    });

    esms.node.addDependency(topicCreateCustomResource);
  }
}
