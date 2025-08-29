SHELL := /bin/bash
REGION ?= $(shell echo $${AWS_REGION:-$${CDK_DEFAULT_REGION:-us-east-1}})
PROFILE ?= default
CDK ?= npx cdk
ACCOUNT ?= $(shell aws sts get-caller-identity --query Account --output text --profile $(PROFILE))
CONTEXT ?=

export CDK_DEFAULT_ACCOUNT ?= $(ACCOUNT)
export CDK_DEFAULT_REGION  ?= $(REGION)
CDK_FLAGS ?= --profile $(PROFILE)

STACK_NETWORK ?= MskDemo-Network
STACK_MSK ?= MskDemo-Msk
STACK_DB ?= MskDemo-Database
STACK_COMPUTE ?= MskDemo-Compute
STACK_INTEGRATION ?= MskDemo-Integration

.PHONY: help deps bootstrap synth diff deploy-network deploy-msk deploy-db deploy-compute deploy-integration deploy-all deploy destroy-all outputs stack-outputs loadgen loadgen-parallel install forward-rds-port

help:
	@echo "make deps # npm ci install dependencies"
	@echo "make bootstrap # npm ci + CDK bootstrap for $(ACCOUNT)/$(REGION)"
	@echo "make synth # build tsc then cdk synth (all stacks)"
	@echo "make diff # cdk diff (all stacks)"
	@echo "make deploy-all # deploy in dependency order"
	@echo "make deploy # deploy --all with CDK-managed ordering"
	@echo "make destroy # destroy in reverse order"
	@echo "make outputs # show outputs for all stacks"
	@echo "make stack-outputs STACK=<Name> # show outputs for one stack"
	@echo "make loadgen # generate load in a single thread (single lambda) with the default values"
	@echo "make loadgen-parallel # generate load in parallel to achieve spike in traffic with defaults close to 360 msg/s"
	@echo "make forward-rds-port # forward 3307 to remote 3306 over bastion to connect to private network from localhost"

deps:
	npm ci

bootstrap: deps
	$(CDK) bootstrap aws://$(ACCOUNT)/$(REGION) $(CDK_FLAGS) 

synth:
	npm run build && $(CDK) synth $(CDK_FLAGS) 

diff:
	$(CDK) diff --all $(CDK_FLAGS) 

# Explicit deploys in order

deploy-network:
	$(CDK) deploy $(STACK_NETWORK) --require-approval never $(CDK_FLAGS) 

deploy-msk: deploy-network
	$(CDK) deploy $(STACK_MSK) --require-approval never $(CDK_FLAGS) 

deploy-db: deploy-network
	$(CDK) deploy $(STACK_DB) --require-approval never $(CDK_FLAGS) 

deploy-compute: deploy-msk deploy-db
	$(CDK) deploy $(STACK_COMPUTE) --require-approval never $(CDK_FLAGS) 

deploy-integration: deploy-compute
	$(CDK) deploy $(STACK_INTEGRATION) --require-approval never $(CDK_FLAGS) 

deploy-all: deploy-integration

# Let CDK decide on dependencies and do it in parallel

deploy:
	$(CDK) deploy --all --require-approval never --concurrency 2 $(CDK_FLAGS) 

destroy:
	$(CDK) destroy $(STACK_INTEGRATION) $(STACK_COMPUTE) $(STACK_DB) $(STACK_MSK) $(STACK_NETWORK) --force $(CDK_FLAGS)

outputs:
	$(CDK) outputs $(CDK_FLAGS)

stack-outputs:
	@if [ -z "$(STACK)" ]; then echo "Usage: make stack-outputs STACK=<StackName>"; exit 1; fi
	aws cloudformation describe-stacks \
	  --stack-name "$(STACK)" \
	  --query "Stacks[0].Outputs" \
	  --output table \
	  --profile $(PROFILE) \
		--region $(REGION)

forward-rds-port:
	@RDS=$$(aws cloudformation describe-stacks \
		--stack-name $(STACK_DB) \
		--region $(REGION) \
		--query "Stacks[0].Outputs[?OutputKey=='RdsProxyEndpoint'].OutputValue" \
		--output text); \
	BASTION=$$(aws cloudformation describe-stacks \
		--stack-name $(STACK_DB) \
		--region $(REGION) \
		--query "Stacks[0].Outputs[?OutputKey=='BastionInstanceId'].OutputValue" \
		--output text); \
	echo "Forwarding localhost:3307 to $$RDS:3306 using bastion $$BASTION"; \
	aws ssm start-session \
		--target "$$BASTION" \
		--document-name AWS-StartPortForwardingSessionToRemoteHost \
		--parameters "portNumber=3306,localPortNumber=3307,host=$$RDS" \
		--region $(REGION)


# generate load with defauls or custom values in one instance
LOAD_RATE ?= 1000
LOAD_SECONDS ?= 10
LOAD_DELETE_PCT ?= 0.01
TOPIC ?= "msk-demo-source"

loadgen:
	@FN=$$(aws cloudformation describe-stacks \
		--stack-name $(STACK_COMPUTE) \
		--region $(REGION) \
		$(if $(PROFILE),--profile $(PROFILE),) \
		--query "Stacks[0].Outputs[?OutputKey=='LoadGeneratorFunction'].OutputValue" \
		--output text); \
	PAYLOAD=$$(printf '{"topic":"%s","rate":%s,"seconds":%s,"deletePct":%s}' \
	  "$(TOPIC)" "$(LOAD_RATE)" "$(LOAD_SECONDS)" "$(LOAD_DELETE_PCT)"); \
	echo "Invoking $$FN in $(REGION) with payload: $$PAYLOAD"; \
	aws lambda invoke --region $(REGION) \
	  $(if $(PROFILE),--profile $(PROFILE),) \
	  --function-name $$FN \
	  --cli-binary-format raw-in-base64-out \
	  --payload "$$PAYLOAD" out.json >/dev/null; \
	cat out.json


# this is long running Lambda. Adjust Lambda timeout to be greater than or equal to PARALLEL_SECONDS.

SHELL := /bin/bash

PARALLEL ?= 5 # number of parallel invocations (ideally == number of partitions)
PARALLEL_TOTAL_RATE ?= 360 # target total msg/s across all workers
PARALLEL_SECONDS ?= 120 # how long to run (sec)
PARALLEL_DELETE_PCT ?= 0.09 # percentage of soft deleted records flags in the batch ( use fraction 0..1)
PARALLEL_TOPIC ?= msk-demo-source

loadgen-parallel:
	@set -euo pipefail; \
	FN=$$(aws cloudformation describe-stacks \
		--stack-name $(STACK_COMPUTE) \
		--region $(REGION) \
		$(if $(PROFILE),--profile $(PROFILE),) \
		--query "Stacks[0].Outputs[?OutputKey=='LoadGeneratorFunction'].OutputValue" \
		--output text); \
	PAR=$(PARALLEL); TOTAL=$(PARALLEL_TOTAL_RATE); \
	BASE=$$(( TOTAL / PAR )); REM=$$(( TOTAL % PAR )); \
	echo "Launching $$PAR workers -> target $$TOTAL msg/s for $(PARALLEL_SECONDS)s on topic $(PARALLEL_TOPIC)"; \
	for i in $$(seq 1 $$PAR); do \
	  EXTRA=$$( [ $$i -le $$REM ] && echo 1 || echo 0 ); \
	  RATE=$$(( BASE + EXTRA )); \
	  PAYLOAD=$$(printf '{"topic":"%s","rate":%s,"seconds":%s,"deletePct":%s,"shard":%s,"shards":%s}' \
	    "$(PARALLEL_TOPIC)" "$$RATE" "$(PARALLEL_SECONDS)" "$(PARALLEL_DELETE_PCT)" "$$i" "$$PAR"); \
	  echo "[$$i/$$PAR] $$FN rate=$$RATE payload=$$PAYLOAD"; \
	  aws lambda invoke --region $(REGION) \
	    $(if $(PROFILE),--profile $(PROFILE),) \
	    --invocation-type Event \
	    --function-name "$$FN" \
	    --cli-binary-format raw-in-base64-out \
	    --payload "$$PAYLOAD" >/dev/null & \
	done; \
	wait; \
	echo "All $$PAR invokes dispatched."
