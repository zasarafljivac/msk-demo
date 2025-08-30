# Near‑Real‑Time Event Ingestion at Scale (MSK -> Lambda -> RDS Proxy -> Aurora)

## High Level Overview of the Stack

This CDK app provisions an infrastructure for the [AWS Community Adria 2025 talk demo](https://awscommunityadria.com/sessions/304-drina/)

- **MSK** (IAM auth)
- **Kafka topics**: `*-source`, `*-buffer`
- **Transform Lambda**: consumes from `source`, transforms, **produces** to `buffer`
- **Write Lambda**: consumes from `buffer`, rate-limited inserts, using **RDS Proxy** to **Aurora MySQL** for connection pooling
- **Event Source Mappings** (ESM) from MSK to both Lambdas
- **Custom resource** to fetch **MSK bootstrap (IAM)** and create topics. Also creates schema and tables when RDS is ready.
- **Load generator** to publish test data to `source` (IAM-auth Kafka)
- **schema.sql** sample schema
- **Makefile** for common tasks including deploy, destroy and loadgen
- **Use** `INSERT_CONCURRENCY` and `INSERT_RATE_CAP` env vars to tune the load in Write Lambda.

## How to use Makefile

This will display available options.

```bash
make help
```

**Set your region/profile**

```bash
export AWS_REGION=eu-central-1
export AWS_PROFILE=dev
```

**If you ommit setting env vars, then default values are:**

```bash
export AWS_REGION=us-east-1
export AWS_PROFILE=default
```

You can pass region and profile explicitly in your commands when using make, otherwise it will use default values.

1. Bootstrap needed only once per account/region

```bash
make bootstrap
```

2. Deploy with explicit ordering

```bash
make deploy-all
```

3. Or let CDK manage order (Network goes first, then others)

```bash
make deploy
```

4. See outputs

```bash
make outputs
make stack-outputs STACK=MskDemo-Integration
```

## DEMO

[DEMO README](./README.DEMO.md)

## Useful links

- https://almirzulic.com/posts/aws-community-day-adria/
- https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-install-cli.html
- https://github.com/aws/aws-msk-iam-auth/releases
- https://kafka.apache.org/downloads
