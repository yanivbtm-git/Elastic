# Elastic

Utilities for working with Elasticsearch.

## Generate and upload synthetic transactions

Use `scripts/upload_synthetic_transactions.py` to stream synthetic payment
transactions directly into Elasticsearch with the Bulk API. The script generates
documents lazily and sends them in compact NDJSON batches, so the default
one-million-document run does not keep the full dataset in memory.

### Authentication

Set the Elasticsearch endpoint and either an API key or basic auth credentials:

```bash
export ELASTICSEARCH_URL="https://your-deployment.es.region.aws.elastic.cloud:443"
export ELASTICSEARCH_API_KEY="base64-api-key"
```

or:

```bash
export ELASTICSEARCH_URL="https://localhost:9200"
export ELASTICSEARCH_USERNAME="elastic"
export ELASTICSEARCH_PASSWORD="changeme"
```

### Upload one million transactions

```bash
python3 scripts/upload_synthetic_transactions.py \
  --index synthetic-transactions \
  --count 1000000 \
  --batch-size 5000
```

Useful options:

- `--dry-run` generates and batches documents without sending them.
- `--seed 123` makes generated data deterministic.
- `--start-time 2026-01-01T00:00:00Z` controls the first timestamp.
- `--pipeline my-pipeline` runs an ingest pipeline for each bulk request.
- `--no-compress` disables gzip request compression.
- `--no-verify-certs` allows connecting to a local cluster with self-signed TLS.

The generated document shape includes `@timestamp`, `transaction_id`,
`account_id`, `customer_id`, merchant details, amount, currency, status,
payment method, channel, latency, risk score, and fraud flag fields.

### Validate locally without Elasticsearch

```bash
python3 scripts/upload_synthetic_transactions.py --dry-run --count 10000
python3 -m unittest discover -s tests
```
