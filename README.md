# example-beam-python

Example Apache Beam Python pipelines demonstrating batch and streaming data processing patterns.

## Project Structure

```
src/example_beam_python/
├── __init__.py
├── io/                    # I/O utilities
│   └── __init__.py
└── pipelines/             # Data pipelines
    ├── __init__.py
    ├── batch_pipeline.py  # Batch pipeline using BigQuery
    └── streaming_pipeline.py  # Streaming pipeline using Pub/Sub
```

## Configuration

Set up your GCP environment variables in `mise.toml` or export them in your shell:

```bash
GCP_PROJECT_ID=your-project
GCP_DATASET=your_dataset
GCP_BUCKET=your-bucket
GCP_REGION=your-region
GCP_SUBSCRIPTION_NAME=your-subscription-name
```

## Pipelines

### Batch Pipeline

The batch pipeline reads from the BigQuery public dataset `usa_names` and aggregates name counts by state.

```bash
uv run python -m example_beam_python.pipelines.batch_pipeline \
    --output_table=$GCP_PROJECT_ID:$GCP_DATASET.example_beam_python_batch \
    --temp_location=gs://$GCP_BUCKET/temp \
    --project=$GCP_PROJECT_ID \
    --region=$GCP_REGION \
    --runner=DataflowRunner \
    --no_use_public_ips
```

### Streaming Pipeline

The streaming pipeline reads from a Pub/Sub topic (by default, the public NYC Taxi Rides stream), applies windowing, and writes aggregated statistics to BigQuery.

The pipeline uses the public Pub/Sub topic `projects/pubsub-public-data/topics/taxirides-realtime` by default, which provides real-time NYC taxi ride data. To use it, you need to create a subscription:

```bash
gcloud pubsub subscriptions create $GCP_SUBSCRIPTION_NAME \
    --topic=projects/pubsub-public-data/topics/taxirides-realtime \
    --message-retention-duration=10m
```

```bash
uv run python -m example_beam_python.pipelines.streaming_pipeline \
    --input_subscription=projects/$GCP_PROJECT_ID/subscriptions/$GCP_SUBSCRIPTION_NAME \
    --output_table=$GCP_PROJECT_ID:$GCP_DATASET.example_beam_python_streaming \
    --temp_location=gs://$GCP_BUCKET/temp \
    --project=$GCP_PROJECT_ID \
    --region=$GCP_REGION \
    --runner=DataflowRunner \
    --no_use_public_ips
```

