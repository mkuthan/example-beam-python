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
GCP_REGION=europe-west1
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

The streaming pipeline reads from a Pub/Sub subscription, applies windowing, and writes aggregated counts to BigQuery.

```bash
uv run python -m example_beam_python.pipelines.streaming_pipeline \
    --input_subscription=projects/$GCP_PROJECT_ID/subscriptions/your-subscription \
    --output_table=$GCP_PROJECT_ID:$GCP_DATASET.example_beam_python_streaming \
    --temp_location=gs://$GCP_BUCKET/temp \
    --project=$GCP_PROJECT_ID \
    --region=$GCP_REGION \
    --runner=DataflowRunner \
    --no_use_public_ips
```
