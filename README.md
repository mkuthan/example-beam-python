# example-beam-python

Example Apache Beam Python pipelines demonstrating batch and streaming data processing patterns.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) - Fast Python package installer and resolver

## Installation

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Install with dev dependencies
uv sync --extra dev
```

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

## Pipelines

### Batch Pipeline

The batch pipeline reads from the BigQuery public dataset `usa_names` and aggregates name counts by state.

```bash
uv run python -m example_beam_python.pipelines.batch_pipeline \
    --output_table=your-project:your_dataset.output_table \
    --temp_location=gs://your-bucket/temp \
    --project=your-project \
    --runner=DataflowRunner \
    --region=us-central1
```

### Streaming Pipeline

The streaming pipeline reads from a Pub/Sub subscription, applies windowing, and writes aggregated counts to BigQuery.

```bash
uv run python -m example_beam_python.pipelines.streaming_pipeline \
    --input_subscription=projects/your-project/subscriptions/your-subscription \
    --output_table=your-project:your_dataset.output_table \
    --temp_location=gs://your-bucket/temp \
    --project=your-project \
    --runner=DataflowRunner \
    --region=us-central1
```

## Development

### Linting

```bash
uv run ruff check src tests
uv run ruff format --check src tests
```

### Formatting

```bash
uv run ruff format src tests
```

### Testing

```bash
uv run pytest
```

## License

Apache License 2.0