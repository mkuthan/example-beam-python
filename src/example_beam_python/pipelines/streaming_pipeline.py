"""Streaming pipeline using Pub/Sub.

This pipeline reads from a Pub/Sub topic, performs windowed
aggregations, and writes results to BigQuery.
"""

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

# BigQuery schema for output table
OUTPUT_SCHEMA = {
    "fields": [
        {"name": "key", "type": "STRING", "mode": "REQUIRED"},
        {"name": "count", "type": "INTEGER", "mode": "REQUIRED"},
    ]
}


def parse_message(message: bytes) -> dict | None:
    """Parse a Pub/Sub message as JSON."""
    try:
        return json.loads(message.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.warning("Failed to parse message: %s", e)
        return None


def run(argv=None):
    """Run the streaming pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        required=True,
        help="Pub/Sub subscription to read from (format: projects/project/subscriptions/subscription)",
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery table to write to (format: project:dataset.table)",
    )
    parser.add_argument(
        "--window_size",
        type=int,
        default=60,
        help="Window size in seconds (default: 60)",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    # Enable streaming mode
    pipeline_options.view_as(StandardOptions).streaming = True

    # Validate that temp_location is set in pipeline options
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    if not gcp_options.temp_location:
        parser.error("--temp_location is required in pipeline options")

    with beam.Pipeline(options=pipeline_options) as p:
        # Read from Pub/Sub subscription
        messages = p | "ReadFromPubSub" >> ReadFromPubSub(
            subscription=known_args.input_subscription,
        )

        # Parse messages and apply windowing
        parsed = (
            messages
            | "ParseMessages" >> beam.Map(parse_message)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
            | "Window" >> beam.WindowInto(FixedWindows(known_args.window_size))
        )

        # Count messages per window
        windowed_counts = (
            parsed
            | "AddKey" >> beam.Map(lambda _: ("message_count", 1))
            | "CountPerWindow" >> beam.CombinePerKey(sum)
            | "FormatOutput"
            >> beam.Map(
                lambda x: {
                    "key": x[0],
                    "count": x[1],
                }
            )
        )

        # Write results to BigQuery
        windowed_counts | "WriteToBigQuery" >> WriteToBigQuery(
            table=known_args.output_table,
            schema=OUTPUT_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
