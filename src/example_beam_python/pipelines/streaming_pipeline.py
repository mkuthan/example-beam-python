"""Streaming pipeline using Pub/Sub.

This pipeline reads from a Pub/Sub topic (e.g., NYC taxi rides stream),
performs windowed aggregations, and writes results to BigQuery.
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
        {"name": "ride_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "avg_fare", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


def aggregate_stats(values):
    """Aggregate ride statistics from values."""
    values_list = list(values)
    return {
        "ride_count": sum(v.get("ride_count", 0) for v in values_list),
        "total_fare": sum(v.get("fare", 0) for v in values_list),
        "num_rides": len(values_list),
    }


def parse_message(message: bytes) -> dict | None:
    """Parse a Pub/Sub message as JSON.

    Expected message format (NYC taxi rides):
    {
        "ride_id": "...",
        "point_idx": 0,
        "latitude": 40.7...,
        "longitude": -73.9...,
        "timestamp": "2021-01-01T00:00:00",
        "meter_reading": 1.5,
        "meter_increment": 0.05,
        "ride_status": "pickup"
    }
    """
    try:
        return json.loads(message.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.warning("Failed to parse message: %s", e)
        return None


def run(argv=None):
    """Run the streaming pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        default="projects/pubsub-public-data/topics/taxirides-realtime",
        help="Pub/Sub topic to read from (default: public NYC taxi rides topic)",
    )
    parser.add_argument(
        "--input_subscription",
        help="Pub/Sub subscription to read from (format: projects/project/subscriptions/subscription). "
        "If not provided, will use --input_topic with auto-created subscription.",
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
        # Read from Pub/Sub topic or subscription
        if known_args.input_subscription:
            messages = p | "ReadFromPubSub" >> ReadFromPubSub(
                subscription=known_args.input_subscription,
            )
        else:
            messages = p | "ReadFromPubSub" >> ReadFromPubSub(
                topic=known_args.input_topic,
            )

        # Parse messages and apply windowing
        parsed = (
            messages
            | "ParseMessages" >> beam.Map(parse_message)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
            | "Window" >> beam.WindowInto(FixedWindows(known_args.window_size))
        )

        # Extract ride data and aggregate
        windowed_stats = (
            parsed
            | "ExtractRideData"
            >> beam.Map(
                lambda ride: (
                    1,
                    {
                        "ride_count": 1,
                        "fare": ride.get("meter_reading", 0),
                    },
                )
            )
            | "AggregateStats" >> beam.CombinePerKey(aggregate_stats)
            | "ComputeAverages"
            >> beam.Map(
                lambda x: {
                    "ride_count": x[1]["ride_count"],
                    "avg_fare": x[1]["total_fare"] / x[1]["num_rides"] if x[1]["num_rides"] > 0 else 0,
                    "window_start": beam.utils.timestamp.Timestamp.now().to_rfc3339(),
                }
            )
        )

        # Write results to BigQuery
        windowed_stats | "WriteToBigQuery" >> WriteToBigQuery(
            table=known_args.output_table,
            schema=OUTPUT_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
