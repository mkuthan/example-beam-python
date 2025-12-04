"""Batch pipeline using BigQuery public dataset.

This pipeline reads from the BigQuery public dataset 'usa_names'
and performs aggregation to find the most popular names by state.
"""

import argparse
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

# BigQuery schema for output table
OUTPUT_SCHEMA = {
    "fields": [
        {"name": "state", "type": "STRING", "mode": "REQUIRED"},
        {"name": "total_count", "type": "INTEGER", "mode": "REQUIRED"},
    ]
}


def run(argv=None):
    """Run the batch pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_table",
        default="bigquery-public-data:usa_names.usa_1910_current",
        help="BigQuery table to read from (format: project:dataset.table)",
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery table to write to (format: project:dataset.table)",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        names = p | "ReadFromBigQuery" >> ReadFromBigQuery(
            table=known_args.input_table,
            use_standard_sql=True,
        )

        state_counts = (
            names
            | "ExtractStateAndCount" >> beam.Map(lambda row: (row["state"], row["number"]))
            | "SumByState" >> beam.CombinePerKey(sum)
            | "FormatOutput" >> beam.Map(lambda x: {"state": x[0], "total_count": x[1]})
        )

        state_counts | "WriteToBigQuery" >> WriteToBigQuery(
            table=known_args.output_table,
            schema=OUTPUT_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
