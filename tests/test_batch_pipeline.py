"""Tests for batch pipeline."""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to


def test_aggregate_names_by_state():
    """Test that names are correctly aggregated by state."""
    input_data = [
        {"state": "CA", "name": "John", "number": 100},
        {"state": "CA", "name": "Jane", "number": 200},
        {"state": "TX", "name": "Bob", "number": 150},
        {"state": "TX", "name": "Alice", "number": 50},
    ]

    expected_output = [
        {"state": "CA", "total_count": 300},
        {"state": "TX", "total_count": 200},
    ]

    with TestPipeline() as p:
        input_pcoll = p | beam.Create(input_data)

        result = (
            input_pcoll
            | "ExtractStateAndCount" >> beam.Map(lambda row: (row["state"], row["number"]))
            | "SumByState" >> beam.CombinePerKey(sum)
            | "FormatOutput" >> beam.Map(lambda x: {"state": x[0], "total_count": x[1]})
        )

        assert_that(result, equal_to(expected_output))
