"""Tests for streaming pipeline."""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from example_beam_python.pipelines.streaming_pipeline import parse_message


def test_parse_message_valid_json():
    """Test parsing valid JSON message."""
    message = b'{"key": "value", "number": 42}'
    result = parse_message(message)
    assert result == {"key": "value", "number": 42}


def test_parse_message_invalid_json():
    """Test parsing invalid JSON message."""
    message = b"not valid json"
    result = parse_message(message)
    assert result is None


def test_parse_message_empty():
    """Test parsing empty message."""
    message = b""
    result = parse_message(message)
    assert result is None


def test_message_count_pipeline():
    """Test message counting logic."""
    input_data = [
        {"type": "event1"},
        {"type": "event2"},
        {"type": "event1"},
    ]

    expected_output = [
        {"key": "message_count", "count": 3},
    ]

    with TestPipeline() as p:
        input_pcoll = p | beam.Create(input_data)

        result = (
            input_pcoll
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

        assert_that(result, equal_to(expected_output))
