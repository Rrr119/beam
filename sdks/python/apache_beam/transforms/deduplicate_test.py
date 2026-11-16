#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file

"""Unit tests for deduplicate transform by using TestStream."""

import unittest

import pytest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import equal_to_per_window
from apache_beam.transforms import deduplicate
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp


# TestStream is only supported in streaming pipeline. The Deduplicate transform
# also requires Timer support. Sickbaying this testsuite until dataflow runner
# supports both TestStream and user timer.
@pytest.mark.no_sickbay_batch
@pytest.mark.no_sickbay_streaming
@pytest.mark.it_validatesrunner
class DeduplicateTest(unittest.TestCase):
  def __init__(self, *args, **kwargs):
    self.runner = None
    self.options = None
    super().__init__(*args, **kwargs)

  def set_runner(self, runner):
    self.runner = runner

  def set_options(self, options):
    self.options = options

  def create_pipeline(self):
    if self.runner and self.options:
      return TestPipeline(runner=self.runner, options=self.options)
    elif self.runner:
      return TestPipeline(runner=self.runner)
    elif self.options:
      return TestPipeline(options=self.options)
    else:
      return TestPipeline()

  def test_deduplication_in_different_windows(self):
    with self.create_pipeline() as p:
      test_stream = (
          TestStream(
              coder=coders.StrUtf8Coder()).advance_watermark_to(0).add_elements(
                  [
                      window.TimestampedValue('k1', 0),
                      window.TimestampedValue('k2', 10),
                      window.TimestampedValue('k3', 20),
                      window.TimestampedValue('k1', 30),
                      window.TimestampedValue('k2', 40),
                      window.TimestampedValue('k3', 50),
                      window.TimestampedValue('k4', 60),
                      window.TimestampedValue('k5', 70),
                      window.TimestampedValue('k6', 80)
                  ]).advance_watermark_to_infinity())

      res = (
          p
          | test_stream
          | beam.WindowInto(window.FixedWindows(30))
          | deduplicate.Deduplicate(processing_time_duration=10 * 60)
          | beam.Map(lambda e, ts=beam.DoFn.TimestampParam: (e, ts)))
      # Deduplication should happen per window.
      expect_unique_keys_per_window = {
          window.IntervalWindow(0, 30): [('k1', Timestamp(0)),
                                         ('k2', Timestamp(10)),
                                         ('k3', Timestamp(20))],
          window.IntervalWindow(30, 60): [('k1', Timestamp(30)),
                                          ('k2', Timestamp(40)),
                                          ('k3', Timestamp(50))],
          window.IntervalWindow(60, 90): [('k4', Timestamp(60)),
                                          ('k5', Timestamp(70)),
                                          ('k6', Timestamp(80))],
      }
      assert_that(
          res,
          equal_to_per_window(expect_unique_keys_per_window),
          use_global_window=False,
          label='assert per window')

  @unittest.skip('TestStream not yet supported')
  def test_deduplication_with_event_time(self):
    deduplicate_duration = 60
    with self.create_pipeline() as p:
      test_stream = (
          TestStream(coder=coders.StrUtf8Coder()).with_output_types(
              str).advance_watermark_to(0).add_elements([
                  window.TimestampedValue('k1', 0),
                  window.TimestampedValue('k2', 20),
                  window.TimestampedValue('k3', 30)
              ]).advance_watermark_to(30).add_elements([
                  window.TimestampedValue('k1', 40),
                  window.TimestampedValue('k2', 50),
                  window.TimestampedValue('k3', 60)
              ]).advance_watermark_to(deduplicate_duration).add_elements([
                  window.TimestampedValue('k1', 70)
              ]).advance_watermark_to_infinity())
      res = (
          p
          | test_stream
          | deduplicate.Deduplicate(
              event_time_duration=Duration(deduplicate_duration))
          | beam.Map(lambda e, ts=beam.DoFn.TimestampParam: (e, ts)))

      assert_that(
          res,
          equal_to([('k1', Timestamp(0)), ('k2', Timestamp(20)),
                    ('k3', Timestamp(30)), ('k1', Timestamp(70))]))

  @unittest.skip('TestStream not yet supported')
  def test_deduplication_with_processing_time(self):
    deduplicate_duration = 60
    with self.create_pipeline() as p:
      test_stream = (
          TestStream(coder=coders.StrUtf8Coder()).with_output_types(
              str).advance_watermark_to(0).add_elements([
                  window.TimestampedValue('k1', 0),
                  window.TimestampedValue('k2', 20),
                  window.TimestampedValue('k3', 30)
              ]).advance_processing_time(30).add_elements([
                  window.TimestampedValue('k1', 40),
                  window.TimestampedValue('k2', 50),
                  window.TimestampedValue('k3', 60)
              ]).advance_processing_time(deduplicate_duration).add_elements([
                  window.TimestampedValue('k1', 70)
              ]).advance_watermark_to_infinity())
      res = (
          p
          | test_stream
          | deduplicate.Deduplicate(
              processing_time_duration=Duration(deduplicate_duration))
          | beam.Map(lambda e, ts=beam.DoFn.TimestampParam: (e, ts)))
      assert_that(
          res,
          equal_to([('k1', Timestamp(0)), ('k2', Timestamp(20)),
                    ('k3', Timestamp(30)), ('k1', Timestamp(70))]))


@pytest.mark.no_sickbay_batch
@pytest.mark.no_sickbay_streaming
@pytest.mark.it_validatesrunner
class DeduplicatePerWindowTest(unittest.TestCase):
  """Tests for the window-aware deduplication transforms.

  These tests exercise both non-merging (fixed and sliding) and merging
  (session) window functions on top of TestStream, which requires timer and
  state support from the runner.
  """
  def __init__(self, *args, **kwargs):
    self.runner = None
    self.options = None
    super().__init__(*args, **kwargs)

  def set_runner(self, runner):
    self.runner = runner

  def set_options(self, options):
    self.options = options

  def create_pipeline(self):
    if self.runner and self.options:
      return TestPipeline(runner=self.runner, options=self.options)
    elif self.runner:
      return TestPipeline(runner=self.runner)
    elif self.options:
      return TestPipeline(options=self.options)
    else:
      return TestPipeline()

  def test_deduplicate_windowed_with_fixed_windows(self):
    with self.create_pipeline() as p:
      test_stream = (
          TestStream(
              coder=coders.StrUtf8Coder()).advance_watermark_to(0).add_elements(
                  [
                      window.TimestampedValue('k1', 0),
                      window.TimestampedValue('k1', 10),
                      window.TimestampedValue('k2', 20),
                      window.TimestampedValue('k1', 30),
                  ]).advance_watermark_to_infinity())

      res = (
          p
          | test_stream
          | beam.WindowInto(window.FixedWindows(30))
          | deduplicate.DeduplicateWindowed(processing_time_duration=10 * 60)
          | beam.Map(lambda e, ts=beam.DoFn.TimestampParam: (e, ts)))

      expected = {
          window.IntervalWindow(0, 30): [('k1', Timestamp(0)),
                                         ('k2', Timestamp(20))],
          window.IntervalWindow(30, 60): [('k1', Timestamp(30))],
      }
      assert_that(
          res,
          equal_to_per_window(expected),
          use_global_window=False,
          label='assert per window')

  def test_deduplicate_windowed_with_sliding_windows(self):
    # Sliding windows of size 10, period 5. An element may land in multiple
    # windows, but within each window duplicates must be suppressed.
    with self.create_pipeline() as p:
      test_stream = (
          TestStream(
              coder=coders.StrUtf8Coder()).advance_watermark_to(0).add_elements(
                  [
                      window.TimestampedValue('k1', 0),
                      window.TimestampedValue('k1', 5),
                      window.TimestampedValue('k2', 7),
                  ]).advance_watermark_to_infinity())

      res = (
          p
          | test_stream
          | beam.WindowInto(window.SlidingWindows(10, 5))
          | deduplicate.DeduplicateWindowed(processing_time_duration=10 * 60)
          | beam.Map(lambda e, w=beam.DoFn.WindowParam: (e, w)))

      expected = {
          window.IntervalWindow(-5, 5): [('k1', window.IntervalWindow(-5, 5))],
          window.IntervalWindow(0, 10): [
              ('k1', window.IntervalWindow(0, 10)),
              ('k2', window.IntervalWindow(0, 10)),
          ],
          window.IntervalWindow(5, 15): [
              ('k1', window.IntervalWindow(5, 15)),
              ('k2', window.IntervalWindow(5, 15)),
          ],
      }
      assert_that(
          res,
          equal_to_per_window(expected),
          use_global_window=False,
          label='assert per window')

  def test_deduplicate_windowed_with_session_windows(self):
    # Session windows merge as elements arrive close together. The duplicate
    # arriving after the merge must still be suppressed, proving that the
    # seen state survives the window merge.
    with self.create_pipeline() as p:
      test_stream = (
          TestStream(
              coder=coders.StrUtf8Coder()).advance_watermark_to(0).add_elements(
                  [
                      window.TimestampedValue('k1', 0),
                      window.TimestampedValue('k1', 5),
                      window.TimestampedValue('k2', 60),
                  ]).advance_watermark_to(10).add_elements([
                      # Merges the [0, 10) session into [0, 22).
                      window.TimestampedValue('k1', 12),
                  ]).advance_watermark_to_infinity())

      res = (
          p
          | test_stream
          | beam.WindowInto(window.Sessions(10))
          | deduplicate.DeduplicateWindowed(processing_time_duration=10 * 60)
          | beam.Map(lambda e, w=beam.DoFn.WindowParam: (e, w)))

      expected = {
          window.IntervalWindow(0, 22): [
              ('k1', window.IntervalWindow(0, 22)),
          ],
          window.IntervalWindow(60, 70): [
              ('k2', window.IntervalWindow(60, 70)),
          ],
      }
      assert_that(
          res,
          equal_to_per_window(expected),
          use_global_window=False,
          label='assert per window')

  def test_deduplicate_windowed_with_repeated_session_merges(self):
    # A chain of overlapping sessions that merge several times. The seen
    # marker must keep suppressing duplicates after every merge on every
    # state backend, which only works with mergeable state.
    with self.create_pipeline() as p:
      test_stream = (
          TestStream(
              coder=coders.StrUtf8Coder()).advance_watermark_to(0).add_elements(
                  [
                      window.TimestampedValue('k1', 0),
                  ]).advance_watermark_to(5).add_elements([
                      window.TimestampedValue('k1', 8),
                      window.TimestampedValue('k2', 8),
                  ]).advance_watermark_to(10).add_elements([
                      window.TimestampedValue('k1', 15),
                  ]).advance_watermark_to(20).add_elements([
                      window.TimestampedValue('k1', 22),
                  ]).advance_watermark_to_infinity())

      res = (
          p
          | test_stream
          | beam.WindowInto(window.Sessions(10))
          | deduplicate.DeduplicateWindowed(processing_time_duration=10 * 60))

      assert_that(res, equal_to(['k1', 'k2']))

  def test_deduplicate_per_window_with_session_windows(self):
    # Same merging scenario as above but on keyed input, verifying that the
    # first value of a key is retained through the internal GroupByKey.
    kv_coder = coders.TupleCoder((coders.StrUtf8Coder(), coders.VarIntCoder()))
    with self.create_pipeline() as p:
      test_stream = (
          TestStream(coder=kv_coder).advance_watermark_to(0).add_elements([
              window.TimestampedValue(('k1', 100), 0),
              window.TimestampedValue(('k1', 200), 5),
          ]).advance_watermark_to(10).add_elements([
              window.TimestampedValue(('k1', 400), 12),
          ]).advance_watermark_to_infinity())

      res = (
          p
          | test_stream
          | beam.WindowInto(window.Sessions(10))
          | deduplicate.DeduplicatePerWindow(processing_time_duration=10 * 60))

      assert_that(res, equal_to([('k1', 100)]))

  def test_deduplicate_windowed_with_event_time_duration(self):
    # The event-time timer clears the seen marker once the threshold passes,
    # so the same value is emitted again after the duration elapses.
    with self.create_pipeline() as p:
      test_stream = (
          TestStream(
              coder=coders.StrUtf8Coder()).advance_watermark_to(0).add_elements(
                  [
                      window.TimestampedValue('k1', 0),
                      window.TimestampedValue('k1', 5),
                  ]).advance_watermark_to(30).add_elements([
                      window.TimestampedValue('k1', 40),
                  ]).advance_watermark_to_infinity())

      res = (
          p
          | test_stream
          | beam.WindowInto(window.FixedWindows(100))
          | deduplicate.DeduplicateWindowed(event_time_duration=Duration(20))
          | beam.Map(lambda e, ts=beam.DoFn.TimestampParam: (e, ts)))

      assert_that(res, equal_to([('k1', Timestamp(0)), ('k1', Timestamp(40))]))

  def test_deduplicate_windowed_requires_duration(self):
    with self.assertRaises(ValueError):
      deduplicate.DeduplicateWindowed()
    with self.assertRaises(ValueError):
      deduplicate.DeduplicatePerWindow()

  def test_deduplicate_windowed_with_global_window(self):
    # Without explicit windowing the input stays in the global window, which
    # is non-merging and therefore requires no internal grouping step.
    with self.create_pipeline() as p:
      test_stream = (
          TestStream(
              coder=coders.StrUtf8Coder()).advance_watermark_to(0).add_elements(
                  [
                      window.TimestampedValue('k1', 0),
                      window.TimestampedValue('k1', 5),
                      window.TimestampedValue('k2', 7),
                  ]).advance_watermark_to_infinity())

      res = (
          p
          | test_stream
          | deduplicate.DeduplicateWindowed(processing_time_duration=10 * 60))

      assert_that(res, equal_to(['k1', 'k2']))

  def test_legacy_deduplicate_requires_duration(self):
    with self.assertRaises(ValueError):
      deduplicate.Deduplicate()
    with self.assertRaises(ValueError):
      deduplicate.DeduplicatePerKey()


if __name__ == '__main__':
  unittest.main()
