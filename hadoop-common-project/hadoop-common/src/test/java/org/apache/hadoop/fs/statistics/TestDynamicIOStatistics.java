/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.statistics;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticIsTracked;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticIsUnknown;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertStatisticIsUntracked;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.verifyStatisticValue;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.iostatisticsToString;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.dynamicIOStatistics;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.emptyStatistics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * verify dynamic statistics are dynamic, except when you iterate through
 * them, along with other tests of the class's behavior.
 */
public class TestDynamicIOStatistics extends AbstractHadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestDynamicIOStatistics.class);

  private static final String ALONG = "along";

  private static final String AINT = "aint";

  private static final String COUNT = "count";

  private static final String EVAL = "eval";

  private IOStatistics statistics = emptyStatistics();

  private AtomicLong aLong = new AtomicLong();

  private AtomicInteger aInt = new AtomicInteger();

  private MutableCounterLong counter = new MutableCounterLong(
      new Info("counter"), 0);

  private long evalLong;

  private static final String[] keys = new String[]{ALONG, AINT, COUNT, EVAL};

  @Before
  public void setUp() throws Exception {
    statistics = dynamicIOStatistics()
        .add(ALONG, aLong)
        .add(AINT, aInt)
        .add(COUNT, counter)
        .add(EVAL, x -> evalLong)
        .build();
  }

  /**
   * The eval operation is foundational.
   */
  @Test
  public void testEval() throws Throwable {
    verifyStatisticValue(statistics, EVAL, 0);
    evalLong = 10;
    verifyStatisticValue(statistics, EVAL, 10);
  }

  /**
   * Atomic Long statistic.
   */
  @Test
  public void testAlong() throws Throwable {
    verifyStatisticValue(statistics, ALONG, 0);
    aLong.addAndGet(1);
    verifyStatisticValue(statistics, ALONG, 1);
  }

  /**
   * Atomic Int statistic.
   */
  @Test
  public void testAint() throws Throwable {
    verifyStatisticValue(statistics, AINT, 0);
    aInt.addAndGet(1);
    verifyStatisticValue(statistics, AINT, 1);
  }

  /**
   * Metrics2 counter.
   */
  @Test
  public void testCounter() throws Throwable {
    verifyStatisticValue(statistics, COUNT, 0);
    counter.incr();
    verifyStatisticValue(statistics, COUNT, 1);
  }

  /**
   * keys() returns all the keys.
   */
  @Test
  public void testKeys() throws Throwable {
    Assertions.assertThat(statistics.keys())
        .describedAs("statistic keys of %s", statistics)
        .containsExactlyInAnyOrder(keys);
  }

  @Test
  public void testIteratorHasAllKeys() throws Throwable {
    // go through the statistics iterator and assert that it contains exactly
    // the values.
    assertThat(statistics)
        .extracting(s -> s.getKey())
        .containsExactlyInAnyOrder(keys);
  }

  /**
   * Verify that the iterator is taken from
   * a snapshot of the values.
   */
  @Test
  public void testIteratorIsSnapshot() throws Throwable {
    // set the counters all to 1
    incrementAllCounters();
    // take the snapshot
    final Iterator<Map.Entry<String, Long>> it = statistics.iterator();
    // reset the counters
    incrementAllCounters();
    // now assert that all the iterator values are of value 1
    while (it.hasNext()) {
      Map.Entry<String, Long> next = it.next();
      assertThat(next.getValue())
          .describedAs("Value of entry %s", next)
          .isEqualTo(1);
    }
  }

  @Test
  public void testUnknownStatistic() throws Throwable {
    assertStatisticIsUnknown(statistics, "anything");
    assertStatisticIsUntracked(statistics, "anything");
  }

  @Test
  public void testStatisticsTrackedAssertion() throws Throwable {
    // expect an exception to be raised when an assertion
    // is made that an unknown statistic is tracked,.
    assertThatThrownBy(() ->
        assertStatisticIsTracked(statistics, "anything"))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  public void testStatisticsValueAssertion() throws Throwable {
    // expect an exception to be raised when
    // an assertion is made about the value of an unknown statistics
    assertThatThrownBy(() ->
        verifyStatisticValue(statistics, "anything", 0))
        .isInstanceOf(AssertionError.class);
  }

  /**
   * Serialization round trip will preserve all the values.
   */
  @Test
  public void testSerDeser() throws Throwable {
    incrementAllCounters();
    IOStatistics stat = IOStatisticsSupport.snapshot(statistics);
    incrementAllCounters();
    IOStatistics deser = IOStatisticAssertions.roundTrip(stat);
    assertThat(deser)
        .extracting(s -> s.getKey())
        .containsExactlyInAnyOrder(keys);
    for (Map.Entry<String, Long> e: deser) {
      assertThat(e.getValue())
          .describedAs("Value of entry %s", e)
          .isEqualTo(1);
    }
  }

  @Test
  public void testStringification() throws Throwable {
    assertThat(iostatisticsToString(statistics))
        .isNotBlank()
        .contains(keys);
  }

  @Test
  public void testStringification2() throws Throwable {
    assertThat(IOStatisticsLogging.stringify(statistics)
        .toString())
        .contains(keys);
  }

  /**
   * Increment all the counters from their current value.
   */
  public void incrementAllCounters() {
    aLong.incrementAndGet();
    aInt.incrementAndGet();
    evalLong += 1;
    counter.incr();
  }

  /**
   * Needed to provide a metrics info instance for the counter
   * constructor.
   */
  private final class Info implements MetricsInfo {

    private final String name;

    private Info(final String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String description() {
      return name;
    }
  }

}
