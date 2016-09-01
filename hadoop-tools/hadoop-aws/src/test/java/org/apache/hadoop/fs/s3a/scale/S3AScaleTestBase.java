/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Base class for scale tests; here is where the common scale configuration
 * keys are defined.
 */
public class S3AScaleTestBase extends Assert implements S3ATestConstants {

  @Rule
  public TestName methodName = new TestName();

  @Rule
  public Timeout testTimeout = createTestTimeout();

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  public static final long _1KB = 1024L;
  public static final long _1MB = _1KB * _1KB;
  public static final long _10MB = _1MB * 10;
  public static final long _1GB = _1KB * _1MB;

  /**
   * The number of operations to perform: {@value}.
   */
  public static final String KEY_OPERATION_COUNT =
      SCALE_TEST + "operation.count";

  /**
   * The number of directory operations to perform: {@value}.
   */
  public static final String KEY_DIRECTORY_COUNT =
      SCALE_TEST + "directory.count";

  /**
   * The readahead buffer: {@value}.
   */
  public static final String KEY_READ_BUFFER_SIZE =
      S3A_SCALE_TEST + "read.buffer.size";

  public static final int DEFAULT_READ_BUFFER_SIZE = 16384;

  /**
   * Key for a multi MB test file: {@value}.
   */
  public static final String KEY_CSVTEST_FILE =
      S3A_SCALE_TEST + "csvfile";
  /**
   * Default path for the multi MB test file: {@value}.
   */
  public static final String DEFAULT_CSVTEST_FILE
      = "s3a://landsat-pds/scene_list.gz";

  /**
   * Endpoint for the S3 CSV/scale tests. This defaults to
   * being us-east.
   */
  public static final String KEY_CSVTEST_ENDPOINT =
      S3A_SCALE_TEST + "csvfile.endpoint";

  /**
   * Endpoint for the S3 CSV/scale tests. This defaults to
   * being us-east.
   */
  public static final String DEFAULT_CSVTEST_ENDPOINT =
      "s3.amazonaws.com";

  /**
   * Name of the property to define the timeout for scale tests: {@value}.
   * Measured in seconds.
   */
  public static final String KEY_TEST_TIMEOUT = S3A_SCALE_TEST + "timeout";

  /**
   * Name of the property to define the file size for the huge file
   * tests: {@value}. Measured in MB.
   */
  public static final String KEY_HUGE_FILESIZE = S3A_SCALE_TEST + "huge.filesize";

  /**
   * Name of the property to define the partition size for the huge file
   * tests: {@value}. Measured in MB.
   */
  public static final String KEY_HUGE_PARTITION_SIZE =
      S3A_SCALE_TEST + "huge.partition.size";

  /**
   * The default huge size is small —full 5GB+ scale tests are something
   * to run in long test runs on EC2 VMs. {@value}.
   */
  public static final long DEFAULT_HUGE_FILESIZE = 10L;

  /**
   * The default number of operations to perform: {@value}.
   */
  public static final long DEFAULT_OPERATION_COUNT = 2005;

  /**
   * Default number of directories to create when performing
   * directory performance/scale tests.
   */
  public static final int DEFAULT_DIRECTORY_COUNT = 2;

  /**
   * Default scale test timeout in seconds: {@value}.
   */
  public static final long DEFAULT_TEST_TIMEOUT = 30 * 60;

  protected S3AFileSystem fs;

  protected static final Logger LOG =
      LoggerFactory.getLogger(S3AScaleTestBase.class);

  private Configuration conf;

  /**
   * Configuration generator. May be overridden to inject
   * some custom options.
   * @return a configuration with which to create FS instances
   */
  protected Configuration createConfiguration() {
    return new Configuration();
  }

  /**
   * Get the configuration used to set up the FS.
   * @return the configuration
   */
  public Configuration getConf() {
    return conf;
  }

  @Before
  public void setUp() throws Exception {
    demandCreateConfiguration();
    LOG.debug("Scale test operation count = {}", getOperationCount());
    // multipart purges are disabled on the scale tests
    fs = S3ATestUtils.createTestFileSystem(conf, false);
  }

  private void demandCreateConfiguration() {
    if (conf == null) {
      conf = createConfiguration();
    }
  }

  @After
  public void tearDown() throws Exception {
    ContractTestUtils.rm(fs, getTestPath(), true, true);
  }

  protected Path getTestPath() {
    String testUniqueForkId = System.getProperty("test.unique.fork.id");
    return testUniqueForkId == null ? new Path("/tests3a") :
        new Path("/" + testUniqueForkId, "tests3a");
  }

  protected long getOperationCount() {
    return getConf().getLong(KEY_OPERATION_COUNT, DEFAULT_OPERATION_COUNT);
  }

  /**
   * Create the timeout for tests. Some large tests may need a larger value.
   * @return the test timeout to use
   */
  protected Timeout createTestTimeout() {
    return new Timeout((int)getTestProperty(KEY_TEST_TIMEOUT,
        DEFAULT_TEST_TIMEOUT) * 1000);
  }

  /**
   * Get a test property which can defined first as a system property
   * and then potentially overridden in a configuration property.
   * @param key key to look up
   * @param defVal default value
   * @return the evaluated test property.
   */
  protected long getTestProperty(String key, long defVal) {
    demandCreateConfiguration();
    String propval = System.getProperty(key, Long.toString(defVal));
    long longVal =  propval!=null? Long.valueOf(propval): defVal;
    return getConf().getLong(key, longVal);
  }

  /**
   * Describe a test in the logs
   * @param text text to print
   * @param args arguments to format in the printing
   */
  protected void describe(String text, Object... args) {
    LOG.info("\n\n{}: {}\n",
        methodName.getMethodName(),
        String.format(text, args));
  }

  /**
   * Get the input stream statistics of an input stream.
   * Raises an exception if the inner stream is not an S3A input stream
   * @param in wrapper
   * @return the statistics for the inner stream
   */
  protected S3AInstrumentation.InputStreamStatistics getInputStreamStatistics(
      FSDataInputStream in) {
    InputStream inner = in.getWrappedStream();
    if (inner instanceof S3AInputStream) {
      S3AInputStream s3a = (S3AInputStream) inner;
      return s3a.getS3AStreamStatistics();
    } else {
      Assert.fail("Not an S3AInputStream: " + inner);
      // never reached
      return null;
    }
  }

  /**
   * Get the gauge value of a statistic. Raises an assertion if
   * there is no such gauge.
   * @param statistic statistic to look up
   * @return the value.
   */
  public long gaugeValue(Statistic statistic) {
    S3AInstrumentation instrumentation = fs.getInstrumentation();
    MutableGaugeLong gauge = instrumentation.lookupGauge(statistic.getSymbol());
    assertNotNull("No gauge " + statistic
        + " in " + instrumentation.dump("", " = ", "\n", true), gauge);
    return gauge.value();
  }
}
