/**
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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Linear Retry policy used by AbfsClient.
 * */
public class StaticRetryPolicy extends RetryPolicy {

  private static final int STATIC_RETRY_INTERVAL_DEFAULT = 2000; // 2s

  /**
   * Represents the constant retry interval to be used with Static Retry Policy
   */
  private int retryInterval;

  /**
   * Initializes a new instance of the {@link StaticRetryPolicy} class.
   * @param maxIoRetries Maximum Retry Count Allowed
   */
  public StaticRetryPolicy(final int maxIoRetries) {
    super(maxIoRetries);
    this.retryInterval = STATIC_RETRY_INTERVAL_DEFAULT;
  }

  /**
   * Initializes a new instance of the {@link StaticRetryPolicy} class.
   * @param conf The {@link AbfsConfiguration} from which to retrieve retry configuration.
   */
  public StaticRetryPolicy(AbfsConfiguration conf) {
    this(conf.getMaxIoRetries());
    this.retryInterval = conf.getStaticRetryInterval();
  }

  /**
   * Returns a constant backoff interval independent of retry count;
   *
   * @param retryCount The current retry attempt count.
   * @return backoff Interval time
   */
  @Override
  public long getRetryInterval(final int retryCount) {
    return retryInterval;
  }
}
