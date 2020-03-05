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

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface for {@link AbfsOutputStream} statistics.
 */
@InterfaceStability.Unstable
public interface AbfsOutputStreamStatistics {

  /**
   * Number of bytes to be uploaded.
   *
   * @param bytes number of bytes to upload.
   */
  void bytesToUpload(long bytes);

  /**
   * Records a successful upload and the number of bytes uploaded.
   *
   * @param bytes number of bytes that were successfully uploaded.
   */
  void uploadSuccessful(long bytes);

  /**
   * Records that upload is failed and the number of bytes.
   *
   * @param bytes number of bytes that failed to upload.
   */
  void uploadFailed(long bytes);

  /**
   * Time spent in waiting for tasks to be completed in the blocking Queue.
   *
   * @param start millisecond at which the wait for task to be complete begins.
   * @param end   millisecond at which the wait is completed for the task.
   */
  void timeSpentTaskWait(long start, long end);

  /**
   * Number of times {@code AbfsOutputStream#shrinkWriteOperationQueue()}
   * method was called.
   */
  void queueShrunk();

  /**
   * Number of times
   * {@code AbfsOutputStream#writeCurrentBufferToService(boolean, boolean)}
   * method was called.
   */
  void writeCurrentBuffer();

  /**
   * Method to form a String of all AbfsOutputStream counters and their values.
   *
   * @return AbfsOutputStream statistics.
   */
  @Override
  String toString();

}
