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

package org.apache.hadoop.fs.s3a.commit.magic;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.files.SinglePendingCommit;
import org.apache.hadoop.fs.s3a.commit.impl.CommitContext;
import org.apache.hadoop.fs.s3a.commit.impl.CommitOperations;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;

import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_MULTIPART_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_ARRAY;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_DISK;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BYTEBUFFER;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.enableLoggingAuditor;
import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.resetAuditOptions;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_EXECUTION_INTERCEPTORS;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.BASE;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_PATH_PREFIX;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_500_INTERNAL_SERVER_ERROR;
import static software.amazon.awssdk.core.sync.RequestBody.fromInputStream;

/**
 * Test datablock recovery by injecting failures into the response chain.
 */
@RunWith(Parameterized.class)
public class ITestUploadRecovery extends AbstractS3ACostTest {

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}-wrap-{1}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {FAST_UPLOAD_BUFFER_ARRAY, true },
        {FAST_UPLOAD_BUFFER_ARRAY, false},
        {FAST_UPLOAD_BUFFER_DISK, true },
        {FAST_UPLOAD_BUFFER_DISK, false},
        {FAST_UPLOAD_BYTEBUFFER, true },
        {FAST_UPLOAD_BYTEBUFFER, false},
    });
  }
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestUploadRecovery.class);

  private static final String JOB_ID = UUID.randomUUID().toString();

  /**
   * Always allow requests.
   */
  public static final Function<Context.ModifyHttpResponse, Boolean>
      ALWAYS_ALLOW = (c) -> false;

  /**
   * How many requests with the matching evaluator to fail on.
   */
  public static final AtomicInteger requestFailureCount = new AtomicInteger(1);

  /**
   * How many requests triggered a failure?
   */
  public static final AtomicInteger requestTriggerCount = new AtomicInteger(0);

  /**
   * Evaluator for responses.
   */
  private static Function<Context.ModifyHttpResponse, Boolean> evaluator;

  /**
   * should the new content provider be wrapped?
   */
  private static boolean wrapContentProvider;

  /**
   * should the new content provider be wrapped?
   */
  private final boolean wrap;

  /**
   * Buffer type.
   */
  private final String buffer;

  public ITestUploadRecovery(final String buffer, final boolean wrap) {
    this.wrap = wrap;
    this.buffer = buffer;
  }

  /**
   * Reset the evaluator to enable everything.
   */
  private static void resetEvaluator() {
    setEvaluator(ALWAYS_ALLOW);
  }

  /**
   * Set the failure count;
   * @param count failure count;
   */
  private static void setRequestFailureCount(int count) {
    LOG.debug("Failure count set to {}", count);
    requestFailureCount.set(count);
  }

  private static void setEvaluator(Function<Context.ModifyHttpResponse, Boolean> evaluator) {
    ITestUploadRecovery.evaluator = evaluator;
  }

  private static void setWrapContentProvider(boolean wrapContentProvider) {
    ITestUploadRecovery.wrapContentProvider = wrapContentProvider;
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    resetAuditOptions(conf);
    enableLoggingAuditor(conf);
    removeBaseAndBucketOverrides(conf, FAST_UPLOAD_BUFFER);
    conf.set(FAST_UPLOAD_BUFFER, buffer);
    // use the fault injector
    conf.set(AUDIT_EXECUTION_INTERCEPTORS, FaultInjector.class.getName());
    return conf;
  }

  /**
   * Setup MUST set up the evaluator before the FS is created.
   */
  @Override
  public void setup() throws Exception {
    setRequestFailureCount(2);
    resetEvaluator();
    setWrapContentProvider(wrap);
    super.setup();
  }

  @Override
  public void teardown() throws Exception {
    resetEvaluator();
    final S3AFileSystem fs = getFileSystem();
    // cancel any incomplete uploads
    fs.getS3AInternals().abortMultipartUploads(methodPath());
    super.teardown();
  }

  @Test
  public void testPutRecovery() throws Throwable {
    describe("test put recovery");
    final S3AFileSystem fs = getFileSystem();
    final Path path = methodPath();
    setEvaluator(ITestUploadRecovery::isPartUpload);
    setRequestFailureCount(2);
    final FSDataOutputStream out = fs.create(path);
    out.writeUTF("utfstring");
    out.close();
  }

  private static boolean isPutRequest(final Context.ModifyHttpResponse context) {
    return context.httpRequest().method().equals(SdkHttpMethod.PUT);
  }

  @Test
  public void testMagicWriteRecovery() throws Throwable {
    describe("test post recovery");

    final S3AFileSystem fs = getFileSystem();
    final Path path = new Path(methodPath(),
        MAGIC_PATH_PREFIX + buffer + "/" + BASE + "/file.txt");

    setEvaluator(ITestUploadRecovery::isPartUpload);
    final FSDataOutputStream out = fs.create(path);

    // set the failure count again
    setRequestFailureCount(2);

    out.writeUTF("utfstring");
    out.close();
  }

  private static boolean isPostRequest(final Context.ModifyHttpResponse context) {
    return context.httpRequest().method().equals(SdkHttpMethod.POST);
  }

  /**
   * Is the request a commit completion request?
   * @param context response
   * @return true if the predicate matches
   */
  private static boolean isCommitCompletion(final Context.ModifyHttpResponse context) {
    return context.request() instanceof CompleteMultipartUploadRequest;
  }

  /**
   * Is the request a commit completion request?
   * @param context response
   * @return true if the predicate matches
   */
  private static boolean isPartUpload(final Context.ModifyHttpResponse context) {
    return context.request() instanceof UploadPartRequest;
  }


  @Test
  public void testCommitOperations() throws Throwable {
    describe("test staging upload");
    final S3AFileSystem fs = getFileSystem();
    final byte[] dataset = ContractTestUtils.dataset(1024 * 256, '0', 36);
    File tempFile = File.createTempFile("commit", ".txt");
    String text = "hello, world";
    FileUtils.writeByteArrayToFile(tempFile, dataset);
    CommitOperations actions = new CommitOperations(fs);
    Path dest = methodPath();
    setRequestFailureCount(2);
    setEvaluator(ITestUploadRecovery::isPartUpload);

    SinglePendingCommit commit =
        actions.uploadFileToPendingCommit(tempFile,
            dest,
            null,
            DEFAULT_MULTIPART_SIZE,
            () -> {});
    setRequestFailureCount(2);
    try (CommitContext commitContext
             = actions.createCommitContextForTesting(dest, JOB_ID, 0)) {
      commitContext.commitOrFail(commit);
    }
  }

  /**
   * This runs inside the AWS execution pipeline so can insert faults and so
   * trigger recovery in the SDK.
   * We use this to verify that recovery works.
   */

  public static final class FaultInjector implements ExecutionInterceptor {


    @Override
    public Optional<RequestBody> modifyHttpContent(final Context.ModifyHttpRequest context,
        final ExecutionAttributes executionAttributes) {
      SdkRequest request = context.request();
      Optional<RequestBody> body = context.requestBody();
      if (wrapContentProvider && body.isPresent()) {
        if (request instanceof UploadPartRequest && shouldFail()) {
          LOG.info("wrapping body of request {}", request);
          final RequestBody rb = body.get();
          body = Optional.of(fromInputStream(
              rb.contentStreamProvider().newStream(),
              rb.contentLength()));
          // and set the post process to fail too
          requestFailureCount.incrementAndGet();
        }
      }
      return body;
    }

    @Override
    public SdkHttpResponse modifyHttpResponse(final Context.ModifyHttpResponse context,
        final ExecutionAttributes executionAttributes) {
      SdkRequest request = context.request();
      SdkHttpResponse httpResponse = context.httpResponse();
      if (evaluator.apply(context) && shouldFail()) {
        LOG.info("reporting 500 error code for request {}", request);

        return httpResponse.copy(b -> {
          b.statusCode(SC_500_INTERNAL_SERVER_ERROR);
        });

      } else {
        return httpResponse;
      }
    }
  }

  private static boolean shouldFail() {
    return requestFailureCount.decrementAndGet() > 0;
  }
}
