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

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;

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
import static software.amazon.awssdk.core.sync.RequestBody.fromInputStream;

/**
 * Test datablock recovery by injecting failures into the response chain.
 */
@RunWith(Parameterized.class)
public class ITestUploadRecovery extends AbstractS3ACostTest {

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {FAST_UPLOAD_BUFFER_ARRAY},
        {FAST_UPLOAD_BUFFER_DISK},
        {FAST_UPLOAD_BYTEBUFFER},
    });
  }

  public static final Function<Context.ModifyHttpResponse, Boolean>
      ALWAYS_ALLOW = (c) -> false;

  /**
   * How many faults to trigger.
   * Reset in setup.
   */
  public static final AtomicInteger failureCount = new AtomicInteger(1);

  private static Function<Context.ModifyHttpResponse, Boolean> evaluator;

  private static boolean wrapContentProvider = true;
  /**
   * Buffer type.
   */
  private final String buffer;

  public ITestUploadRecovery(final String buffer) {
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
  private static void setFailureCount(int count) {
    failureCount.set(count);
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
    setFailureCount(2);
    resetEvaluator();
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
    setFailureCount(2);
    final S3AFileSystem fs = getFileSystem();
    final Path path = methodPath();
    setEvaluator((context) ->
        context.httpRequest().method().equals(SdkHttpMethod.PUT));
    final FSDataOutputStream out = fs.create(path);
    out.writeUTF("utfstring");
    out.close();
  }

  @Test
  public void testMagicWriteRecovery() throws Throwable {
    describe("test post recovery");

    final S3AFileSystem fs = getFileSystem();
    final Path path = new Path(methodPath(),
        MAGIC_PATH_PREFIX + buffer + "/" + BASE + "/file.txt");

    setEvaluator((context) ->
        context.httpRequest().method().equals(SdkHttpMethod.POST));
    final FSDataOutputStream out = fs.create(path);

    // set the failure count again
    setFailureCount(2);

    out.writeUTF("utfstring");
    out.close();
  }


  @Test
  public void testMagicWriteRecoveryWrapped() throws Throwable {
    describe("test recovery of original");

    final S3AFileSystem fs = getFileSystem();
    final Path path = new Path(methodPath(),
        MAGIC_PATH_PREFIX + buffer + "/" + BASE + "/file.txt");

    setEvaluator((context) ->
        context.httpRequest().method().equals(SdkHttpMethod.POST));
    final FSDataOutputStream out = fs.create(path);

    // set the failure count again
    setFailureCount(1);

    out.writeUTF("utfstring");
    out.close();
  }



  /**
   * This runs inside the AWS execution pipeline so can insert faults and so
   * trigger recovery in the SDK.
   * We use this to verify that recovery works.
   */

  public static final class FaultInjector implements ExecutionInterceptor {

    @Override
    public SdkRequest modifyRequest(final Context.ModifyRequest context,
        final ExecutionAttributes executionAttributes) {

      return ExecutionInterceptor.super.modifyRequest(context, executionAttributes);
    }

    @Override
    public Optional<RequestBody> modifyHttpContent(final Context.ModifyHttpRequest context,
        final ExecutionAttributes executionAttributes) {
      SdkRequest request = context.request();
      Optional<RequestBody> body = context.requestBody();
      if (wrapContentProvider && body.isPresent()) {
        if (request instanceof UploadPartRequest && shouldFail()) {
          final RequestBody rb = body.get();
          body = Optional.of(fromInputStream(
              rb.contentStreamProvider().newStream(),
              rb.contentLength()));
          // and set the post process to fail too
          setFailureCount(1);
        }
      }
      return body;
    }

    @Override
    public SdkHttpResponse modifyHttpResponse(final Context.ModifyHttpResponse context,
        final ExecutionAttributes executionAttributes) {
      SdkHttpResponse httpResponse = context.httpResponse();
      final int failures = failureCount.get();
      if (evaluator.apply(context) && shouldFail()) {

        return httpResponse.copy(b -> {
          b.statusCode(500);
        });

      } else {
        return httpResponse;
      }
    }
  }

  private static boolean shouldFail() {
    return failureCount.decrementAndGet() > 0;
  }
}
