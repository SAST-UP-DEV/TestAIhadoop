/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_CONTEXT;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.SSE_KMS;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;

/**
 * Concrete class that extends {@link AbstractTestS3AEncryption}
 * and tests SSE-KMS encryption when no KMS encryption key is provided and AWS
 * uses the default.  Since this resource changes for every account and region,
 * there is no good way to explicitly set this value to do a equality check
 * in the response.
 */
public class ITestS3AEncryptionSSEKMSDefaultKeyWithEncryptionContext
    extends ITestS3AEncryptionSSEKMSDefaultKey {

  @Override
  protected Configuration createConfiguration() {
    // get the KMS context for this test.
    Configuration c = new Configuration();
    String encryptionContext = S3AUtils.getS3EncryptionContext(getTestBucketName(c), c);
    if (StringUtils.isBlank(encryptionContext)) {
      skip(S3_ENCRYPTION_CONTEXT + " is not set for " +
              SSE_KMS.getMethod());
    }
    Configuration conf = super.createConfiguration();
    conf.set(Constants.S3_ENCRYPTION_KEY, "");
    conf.set(S3_ENCRYPTION_CONTEXT, encryptionContext);
    return conf;
  }
}
