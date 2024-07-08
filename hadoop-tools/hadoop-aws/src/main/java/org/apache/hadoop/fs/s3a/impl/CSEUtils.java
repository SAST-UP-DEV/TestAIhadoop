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

package org.apache.hadoop.fs.s3a.impl;

import io.netty.util.internal.StringUtil;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_CSE_INSTRUCTION_FILE_SUFFIX;
import static org.apache.hadoop.fs.s3a.S3AUtils.formatRange;
import static org.apache.hadoop.fs.s3a.impl.AWSHeaders.CRYPTO_CEK_ALGORITHM;
import static org.apache.hadoop.fs.s3a.impl.AWSHeaders.UNENCRYPTED_CONTENT_LENGTH;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.CSE_PADDING_LENGTH;

/**
 * S3 client side encryption (CSE) utility class.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class CSEUtils {

  private CSEUtils() {
  }

  /**
   * Checks if the file suffix ends with
   * {@link org.apache.hadoop.fs.s3a.Constants#S3_ENCRYPTION_CSE_INSTRUCTION_FILE_SUFFIX}
   * when the config
   * {@link org.apache.hadoop.fs.s3a.Constants#S3_ENCRYPTION_CSE_SKIP_INSTRUCTION_FILE_DEFAULT}
   * is enabled and CSE is used.
   * @param skipCSEInstructionFile whether to skip checking for the filename suffix
   * @param key file name
   * @return true if cse is disabled or if skipping of instruction file is disabled or file name
   * does not end with defined suffix
   */
  public static boolean isCSEInstructionFile(boolean skipCSEInstructionFile, String key) {
    if (!skipCSEInstructionFile) {
      return true;
    }
    return !key.endsWith(S3_ENCRYPTION_CSE_INSTRUCTION_FILE_SUFFIX);
  }

  /**
   * Checks if CSE-KMS or CSE-CUSTOM is set.
   * @param encryptionMethod type of encryption used
   * @return true if encryption method is CSE-KMS or CSE-CUSTOM
   */
  public static boolean isCSEKmsOrCustom(String encryptionMethod) {
    return S3AEncryptionMethods.CSE_KMS.getMethod().equals(encryptionMethod) ||
        S3AEncryptionMethods.CSE_CUSTOM.getMethod().equals(encryptionMethod);
  }

  /**
   * Checks if a given S3 object is encrypted or not by checking following two conditions
   * 1. if object metadata contains x-amz-cek-alg
   * 2. if instruction file is present
   *
   * @param s3Client S3 client
   * @param bucket   bucket name of the s3 object
   * @param key      key value of the s3 object
   * @return true if S3 object is encrypted
   */
  public static boolean isObjectEncrypted(S3Client s3Client, String bucket, String key) {
    HeadObjectRequest request = HeadObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build();
    HeadObjectResponse headObjectResponse = s3Client.headObject(request);
    if (headObjectResponse.hasMetadata() &&
        headObjectResponse.metadata().get(CRYPTO_CEK_ALGORITHM) != null) {
      return true;
    }
    HeadObjectRequest instructionFileCheckRequest = HeadObjectRequest.builder()
        .bucket(bucket)
        .key(key + S3_ENCRYPTION_CSE_INSTRUCTION_FILE_SUFFIX)
        .build();
    try {
      s3Client.headObject(instructionFileCheckRequest);
      return true;
    } catch (NoSuchKeyException e) {
      // Ignore. This indicates no instruction file is present
    }
    return false;
  }

  /**
   * Get the unencrypted object length by either subtracting
   * {@link InternalConstants#CSE_PADDING_LENGTH} from the object size or calculating the
   * actual size by doing S3 ranged GET operation.
   *
   * @param s3Client           S3 client
   * @param bucket             bucket name of the s3 object
   * @param key                key value of the s3 object
   * @param contentLength      S3 object length
   * @param headObjectResponse response from headObject call
   * @param cseRangedGetEnabled is ranged get enabled
   * @param cseReadUnencryptedObjects is reading of une
   * @return unencrypted length of the object
   * @throws IOException IO failures
   */
  public static long getUnencryptedObjectLength(S3Client s3Client,
      String bucket,
      String key,
      long contentLength,
      HeadObjectResponse headObjectResponse,
      boolean cseRangedGetEnabled,
      boolean cseReadUnencryptedObjects) throws IOException {

    if (cseReadUnencryptedObjects) {
      // if object is unencrypted, return the actual size
      if (!isObjectEncrypted(s3Client, bucket, key)) {
        return contentLength;
      }
    }

    // check if unencrypted content length metadata is present or not.
    if (headObjectResponse != null) {
      String plaintextLength = headObjectResponse.metadata().get(UNENCRYPTED_CONTENT_LENGTH);
      if (headObjectResponse.hasMetadata() && !StringUtil.isNullOrEmpty(plaintextLength)) {
        return Long.parseLong(plaintextLength);
      }
    }

    if (cseRangedGetEnabled) {
      // identify the unencrypted length by doing a ranged GET operation.
      if (contentLength >= CSE_PADDING_LENGTH) {
        long minPlaintextLength = contentLength - CSE_PADDING_LENGTH;
        if (minPlaintextLength < 0) {
          minPlaintextLength = 0;
        }
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .range(formatRange(minPlaintextLength, contentLength))
            .build();
        InputStream inputStream = s3Client.getObject(getObjectRequest);
        try (InputStream is = inputStream) {
          int i = 0;
          while (is.read() != -1) {
            i++;
          }
          return minPlaintextLength + i;
        }
      }
      return contentLength;
    }

    long unpaddedLength = contentLength - CSE_PADDING_LENGTH;
    if (unpaddedLength >= 0) {
      return unpaddedLength;
    }
    return contentLength;
  }
}
