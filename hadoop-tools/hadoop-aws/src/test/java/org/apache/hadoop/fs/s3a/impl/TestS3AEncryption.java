package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.s3a.Constants.S3_ENCRYPTION_CONTEXT;

public class TestS3AEncryption {

  private static final String GLOBAL_CONTEXT = "  project=hadoop, jira=HADOOP-19197  ";
  private static final String BUCKET_CONTEXT = "component=fs/s3";

  @Test
  public void testGetS3EncryptionContextPerBucket() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.set("fs.s3a.bucket.bucket1.encryption.context", BUCKET_CONTEXT);
    configuration.set(S3_ENCRYPTION_CONTEXT, GLOBAL_CONTEXT);
    final String result = S3AEncryption.getS3EncryptionContext("bucket1", configuration);
    Assert.assertEquals(BUCKET_CONTEXT, result);
  }

  @Test
  public void testGetS3EncryptionContextFromGlobal() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.set("fs.s3a.bucket.bucket1.encryption.context", BUCKET_CONTEXT);
    configuration.set(S3_ENCRYPTION_CONTEXT, GLOBAL_CONTEXT);
    final String result = S3AEncryption.getS3EncryptionContext("bucket2", configuration);
    Assert.assertEquals(GLOBAL_CONTEXT.trim(), result);
  }

  @Test
  public void testGetS3EncryptionContextNoSet() throws IOException {
    Configuration configuration = new Configuration(false);
    final String result = S3AEncryption.getS3EncryptionContext("bucket1", configuration);
    Assert.assertEquals("", result);
  }

  @Test
  public void testGetS3EncryptionContextBase64Encoded() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.set(S3_ENCRYPTION_CONTEXT, GLOBAL_CONTEXT);
    final String result = S3AEncryption.getS3EncryptionContextBase64Encoded("bucket",
        configuration, true);
    final String decoded = new String(Base64.decodeBase64(result), StandardCharsets.UTF_8);
    final TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>() {};
    final Map<String, String> resultMap = new ObjectMapper().readValue(decoded, typeRef);
    Assert.assertEquals("hadoop", resultMap.get("project"));
    Assert.assertEquals("HADOOP-19197", resultMap.get("jira"));
  }
  
}
