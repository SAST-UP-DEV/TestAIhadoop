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
package org.apache.hadoop.fs.viewfs;

import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestHDFSFileSystemContract;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests ViewFsOverloadScheme with file system contract tests.
 */
public class TestViewFsOverloadSchemeHdfsFileSystemContract
    extends TestHDFSFileSystemContract {

  private MiniDFSCluster cluster;
  private String defaultWorkingDirectory;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,
        FileSystemContractBaseTest.TEST_UMASK);
    final File basedir = GenericTestUtils.getRandomizedTestDir();
    cluster = new MiniDFSCluster.Builder(conf, basedir)
        .numDataNodes(2)
        .build();
    defaultWorkingDirectory =
        "/user/" + UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set(
        String.format(FsConstants.FS_IMPL_PATTERN_KEY,
            FsConstants.VIEWFS_OVERLOAD_SCHEME_DEFAULT),
        ViewFsOverloadScheme.class.getName());
    conf.set(String.format(
        FsConstants.FS_VIEWFS_OVERLOAD_SCHEME_TARGET_FS_IMPL_PATTERN_KEY,
        FsConstants.VIEWFS_OVERLOAD_SCHEME_DEFAULT),
        DistributedFileSystem.class.getName());
    conf.set(FsConstants.VIEWFS_OVERLOAD_SCHEME_KEY,
        FsConstants.VIEWFS_OVERLOAD_SCHEME_DEFAULT);
    URI defaultFSURI =
        URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    ConfigUtil.addLink(conf, defaultFSURI.getAuthority(), "/user",
        defaultFSURI);
    ConfigUtil.addLink(conf, defaultFSURI.getAuthority(), "/append",
        defaultFSURI);
    ConfigUtil.addLink(conf, defaultFSURI.getAuthority(),
        "/FileSystemContractBaseTest/",
        new URI(defaultFSURI.toString() + "/FileSystemContractBaseTest/"));
    fs = (ViewFsOverloadScheme) FileSystem.get(conf);
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Override
  protected String getDefaultWorkingDirectory() {
    return defaultWorkingDirectory;
  }

  @Override
  @Test
  public void testAppend() throws IOException {
    AppendTestUtil.testAppend(fs, new Path("/append/f"));
  }

  @Override
  @Test(expected = AccessControlException.class)
  public void testRenameRootDirForbidden() throws Exception {
    super.testRenameRootDirForbidden();
  }

  @Override
  @Test
  public void testListStatusRootDir() throws Throwable {
    assumeTrue(rootDirTestEnabled());
    Path dir = path("/");
    Path child = path("/FileSystemContractBaseTest");
    assertListStatusFinds(dir, child);
  }

  @Override
  @Ignore // This test same as above in this case.
  public void testLSRootDir() throws Throwable {
  }
}
