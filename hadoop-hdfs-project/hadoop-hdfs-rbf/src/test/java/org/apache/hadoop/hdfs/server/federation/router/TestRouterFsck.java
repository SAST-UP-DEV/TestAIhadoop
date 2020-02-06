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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * End-to-end tests for fsck via DFSRouter
 */
public class TestRouterFsck {

  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static MountTableResolver mountTable;
  private static FileSystem routerFs;
  private static InetSocketAddress webAddress;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .http()
        .build();
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    // Get the end points
    routerContext = cluster.getRandomRouter();
    routerFs = routerContext.getFileSystem();
    Router router = routerContext.getRouter();
    mountTable = (MountTableResolver) router.getSubclusterResolver();
    webAddress = router.getHttpServerAddress();
    Assert.assertNotNull(webAddress);
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @After
  public void clearMountTable() throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    GetMountTableEntriesRequest req1 =
        GetMountTableEntriesRequest.newInstance("/");
    GetMountTableEntriesResponse response =
        mountTableManager.getMountTableEntries(req1);
    for (MountTable entry : response.getEntries()) {
      RemoveMountTableEntryRequest req2 =
          RemoveMountTableEntryRequest.newInstance(entry.getSourcePath());
      mountTableManager.removeMountTableEntry(req2);
    }
  }

  private boolean addMountTable(final MountTable entry) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(entry);
    AddMountTableEntryResponse addResponse =
        mountTableManager.addMountTableEntry(addRequest);
    // Reload the Router cache
    mountTable.loadCache(true);
    return addResponse.getStatus();
  }

  @Test
  public void testFsck() throws Exception {
    MountTable addEntry = MountTable.newInstance("/testdir",
        Collections.singletonMap("ns0", "/testdir"));
    Assert.assertTrue(addMountTable(addEntry));
    addEntry = MountTable.newInstance("/testdir2",
        Collections.singletonMap("ns1", "/testdir2"));
    Assert.assertTrue(addMountTable(addEntry));
    // create 1 file on ns0
    routerFs.createNewFile(new Path("/testdir/testfile"));
    // create 3 files on ns1
    routerFs.createNewFile(new Path("/testdir2/testfile2"));
    routerFs.createNewFile(new Path("/testdir2/testfile3"));
    routerFs.createNewFile(new Path("/testdir2/testfile4"));

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      // TODO: support https
      HttpGet httpGet = new HttpGet(
          "http://" + webAddress.getHostName() +
              ":" + webAddress.getPort() + "/fsck");
      try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
        Assert.assertEquals(HttpStatus.SC_OK,
            httpResponse.getStatusLine().getStatusCode());
        String out = EntityUtils.toString(
            httpResponse.getEntity(), StandardCharsets.UTF_8);
        System.out.println(out);
        Assert.assertTrue(out.contains("Federated FSCK started"));
        // assert 1 file exists in a cluster and 3 files exist in another cluster
        Assert.assertTrue(out.contains("Total files:\t1"));
        Assert.assertTrue(out.contains("Total files:\t3"));
        Assert.assertTrue(out.contains("Federated FSCK ended"));
      }
    }
  }
}
