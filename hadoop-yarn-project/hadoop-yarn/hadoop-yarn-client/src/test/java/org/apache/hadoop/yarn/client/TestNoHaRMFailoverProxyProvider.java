/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestNoHaRMFailoverProxyProvider {
    private final int NODE_MANAGER_COUNT = 1;
    private Configuration conf;

    @Before
    public void setUp() throws IOException, YarnException {
        conf = new YarnConfiguration();
    }

    @Test
    public void testRestartedRM() throws Exception {
        MiniYARNCluster cluster =
                new MiniYARNCluster(
                        "testRestartedRMNegative", NODE_MANAGER_COUNT, 1, 1);
        YarnClient rmClient = YarnClient.createYarnClient();
        try {
            cluster.init(conf);
            cluster.start();
            final Configuration yarnConf = cluster.getConfig();
            rmClient = YarnClient.createYarnClient();
            rmClient.init(yarnConf);
            rmClient.start();
            List<NodeReport> nodeReports = rmClient.getNodeReports();
            Assert.assertEquals(
                    "The proxy didn't get expected number of node reports",
                    NODE_MANAGER_COUNT, nodeReports.size());
        } finally {
            if (rmClient != null) {
                rmClient.stop();
            }
            cluster.stop();
        }
    }

    /**
     * Tests the proxy generated by {@link AutoRefreshNoHARMFailoverProxyProvider}
     * will connect to RM.
     */
    @Test
    public void testConnectingToRM() throws Exception {
        conf.set(YarnConfiguration.CLIENT_FAILOVER_NO_HA_PROXY_PROVIDER,
                AutoRefreshNoHARMFailoverProxyProvider.class.getName());

        MiniYARNCluster cluster =
                new MiniYARNCluster(
                        "testRestartedRMNegative", NODE_MANAGER_COUNT, 1, 1);
        YarnClient rmClient = null;
        try {
            cluster.init(conf);
            cluster.start();
            final Configuration yarnConf = cluster.getConfig();
            rmClient = YarnClient.createYarnClient();
            rmClient.init(yarnConf);
            rmClient.start();
            List<NodeReport> nodeReports = rmClient.getNodeReports();
            Assert.assertEquals(
                    "The proxy didn't get expected number of node reports",
                    NODE_MANAGER_COUNT, nodeReports.size());
        } finally {
            if (rmClient != null) {
                rmClient.stop();
            }
            cluster.stop();
        }
    }

    @Test
    public void testDefaultFPPGetOneProxy() throws Exception {
        class TestProxy extends Proxy implements Closeable {
            protected TestProxy(InvocationHandler h) {
                super(h);
            }

            @Override
            public void close() throws IOException {
            }
        }

        // Create two proxies and mock a RMProxy
        Proxy mockProxy1 = new TestProxy((proxy, method, args) -> null);
        Class protocol = ApplicationClientProtocol.class;
        RMProxy mockRMProxy = mock(RMProxy.class);
        DefaultNoHARMFailoverProxyProvider<RMProxy> fpp =
                new DefaultNoHARMFailoverProxyProvider<RMProxy>();

        Random rand = new Random();
        int port1 = rand.nextInt(65535);

        InetSocketAddress mockAdd1 = new InetSocketAddress(port1);

        // Mock RMProxy methods
        when(mockRMProxy.getRMAddress(any(YarnConfiguration.class),
                any(Class.class))).thenReturn(mockAdd1);
        when(mockRMProxy.getProxy(any(YarnConfiguration.class),
                any(Class.class), eq(mockAdd1))).thenReturn(mockProxy1);

        // Initialize failover proxy provider and get proxy from it.
        fpp.init(conf, mockRMProxy, protocol);
        FailoverProxyProvider.ProxyInfo<RMProxy> actualProxy1 = fpp.getProxy();
        Assert.assertEquals(
                "AutoFefreshRMFailoverProxyProvider doesn't generate " +
                        "expected proxy",
                mockProxy1, actualProxy1.proxy);

        // Invoke fpp.getProxy() multiple times and
        // validate the returned proxy is always mockProxy1
        actualProxy1 = fpp.getProxy();
        Assert.assertEquals(
                "AutoFefreshRMFailoverProxyProvider doesn't generate " +
                        "expected proxy",
                mockProxy1, actualProxy1.proxy);
        actualProxy1 = fpp.getProxy();
        Assert.assertEquals(
                "AutoFefreshRMFailoverProxyProvider doesn't generate " +
                        "expected proxy",
                mockProxy1, actualProxy1.proxy);

        // verify that mockRMProxy.getProxy() is invoked once only.
        verify(mockRMProxy, times(1))
                .getProxy(any(YarnConfiguration.class), any(Class.class),
                        eq(mockAdd1));

        // Perform Failover and get proxy again from failover proxy provider
        fpp.performFailover(actualProxy1.proxy);
        FailoverProxyProvider.ProxyInfo<RMProxy> actualProxy2 = fpp.getProxy();
        Assert.assertEquals("AutoFefreshRMFailoverProxyProvider " +
                        "doesn't generate expected proxy after failover",
                mockProxy1, actualProxy2.proxy);

        // verify that mockRMProxy.getProxy() didn't get invoked again after
        // performFailover()
        verify(mockRMProxy, times(1))
                .getProxy(any(YarnConfiguration.class), any(Class.class),
                        eq(mockAdd1));
    }

    /**
     * Test that the {@link AutoRefreshNoHARMFailoverProxyProvider}
     * will generate different proxies after RM IP changed
     * and {@link AutoRefreshNoHARMFailoverProxyProvider#performFailover(Object)}
     * get called.
     */
    @Test
    public void testAutoRefreshIPChange() throws Exception {
        conf.set(YarnConfiguration.CLIENT_FAILOVER_NO_HA_PROXY_PROVIDER,
                AutoRefreshNoHARMFailoverProxyProvider.class.getName());

        class TestProxy extends Proxy implements Closeable {
            protected TestProxy(InvocationHandler h) {
                super(h);
            }

            @Override
            public void close() throws IOException {
            }
        }

        // Create two proxies and mock a RMProxy
        Proxy mockProxy1 = new TestProxy((proxy, method, args) -> null);
        Proxy mockProxy2 = new TestProxy((proxy, method, args) -> null);
        Class protocol = ApplicationClientProtocol.class;
        RMProxy mockRMProxy = mock(RMProxy.class);
        AutoRefreshNoHARMFailoverProxyProvider<RMProxy> fpp =
                new AutoRefreshNoHARMFailoverProxyProvider<RMProxy>();

        // generate two address with different random port.
        Random rand = new Random();
        int port1 = rand.nextInt(65535);
        int port2 = rand.nextInt(65535);
        while (port1 == port2) {
            port2 = rand.nextInt(65535);
        }
        InetSocketAddress mockAdd1 = new InetSocketAddress(port1);
        InetSocketAddress mockAdd2 = new InetSocketAddress(port2);

        // Mock RMProxy methods
        when(mockRMProxy.getRMAddress(any(YarnConfiguration.class),
                any(Class.class))).thenReturn(mockAdd1);
        when(mockRMProxy.getProxy(any(YarnConfiguration.class),
                any(Class.class), eq(mockAdd1))).thenReturn(mockProxy1);

        // Initialize proxy provider and get proxy from it.
        fpp.init(conf, mockRMProxy, protocol);
        FailoverProxyProvider.ProxyInfo<RMProxy> actualProxy1 = fpp.getProxy();
        Assert.assertEquals(
                "AutoFefreshRMFailoverProxyProvider doesn't generate " +
                        "expected proxy",
                mockProxy1, actualProxy1.proxy);

        // Invoke fpp.getProxy() multiple times and
        // validate the returned proxy is always mockProxy1
        actualProxy1 = fpp.getProxy();
        Assert.assertEquals(
                "AutoFefreshRMFailoverProxyProvider doesn't generate " +
                        "expected proxy",
                mockProxy1, actualProxy1.proxy);
        actualProxy1 = fpp.getProxy();
        Assert.assertEquals(
                "AutoFefreshRMFailoverProxyProvider doesn't generate " +
                        "expected proxy",
                mockProxy1, actualProxy1.proxy);

        // verify that mockRMProxy.getProxy() is invoked once only.
        verify(mockRMProxy, times(1))
                .getProxy(any(YarnConfiguration.class), any(Class.class),
                        eq(mockAdd1));

        // Mock RMProxy methods to generate different proxy
        // based on different IP address.
        when(mockRMProxy.getRMAddress(
                any(YarnConfiguration.class),
                any(Class.class))).thenReturn(mockAdd2);
        when(mockRMProxy.getProxy(
                any(YarnConfiguration.class),
                any(Class.class), eq(mockAdd2))).thenReturn(mockProxy2);

        // Perform Failover and get proxy again from failover proxy provider
        fpp.performFailover(actualProxy1.proxy);
        FailoverProxyProvider.ProxyInfo<RMProxy> actualProxy2 = fpp.getProxy();
        Assert.assertEquals("AutoRefreshNoHARMFailoverProxyProvider " +
                        "doesn't generate expected proxy after failover",
                mockProxy2, actualProxy2.proxy);

        // check the proxy is different with the one we created before.
        Assert.assertNotEquals("AutoRefreshNoHARMFailoverProxyProvider " +
                        "shouldn't generate same proxy after failover",
                actualProxy1.proxy, actualProxy2.proxy);
    }
}
