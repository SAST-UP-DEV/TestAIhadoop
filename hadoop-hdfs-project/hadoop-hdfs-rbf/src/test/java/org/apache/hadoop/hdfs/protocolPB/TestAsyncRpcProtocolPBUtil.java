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

package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.TestRPC;
import org.apache.hadoop.ipc.TestRpcBase;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.syncReturn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAsyncRpcProtocolPBUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncRpcProtocolPBUtil.class);
  private static final int SERVER_PROCESS_COST_MS = 100;
  private ClientPBImpl clientPB;
  private Server server;

  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    RPC.setProtocolEngine(conf, TestRpcBase.TestRpcService.class,
        ProtobufRpcEngine2.class);

    // Create server side implementation
    ServerPBImpl serverImpl = new ServerPBImpl();
    BlockingService service = TestRpcServiceProtos.TestProtobufRpcProto
        .newReflectiveBlockingService(serverImpl);

    // start the IPC server
    server = new RPC.Builder(conf)
        .setProtocol(TestRpcBase.TestRpcService.class)
        .setInstance(service).setBindAddress("0.0.0.0")
        .setPort(0).setNumHandlers(1).setVerbose(true).build();

    server.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(server);

    TestRpcBase.TestRpcService proxy = RPC.getProxy(TestRpcBase.TestRpcService.class,
        TestRPC.TestProtocol.versionID, addr, conf);
    clientPB = new ClientPBImpl(proxy);
    Client.setAsynchronousMode(true);
    clientPB.ping();
  }

  @After
  public void clear() {
    if (clientPB != null) {
      clientPB.close();
    }
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testAsyncIpcClient() throws Exception {
    Client.setAsynchronousMode(true);
    long start = Time.monotonicNow();
    clientPB.add(1, 2);
    long cost = Time.monotonicNow() - start;
    LOG.info("rpc client add {} {}, cost: {}ms", 1, 2, cost);
    Integer res = syncReturn(Integer.class);
    checkResult(3, res, cost);

    start = Time.monotonicNow();
    clientPB.echo("test echo!");
    cost = Time.monotonicNow() - start;
    LOG.info("rpc client echo {}, cost: {}ms", "test echo!", cost);
    String value = syncReturn(String.class);
    checkResult("test echo!", value, cost);

    start = Time.monotonicNow();
    clientPB.error();
    LOG.info("rpc client error, cost: {}ms", Time.monotonicNow() - start);
    LambdaTestUtils.intercept(RemoteException.class, "test!",
        () -> AsyncUtil.syncReturn(String.class));
  }

  private void checkResult(Object expected, Object actual, long cost) {
    assertTrue(cost < SERVER_PROCESS_COST_MS);
    assertEquals(expected, actual);
  }

  public static class ClientPBImpl implements TestClientProtocol, Closeable {

    final private TestRpcBase.TestRpcService rpcProxy;

    ClientPBImpl(TestRpcBase.TestRpcService rpcProxy) {
      this.rpcProxy = rpcProxy;
    }

    @Override
    public void ping() throws IOException {
      TestProtos.EmptyRequestProto req = TestProtos.EmptyRequestProto.newBuilder()
          .build();

      AsyncRpcProtocolPBUtil.asyncIpcClient(() -> rpcProxy.ping(null, req),
          res -> null, null);
    }

    @Override
    public String echo(String echoMessage) throws IOException {
      TestProtos.EchoRequestProto req = TestProtos.EchoRequestProto.newBuilder()
          .setMessage(echoMessage)
          .build();

      return AsyncRpcProtocolPBUtil.asyncIpcClient(() -> rpcProxy.echo(null, req),
          res -> res.getMessage(), String.class);
    }

    @Override
    public void error() throws IOException {
      TestProtos.EmptyRequestProto req = TestProtos.EmptyRequestProto.newBuilder()
          .build();

      AsyncRpcProtocolPBUtil.asyncIpcClient(() -> rpcProxy.error(null, req),
          res -> null, null);
    }

    @Override
    public int add(int num1, int num2) throws IOException {
      TestProtos.AddRequestProto req = TestProtos.AddRequestProto.newBuilder()
          .setParam1(num1)
          .setParam2(num2)
          .build();

      return AsyncRpcProtocolPBUtil.asyncIpcClient(() -> rpcProxy.add(null, req),
          res -> res.getResult(), Integer.class);
    }

    @Override
    public void close() {
      RPC.stopProxy(rpcProxy);
    }
  }

  public static class ServerPBImpl extends TestRpcBase.PBServerImpl {

    @Override
    public TestProtos.EmptyResponseProto error(
        RpcController unused, TestProtos.EmptyRequestProto request)
        throws ServiceException {
      long start = Time.monotonicNow();
      try {
        Thread.sleep(SERVER_PROCESS_COST_MS);
        throw new ServiceException("error", new StandbyException("test!"));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        LOG.info("rpc server error cost: {}ms", Time.monotonicNow() - start);
      }
      return null;
    }

    @Override
    public TestProtos.EchoResponseProto echo(
        RpcController unused, TestProtos.EchoRequestProto request) throws ServiceException {
      TestProtos.EchoResponseProto res = null;
      long start = Time.monotonicNow();
      try {
        Thread.sleep(SERVER_PROCESS_COST_MS);
        res = super.echo(unused, request);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        LOG.info("rpc server echo: {}, result: {}, cost: {}ms", request.getMessage(),
            res.getMessage(), Time.monotonicNow() - start);
      }
      return res;
    }

    @Override
    public TestProtos.AddResponseProto add(
        RpcController controller, TestProtos.AddRequestProto request) throws ServiceException {
      TestProtos.AddResponseProto res = null;
      long start = Time.monotonicNow();
      try {
        Thread.sleep(SERVER_PROCESS_COST_MS);
        res = super.add(controller, request);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        LOG.info("rpc server add: {} {}, result: {}, cost: {}ms",
            request.getParam1(), request.getParam2(), res.getResult(), Time.monotonicNow() - start);
      }
      return res;
    }
  }

  interface TestClientProtocol {
    void ping() throws IOException;

    String echo(String echoMessage) throws IOException;

    void error() throws IOException;

    int add(int num1, int num2) throws IOException;
  }
}