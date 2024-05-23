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

package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.hdfs.server.federation.router.RouterAsyncRpcUtil;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.ProtobufRpcEngineCallback2;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.internal.ShadedProtobufHelper;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;


import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.ipc;

public final class AsyncRpcProtocolPBUtil {
  public static final Logger LOG = LoggerFactory.getLogger(AsyncRpcProtocolPBUtil.class);
  public static final ThreadLocal<CompletableFuture<Object>> completableFutureThreadLocal
      = new ThreadLocal<>();

  private AsyncRpcProtocolPBUtil() {}

  public static  <T> AsyncGet<T, Exception> asyncIpc(
      ShadedProtobufHelper.IpcCall<T> call) throws IOException {
    CompletableFuture<Object> completableFuture = new CompletableFuture<>();
    Client.COMPLETABLE_FUTURE_THREAD_LOCAL.set(completableFuture);
    ipc(call);
    return (AsyncGet<T, Exception>) ProtobufRpcEngine2.getAsyncReturnMessage();
  }

  public static <T> void asyncResponse(Response<T> response) {
    CompletableFuture<T> completableFuture =
        (CompletableFuture<T>) Client.COMPLETABLE_FUTURE_THREAD_LOCAL.get();
    // transfer originCall & callerContext to worker threads of executor.
    final Server.Call originCall = Server.getCurCall().get();
    final CallerContext originContext = CallerContext.getCurrent();

    CompletableFuture<Object> resCompletableFuture = completableFuture.thenApplyAsync(t -> {
      try {
        Server.getCurCall().set(originCall);
        CallerContext.setCurrent(originContext);
        return response.response();
      }catch (Exception e) {
        throw new CompletionException(e);
      }
    }, RouterRpcServer.getAsyncRouterResponse());
    setThreadLocal(resCompletableFuture);
  }

  public static <T> void asyncRouterServer(ServerReq<T> req, ServerRes<T> res) {
    final ProtobufRpcEngineCallback2 callback =
        ProtobufRpcEngine2.Server.registerForDeferredResponse2();
    CompletableFuture<Object> completableFuture =
        CompletableFuture.completedFuture(null);
    completableFuture.thenCompose(o -> {
      try {
        RouterRpcServer.acquire();
        req.req();
        return (CompletableFuture<T>)RouterAsyncRpcUtil.getCompletableFuture();
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    }).handle((result, e) -> {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Async response, callback: {}, CallerContext: {}, result: [{}]",
            callback, CallerContext.getCurrent(), result, e);
      }
      if (e == null) {
        Message value = null;
        try {
          value = res.res(result);
        } catch (RuntimeException re) {
          callback.error(re);
          RouterRpcServer.release();
          return null;
        }
        callback.setResponse(value);
      } else {
        callback.error(e.getCause());
      }
      RouterRpcServer.release();
      return null;
    });
  }

  public static void setThreadLocal(CompletableFuture<Object> completableFuture) {
    completableFutureThreadLocal.set(completableFuture);
  }

  public static CompletableFuture<Object> getCompletableFuture() {
    return completableFutureThreadLocal.get();
  }

  @FunctionalInterface
   interface Response<T> {
    T response() throws Exception;
  }

  @FunctionalInterface
  interface ServerReq<T> {
    T req() throws Exception;
  }

  @FunctionalInterface
  interface ServerRes<T> {
    Message res(T result) throws RuntimeException;
  }
}
