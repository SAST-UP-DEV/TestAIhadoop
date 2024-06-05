package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCPerformanceMonitor;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;

public class ThreadLocalContext {
  private final Server.Call call;
  private final CallerContext context;
  private final long startOpTime;
  private final long proxyOpTime;

  public ThreadLocalContext() {
    this.call = Server.getCurCall().get();
    this.context = CallerContext.getCurrent();
    this.startOpTime = FederationRPCPerformanceMonitor.getStartOpTime();
    this.proxyOpTime =  FederationRPCPerformanceMonitor.getProxyOpTime();
  }

  public void transfer() {
    Server.getCurCall().set(call);
    CallerContext.setCurrent(context);
    FederationRPCPerformanceMonitor.setStartOpTime(startOpTime);
    FederationRPCPerformanceMonitor.setProxyOpTime(proxyOpTime);
  }
}
