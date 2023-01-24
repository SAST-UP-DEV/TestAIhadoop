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
package org.apache.hadoop.yarn.client.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ha.HAAdmin.UsageInfo;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusters;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RouterCLI extends Configured implements Tool {

  protected final static Map<String, UsageInfo> ADMIN_USAGE =
      ImmutableMap.<String, UsageInfo>builder().put("-deregisterSubCluster",
        new UsageInfo("[-sc|subClusterId [subCluster id]]",
        "deregister subCluster, if the interval between the heartbeat time of the subCluster " +
        "and the current time exceeds the timeout period, " +
        "set the state of the subCluster to SC_LOST")).build();

  // title information
  private final static String SUB_CLUSTER_ID = "SubClusterId";
  private final static String DEREGISTER_STATE = "DeregisterState";
  private final static String LAST_HEARTBEAT_TIME = "LastHeartBeatTime";
  private final static String INFORMATION = "Information";
  private final static String SUB_CLUSTER_STATE = "SubClusterState";

  private static final String DEREGISTER_SUBCLUSTER_PATTERN = "%30s\t%20s\t%30s\t%30s\t%20s";

  public RouterCLI() {
    super();
  }

  public RouterCLI(Configuration conf) {
    super(conf);
  }

  private static void buildHelpMsg(String cmd, StringBuilder builder) {
    UsageInfo usageInfo = ADMIN_USAGE.get(cmd);
    if (usageInfo == null) {
      return;
    }
    if (usageInfo.args == null) {
      builder.append("   " + cmd + ": " + usageInfo.help);
    } else {
      String space = (usageInfo.args == "") ? "" : " ";
      builder.append("   " + cmd + space + usageInfo.args + ": "
          + usageInfo.help);
    }
  }

  private static void buildIndividualUsageMsg(String cmd, StringBuilder builder) {
    UsageInfo usageInfo = ADMIN_USAGE.get(cmd);
    if (usageInfo == null) {
      return;
    }
    if (usageInfo.args == null) {
      builder.append("Usage: router rmadmin [" + cmd + "]\n");
    } else {
      String space = (usageInfo.args == "") ? "" : " ";
      builder.append("Usage: router rmadmin [" + cmd + space + usageInfo.args + "]\n");
    }
  }

  private static void buildUsageMsg(StringBuilder builder) {
    builder.append("Usage: router rmadmin\n");
    for (Map.Entry<String, UsageInfo> cmdEntry : ADMIN_USAGE.entrySet()) {
      UsageInfo usageInfo = cmdEntry.getValue();
      builder.append("   " + cmdEntry.getKey() + " " + usageInfo.args + "\n");
    }
    builder.append("   -help" + " [cmd]\n");
  }

  private static void printHelp(String cmd, boolean isHAEnabled) {
    StringBuilder summary = new StringBuilder();
    summary.append("router rmadmin is the command to execute YARN Federation administrative commands.\n");
    summary.append("The full syntax is: \n\n"
        + "router rmadmin"
        + " [-deregisterSubCluster [-c|clusterId [subClusterId]]");
    summary.append(" [-help [cmd]]").append("\n");
    StringBuilder helpBuilder = new StringBuilder();
    System.out.println(summary);
    for (String cmdKey : ADMIN_USAGE.keySet()) {
      buildHelpMsg(cmdKey, helpBuilder);
      helpBuilder.append("\n");
    }
    helpBuilder.append("   -help [cmd]: Displays help for the given command or all commands" +
        " if none is specified.");
    System.out.println(helpBuilder);
    System.out.println();
    ToolRunner.printGenericCommandUsage(System.out);
  }

  private static void printUsage(String cmd, boolean isFederationEnabled) {
    StringBuilder usageBuilder = new StringBuilder();
    if (ADMIN_USAGE.containsKey(cmd)) {
      buildIndividualUsageMsg(cmd, usageBuilder);
    } else {
      buildUsageMsg(usageBuilder);
    }
    System.err.println(usageBuilder);
    ToolRunner.printGenericCommandUsage(System.err);
  }

  protected ResourceManagerAdministrationProtocol createAdminProtocol()
      throws IOException {
    // Get the current configuration
    final YarnConfiguration conf = new YarnConfiguration(getConf());
    return ClientRMProxy.createRMProxy(conf, ResourceManagerAdministrationProtocol.class);
  }

  private int handleDeregisterSubCluster(String[] args, String cmd)
          throws IOException, YarnException, ParseException {
    Options opts = new Options();
    opts.addOption("deregisterSubCluster", false,
            "Refresh the hosts information at the ResourceManager.");
    Option gracefulOpt = new Option("c", "clusterId", true,
            "Wait for timeout before marking the NodeManager as decommissioned.");
    gracefulOpt.setOptionalArg(true);
    opts.addOption(gracefulOpt);

    int exitCode = -1;
    CommandLine cliParser = null;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      System.out.println("Missing argument for options");
      // printUsage(args[0], isHAEnabled);
      return exitCode;
    }

    if (cliParser.hasOption("c")) {
      String subClusterId = cliParser.getOptionValue("c");
      return deregisterSubCluster(subClusterId);
    } else {
      // return refreshNodes();
      return -1;
    }
  }

  private int deregisterSubCluster(String subClusterId)
      throws IOException, YarnException {
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    DeregisterSubClusterRequest request =
        DeregisterSubClusterRequest.newInstance(subClusterId);
    DeregisterSubClusterResponse response = adminProtocol.deregisterSubCluster(request);
    System.out.println(String.format(DEREGISTER_SUBCLUSTER_PATTERN,
        SUB_CLUSTER_ID, DEREGISTER_STATE, LAST_HEARTBEAT_TIME, INFORMATION, SUB_CLUSTER_STATE));
    List<DeregisterSubClusters> deregisterSubClusters = response.getDeregisterSubClusters();
    deregisterSubClusters.forEach(deregisterSubCluster -> {
      String responseSubClusterId = deregisterSubCluster.getSubClusterId();
      String deregisterState = deregisterSubCluster.getDeregisterState();
      String lastHeartBeatTime = deregisterSubCluster.getLastHeartBeatTime();
      String info = deregisterSubCluster.getInformation();
      String subClusterState = deregisterSubCluster.getSubClusterState();
      System.out.println(String.format(DEREGISTER_SUBCLUSTER_PATTERN,
          responseSubClusterId, deregisterState, lastHeartBeatTime, info, subClusterState));
    });
    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    YarnConfiguration yarnConf = getConf() == null ?
        new YarnConfiguration() : new YarnConfiguration(getConf());
    boolean isFederationEnabled = yarnConf.getBoolean(YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);

    if (args.length < 1) {
      printUsage("", isFederationEnabled);
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = args[i++];

    exitCode = 0;
    if ("-help".equals(cmd)) {
      if (i < args.length) {
        printUsage(args[i], isFederationEnabled);
      } else {
        printHelp("", isFederationEnabled);
      }
      return exitCode;
    }

    if ("-deregisterSubCluster".equals(cmd)) {
      exitCode = handleDeregisterSubCluster(args, cmd);
    }

    return exitCode;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new RouterCLI(), args);
    System.exit(result);
  }
}
