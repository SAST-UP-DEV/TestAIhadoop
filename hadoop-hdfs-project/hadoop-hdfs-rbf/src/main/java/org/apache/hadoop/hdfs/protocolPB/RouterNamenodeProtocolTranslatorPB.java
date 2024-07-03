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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.ipc.Client;

import java.io.IOException;
import static org.apache.hadoop.hdfs.protocolPB.AsyncRpcProtocolPBUtil.asyncIpcClient;

public class RouterNamenodeProtocolTranslatorPB extends NamenodeProtocolTranslatorPB{
  private final NamenodeProtocolPB rpcProxy;

  public RouterNamenodeProtocolTranslatorPB(NamenodeProtocolPB rpcProxy) {
    super(rpcProxy);
    this.rpcProxy = rpcProxy;
  }

  @Override
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size, long
      minBlockSize, long timeInterval, StorageType storageType)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getBlocks(datanode, size, minBlockSize, timeInterval, storageType);
    }
    NamenodeProtocolProtos.GetBlocksRequestProto.Builder builder =
        NamenodeProtocolProtos.GetBlocksRequestProto.newBuilder()
            .setDatanode(PBHelperClient.convert((DatanodeID)datanode)).setSize(size)
            .setMinBlockSize(minBlockSize).setTimeInterval(timeInterval);
    if (storageType != null) {
      builder.setStorageType(PBHelperClient.convertStorageType(storageType));
    }
    NamenodeProtocolProtos.GetBlocksRequestProto req = builder.build();

    return asyncIpcClient(() -> rpcProxy.getBlocks(NULL_CONTROLLER, req),
        res -> PBHelper.convert(res.getBlocks()),
        BlocksWithLocations.class);
  }

  @Override
  public ExportedBlockKeys getBlockKeys() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getBlockKeys();
    }

    return asyncIpcClient(() -> rpcProxy.getBlockKeys(NULL_CONTROLLER,
            VOID_GET_BLOCKKEYS_REQUEST),
        res -> res.hasKeys() ? PBHelper.convert(res.getKeys()) : null,
        ExportedBlockKeys.class);
  }

  @Override
  public long getTransactionID() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getTransactionID();
    }

    return asyncIpcClient(() -> rpcProxy.getTransactionId(NULL_CONTROLLER,
            VOID_GET_TRANSACTIONID_REQUEST),
        res -> res.getTxId(), Long.class);
  }

  @Override
  public long getMostRecentCheckpointTxId() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getMostRecentCheckpointTxId();
    }

    return asyncIpcClient(() -> rpcProxy.getMostRecentCheckpointTxId(NULL_CONTROLLER,
            NamenodeProtocolProtos
                .GetMostRecentCheckpointTxIdRequestProto
                .getDefaultInstance()),
        res -> res.getTxId(), Long.class);
  }

  @Override
  public long getMostRecentNameNodeFileTxId(NNStorage.NameNodeFile nnf) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getMostRecentNameNodeFileTxId(nnf);
    }

    return asyncIpcClient(() -> rpcProxy.getMostRecentNameNodeFileTxId(NULL_CONTROLLER,
            NamenodeProtocolProtos
                .GetMostRecentNameNodeFileTxIdRequestProto
                .newBuilder()
                .setNameNodeFile(nnf.toString())
                .build()),
        res -> res.getTxId(), Long.class);
  }

  @Override
  public CheckpointSignature rollEditLog() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.rollEditLog();
    }

    return asyncIpcClient(() -> rpcProxy.rollEditLog(NULL_CONTROLLER,
        VOID_ROLL_EDITLOG_REQUEST),
        res -> PBHelper.convert(res.getSignature()), CheckpointSignature.class);
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.versionRequest();
    }
    return asyncIpcClient(() -> rpcProxy.versionRequest(NULL_CONTROLLER,
            VOID_VERSION_REQUEST),
        res -> PBHelper.convert(res.getInfo()),
        NamespaceInfo.class);
  }

  @Override
  public void errorReport(NamenodeRegistration registration, int errorCode,
                          String msg) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.errorReport(registration, errorCode, msg);
      return;
    }
    NamenodeProtocolProtos.ErrorReportRequestProto req =
        NamenodeProtocolProtos.ErrorReportRequestProto.newBuilder()
        .setErrorCode(errorCode).setMsg(msg)
        .setRegistration(PBHelper.convert(registration)).build();

    asyncIpcClient(() -> rpcProxy.errorReport(NULL_CONTROLLER, req),
        res -> null, Void.class);
  }

  @Override
  public NamenodeRegistration registerSubordinateNamenode(
      NamenodeRegistration registration) throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.registerSubordinateNamenode(registration);
    }
    NamenodeProtocolProtos.RegisterRequestProto req =
        NamenodeProtocolProtos.RegisterRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration)).build();

    return asyncIpcClient(() -> rpcProxy.registerSubordinateNamenode(NULL_CONTROLLER, req),
        res -> PBHelper.convert(res.getRegistration()),
        NamenodeRegistration.class);
  }

  @Override
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.startCheckpoint(registration);
    }
    NamenodeProtocolProtos.StartCheckpointRequestProto req =
        NamenodeProtocolProtos.StartCheckpointRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration)).build();

    return asyncIpcClient(() -> rpcProxy.startCheckpoint(NULL_CONTROLLER, req),
        res -> {
          HdfsServerProtos.NamenodeCommandProto cmd = res.getCommand();
          return PBHelper.convert(cmd);
        }, NamenodeCommand.class);
  }

  @Override
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    if (!Client.isAsynchronousMode()) {
      super.endCheckpoint(registration, sig);
      return;
    }
    NamenodeProtocolProtos.EndCheckpointRequestProto req =
        NamenodeProtocolProtos.EndCheckpointRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration))
        .setSignature(PBHelper.convert(sig)).build();

    asyncIpcClient(() -> rpcProxy.endCheckpoint(NULL_CONTROLLER, req),
        res -> null, Void.class);
  }

  @Override
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getEditLogManifest(sinceTxId);
    }
    NamenodeProtocolProtos.GetEditLogManifestRequestProto req =
        NamenodeProtocolProtos.GetEditLogManifestRequestProto
        .newBuilder().setSinceTxId(sinceTxId).build();

    return asyncIpcClient(() -> rpcProxy.getEditLogManifest(NULL_CONTROLLER, req),
        res -> PBHelper.convert(res.getManifest()), RemoteEditLogManifest.class);
  }

  @Override
  public boolean isUpgradeFinalized() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.isUpgradeFinalized();
    }
    NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto req =
        NamenodeProtocolProtos.IsUpgradeFinalizedRequestProto
        .newBuilder().build();

    return asyncIpcClient(() -> rpcProxy.isUpgradeFinalized(NULL_CONTROLLER, req),
        res -> res.getIsUpgradeFinalized(), Boolean.class);
  }

  @Override
  public boolean isRollingUpgrade() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.isRollingUpgrade();
    }
    NamenodeProtocolProtos.IsRollingUpgradeRequestProto req =
        NamenodeProtocolProtos.IsRollingUpgradeRequestProto
        .newBuilder().build();

    return asyncIpcClient(() -> rpcProxy.isRollingUpgrade(NULL_CONTROLLER, req),
        res -> res.getIsRollingUpgrade(), Boolean.class);
  }

  @Override
  public Long getNextSPSPath() throws IOException {
    if (!Client.isAsynchronousMode()) {
      return super.getNextSPSPath();
    }
    NamenodeProtocolProtos.GetNextSPSPathRequestProto req =
        NamenodeProtocolProtos.GetNextSPSPathRequestProto.newBuilder().build();

    return asyncIpcClient(() -> rpcProxy.getNextSPSPath(NULL_CONTROLLER, req),
        res -> res.hasSpsPath() ? res.getSpsPath() : null, Long.class);
  }
}
