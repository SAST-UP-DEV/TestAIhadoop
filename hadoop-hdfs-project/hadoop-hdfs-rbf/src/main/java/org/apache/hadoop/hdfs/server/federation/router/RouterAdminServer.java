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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.proto.RouterProtocolProtos.RouterAdminProtocolService;
import org.apache.hadoop.hdfs.protocolPB.RouterAdminProtocol;
import org.apache.hadoop.hdfs.protocolPB.RouterAdminProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.RouterAdminProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.RouterPolicyProvider;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.DisabledNameserviceStore;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreCache;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.DisableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.DisableNameserviceResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnableNameserviceRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnableNameserviceResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnterSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnterSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDisabledNameservicesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDisabledNameservicesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDestinationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetDestinationResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.RefreshRegistry;
import org.apache.hadoop.ipc.RefreshResponse;
import org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;

/**
 * This class is responsible for handling all of the Admin calls to the HDFS
 * router. It is created, started, and stopped by {@link Router}.
 */
public class RouterAdminServer extends AbstractService
    implements RouterAdminProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAdminServer.class);

  private Configuration conf;

  private final Router router;

  private MountTableStore mountTableStore;

  private DisabledNameserviceStore disabledStore;

  /** The Admin server that listens to requests from clients. */
  private final Server adminServer;
  private final InetSocketAddress adminAddress;

  /**
   * Permission related info used for constructing new router permission
   * checker instance.
   */
  private static String routerOwner;
  private static String superGroup;
  private static boolean isPermissionEnabled;
  private boolean iStateStoreCache;
  private final long maxComponentLength;

  public RouterAdminServer(Configuration conf, Router router)
      throws IOException {
    super(RouterAdminServer.class.getName());

    this.conf = conf;
    this.router = router;

    int handlerCount = this.conf.getInt(
        RBFConfigKeys.DFS_ROUTER_ADMIN_HANDLER_COUNT_KEY,
        RBFConfigKeys.DFS_ROUTER_ADMIN_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(this.conf, RouterAdminProtocolPB.class,
        ProtobufRpcEngine.class);

    RouterAdminProtocolServerSideTranslatorPB routerAdminProtocolTranslator =
        new RouterAdminProtocolServerSideTranslatorPB(this);
    BlockingService clientNNPbService = RouterAdminProtocolService.
        newReflectiveBlockingService(routerAdminProtocolTranslator);

    InetSocketAddress confRpcAddress = conf.getSocketAddr(
        RBFConfigKeys.DFS_ROUTER_ADMIN_BIND_HOST_KEY,
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_DEFAULT,
        RBFConfigKeys.DFS_ROUTER_ADMIN_PORT_DEFAULT);

    String bindHost = conf.get(
        RBFConfigKeys.DFS_ROUTER_ADMIN_BIND_HOST_KEY,
        confRpcAddress.getHostName());
    LOG.info("Admin server binding to {}:{}",
        bindHost, confRpcAddress.getPort());

    initializePermissionSettings(this.conf);
    this.adminServer = new RPC.Builder(this.conf)
        .setProtocol(RouterAdminProtocolPB.class)
        .setInstance(clientNNPbService)
        .setBindAddress(bindHost)
        .setPort(confRpcAddress.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .build();

    // Set service-level authorization security policy
    if (conf.getBoolean(HADOOP_SECURITY_AUTHORIZATION, false)) {
      this.adminServer.refreshServiceAcl(conf, new RouterPolicyProvider());
    }

    // The RPC-server port can be ephemeral... ensure we have the correct info
    InetSocketAddress listenAddress = this.adminServer.getListenerAddress();
    this.adminAddress = new InetSocketAddress(
        confRpcAddress.getHostName(), listenAddress.getPort());
    router.setAdminServerAddress(this.adminAddress);
    iStateStoreCache =
        router.getSubclusterResolver() instanceof StateStoreCache;
    // The mount table destination path length limit keys.
    this.maxComponentLength = (int) conf.getLongBytes(
        RBFConfigKeys.DFS_ROUTER_ADMIN_MAX_COMPONENT_LENGTH_KEY,
        RBFConfigKeys.DFS_ROUTER_ADMIN_MAX_COMPONENT_LENGTH_DEFAULT);

    GenericRefreshProtocolServerSideTranslatorPB genericRefreshXlator =
        new GenericRefreshProtocolServerSideTranslatorPB(this);
    BlockingService genericRefreshService =
        GenericRefreshProtocolProtos.GenericRefreshProtocolService.
        newReflectiveBlockingService(genericRefreshXlator);

    DFSUtil.addPBProtocol(conf, GenericRefreshProtocolPB.class,
        genericRefreshService, adminServer);
  }

  /**
   * Initialize permission related settings.
   *
   * @param routerConf
   * @throws IOException
   */
  private static void initializePermissionSettings(Configuration routerConf)
      throws IOException {
    routerOwner = UserGroupInformation.getCurrentUser().getShortUserName();
    superGroup = routerConf.get(
        DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
        DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
    isPermissionEnabled = routerConf.getBoolean(DFS_PERMISSIONS_ENABLED_KEY,
        DFS_PERMISSIONS_ENABLED_DEFAULT);
  }

  /** Allow access to the client RPC server for testing. */
  @VisibleForTesting
  Server getAdminServer() {
    return this.adminServer;
  }

  private MountTableStore getMountTableStore() throws IOException {
    if (this.mountTableStore == null) {
      this.mountTableStore = router.getStateStore().getRegisteredRecordStore(
          MountTableStore.class);
      if (this.mountTableStore == null) {
        throw new IOException("Mount table state store is not available.");
      }
    }
    return this.mountTableStore;
  }

  private DisabledNameserviceStore getDisabledNameserviceStore()
      throws IOException {
    if (this.disabledStore == null) {
      this.disabledStore = router.getStateStore().getRegisteredRecordStore(
          DisabledNameserviceStore.class);
      if (this.disabledStore == null) {
        throw new IOException(
            "Disabled Nameservice state store is not available.");
      }
    }
    return this.disabledStore;
  }

  /**
   * Get the RPC address of the admin service.
   * @return Administration service RPC address.
   */
  public InetSocketAddress getRpcAddress() {
    return this.adminAddress;
  }

  void checkSuperuserPrivilege() throws AccessControlException {
    RouterPermissionChecker pc = RouterAdminServer.getPermissionChecker();
    if (pc != null) {
      pc.checkSuperuserPrivilege();
    }
  }

  /**
   * Verify each component name of a destination path for fs limit.
   *
   * @param destPath destination path name of mount point.
   * @throws PathComponentTooLongException destination path name is too long.
   */
  void verifyMaxComponentLength(String destPath)
      throws PathComponentTooLongException {
    if (maxComponentLength <= 0) {
      return;
    }
    if (destPath == null) {
      return;
    }
    String[] components = destPath.split(Path.SEPARATOR);
    for (String component : components) {
      int length = component.length();
      if (length > maxComponentLength) {
        PathComponentTooLongException e = new PathComponentTooLongException(
            maxComponentLength, length, destPath, component);
        throw e;
      }
    }
  }

  /**
   * Verify each component name of every destination path of mount table
   * for fs limit.
   *
   * @param mountTable mount point.
   * @throws PathComponentTooLongException destination path name is too long.
   */
  void verifyMaxComponentLength(MountTable mountTable)
      throws PathComponentTooLongException {
    if (mountTable != null) {
      List<RemoteLocation> dests = mountTable.getDestinations();
      if (dests != null && !dests.isEmpty()) {
        for (RemoteLocation dest : dests) {
          verifyMaxComponentLength(dest.getDest());
        }
      }
    }
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.conf = configuration;
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    this.adminServer.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.adminServer != null) {
      this.adminServer.stop();
    }
    super.serviceStop();
  }

  @Override
  public AddMountTableEntryResponse addMountTableEntry(
      AddMountTableEntryRequest request) throws IOException {
    // Checks max component length limit.
    MountTable mountTable = request.getEntry();
    verifyMaxComponentLength(mountTable);
    return getMountTableStore().addMountTableEntry(request);
  }

  @Override
  public UpdateMountTableEntryResponse updateMountTableEntry(
      UpdateMountTableEntryRequest request) throws IOException {
    MountTable updateEntry = request.getEntry();
    MountTable oldEntry = null;
    // Checks max component length limit.
    verifyMaxComponentLength(updateEntry);
    if (this.router.getSubclusterResolver() instanceof MountTableResolver) {
      MountTableResolver mResolver =
          (MountTableResolver) this.router.getSubclusterResolver();
      oldEntry = mResolver.getMountPoint(updateEntry.getSourcePath());
    }
    UpdateMountTableEntryResponse response = getMountTableStore()
        .updateMountTableEntry(request);
    try {
      if (updateEntry != null && router.isQuotaEnabled()) {
        // update quota.
        if (isQuotaUpdated(request, oldEntry)) {
          synchronizeQuota(updateEntry.getSourcePath(),
              updateEntry.getQuota().getQuota(),
              updateEntry.getQuota().getSpaceQuota(), null);
        }
        // update storage type quota.
        RouterQuotaUsage newQuota = request.getEntry().getQuota();
        boolean locationsChanged = oldEntry == null ||
            !oldEntry.getDestinations().equals(updateEntry.getDestinations());
        for (StorageType t : StorageType.values()) {
          if (locationsChanged || oldEntry.getQuota().getTypeQuota(t)
              != newQuota.getTypeQuota(t)) {
            synchronizeQuota(updateEntry.getSourcePath(),
                HdfsConstants.QUOTA_DONT_SET, newQuota.getTypeQuota(t), t);
          }
        }
      }
    } catch (Exception e) {
      // Ignore exception, if any while reseting quota. Specifically to handle
      // if the actual destination doesn't exist.
      LOG.warn("Unable to reset quota at the destinations for {}: {}",
          request.getEntry(), e.getMessage());
    }
    return response;
  }

  /**
   * Checks whether quota needs to be synchronized with namespace or not. Quota
   * needs to be synchronized either if there is change in mount entry quota or
   * there is change in remote destinations.
   * @param request the update request.
   * @param oldEntry the mount entry before getting updated.
   * @return true if quota needs to be updated.
   * @throws IOException
   */
  private boolean isQuotaUpdated(UpdateMountTableEntryRequest request,
      MountTable oldEntry) throws IOException {
    if (oldEntry != null) {
      MountTable updateEntry = request.getEntry();
      // If locations are changed, the new destinations need to be in sync with
      // the mount quota.
      if (!oldEntry.getDestinations().equals(updateEntry.getDestinations())) {
        return true;
      }
      // Previous quota.
      RouterQuotaUsage preQuota = oldEntry.getQuota();
      long nsQuota = preQuota.getQuota();
      long ssQuota = preQuota.getSpaceQuota();
      // New quota
      RouterQuotaUsage mountQuota = updateEntry.getQuota();
      // If there is change in quota, the new quota needs to be synchronized.
      if (nsQuota != mountQuota.getQuota()
          || ssQuota != mountQuota.getSpaceQuota()) {
        return true;
      }
      return false;
    } else {
      // If old entry is not available, sync quota always, since we can't
      // conclude no change in quota.
      return true;
    }
  }

  /**
   * Synchronize the quota value across mount table and subclusters.
   * @param path Source path in given mount table.
   * @param nsQuota Name quota definition in given mount table.
   * @param ssQuota Space quota definition in given mount table.
   * @param type Storage type of quota. Null if it's not a storage type quota.
   * @throws IOException
   */
  private void synchronizeQuota(String path, long nsQuota, long ssQuota,
      StorageType type) throws IOException {
    if (isQuotaSyncRequired(nsQuota, ssQuota)) {
      if (iStateStoreCache) {
        ((StateStoreCache) this.router.getSubclusterResolver()).loadCache(true);
      }
      Quota routerQuota = this.router.getRpcServer().getQuotaModule();
      routerQuota.setQuota(path, nsQuota, ssQuota, type, false);
    }
  }

  /**
   * Checks if quota needs to be synchronized or not.
   * @param nsQuota namespace quota to be set.
   * @param ssQuota space quota to be set.
   * @return true if the quota needs to be synchronized.
   */
  private boolean isQuotaSyncRequired(long nsQuota, long ssQuota) {
    // Check if quota is enabled for router or not.
    if (router.isQuotaEnabled()) {
      if ((nsQuota != HdfsConstants.QUOTA_DONT_SET
          || ssQuota != HdfsConstants.QUOTA_DONT_SET)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public RemoveMountTableEntryResponse removeMountTableEntry(
      RemoveMountTableEntryRequest request) throws IOException {
    // clear sub-cluster's quota definition
    try {
      synchronizeQuota(request.getSrcPath(), HdfsConstants.QUOTA_RESET,
          HdfsConstants.QUOTA_RESET, null);
    } catch (Exception e) {
      // Ignore exception, if any while reseting quota. Specifically to handle
      // if the actual destination doesn't exist.
      LOG.warn("Unable to clear quota at the destinations for {}: {}",
          request.getSrcPath(), e.getMessage());
    }
    return getMountTableStore().removeMountTableEntry(request);
  }

  @Override
  public GetMountTableEntriesResponse getMountTableEntries(
      GetMountTableEntriesRequest request) throws IOException {
    return getMountTableStore().getMountTableEntries(request);
  }

  @Override
  public EnterSafeModeResponse enterSafeMode(EnterSafeModeRequest request)
      throws IOException {
    checkSuperuserPrivilege();
    boolean success = false;
    RouterSafemodeService safeModeService = this.router.getSafemodeService();
    if (safeModeService != null) {
      this.router.updateRouterState(RouterServiceState.SAFEMODE);
      safeModeService.setManualSafeMode(true);
      success = verifySafeMode(true);
      if (success) {
        LOG.info("STATE* Safe mode is ON.\n" + "It was turned on manually. "
            + "Use \"hdfs dfsrouteradmin -safemode leave\" to turn"
            + " safe mode off.");
      } else {
        LOG.error("Unable to enter safemode.");
      }
    }
    return EnterSafeModeResponse.newInstance(success);
  }

  @Override
  public LeaveSafeModeResponse leaveSafeMode(LeaveSafeModeRequest request)
      throws IOException {
    checkSuperuserPrivilege();
    boolean success = false;
    RouterSafemodeService safeModeService = this.router.getSafemodeService();
    if (safeModeService != null) {
      this.router.updateRouterState(RouterServiceState.RUNNING);
      safeModeService.setManualSafeMode(false);
      success = verifySafeMode(false);
      if (success) {
        LOG.info("STATE* Safe mode is OFF.\n" + "It was turned off manually.");
      } else {
        LOG.error("Unable to leave safemode.");
      }
    }
    return LeaveSafeModeResponse.newInstance(success);
  }

  @Override
  public GetSafeModeResponse getSafeMode(GetSafeModeRequest request)
      throws IOException {
    boolean isInSafeMode = false;
    RouterSafemodeService safeModeService = this.router.getSafemodeService();
    if (safeModeService != null) {
      isInSafeMode = safeModeService.isInSafeMode();
      LOG.info("Safemode status retrieved successfully.");
    }
    return GetSafeModeResponse.newInstance(isInSafeMode);
  }

  @Override
  public RefreshMountTableEntriesResponse refreshMountTableEntries(
      RefreshMountTableEntriesRequest request) throws IOException {
    if (iStateStoreCache) {
      /*
       * MountTableResolver updates MountTableStore cache also. Expecting other
       * SubclusterResolver implementations to update MountTableStore cache also
       * apart from updating its cache.
       */
      boolean result = ((StateStoreCache) this.router.getSubclusterResolver())
          .loadCache(true);
      RefreshMountTableEntriesResponse response =
          RefreshMountTableEntriesResponse.newInstance();
      response.setResult(result);
      return response;
    } else {
      return getMountTableStore().refreshMountTableEntries(request);
    }
  }

  @Override
  public GetDestinationResponse getDestination(
      GetDestinationRequest request) throws IOException {
    final String src = request.getSrcPath();
    final List<String> nsIds = new ArrayList<>();
    RouterRpcServer rpcServer = this.router.getRpcServer();
    List<RemoteLocation> locations = rpcServer.getLocationsForPath(src, false);
    RouterRpcClient rpcClient = rpcServer.getRPCClient();
    RemoteMethod method = new RemoteMethod("getFileInfo",
        new Class<?>[] {String.class}, new RemoteParam());
    try {
      Map<RemoteLocation, HdfsFileStatus> responses =
          rpcClient.invokeConcurrent(
              locations, method, false, false, HdfsFileStatus.class);
      for (RemoteLocation location : locations) {
        if (responses.get(location) != null) {
          nsIds.add(location.getNameserviceId());
        }
      }
    } catch (IOException ioe) {
      LOG.error("Cannot get location for {}: {}",
          src, ioe.getMessage());
    }
    if (nsIds.isEmpty() && !locations.isEmpty()) {
      String nsId = locations.get(0).getNameserviceId();
      nsIds.add(nsId);
    }
    return GetDestinationResponse.newInstance(nsIds);
  }

  /**
   * Verify if Router set safe mode state correctly.
   * @param isInSafeMode Expected state to be set.
   * @return
   */
  private boolean verifySafeMode(boolean isInSafeMode) {
    Preconditions.checkNotNull(this.router.getSafemodeService());
    boolean serverInSafeMode = this.router.getSafemodeService().isInSafeMode();
    RouterServiceState currentState = this.router.getRouterState();

    return (isInSafeMode && currentState == RouterServiceState.SAFEMODE
        && serverInSafeMode)
        || (!isInSafeMode && currentState != RouterServiceState.SAFEMODE
            && !serverInSafeMode);
  }

  @Override
  public DisableNameserviceResponse disableNameservice(
      DisableNameserviceRequest request) throws IOException {
    checkSuperuserPrivilege();

    String nsId = request.getNameServiceId();
    boolean success = false;
    if (namespaceExists(nsId)) {
      success = getDisabledNameserviceStore().disableNameservice(nsId);
      if (success) {
        LOG.info("Nameservice {} disabled successfully.", nsId);
      } else {
        LOG.error("Unable to disable Nameservice {}", nsId);
      }
    } else {
      LOG.error("Cannot disable {}, it does not exists", nsId);
    }
    return DisableNameserviceResponse.newInstance(success);
  }

  private boolean namespaceExists(final String nsId) throws IOException {
    boolean found = false;
    ActiveNamenodeResolver resolver = router.getNamenodeResolver();
    Set<FederationNamespaceInfo> nss = resolver.getNamespaces();
    for (FederationNamespaceInfo ns : nss) {
      if (nsId.equals(ns.getNameserviceId())) {
        found = true;
        break;
      }
    }
    return found;
  }

  @Override
  public EnableNameserviceResponse enableNameservice(
      EnableNameserviceRequest request) throws IOException {
    checkSuperuserPrivilege();

    String nsId = request.getNameServiceId();
    DisabledNameserviceStore store = getDisabledNameserviceStore();
    Set<String> disabled = store.getDisabledNameservices();
    boolean success = false;
    if (disabled.contains(nsId)) {
      success = store.enableNameservice(nsId);
      if (success) {
        LOG.info("Nameservice {} enabled successfully.", nsId);
      } else {
        LOG.error("Unable to enable Nameservice {}", nsId);
      }
    } else {
      LOG.error("Cannot enable {}, it was not disabled", nsId);
    }
    return EnableNameserviceResponse.newInstance(success);
  }

  @Override
  public GetDisabledNameservicesResponse getDisabledNameservices(
      GetDisabledNameservicesRequest request) throws IOException {
    Set<String> nsIds =
        getDisabledNameserviceStore().getDisabledNameservices();
    return GetDisabledNameservicesResponse.newInstance(nsIds);
  }

  /**
   * Get a new permission checker used for making mount table access
   * control. This method will be invoked during each RPC call in router
   * admin server.
   *
   * @return Router permission checker.
   * @throws AccessControlException If the user is not authorized.
   */
  public static RouterPermissionChecker getPermissionChecker()
      throws AccessControlException {
    if (!isPermissionEnabled) {
      return null;
    }

    try {
      return new RouterPermissionChecker(routerOwner, superGroup,
          NameNode.getRemoteUser());
    } catch (IOException e) {
      throw new AccessControlException(e);
    }
  }

  /**
   * Get super user name.
   *
   * @return String super user name.
   */
  public static String getSuperUser() {
    return routerOwner;
  }

  /**
   * Get super group name.
   *
   * @return String super group name.
   */
  public static String getSuperGroup(){
    return superGroup;
  }

  @Override // GenericRefreshProtocol
  public Collection<RefreshResponse> refresh(String identifier, String[] args) {
    // Let the registry handle as needed
    return RefreshRegistry.defaultRegistry().dispatch(identifier, args);
  }

  @Override // RouterGenericManager
  public boolean refreshSuperUserGroupsConfiguration() throws IOException {
    ProxyUsers.refreshSuperUserGroupsConfiguration();
    return true;
  }
}
