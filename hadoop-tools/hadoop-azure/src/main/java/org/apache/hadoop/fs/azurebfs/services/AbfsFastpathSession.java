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

package org.apache.hadoop.fs.azurebfs.services;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.CachedSASToken;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_FASTPATH_SESSION_AUTH;

public class AbfsFastpathSession {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsInputStream.class);
  private static final double SESSION_REFRESH_INTERVAL_FACTOR = 0.75;

  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  protected AbfsFastpathSessionInfo fastpathSessionInfo;
  protected AbfsClient client;
  protected String path;
  protected String eTag;
  protected TracingContext tracingContext;

  private final ScheduledExecutorService scheduledExecutorService
      = Executors.newScheduledThreadPool(1);
  private int sessionRefreshIntervalInSec = -1;

  public AbfsFastpathSession(final AbfsClient client,
      final String path,
      final String eTag,
      TracingContext tracingContext) {
    this.client = client;
    this.path = path;
    this.eTag = eTag;
    this.tracingContext = tracingContext;
    fetchSessionTokenAndFileHandle();
  }

  /**
   * This returns a snap of the current sessionInfo
   * SessionInfo returned can be updated by request processors,
   * which should not reflect onto active instance.
   * @return
   */
  public AbfsFastpathSessionInfo getCurrentAbfsFastpathSessionInfoCopy() {
    rwLock.readLock().lock();
    try {
      if (fastpathSessionInfo.isValidSession()) {
        return new AbfsFastpathSessionInfo(fastpathSessionInfo);
      }

      LOG.debug("There is no valid Fastpath session currently");
      return null;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public void updateConnectionModeForFailures(AbfsConnectionMode connectionMode) {
    // Fastpath connection and session refresh failures are not recoverable,
    // update connection mode if that happens
    if ((connectionMode == AbfsConnectionMode.REST_ON_FASTPATH_CONN_FAILURE) ||
        (connectionMode == AbfsConnectionMode.REST_ON_FASTPATH_SESSION_UPD_FAILURE)) {
      updateConnectionMode(connectionMode);
    }
  }

  public void close() {
    if ((fastpathSessionInfo != null)
        && (fastpathSessionInfo.getFastpathFileHandle() != null)) {
      try {
        executeFastpathClose();
      } catch (AzureBlobFileSystemException e) {
        LOG.debug("Fastpath handle close failed - {} - {}",
            fastpathSessionInfo.getFastpathFileHandle(), e);
      }
    }
  }

  protected void fetchSessionTokenAndFileHandle() {
    fetchFastpathSessionToken();
    if ((fastpathSessionInfo != null) &&
        (fastpathSessionInfo.isValidSession())) {
      fetchFastpathFileHandle();
    }
  }

  @VisibleForTesting
  protected void updateAbfsFastpathSessionToken(String token, OffsetDateTime expiry) {
    rwLock.writeLock().lock();
    try {
      if (fastpathSessionInfo == null) {
        fastpathSessionInfo = new AbfsFastpathSessionInfo(token, expiry);
      } else {
        fastpathSessionInfo.updateSessionToken(token, expiry);
      }

      OffsetDateTime utcNow = OffsetDateTime.now(ZoneOffset.UTC);
      sessionRefreshIntervalInSec = (int) Math.floor(
          utcNow.until(expiry, ChronoUnit.SECONDS)
              * SESSION_REFRESH_INTERVAL_FACTOR);

      // 0 or negative sessionRefreshIntervalInSec indicates a session token
      // whose expiry is near as soon as its received. This will end up
      // generating a lot of REST calls refreshing the session. Better to
      // switch off Fastpath in that case.
      if (sessionRefreshIntervalInSec <= 0) {
        LOG.debug(
            "Expiry time at present or past. Drop Fastpath session (could be clock skew). Received expiry {} ",
            expiry);
        tracingContext.setConnectionMode(
            AbfsConnectionMode.REST_ON_FASTPATH_SESSION_UPD_FAILURE);
        fastpathSessionInfo.setConnectionMode(
            AbfsConnectionMode.REST_ON_FASTPATH_SESSION_UPD_FAILURE);
        return;
      }

      // schedule for refresh right away
      ScheduledFuture scheduledFuture =
          scheduledExecutorService.schedule(new Callable() {
            public Boolean call() throws Exception {
              return fetchFastpathSessionToken();
            }
          }, sessionRefreshIntervalInSec, TimeUnit.SECONDS);
      LOG.debug(
          "Fastpath session token fetch successful, valid till {}. Refresh scheduled after {} secs",
          expiry, sessionRefreshIntervalInSec);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @VisibleForTesting
  protected void updateConnectionMode(AbfsConnectionMode connectionMode) {
    rwLock.writeLock().lock();
    try {
      tracingContext.setConnectionMode(connectionMode);
      if (fastpathSessionInfo != null) {
        fastpathSessionInfo.setConnectionMode(connectionMode);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private void updateFastpathFileHandle(String fileHandle) {
    rwLock.writeLock().lock();
    try {
      fastpathSessionInfo.setFastpathFileHandle(fileHandle);
      LOG.debug("Fastpath handled opened {}", fastpathSessionInfo.getFastpathFileHandle());
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @VisibleForTesting
  boolean fetchFastpathSessionToken() {
    if ((fastpathSessionInfo != null) &&
        AbfsConnectionMode.isErrorConnectionMode(
            fastpathSessionInfo.getConnectionMode())) {
      // no need to refresh or schedule another
      return false;
    }

    try {
      AbfsRestOperation op = executeFetchFastpathSessionToken();
      byte[] buffer = op.getResult().getResponseContentBuffer();
      updateAbfsFastpathSessionToken(Base64.getEncoder().encodeToString(buffer),
          CachedSASToken.getExpiry(
              op.getResult().getResponseHeader(X_MS_FASTPATH_SESSION_AUTH)));
      return true;
    } catch (AzureBlobFileSystemException e) {
      LOG.debug("Fastpath session token fetch unsuccessful {}", e);
      updateConnectionMode(
          AbfsConnectionMode.REST_ON_FASTPATH_SESSION_UPD_FAILURE);
    }

    return false;
  }

  @VisibleForTesting
  private boolean fetchFastpathFileHandle() {
    try {
      AbfsRestOperation op = executeFastpathOpen();
      String fileHandle
          = ((AbfsFastpathConnection) op.getResult()).getFastpathFileHandle();
      updateFastpathFileHandle(fileHandle);
      return true;
    } catch (AzureBlobFileSystemException e) {
      LOG.debug("Fastpath  open failed with {}", e);
      updateConnectionMode(AbfsConnectionMode.REST_ON_FASTPATH_CONN_FAILURE);
    }

    return false;
  }

  @VisibleForTesting
  protected AbfsRestOperation executeFetchFastpathSessionToken()
      throws AzureBlobFileSystemException {
    return client.getReadFastpathSessionToken(path, eTag, tracingContext);
  }

  @VisibleForTesting
  protected AbfsRestOperation executeFastpathOpen()
      throws AzureBlobFileSystemException {
    return client.fastPathOpen(path, eTag, fastpathSessionInfo, tracingContext);
  }

  @VisibleForTesting
  protected AbfsRestOperation executeFastpathClose()
      throws AzureBlobFileSystemException {
    return client.fastPathClose(path, eTag, fastpathSessionInfo,
        tracingContext);
  }

  @VisibleForTesting
  int getSessionRefreshIntervalInSec() {
    rwLock.readLock().lock();
    try {
      return sessionRefreshIntervalInSec;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @VisibleForTesting
  void setConnectionMode(AbfsConnectionMode connMode) {
    updateConnectionMode(connMode);
  }

  @VisibleForTesting
  void setAbfsFastpathSessionInfo(AbfsFastpathSessionInfo sessionInfo) {
    updateAbfsFastpathSessionToken(sessionInfo.getSessionToken(), sessionInfo.getSessionTokenExpiry());
    updateConnectionMode(sessionInfo.getConnectionMode());
    updateFastpathFileHandle(sessionInfo.getFastpathFileHandle());
  }
}
