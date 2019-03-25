/*
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

package org.apache.hadoop.fs.s3a.impl;

import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.model.CopyResult;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.s3a.NoVersionAttributeException;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.RemoteFileChangedException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Change tracking for input streams: the version ID or etag of the object is
 * tracked and compared on open/re-open.  An initial version ID or etag may or
 * may not be available, depending on usage (e.g. if S3Guard is utilized).
 *
 * Self-contained for testing and use in different streams.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ChangeTracker {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChangeTracker.class);

  public static final String CHANGE_REPORTED_BY_S3 = "reported by S3";

  /** Policy to use. */
  private final ChangeDetectionPolicy policy;

  /**
   * URI of file being read.
   */
  private final String uri;

  /**
   * Mismatch counter; expected to be wired up to StreamStatistics except
   * during testing.
   */
  private final AtomicLong versionMismatches;

  /**
   * Revision identifier (e.g. eTag or versionId, depending on change
   * detection policy).
   */
  private String revisionId;

  /**
   * Create a change tracker.
   * @param uri URI of object being tracked
   * @param policy policy to track.
   * @param versionMismatches reference to the version mismatch counter
   * @param s3ObjectAttributes attributes of the object, potentially including
   * an eTag or versionId to match depending on {@code policy}
   */
  public ChangeTracker(final String uri,
      final ChangeDetectionPolicy policy,
      final AtomicLong versionMismatches,
      final S3ObjectAttributes s3ObjectAttributes) {
    this.policy = checkNotNull(policy);
    this.uri = uri;
    this.versionMismatches = versionMismatches;
    this.revisionId = policy.getRevisionId(s3ObjectAttributes);
    if (revisionId != null) {
      LOG.debug("Revision ID for object at {}: {}", uri, revisionId);
    }
  }

  public String getRevisionId() {
    return revisionId;
  }

  public ChangeDetectionPolicy.Source getSource() {
    return policy.getSource();
  }

  @VisibleForTesting
  public AtomicLong getVersionMismatches() {
    return versionMismatches;
  }

  /**
   * Apply any revision control set by the policy if it is to be
   * enforced on the server.
   * @param request request to modify
   * @return true iff a constraint was added.
   */
  public boolean maybeApplyConstraint(
      final GetObjectRequest request) {

    if (policy.getMode() == ChangeDetectionPolicy.Mode.Server
        && revisionId != null) {
      policy.applyRevisionConstraint(request, revisionId);
      return true;
    }
    return false;
  }

  /**
   * Apply any revision control set by the policy if it is to be
   * enforced on the server.
   * @param request request to modify
   * @return true iff a constraint was added.
   */
  public boolean maybeApplyConstraint(
      final CopyObjectRequest request) {

    if (policy.getMode() == ChangeDetectionPolicy.Mode.Server
        && revisionId != null) {
      policy.applyRevisionConstraint(request, revisionId);
      return true;
    }
    return false;
  }


  /**
   * Process the response from the server for validation against the
   * change policy.
   * @param object object returned; may be null.
   * @param operation operation in progress.
   * @param pos offset of read
   * @throws PathIOException raised on failure
   * @throws RemoteFileChangedException if the remote file has changed.
   */
  public void processResponse(final S3Object object,
      final String operation,
      final long pos) throws PathIOException {
    if (object == null) {
      // no object returned. Either mismatch or something odd.
      if (revisionId != null) {
        // the requirements of the change detection policy wasn't met: the
        // object was not returned.
        versionMismatches.incrementAndGet();
        throw new RemoteFileChangedException(uri, operation,
            String.format("%s change "
                    + CHANGE_REPORTED_BY_S3
                    + " while reading"
                    + " at position %s."
                    + " Version %s was unavailable",
                getSource(),
                pos,
                getRevisionId()));
      } else {
        throw new PathIOException(uri, "No data returned from GET request");
      }
    }

    processMetadata(object.getObjectMetadata(), operation, pos);
  }

  /**
   * Process the response from the server for validation against the
   * change policy.
   * @param copyResult result of a copy operation
   * @throws PathIOException raised on failure
   * @throws RemoteFileChangedException if the remote file has changed.
   */
  public void processResponse(final CopyResult copyResult)
      throws PathIOException {
    // ETag (sometimes, depending on encryption and/or multipart) is not the
    // same on the copied object as the original.  Version Id seems to never
    // be the same on the copy.  As such, there isn't really anything that
    // can be verified on the response.
  }

  /**
   * Process metadata response from server for validation against the change
   * policy.
   * @param metadata metadata returned from server
   * @param operation operation in progress
   * @param pos offset of read
   * @throws PathIOException raised on failure
   * @throws RemoteFileChangedException if the remote file has changed.
   */
  public void processMetadata(final ObjectMetadata metadata,
      final String operation,
      final long pos) throws PathIOException {
    final String newRevisionId = policy.getRevisionId(metadata, uri);
    processNewRevision(newRevisionId, operation, pos);
  }

  private void processNewRevision(final String newRevisionId,
      final String operation, final long pos) throws PathIOException {
    if (newRevisionId == null && policy.isRequireVersion()) {
      throw new NoVersionAttributeException(uri, String.format(
          "Change detection policy requires %s",
          policy.getSource()));
    }
    if (revisionId == null) {
      // revisionId may be null on first (re)open. Pin it so change can be
      // detected if object has been updated
      LOG.debug("Setting revision ID for object at {}: {}",
          uri, newRevisionId);
      revisionId = newRevisionId;
    } else if (!revisionId.equals(newRevisionId)) {
      LOG.debug("Revision ID changed from {} to {}",
          revisionId, newRevisionId);
      ImmutablePair<Boolean, RemoteFileChangedException> pair =
          policy.onChangeDetected(
              revisionId,
              newRevisionId,
              uri,
              pos,
              operation,
              versionMismatches.get());
      if (pair.left) {
        // an mismatch has occurred: note it.
        versionMismatches.incrementAndGet();
      }
      if (pair.right != null) {
        // there's an exception to raise: do it
        throw pair.right;
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "ChangeTracker{");
    sb.append("changeDetectionPolicy=").append(policy);
    sb.append(", revisionId='").append(revisionId).append('\'');
    sb.append('}');
    return sb.toString();
  }

}
