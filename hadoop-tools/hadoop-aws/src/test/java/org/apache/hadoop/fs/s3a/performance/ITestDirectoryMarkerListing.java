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

package org.apache.hadoop.fs.s3a.performance;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.impl.DirectoryPolicy;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * This is a test suite designed to verify that directory markers do
 * not get misconstrued as empty directories during operations
 * which explicitly or implicitly list directory trees.
 * <p></p>
 * It is also intended it to be backported to all releases
 * which are enhanced to read directory trees where markers have
 * been retained.
 * Hence: it does not use any of the new helper classes to
 * measure the cost of operations or attempt to create markers
 * through the FS APIs.
 * <p></p>
 * Instead, the directory structure to test is created through
 * low-level S3 SDK API calls.
 * We also skip any probes to measure/assert metrics.
 * We're testing the semantics here, not the cost of the operations.
 * Doing that makes it a lot easier to backport.
 *
 * <p></p>
 * Similarly: JUnit assertions over AssertJ.
 * <p></p>
 * The tests work with unguarded buckets only.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestDirectoryMarkerListing extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestDirectoryMarkerListing.class);

  private static final String FILENAME = "fileUnderMarker";

  private static final String HELLO = "hello";

  private static final String MARKER = "marker";

  private static final String MARKER_PEER = "markerpeer";

  /**
   * Does rename copy markers?
   * Value: {@value}
   * <p></p>
   * Older releases: yes.
   * <p></p>
   * The full marker-optimized releases: no.
   */
  private static final boolean RENAME_COPIES_MARKERS = false;

  /**
   * Does rename copy markers?
   * Value: {@value}
   * <p></p>
   * Older releases: yes.
   * <p></p>
   * The full marker-optimized releases: no.
   */
  private boolean renameDeletesParentMarkers = false;

  private Path markerDir;

  private String markerKey;

  private String markerKeySlash;

  private String bucket;

  private AmazonS3 s3client;

  private String fileKeyUnderMarker;

  private Path filePathUnderMarker;

  private Path basePath;

  private Path markerPeer;

  private String markerPeerKey;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);

    removeBaseAndBucketOverrides(bucketName, conf,
        S3_METADATA_STORE_IMPL);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    assume("unguarded FS only",
        !fs.hasMetadataStore());
    s3client = fs.getAmazonS3ClientForTesting("markers");

    bucket = fs.getBucket();
    renameDeletesParentMarkers = fs.getDirectoryMarkerPolicy()
        == DirectoryPolicy.MarkerPolicy.Delete;
    createTestObjects(new Path(methodPath(), "base"));
  }

  @Override
  public void teardown() throws Exception {
    if (s3client != null) {
      delete(markerKey);
      delete(markerKeySlash);
      delete(markerPeerKey);
      delete(fileKeyUnderMarker);
    }
    super.teardown();
  }

  /**
   * Create the test objects under the given path, setting
   * various fields in the process.
   * @param path parent path of everything
   */
  private void createTestObjects(final Path path) throws Exception {
    S3AFileSystem fs = getFileSystem();
    basePath = path;
    markerDir = new Path(basePath, MARKER);
    // peer path has the same initial name to make sure there
    // is no confusion there.
    markerPeer = new Path(basePath, MARKER_PEER);
    markerPeerKey = fs.pathToKey(markerPeer);
    markerKey = fs.pathToKey(markerDir);
    markerKeySlash = markerKey + "/";
    fileKeyUnderMarker = markerKeySlash + FILENAME;
    filePathUnderMarker = new Path(markerDir, FILENAME);
    // put the empty dir
    fs.mkdirs(markerDir);
    touch(fs, markerPeer);
    put(fileKeyUnderMarker, HELLO);
  }

  /*
  =================================================================
    Basic probes
  =================================================================
  */

  @Test
  public void testMarkerExists() throws Throwable {
    describe("Create the test markers for the suite");
    head(markerKeySlash);
    assertIsDirectory(markerDir);
  }

  @Test
  public void testObjectUnderMarker() throws Throwable {
    assertIsFile(filePathUnderMarker);
    assertIsDirectory(markerDir);
    head(fileKeyUnderMarker);
  }

  /*
  =================================================================
    The listing operations
  =================================================================
  */

  @Test
  public void testListStatusMarkerDir() throws Throwable {
    describe("list the marker directory and expect to see the file");
    assertContainsFileUnderMarkerOnly(
        toList(getFileSystem().listStatus(markerDir)));
  }


  @Test
  public void testListFilesMarkerDirFlat() throws Throwable {
    assertContainsFileUnderMarkerOnly(toList(
        getFileSystem().listFiles(markerDir, false)));
  }

  @Test
  public void testListFilesMarkerDirRecursive() throws Throwable {
    List<FileStatus> statuses = toList(
        getFileSystem().listFiles(markerDir, true));
    assertContainsFileUnderMarkerOnly(statuses);
  }

  /**
   * Path listing above the base dir MUST only find the file
   * and not the marker.
   */
  @Test
  public void testListStatusBaseDirRecursive() throws Throwable {
    List<FileStatus> statuses = toList(
        getFileSystem().listFiles(basePath, true));
    assertContainsExactlyStatusOfPaths(statuses, filePathUnderMarker, markerPeer);
  }

  @Test
  public void testGlobStatusBaseDirRecursive() throws Throwable {
    List<FileStatus> statuses =
        exec("glob", () ->
            toList(getFileSystem().globStatus(new Path(basePath, "*"))));
    assertContainsExactlyStatusOfPaths(statuses, markerDir, markerPeer);
    assertIsFileAtPath(markerPeer, statuses.get(1));
  }

  @Test
  public void testGlobStatusMarkerDir() throws Throwable {
    List<FileStatus> statuses =
        exec("glob", () ->
            toList(getFileSystem().globStatus(new Path(markerDir, "*"))));
    assertContainsFileUnderMarkerOnly(statuses);
  }

  /**
   * Call {@code listLocatedStatus(basePath)}
   * <p></p>
   * The list here returns the marker peer before the
   * dir. Reason: the listing iterators return
   * the objects before the common prefixes, and the
   * marker dir is coming back as a prefix.
   */
  @Test
  public void testListLocatedStatusBaseDir() throws Throwable {
    List<FileStatus> statuses =
        exec("listLocatedStatus", () ->
            toList(getFileSystem().listLocatedStatus(basePath)));

    assertContainsExactlyStatusOfPaths(statuses, markerPeer, markerDir);
  }

  /**
   * Call {@code listLocatedStatus(markerDir)}; expect
   * the file entry only.
   */
  @Test
  public void testListLocatedStatusMarkerDir() throws Throwable {
    List<FileStatus> statuses =
        exec("listLocatedStatus", () ->
            toList(getFileSystem().listLocatedStatus(markerDir)));

    assertContainsFileUnderMarkerOnly(statuses);
  }


  /*
  =================================================================
    Creation Rejection
  =================================================================
  */

  @Test
  public void testCreateNoOverwriteMarkerDir() throws Throwable {
    describe("create no-overwrite over the marker dir fails");
    head(markerKeySlash);
    intercept(FileAlreadyExistsException.class, () ->
        exec("create", () ->
            getFileSystem().create(markerDir, false)));
    // dir is still there.
    head(markerKeySlash);
  }

  @Test
  public void testCreateNoOverwriteFile() throws Throwable {
    describe("create-no-overwrite on the file fails");

    head(fileKeyUnderMarker);
    intercept(FileAlreadyExistsException.class, () ->
        exec("create", () ->
            getFileSystem().create(filePathUnderMarker, false)));
    assertTestObjectsExist();
  }

  @Test
  public void testCreateFileNoOverwrite() throws Throwable {
    describe("verify the createFile() API also fails");
    head(fileKeyUnderMarker);
    intercept(FileAlreadyExistsException.class, () ->
        exec("create", () ->
            getFileSystem().createFile(filePathUnderMarker)
                .overwrite(false)
                .build()));
    assertTestObjectsExist();
  }

  /*
  =================================================================
    Delete.
  =================================================================
  */

  @Test
  public void testDelete() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    // a non recursive delete MUST fail because
    // it is not empty
    intercept(PathIsNotEmptyDirectoryException.class, () ->
        fs.delete(markerDir, false));
    // file is still there
    head(fileKeyUnderMarker);

    // recursive delete MUST succeed
    fs.delete(markerDir, true);
    // and the markers are gone
    head404(fileKeyUnderMarker);
    head404(markerKeySlash);
    // just for completeness
    fs.delete(basePath, true);
  }

  /*
  =================================================================
    Rename.
  =================================================================
  */

  /**
   * Rename the base directory, expect the source files to move.
   * <p></p>
   * Whether or not the marker itself is copied depends on whether
   * the release's rename operation explicitly skips
   * markers on renames.
   */
  @Test
  public void testRenameBase() throws Throwable {
    describe("directory rename");

    Path src = basePath;
    Path dest = new Path(methodPath(), "dest");
    assertRenamed(src, dest);

    assertPathDoesNotExist("source", src);
    assertPathDoesNotExist("source", filePathUnderMarker);
    assertPathExists("dest not found", dest);

    // all the paths dest relative
    Path destMarkerDir = new Path(dest, MARKER);
    // peer path has the same initial name to make sure there
    // is no confusion there.
    Path destMarkerPeer = new Path(dest, MARKER_PEER);
    String destMarkerKey = toKey(destMarkerDir);
    String destMarkerKeySlash = destMarkerKey + "/";
    String destFileKeyUnderMarker = destMarkerKeySlash + FILENAME;
    Path destFilePathUnderMarker = new Path(destMarkerDir, FILENAME);
    assertIsFile(destFilePathUnderMarker);
    assertIsFile(destMarkerPeer);
    head(destFileKeyUnderMarker);

    // probe for the marker based on expected rename
    // behavior
    if (RENAME_COPIES_MARKERS) {
      head(destMarkerKeySlash);
    } else {
      head404(destMarkerKeySlash);
    }

  }

  /**
   * Rename under a marker.
   * The marker must no longer exist.
   */
  @Test
  public void testRenameUnderMarker() throws Throwable {
    describe("directory rename");
    S3AFileSystem fs = getFileSystem();
    String name = "sourceFile";
    Path src = new Path(basePath, name);
    String srcKey = toKey(src);
    put(srcKey, name);
    head(srcKey);

    Path dest = markerDir;
    // renamed into the dest dir
    assertRenamed(src, dest);
    assertIsFile(new Path(dest, name));
    if (renameDeletesParentMarkers) {
      head404(markerKeySlash);
    } else {
      head(markerKeySlash);
    }
  }

  /**
   * This test creates an empty dir and renames it over the directory marker.
   * If the dest was considered to be empty, the rename would fail.
   */
  @Test
  public void testRenameEmptyDirOverMarker() throws Throwable {
    describe("rename an empty directory over the marker");
    S3AFileSystem fs = getFileSystem();
    String name = "sourceDir";
    Path src = new Path(basePath, name);
    fs.mkdirs(src);
    assertIsDirectory(src);
    String srcKey = toKey(src) + "/";
    head(srcKey);
    Path dest = markerDir;
    // renamed into the dest dir
    assertFalse("rename(" + src + ", " + dest + ") should have failed",
        getFileSystem().rename(src, dest));
    // source is still there
    assertIsDirectory(src);
    head(srcKey);
    // and a non-recursive delete lets us verify it is considered
    // an empty dir
    assertDeleted(src, false);
    assertTestObjectsExist();
  }

  /*
  =================================================================
    Utility methods and assertions.
  =================================================================
  */

  /**
   * Assert the test objects exist
   */
  private void assertTestObjectsExist() throws Exception {
    head(fileKeyUnderMarker);
    head(markerKeySlash);
  }

  /**
   * Put a string to a path.
   * @param key key
   * @param content string
   */
  private void put(final String key, final String content) throws Exception {
    exec("PUT " + key, () ->
        s3client.putObject(bucket, key, content));
  }
  /**
   * Delete an object.
   * @param key key
   * @param content string
   */
  private void delete(final String key) throws Exception {
    exec("DELETE " + key, () -> {
      s3client.deleteObject(bucket, key);
      return "deleted " + key;
    });
  }

  /**
   * Issue a HEAD request.
   * @param key
   * @return a description of the object.
   */
  private String head(final String key) throws Exception {
    ObjectMetadata md = exec("HEAD " + key, () ->
        s3client.getObjectMetadata(bucket, key));
    return String.format("Object %s of length %d",
        key, md.getInstanceLength());
  }

  /**
   * Issue a HEAD request and expect a 404 back.
   * @param key
   * @return the metadata
   */
  private void head404(final String key) throws Exception {
    intercept(FileNotFoundException.class, "",
        "Expected 404 of " + key, () ->
        head(key));
  }

  /**
   * Execute an operation; transate AWS exceptions.
   * @param op operation
   * @param call call to make
   * @param <T> returned type
   * @return result of the call.
   * @throws Exception failure
   */
  private <T> T exec(String op, Callable<T> call) throws Exception {
    ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    try {
      return call.call();
    } catch (AmazonClientException ex) {
      throw S3AUtils.translateException(op, "", ex);
    } finally {
      timer.end(op);
    }
  }

  /**
   * Assert that the listing contains only the status
   * of the file under the marker.
   * @param statuses status objects
   */
  private void assertContainsFileUnderMarkerOnly(
      final List<FileStatus> statuses) {

    assertContainsExactlyStatusOfPaths(statuses, filePathUnderMarker);
    assertIsFileUnderMarker(statuses.get(0));
  }

  /**
   * Expect the list of status objects to match that of the paths.
   * @param statuses status object list
   * @param paths ordered varargs list of paths
   * @param <T> type of status objects
   */
  private <T extends FileStatus> void assertContainsExactlyStatusOfPaths(
      List<T> statuses, Path... paths) {

    String summary = statuses.stream()
        .map(Object::toString)
        .collect(Collectors.joining(";"));
    assertEquals("mismatch in size of listing " + summary,
        paths.length, statuses.size());
    for (int i = 0; i < statuses.size(); i++) {
      assertEquals("Path mismatch at element " + i + " in " + summary,
          paths[i], statuses.get(i).getPath());
    }
  }

  /**
   * Assert the status object refers to the file created
   * under the marker.
   * @param stat status object
   */
  private void assertIsFileUnderMarker(final FileStatus stat) {
    assertIsFileAtPath(filePathUnderMarker, stat);
  }

  /**
   * Assert the status object refers to a path at the given name
   * @param path path
   * @param stat status object
   */
  private void assertIsFileAtPath(final Path path, final FileStatus stat) {
    assertTrue("Is not file " + stat, stat.isFile());
    assertPathEquals(path, stat);
  }

  /**
   * Assert a status object's path matches expected.
   * @param path path to expect
   * @param stat status object
   */
  private void assertPathEquals(final Path path, final FileStatus stat) {
    assertEquals("filename is not the expected path :" + stat,
        path, stat.getPath());
  }

  /**
   * Given a remote iterator of status objects,
   * build a list of the values.
   * @param status status list
   * @param <T> actual type.
   * @return source.
   * @throws IOException
   */
  private <T extends FileStatus> List<FileStatus> toList(
      RemoteIterator<T> status) throws IOException {

    List<FileStatus> l = new ArrayList<>();
    while (status.hasNext()) {
      l.add(status.next());
    }
    return dump(l);
  }

  /**
   * Given an array of status objects,
   * build a list of the values.
   * @param status status list
   * @param <T> actual type.
   * @return source.
   * @throws IOException
   */
  private <T extends FileStatus> List<FileStatus> toList(
      T[] status) throws IOException {
    return dump(Arrays.asList(status));
  }

  /**
   * Dump the string values of a list to the log; return
   * the list.
   * @param l source.
   * @param <T> source type
   * @return the list
   */
  private <T> List<T> dump(List<T> l) {
    int c = 1;
    for (T t : l) {
      LOG.info("{}\t{}", c++, t);
    }
    return l;
  }

  /**
   * Rename: assert the outcome is true.
   * @param src source path
   * @param dest dest path
   */
  private void assertRenamed(final Path src, final Path dest)
      throws IOException {
    assertTrue("rename(" + src + ", " + dest + ") failed",
        getFileSystem().rename(src, dest));
  }

  private String toKey(final Path destMarkerDir) {
    return getFileSystem().pathToKey(destMarkerDir);
  }

}
