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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.ContentStreamProvider;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.store.ByteBufferInputStream;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;
import static org.apache.hadoop.util.Preconditions.checkArgument;
import static org.apache.hadoop.util.functional.FunctionalIO.uncheckIOExceptions;

/**
 * Implementations of {@code software.amazon.awssdk.http.ContentStreamProvider}.
 * <p>
 * These are required to ensure that retry of multipart uploads are reliable,
 * while also avoiding memory copy/consumption overhead.
 * <p>
 * For these reasons the providers built in to the AWS SDK are not used.
 * <p>
 * See HADOOP-19221 for details.
 */
public final class UploadContentProviders {

  public static final Logger LOG = LoggerFactory.getLogger(UploadContentProviders.class);

  private UploadContentProviders() {
  }

  /**
   * Create a content provider for a file.
   * @param file file to read.
   * @param offset offset in file.
   * @return the provider
   * @throws IllegalArgumentException if the offset is negative.
   */
  public static ContentStreamProvider fileContentProvider(File file, long offset, final int size) {
    return new FileWithOffsetContentProvider(file, offset, size);
  }

  /**
   * Create a content provider for a file.
   * @param byteBuffer buffer to read.
   * @param size size of the data.
   * @return the provider
   * @throws IllegalArgumentException if the arguments are invalid.
   * @throws NullPointerException if the buffer is null
   */
  public static ContentStreamProvider byteBufferContentProvider(
      final ByteBuffer byteBuffer, final int size) {
    return new ByteBufferContentProvider(byteBuffer, size);
  }

  /**
   * Create a content provider for a file.
   * @param bytes buffer to read.
   * @param offset offset in buffer.
   * @param size size of the data.
   * @return the provider
   * @throws IllegalArgumentException if the arguments are invalid.
   * @throws NullPointerException if the buffer is null.
   */
  public static ContentStreamProvider byteArrayContentProvider(
      final byte[] bytes, final int offset, final int size) {
    return new ByteArrayContentProvider(bytes, offset, size);
  }

  /**
   * Base class for content providers; tracks the number of times a stream
   * has been opened.
   *
   * @param <T> type of stream created.
   */
  @VisibleForTesting
  public static abstract class BaseContentProvider<T extends InputStream>
      implements ContentStreamProvider {

    /**
     * Size of the data.
     */
    private final int size;

    /**
     * How many times has a stream been created?
     */
    private int streamCreationCount;

    /**
     * Current stream. Null if not opened yet.
     * When {@link #newStream()} is called, this is set to the new value,
     * Note: when the input stream itself is closed, this reference is not updated.
     * Therefore this field not being null does not imply that the stream is open.
     */
    private T currentStream;

    /**
     * Constructor.
     * @param size size of the data. Must be non-negative.
     */
    protected BaseContentProvider(final int size) {
      checkArgument(size >= 0, "size is negative: %s", size);
      this.size = size;
    }

    /**
     * Note that a stream was created.
     * Logs if this is a subsequent event as it implies a failure of the first attempt.
     * @return the new stream
     */
    @Override
    public final InputStream newStream() {
      cleanupWithLogger(LOG, getCurrentStream());
      streamCreationCount++;
      if (streamCreationCount > 1) {
        LOG.info("Stream created more than once: {}", this);
      }
      return createNewStream();
    }

    /**
     * Override point for subclasses to create their new streams.
     * @return a stream
     */
    protected abstract T createNewStream();

    /**
     * How many times has a stream been created?
     * @return
     */
    int getStreamCreationCount() {
      return streamCreationCount;
    }

    /**
     * Size constructor parameter.
     * @return size of the data
     */

    public int getSize() {
      return size;
    }

    /**
     * Current stream.
     * When {@link #newStream()} is called, this is set to the new value,
     * after closing the previous one.
     * <p>
     * Why? The AWS SDK implementations do this, so there
     * is an implication that it is needed to avoid keeping streams
     * open on retries.
     */
    protected T getCurrentStream() {
      return currentStream;
    }

    /**
     * Set the current stream.
     * @param stream the new stream
     * @return the current stream.
     */
    protected T setCurrentStream(T stream) {
      this.currentStream = stream;
      return stream;
    }

    @Override
    public String toString() {
      return "BaseContentProvider{" +
          "size=" + size +
          ", streamCreationCount=" + streamCreationCount +
          ", currentStream=" + currentStream +
          '}';
    }
  }

  /**
   * Content provider for a file with an offset.
   */
  private static final class FileWithOffsetContentProvider
      extends BaseContentProvider<BufferedInputStream> {

    /**
     * File to read.
     */
    private final File file;

    /**
     * Offset in file.
     */
    private final long offset;

    /**
     * Constructor.
     * @param file file to read.
     * @param offset offset in file.
     * @throws IllegalArgumentException if the offset is negative.
     */
    public FileWithOffsetContentProvider(final File file, final long offset, final int size) {
      super(size);
      this.file = requireNonNull(file);
      checkArgument(offset >= 0, "Offset is negative: %s", offset);
      this.offset = offset;
    }

    @Override
    protected BufferedInputStream createNewStream() {
      // create the stream, seek to the offset.
      final FileInputStream fis = uncheckIOExceptions(() -> {
        final FileInputStream f = new FileInputStream(file);
        f.getChannel().position(offset);
        return f;
      });
      setCurrentStream(new BufferedInputStream(fis));
      return getCurrentStream();
    }

    @Override
    public String toString() {
      return "FileWithOffsetContentProvider{" +
          "file=" + file +
          ", offset=" + offset +
          "} " + super.toString();
    }

  }

  /**
   * Create a content provider for a byte buffer.
   * Uses {@link ByteBufferInputStream} to read the data.
   */
  private static final class ByteBufferContentProvider
      extends BaseContentProvider<ByteBufferInputStream> {

    /**
     * The buffer which will be read; on or off heap.
     */
    private final ByteBuffer blockBuffer;

    /**
     * Size of the data in the buffer.
     */
    private final int dataSize;

    /**
     * Constructor.
     * @param blockBuffer buffer to read.
     * @param size size of the data.
     * @throws IllegalArgumentException if the arguments are invalid.
     * @throws NullPointerException if the buffer is null
     */
    private ByteBufferContentProvider(final ByteBuffer blockBuffer, int size) {
      super(size);
      this.blockBuffer = blockBuffer;
      dataSize = blockBuffer.capacity() - blockBuffer.remaining();
    }

    @Override
    protected ByteBufferInputStream createNewStream() {
      return new ByteBufferInputStream(dataSize, blockBuffer);
    }

    @Override
    public String toString() {
      return "ByteBufferContentProvider{" +
          "blockBuffer=" + blockBuffer +
          ", dataSize=" + dataSize +
          "} " + super.toString();
    }
  }

  /**
   * Simple byte array content provider.
   * The array is not copied; if it is changed during the write the outcome
   * of the upload is undefined.
   */
  private static final class ByteArrayContentProvider
      extends BaseContentProvider<ByteArrayInputStream> {
    /**
     * The buffer where data is stored.
     */
    private final byte[] bytes;

    /**
     * Offset in the buffer.
     */
    private final int offset;

    /**
     * Constructor.
     * @param bytes buffer to read.
     * @param offset offset in buffer.
     * @param size length of the data.
     * @throws IllegalArgumentException if the arguments are invalid.
     * @throws NullPointerException if the buffer is null
     */
    private ByteArrayContentProvider(final byte[] bytes, final int offset, final int size) {
      super(size);
      this.bytes = bytes;
      this.offset = offset;
    }

    @Override
    protected ByteArrayInputStream createNewStream() {
      return new ByteArrayInputStream(this.bytes, offset, getSize());
    }

    @Override
    public String toString() {
      return "ByteArrayContentProvider{" +
          "buf=" + Arrays.toString(bytes) +
          ", offset=" + offset +
          "} " + super.toString();
    }
  }

}
