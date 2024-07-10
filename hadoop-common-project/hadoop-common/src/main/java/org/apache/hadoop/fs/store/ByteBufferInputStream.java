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

package org.apache.hadoop.fs.store;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.util.Preconditions;

/**
 * Provide an input stream from a byte buffer; supporting
 * {@link #mark(int)}, which is required to enable replay of failed
 * PUT attempts.
 */
public final class ByteBufferInputStream extends InputStream {
  private static final Logger LOG =
      LoggerFactory.getLogger(DataBlocks.class);
  
  /** Size of the buffer. */
  private final int size;

  /**
   * Not final so that in close() it will be set to null, which
   * may result in faster cleanup of the buffer.
   */
  private ByteBuffer byteBuffer;

  public ByteBufferInputStream(int size,
      ByteBuffer byteBuffer) {
    LOG.debug("Creating ByteBufferInputStream of size {}", size);
    this.size = size;
    this.byteBuffer = byteBuffer;
  }

  /**
   * After the stream is closed, set the local reference to the byte
   * buffer to null; this guarantees that future attempts to use
   * stream methods will fail.
   */
  @Override
  public synchronized void close() {
    LOG.debug("ByteBufferInputStream.close()");
    byteBuffer = null;
  }

  /**
   * Verify that the stream is open.
   * @throws IOException if the stream is closed
   */
  private void verifyOpen() throws IOException {
    if (byteBuffer == null) {
      throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  public synchronized int read() throws IOException {
    if (available() > 0) {
      return byteBuffer.get() & 0xFF;
    } else {
      return -1;
    }
  }

  @Override
  public synchronized long skip(long offset) throws IOException {
    verifyOpen();
    long newPos = position() + offset;
    if (newPos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    if (newPos > size) {
      throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
    }
    byteBuffer.position((int) newPos);
    return newPos;
  }

  @Override
  public synchronized int available() {
    Preconditions.checkState(byteBuffer != null,
        FSExceptionMessages.STREAM_IS_CLOSED);
    return byteBuffer.remaining();
  }

  /**
   * Get the current buffer position.
   * @return the buffer position
   */
  public synchronized int position() {
    return byteBuffer.position();
  }

  /**
   * Check if there is data left.
   * @return true if there is data remaining in the buffer.
   */
  public synchronized boolean hasRemaining() {
    return byteBuffer.hasRemaining();
  }

  @Override
  public synchronized void mark(int readlimit) {
    LOG.debug("mark at {}", position());
    byteBuffer.mark();
  }

  @Override
  public synchronized void reset() throws IOException {
    LOG.debug("reset");
    byteBuffer.reset();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  /**
   * Read in data.
   * @param b destination buffer.
   * @param offset offset within the buffer.
   * @param length length of bytes to read.
   * @throws EOFException if the position is negative
   * @throws IndexOutOfBoundsException if there isn't space for the
   * amount of data requested.
   * @throws IllegalArgumentException other arguments are invalid.
   */
  @SuppressWarnings("NullableProblems")
  public synchronized int read(byte[] b, int offset, int length)
      throws IOException {
    Preconditions.checkArgument(length >= 0, "length is negative");
    Preconditions.checkArgument(b != null, "Null buffer");
    if (b.length - offset < length) {
      throw new IndexOutOfBoundsException(
          FSExceptionMessages.TOO_MANY_BYTES_FOR_DEST_BUFFER
              + ": request length =" + length
              + ", with offset =" + offset
              + "; buffer capacity =" + (b.length - offset));
    }
    verifyOpen();
    if (!hasRemaining()) {
      return -1;
    }

    int toRead = Math.min(length, available());
    byteBuffer.get(b, offset, toRead);
    return toRead;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "ByteBufferInputStream{");
    sb.append("size=").append(size);
    ByteBuffer buf = this.byteBuffer;
    if (buf != null) {
      sb.append(", available=").append(buf.remaining());
    }
    sb.append('}');
    return sb.toString();
  }
}
