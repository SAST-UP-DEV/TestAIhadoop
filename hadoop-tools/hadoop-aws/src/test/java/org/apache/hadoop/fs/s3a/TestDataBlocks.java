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

package org.apache.hadoop.fs.s3a;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.impl.UploadContentProviders;
import org.apache.hadoop.fs.store.ByteBufferInputStream;
import org.apache.hadoop.test.HadoopTestBase;

import static java.util.Optional.empty;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_ARRAY;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BUFFER_DISK;
import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD_BYTEBUFFER;

/**
 * Unit tests for {@link S3ADataBlocks}.
 * Parameterized on the buffer type.
 */
@RunWith(Parameterized.class)
public class TestDataBlocks extends HadoopTestBase {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {FAST_UPLOAD_BUFFER_DISK},
        {FAST_UPLOAD_BUFFER_ARRAY},
        {FAST_UPLOAD_BYTEBUFFER}
    });
  }

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  /**
   * Buffer type.
   */
  private final String bufferType;

  public TestDataBlocks(final String bufferType) {
    this.bufferType = bufferType;
  }

  /**
   * Create a block factory.
   * @return the factory
   */
  private S3ADataBlocks.BlockFactory createFactory() {
    switch (bufferType) {
    // this one passed in a file allocation function
    case FAST_UPLOAD_BUFFER_DISK:
      return new S3ADataBlocks.DiskBlockFactory((i, l) ->
          tempDir.newFile("file" + i));
    case FAST_UPLOAD_BUFFER_ARRAY:
      return new S3ADataBlocks.ArrayBlockFactory(null);
    case FAST_UPLOAD_BYTEBUFFER:
      return new S3ADataBlocks.ByteBufferBlockFactory(null);
    default:
      throw new IllegalArgumentException("Unknown buffer type: " + bufferType);
    }
  }

  /**
   * Test the content providers from the block factory and the streams
   * they produce.
   * There are extra assertions on the {@link ByteBufferInputStream}.
   */
  @Test
  public void testBlockFactoryIO() throws Throwable {
    try (S3ADataBlocks.BlockFactory factory = createFactory()) {
      int limit = 128;
      S3ADataBlocks.DataBlock block
          = factory.create(1, limit, null);
      maybeAssertOutstandingBuffers(factory, 1);

      byte[] buffer = ContractTestUtils.toAsciiByteArray("test data");
      int bufferLen = buffer.length;
      block.write(buffer, 0, bufferLen);
      assertEquals(bufferLen, block.dataSize());
      assertEquals("capacity in " + block,
          limit - bufferLen, block.remainingCapacity());
      assertTrue("hasCapacity(64) in " + block, block.hasCapacity(64));
      assertTrue("No capacity in " + block,
          block.hasCapacity(limit - bufferLen));

      // now start the write
      S3ADataBlocks.BlockUploadData blockUploadData = block.startUpload();
      final UploadContentProviders.BaseContentProvider<?> cp =
          blockUploadData.getContentProvider();

      assertStreamCreationCount(cp, 0);
      InputStream stream = cp.newStream();
      assertStreamCreationCount(cp, 1);

      Optional<ByteBufferInputStream> bbStream =
          stream instanceof ByteBufferInputStream
              ? Optional.of((ByteBufferInputStream) stream)
              : empty();
      assertTrue("Mark not supported in " + stream, stream.markSupported());

      bbStream.ifPresent(bb -> {
        Assertions.assertThat(bb.hasRemaining())
            .describedAs("hasRemaining() in %s", bb)
            .isTrue();
      });
      int expected = bufferLen;
      assertEquals("wrong available() in " + stream,
          expected, stream.available());

      assertEquals('t', stream.read());
      stream.mark(limit);
      expected--;
      assertEquals("wrong available() in " + stream,
          expected, stream.available());


      // read into a byte array with an offset
      int offset = 5;
      byte[] in = new byte[limit];
      assertEquals(2, stream.read(in, offset, 2));
      assertEquals('e', in[offset]);
      assertEquals('s', in[offset + 1]);
      expected -= 2;
      assertEquals("wrong available() in " + stream,
          expected, stream.available());

      // read to end
      byte[] remainder = new byte[limit];
      int c;
      int index = 0;
      while ((c = stream.read()) >= 0) {
        remainder[index++] = (byte) c;
      }
      assertEquals(expected, index);
      assertEquals('a', remainder[--index]);

      // no more data left
      assertEquals("wrong available() in " + stream,
          0, stream.available());
      bbStream.ifPresent(bb -> {
        Assertions.assertThat(bb.hasRemaining())
            .describedAs("hasRemaining() in %s", bb)
            .isFalse();
      });

      // at the end of the stream, a read fails
      Assertions.assertThat(stream.read())
          .describedAs("EOF in " + stream)
          .isEqualTo(-1);

      // go the mark point
      stream.reset();
      assertEquals('e', stream.read());

      // now ask the content provider for another content stream.
      final InputStream stream2 = cp.newStream();
      assertStreamCreationCount(cp, 2);

      // this must close the old stream
      bbStream.ifPresent(bb -> {
        Assertions.assertThat(bb.isOpen())
            .describedAs("stream %s is open", bb)
            .isFalse();
      });

      // do a read(byte[]) of everything
      byte[] readBuffer = new byte[bufferLen];
      Assertions.assertThat(stream2.read(readBuffer))
          .describedAs("number of bytes read from stream %s", stream2)
          .isEqualTo(bufferLen);
      Assertions.assertThat(readBuffer)
          .describedAs("data read into buffer")
          .isEqualTo(buffer);

      // when the block is closed, the buffer must be returned
      // to the pool.
      block.close();
      maybeAssertOutstandingBuffers(factory, 0);
      stream.close();
      maybeAssertOutstandingBuffers(factory, 0);
    }

  }

  private static void assertStreamCreationCount(
      final UploadContentProviders.BaseContentProvider<?> cp,
      final int count) {
    Assertions.assertThat(cp.getStreamCreationCount())
        .describedAs("stream creation count of %s", cp)
        .isEqualTo(count);
  }

  /**
   * Assert the number of buffers active for a block factory,
   * if the factory is a ByteBufferBlockFactory.
   * <p>
   * If it is of any other type, no checks are made.
   * @param factory factory
   * @param expectedCount expected count.
   */
  private static void maybeAssertOutstandingBuffers(
      S3ADataBlocks.BlockFactory factory,
      int expectedCount) {
    if (factory instanceof S3ADataBlocks.ByteBufferBlockFactory) {
      S3ADataBlocks.ByteBufferBlockFactory bufferFactory =
          (S3ADataBlocks.ByteBufferBlockFactory) factory;
      Assertions.assertThat(bufferFactory.getOutstandingBufferCount())
          .describedAs("outstanding buffers in %s", factory)
          .isEqualTo(expectedCount);
    }
  }

}
