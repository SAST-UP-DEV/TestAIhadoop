/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.lang.Math.min;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FALSE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_OPTIMIZE_FOOTER_READ;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_SMALL_FILES_COMPLETELY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_INPUT_STREAM_LAZY_OPEN_OPTIMIZATION_ENABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestAbfsInputStreamSmallFileReads extends
    AbstractAbfsIntegrationTest {

  private final AbfsInputStreamTestUtils abfsInputStreamTestUtils;
  public ITestAbfsInputStreamSmallFileReads() throws Exception {
    this.abfsInputStreamTestUtils = new AbfsInputStreamTestUtils(this);
  }

  @Test
  public void testOnlyOneServerCallIsMadeWhenTheConfIsTrue() throws Exception {
    testNumBackendCalls(true);
  }

  @Test
  public void testMultipleServerCallsAreMadeWhenTheConfIsFalse()
      throws Exception {
    testNumBackendCalls(false);
  }

  private void testNumBackendCalls(boolean readSmallFilesCompletely)
      throws Exception {
    try (AzureBlobFileSystem fs = abfsInputStreamTestUtils.getFileSystem(
        readSmallFilesCompletely)) {
      validateNumBackendCalls(readSmallFilesCompletely, fs);
    }
  }

  private void validateNumBackendCalls(final boolean readSmallFilesCompletely,
      final AzureBlobFileSystem fs)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    for (int i = 1; i <= 4; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(fileSize);
      Path testFilePath = abfsInputStreamTestUtils.createFileWithContent(fs, fileName, fileContent);
      int length = ONE_KB;
      try (FSDataInputStream iStream = fs.open(testFilePath)) {
        byte[] buffer = new byte[length];

        Map<String, Long> metricMap = getInstrumentationMap(fs);
        long requestsMadeBeforeTest = metricMap
            .get(CONNECTIONS_MADE.getStatName());

        iStream.seek(seekPos(SeekTo.END, fileSize, length));
        iStream.read(buffer, 0, length);

        iStream.seek(seekPos(SeekTo.MIDDLE, fileSize, length));
        iStream.read(buffer, 0, length);

        iStream.seek(seekPos(SeekTo.BEGIN, fileSize, length));
        iStream.read(buffer, 0, length);

        metricMap = getInstrumentationMap(fs);
        long requestsMadeAfterTest = metricMap
            .get(CONNECTIONS_MADE.getStatName());

        if (readSmallFilesCompletely) {
          assertEquals(1, requestsMadeAfterTest - requestsMadeBeforeTest);
        } else {
          assertEquals(3, requestsMadeAfterTest - requestsMadeBeforeTest);
        }
      }
    }
  }

  @Test
  public void testSeekToBeginingAndReadSmallFileWithConfTrue()
      throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 2, 4, true);
  }

  @Test
  public void testSeekToBeginingAndReadSmallFileWithConfFalse()
      throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 2, 4, false);
  }

  @Test
  public void testSeekToBeginingAndReadBigFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 5, 6, true);
  }

  @Test
  public void testSeekToBeginingAndReadBigFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.BEGIN, 5, 6, false);
  }

  @Test
  public void testSeekToEndAndReadSmallFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 2, 4, true);
  }

  @Test
  public void testSeekToEndAndReadSmallFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 2, 4, false);
  }

  @Test
  public void testSeekToEndAndReadBigFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 5, 6, true);
  }

  @Test
  public void testSeekToEndAndReaBigFiledWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.END, 5, 6, false);
  }

  @Test
  public void testSeekToMiddleAndReadSmallFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 2, 4, true);
  }

  @Test
  public void testSeekToMiddleAndReadSmallFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 2, 4, false);
  }

  @Test
  public void testSeekToMiddleAndReaBigFileWithConfTrue() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 5, 6, true);
  }

  @Test
  public void testSeekToMiddleAndReadBigFileWithConfFalse() throws Exception {
    testSeekAndReadWithConf(SeekTo.MIDDLE, 5, 6, false);
  }

  private void testSeekAndReadWithConf(SeekTo seekTo, int startFileSizeInMB,
      int endFileSizeInMB, boolean readSmallFilesCompletely) throws Exception {
    try (AzureBlobFileSystem fs = abfsInputStreamTestUtils.getFileSystem(
        readSmallFilesCompletely)) {
      validateSeekAndReadWithConf(seekTo, startFileSizeInMB, endFileSizeInMB,
          fs);
    }
  }

  private void validateSeekAndReadWithConf(final SeekTo seekTo,
      final int startFileSizeInMB,
      final int endFileSizeInMB,
      final AzureBlobFileSystem fs)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    for (int i = startFileSizeInMB; i <= endFileSizeInMB; i++) {
      String fileName = methodName.getMethodName() + i;
      int fileSize = i * ONE_MB;
      byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(fileSize);
      Path testFilePath = abfsInputStreamTestUtils.createFileWithContent(fs, fileName, fileContent);
      int length = ONE_KB;
      int seekPos = seekPos(seekTo, fileSize, length);
      seekReadAndTest(fs, testFilePath, seekPos, length, fileContent);
    }
  }

  private int seekPos(SeekTo seekTo, int fileSize, int length) {
    if (seekTo == SeekTo.BEGIN) {
      return 0;
    }
    if (seekTo == SeekTo.END) {
      return fileSize - length;
    }
    return fileSize / 2;
  }

  private void seekReadAndTest(FileSystem fs, Path testFilePath, int seekPos,
      int length, byte[] fileContent)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    AbfsConfiguration conf = getConfiguration((AzureBlobFileSystem) fs);
    try (FSDataInputStream iStream = fs.open(testFilePath)) {
      abfsInputStreamTestUtils.seek(iStream, seekPos);
      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(bytesRead, length);
      abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent, seekPos, length, buffer, testFilePath);
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();

      final int readBufferSize = conf.getReadBufferSize();
      final int fileContentLength = fileContent.length;
      final boolean smallFile;
      final boolean headOptimization = getConfiguration().isInputStreamLazyOptimizationEnabled();

      if (headOptimization) {
        smallFile = ((seekPos + length) <= readBufferSize);
      } else {
        smallFile = fileContentLength <= readBufferSize;
      }
      int expectedLimit, expectedFCursor;
      int expectedBCursor;
      if (conf.readSmallFilesCompletely() && smallFile) {
        abfsInputStreamTestUtils.assertAbfsInputStreamBufferNotEqualToContentStartSubsequence(fileContent, abfsInputStream, conf, testFilePath);
        /*
         * If head optimization is enabled. The stream can do full read file optimization on the first read if
         * the seekPos is less than readBufferSize and the length is such that (seekPos + length) < readBufferSize.
         * Since it is unaware of the contentLength, it would try to read the full buffer size.
         *
         * In case of the head optimization is enabled, and readBufferSize < fileContentLength, the stream will
         * read only the readBuffer and would set internal pointers to the end of readBufferLength.
         */
        expectedFCursor = min(readBufferSize, fileContentLength);
        expectedLimit = min(readBufferSize, fileContentLength);
        expectedBCursor = min(readBufferSize, seekPos + length);
      } else {
        if ((seekPos == 0)) {
          abfsInputStreamTestUtils.assertAbfsInputStreamBufferNotEqualToContentStartSubsequence(fileContent, abfsInputStream, conf, testFilePath);
        } else {
          abfsInputStreamTestUtils.assertAbfsInputStreamBufferEqualToContentStartSubsequence(fileContent, abfsInputStream,
              conf, testFilePath);
        }
        expectedBCursor = length;
        expectedFCursor = (fileContentLength < (seekPos + readBufferSize))
            ? fileContentLength
            : (seekPos + readBufferSize);
        expectedLimit = (fileContentLength < (seekPos + readBufferSize))
            ? (fileContentLength - seekPos)
            : readBufferSize;
      }
      assertEquals(expectedFCursor, abfsInputStream.getFCursor());
      assertEquals(expectedFCursor, abfsInputStream.getFCursorAfterLastRead());
      assertEquals(expectedBCursor, abfsInputStream.getBCursor());
      assertEquals(expectedLimit, abfsInputStream.getLimit());
    }
  }

  @Test
  public void testPartialReadWithNoData() throws Exception {
    for (int i = 2; i <= 4; i++) {
      int fileSize = i * ONE_MB;
      try (AzureBlobFileSystem fs = abfsInputStreamTestUtils.getFileSystem(
          true)) {
        String fileName = methodName.getMethodName() + i;
        byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(
            fileSize);
        Path testFilePath = abfsInputStreamTestUtils.createFileWithContent(fs,
            fileName, fileContent);
        partialReadWithNoData(fs, testFilePath, fileSize / 2, fileSize / 4,
            fileContent);
      }
    }
  }

  private void partialReadWithNoData(final FileSystem fs,
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent)
      throws IOException {

    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      Mockito.doReturn((long) fileContent.length).when(abfsInputStream).getContentLength();
      int[] readRemoteIteration = {0};
      Mockito.doAnswer(answer -> {
        readRemoteIteration[0]++;
        if (readRemoteIteration[0] <= 2) {
          return 10;
        }
        return answer.callRealMethod();
      }).when(abfsInputStream).readRemote(anyLong(), any(), anyInt(), anyInt(),
          any(TracingContext.class));

      iStream = new FSDataInputStream(abfsInputStream);
      abfsInputStreamTestUtils.seek(iStream, seekPos);
      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(bytesRead, length);
      abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent, seekPos, length, buffer, testFilePath);
      assertEquals(fileContent.length, abfsInputStream.getFCursor());
      assertEquals(fileContent.length,
          abfsInputStream.getFCursorAfterLastRead());
      assertEquals(length, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() >= length);
    } finally {
      iStream.close();
    }
  }

  @Test
  public void testPartialReadWithSomeData() throws Exception {
    for (int i = 2; i <= 4; i++) {
      int fileSize = i * ONE_MB;
      try (AzureBlobFileSystem fs = abfsInputStreamTestUtils.getFileSystem(
          true)) {
        String fileName = methodName.getMethodName() + i;
        byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(
            fileSize);
        Path testFilePath = abfsInputStreamTestUtils.createFileWithContent(fs,
            fileName, fileContent);
        partialReadWithSomeData(fs, testFilePath, fileSize / 2,
            fileSize / 4, fileContent);
      }
    }
  }

  private void partialReadWithSomeData(final FileSystem fs,
      final Path testFilePath,
      final int seekPos, final int length, final byte[] fileContent)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    FSDataInputStream iStream = fs.open(testFilePath);
    try {
      AbfsInputStream abfsInputStream = (AbfsInputStream) iStream
          .getWrappedStream();
      abfsInputStream = spy(abfsInputStream);
      //  first readRemote, will return first 10 bytes
      //  second readRemote, seekPos - someDataLength(10) will reach the
      //  seekPos as 10 bytes are already read in the first call. Plus
      //  someDataLength(10)
      int someDataLength = 10;
      int secondReturnSize = seekPos - 10 + someDataLength;
      doReturn(10)
          .doReturn(secondReturnSize)
          .doCallRealMethod()
          .when(abfsInputStream)
          .readRemote(anyLong(), any(), anyInt(), anyInt(),
              any(TracingContext.class));

      iStream = new FSDataInputStream(abfsInputStream);
      abfsInputStreamTestUtils.seek(iStream, seekPos);

      byte[] buffer = new byte[length];
      int bytesRead = iStream.read(buffer, 0, length);
      assertEquals(length, bytesRead);
      assertTrue(abfsInputStream.getFCursor() > seekPos + length);
      assertTrue(abfsInputStream.getFCursorAfterLastRead() > seekPos + length);
      //  Optimized read was no complete but it got some user requested data
      //  from server. So obviously the buffer will contain data more than
      //  seekPos + len
      assertEquals(length - someDataLength, abfsInputStream.getBCursor());
      assertTrue(abfsInputStream.getLimit() > length - someDataLength);
    } finally {
      iStream.close();
    }
  }

  /**
   * Test read full file optimization getting executed when there is head
   * optimization enabled and the file is smaller than the buffer size.
   *
   * The read call sent to inputStream is for full buffer size, assert that
   * the inputStream correctly fills the application buffer, and also fills the
   * inputStream buffer with the file content. Any next read should be catered
   * from the inputStream buffer.
   */
  @Test
  public void testHeadOptimizationOnFileLessThanBufferSize() throws Exception {
    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.set(FS_AZURE_INPUT_STREAM_LAZY_OPEN_OPTIMIZATION_ENABLED, TRUE);
    configuration.set(AZURE_READ_SMALL_FILES_COMPLETELY, TRUE);
    try (FileSystem fs = FileSystem.newInstance(configuration)) {
      int readBufferSize = getConfiguration().getReadBufferSize();
      byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(
          readBufferSize / 2);
      Path path = abfsInputStreamTestUtils.createFileWithContent(fs,
          methodName.getMethodName(), fileContent);

      try (FSDataInputStream is = fs.open(path)) {
        is.seek(readBufferSize / 4);
        byte[] buffer = new byte[readBufferSize / 2];
        int readLength = is.read(buffer, 0, readBufferSize / 2);
        assertEquals(readLength, readBufferSize / 4);
        abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent,
            readBufferSize / 4, readLength,
            buffer, path);

        is.seek(0);
        readLength = is.read(buffer, 0, readBufferSize / 2);
        assertEquals(readLength, readBufferSize / 2);
        abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent, 0,
            readLength, buffer, path);

        AbfsInputStream abfsInputStream
            = (AbfsInputStream) is.getWrappedStream();
        AbfsInputStreamStatisticsImpl streamStatistics =
            (AbfsInputStreamStatisticsImpl) abfsInputStream.getStreamStatistics();
        assertEquals(1, streamStatistics.getSeekInBuffer());
        assertEquals(1, streamStatistics.getRemoteReadOperations());
      }
    }
  }

  /**
   * Test read full file optimization getting executed when there is head optimization
   * is there on a file which has more contentLength than the readBufferSize, but the
   * read call results final fcursor lesser than readBufferLength.
   */
  @Test
  public void testHeadOptimizationOnFileBiggerThanBufferSize()
      throws Exception {
    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.set(FS_AZURE_INPUT_STREAM_LAZY_OPEN_OPTIMIZATION_ENABLED, TRUE);
    configuration.set(AZURE_READ_SMALL_FILES_COMPLETELY, TRUE);
    try (FileSystem fs = FileSystem.newInstance(configuration)) {
      int readBufferSize = getConfiguration().getReadBufferSize();
      byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(
          readBufferSize);
      Path path = abfsInputStreamTestUtils.createFileWithContent(fs,
          methodName.getMethodName(), fileContent);

      try (FSDataInputStream is = fs.open(path)) {
        is.seek(readBufferSize / 4);
        byte[] buffer = new byte[readBufferSize / 2];
        int readLength = is.read(buffer, 0, readBufferSize / 2);
        assertEquals(readLength, readBufferSize / 2);
        abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent,
            readBufferSize / 4, readLength, buffer, path);

        readLength = is.read(buffer, 0, readBufferSize / 4);
        assertEquals(readLength, readBufferSize / 4);
        abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent,
            3 * readBufferSize / 4, readLength, buffer, path);

        AbfsInputStream abfsInputStream
            = (AbfsInputStream) is.getWrappedStream();
        AbfsInputStreamStatisticsImpl streamStatistics =
            (AbfsInputStreamStatisticsImpl) abfsInputStream.getStreamStatistics();
        assertEquals(1, streamStatistics.getSeekInBuffer());
        assertEquals(1, streamStatistics.getRemoteReadOperations());
      }

      try (FSDataInputStream is = fs.open(path)) {
        is.seek(readBufferSize / 2);
        byte[] buffer = new byte[readBufferSize];
        int readLength = is.read(buffer, 0, readBufferSize / 2);

        assertEquals(readLength, readBufferSize / 2);
        abfsInputStreamTestUtils.assertContentReadCorrectly(fileContent,
            readBufferSize / 2, readLength, buffer, path);

        byte[] zeroBuffer = new byte[readBufferSize / 2];
        abfsInputStreamTestUtils.assertContentReadCorrectly(buffer,
            readBufferSize / 2, readBufferSize / 2, zeroBuffer, path);
      }
    }
  }

  @Test
  public void testHeadOptimizationPerformingOutOfRangeRead()
      throws Exception {
    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.set(FS_AZURE_INPUT_STREAM_LAZY_OPEN_OPTIMIZATION_ENABLED,
        TRUE);
    configuration.set(AZURE_READ_SMALL_FILES_COMPLETELY, TRUE);
    configuration.set(AZURE_READ_OPTIMIZE_FOOTER_READ, FALSE);

    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        configuration)) {
      int readBufferSize = getConfiguration().getReadBufferSize();
      byte[] fileContent = abfsInputStreamTestUtils.getRandomBytesArray(
          readBufferSize / 2);
      Path path = abfsInputStreamTestUtils.createFileWithContent(fs,
          methodName.getMethodName(), fileContent);

      try (FSDataInputStream is = fs.open(path)) {
        is.seek(readBufferSize / 2 + 1);
        byte[] buffer = new byte[readBufferSize / 2];
        int readLength = is.read(buffer, 0, readBufferSize / 2 - 1);
        assertEquals(readLength, -1);

        is.seek(0);
        readLength = is.read(buffer, 0, readBufferSize / 2);
        assertEquals(readLength, readBufferSize / 2);

        AbfsInputStream abfsInputStream
            = (AbfsInputStream) is.getWrappedStream();
        AbfsInputStreamStatisticsImpl streamStatistics =
            (AbfsInputStreamStatisticsImpl) abfsInputStream.getStreamStatistics();
        assertEquals(1, streamStatistics.getSeekInBuffer());
        assertEquals(1, streamStatistics.getRemoteReadOperations());
      }
    }
  }

  private enum SeekTo {BEGIN, MIDDLE, END}

}
