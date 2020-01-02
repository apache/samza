/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.system.azureblob.avro;

import com.azure.core.http.rest.SimpleResponse;
import com.azure.core.implementation.util.FluxUtil;
import java.util.Arrays;
import org.apache.samza.system.azureblob.compression.Compression;
import org.apache.samza.system.azureblob.producer.AzureBlobWriterMetrics;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockBlobAsyncClient.class})
public class TestAzureBlobOutputStream {
  private ThreadPoolExecutor threadPool;
  private ByteArrayOutputStream mockByteArrayOutputStream;
  private static final int THRESHOLD = 100;
  private BlockBlobAsyncClient mockBlobAsyncClient;
  private AzureBlobOutputStream azureBlobOutputStream;
  private static final String RANDOM_STRING = "roZzozzLiR7GCEjcB0UsRUNgBAip8cSLGXQSo3RQvbIDoxOaaRs4hrec2s5rMPWgTPRY4UnE959worEtyhRjwUFnRnVuNFZ554yuPQCbI69qFkQX7MmrB4blmpSnFeGjWKjFjIRLFNVSsQBYMkr5jT4T83uVtuGumsjACVrpcilihdd194H8Y71rQcrXZoTQtw5OvmPicbwptawpHoRNzHihyaDVYgAs0dQbvVEu1gitKpamzYdMLFtc5h8PFZSVEB";
  private static final byte[] BYTES = RANDOM_STRING.substring(0, THRESHOLD).getBytes();
  private static final byte[] COMPRESSED_BYTES = RANDOM_STRING.substring(0, THRESHOLD / 2).getBytes();
  private AzureBlobWriterMetrics mockMetrics;
  private Compression mockCompression;

  @Before
  public void setup() throws Exception {
    threadPool = new ThreadPoolExecutor(1, 1, 60,
        TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());


    mockByteArrayOutputStream = spy(new ByteArrayOutputStream(THRESHOLD));

    mockBlobAsyncClient = PowerMockito.mock(BlockBlobAsyncClient.class);
    when(mockBlobAsyncClient.stageBlock(anyString(), any(), anyLong())).thenReturn(
        Mono.just(new SimpleResponse(null, 200, null, null)).flatMap(FluxUtil::toMono));
    when(mockBlobAsyncClient.commitBlockListWithResponse(any(), any(), any(), any(), any())).thenReturn(
        Mono.just(new SimpleResponse(null, 200, null, null)));

    when(mockBlobAsyncClient.getBlobUrl()).thenReturn("https://samza.blob.core.windows.net/fake-blob-url");

    mockMetrics = mock(AzureBlobWriterMetrics.class);

    mockCompression = mock(Compression.class);
    doReturn(COMPRESSED_BYTES).when(mockCompression).compress(BYTES);

    azureBlobOutputStream = spy(new AzureBlobOutputStream(mockBlobAsyncClient, threadPool, mockMetrics,
        60000, THRESHOLD, mockByteArrayOutputStream, mockCompression));
  }

  @Test
  public void testWrite() {
    byte[] b = new byte[THRESHOLD - 10];
    azureBlobOutputStream.write(b, 0, THRESHOLD - 10);
    verify(mockBlobAsyncClient, never()).stageBlock(any(), any(), anyLong()); // since size of byte[] written is less than threshold
    verify(mockMetrics).updateWriteByteMetrics(THRESHOLD - 10);
    verify(mockMetrics, never()).updateAzureUploadMetrics();
  }

  @Test
  public void testWriteLargerThanThreshold() {
    byte[] largeRecord = RANDOM_STRING.substring(0, 2 * THRESHOLD).getBytes();
    byte[] largeRecordFirstHalf = RANDOM_STRING.substring(0, THRESHOLD).getBytes();
    byte[] largeRecordSecondHalf = RANDOM_STRING.substring(THRESHOLD, 2 * THRESHOLD).getBytes();

    byte[] compressB1 = RANDOM_STRING.substring(0, THRESHOLD / 2).getBytes();
    byte[] compressB2 = RANDOM_STRING.substring(THRESHOLD / 2, THRESHOLD).getBytes();

    doReturn(compressB1).when(mockCompression).compress(largeRecordFirstHalf);
    doReturn(compressB2).when(mockCompression).compress(largeRecordSecondHalf);

    azureBlobOutputStream.write(largeRecord, 0, 2 * THRESHOLD);
    // azureBlobOutputStream.close waits on the CompletableFuture which does the actual stageBlock in uploadBlockAsync
    azureBlobOutputStream.close();

    // invoked 2 times for the data which is 2*threshold
    verify(mockCompression).compress(largeRecordFirstHalf);
    verify(mockCompression).compress(largeRecordSecondHalf);
    ArgumentCaptor<Flux> argument = ArgumentCaptor.forClass(Flux.class);
    verify(mockBlobAsyncClient).stageBlock(eq(blockIdEncoded(0)), argument.capture(), eq((long) compressB1.length));
    verify(mockBlobAsyncClient).stageBlock(eq(blockIdEncoded(1)), argument.capture(), eq((long) compressB2.length));
    Assert.assertEquals(ByteBuffer.wrap(compressB1), argument.getAllValues().get(0).blockFirst());
    Assert.assertEquals(ByteBuffer.wrap(compressB2), argument.getAllValues().get(1).blockFirst());
    verify(mockMetrics).updateWriteByteMetrics(2 * THRESHOLD);
    verify(mockMetrics, times(2)).updateAzureUploadMetrics();
  }

  @Test
  public void testWriteLargeRecordWithSmallRecordInBuffer() {
    byte[] halfBlock = new byte[THRESHOLD / 2];
    byte[] fullBlock = new byte[THRESHOLD];
    byte[] largeRecord = new byte[2 * THRESHOLD];
    byte[] fullBlockCompressedByte = new byte[50];
    byte[] halfBlockCompressedByte = new byte[25];
    doReturn(fullBlockCompressedByte).when(mockCompression).compress(fullBlock);
    doReturn(halfBlockCompressedByte).when(mockCompression).compress(halfBlock);

    // FIRST write a small record = same as half block
    azureBlobOutputStream.write(halfBlock, 0, THRESHOLD / 2);
    verify(mockMetrics).updateWriteByteMetrics(THRESHOLD / 2);

    // SECOND write the large record
    azureBlobOutputStream.write(largeRecord, 0, 2 * THRESHOLD);
    verify(mockMetrics).updateWriteByteMetrics(2 * THRESHOLD);

    azureBlobOutputStream.flush(); // to flush out buffered data

    // azureBlobOutputStream.close waits on the CompletableFuture which does the actual stageBlock in uploadBlockAsync
    azureBlobOutputStream.close();

    verify(mockCompression, times(2)).compress(fullBlock);
    verify(mockCompression).compress(halfBlock);

    ArgumentCaptor<Flux> argument = ArgumentCaptor.forClass(Flux.class);
    ArgumentCaptor<Flux> argument2 = ArgumentCaptor.forClass(Flux.class);
    verify(mockBlobAsyncClient).stageBlock(eq(blockIdEncoded(0)), argument.capture(), eq((long) fullBlockCompressedByte.length));
    verify(mockBlobAsyncClient).stageBlock(eq(blockIdEncoded(1)), argument.capture(), eq((long) fullBlockCompressedByte.length));
    verify(mockBlobAsyncClient).stageBlock(eq(blockIdEncoded(2)), argument2.capture(), eq((long) halfBlockCompressedByte.length));
    argument.getAllValues().forEach(flux -> {
        Assert.assertEquals(ByteBuffer.wrap(fullBlockCompressedByte), flux.blockFirst());
      });
    Assert.assertEquals(ByteBuffer.wrap(halfBlockCompressedByte), ((Flux) argument2.getValue()).blockFirst());
    verify(mockMetrics, times(3)).updateAzureUploadMetrics();
  }


  @Test
  public void testWriteThresholdCrossed() throws Exception {
    azureBlobOutputStream.write(BYTES, 0, THRESHOLD / 2);
    azureBlobOutputStream.write(BYTES, THRESHOLD / 2, THRESHOLD / 2);
    // azureBlobOutputStream.close waits on the CompletableFuture which does the actual stageBlock in uploadBlockAsync
    azureBlobOutputStream.close();

    verify(mockCompression).compress(BYTES);
    ArgumentCaptor<Flux> argument = ArgumentCaptor.forClass(Flux.class);
    verify(mockBlobAsyncClient).stageBlock(eq(blockIdEncoded(0)), argument.capture(), eq((long) COMPRESSED_BYTES.length)); // since size of byte[] written is less than threshold
    Assert.assertEquals(ByteBuffer.wrap(COMPRESSED_BYTES), ((Flux) argument.getValue()).blockFirst());
    verify(mockMetrics, times(2)).updateWriteByteMetrics(THRESHOLD / 2);
    verify(mockMetrics, times(1)).updateAzureUploadMetrics();
  }

  @Test(expected = RuntimeException.class)
  public void testWriteFailed() {
    when(mockBlobAsyncClient.stageBlock(anyString(), any(), anyLong())).thenThrow(new Exception("Test Failed"));

    byte[] b = new byte[100];
    azureBlobOutputStream.write(b, 0, THRESHOLD); // threshold crossed so stageBlock is scheduled.
    // azureBlobOutputStream.close waits on the CompletableFuture which does the actual stageBlock in uploadBlockAsync
    azureBlobOutputStream.close();
  }

  @Test
  public void testClose() {
    azureBlobOutputStream.write(BYTES, 0, THRESHOLD);
    int blockNum = 0;
    String blockId = String.format("%05d", blockNum);
    String blockIdEncoded = Base64.getEncoder().encodeToString(blockId.getBytes());

    PowerMockito.doAnswer(invocation -> {
        ArrayList<String> blockListArg = (ArrayList<String>) invocation.getArguments()[0];
        String blockIdArg = (String) blockListArg.toArray()[0];
        Assert.assertEquals(blockIdEncoded, blockIdArg);
        Map<String, String> blobMetadata = (Map<String, String>) invocation.getArguments()[2];
        Assert.assertEquals(blobMetadata.get(AzureBlobOutputStream.BLOB_RAW_SIZE_BYTES_METADATA), Long.toString(THRESHOLD));
        return Mono.just(new SimpleResponse(null, 200, null, null));
      }).when(mockBlobAsyncClient).commitBlockListWithResponse(anyList(), any(), any(), any(), any());

    azureBlobOutputStream.close();
    verify(mockMetrics).updateAzureCommitMetrics();
  }

  @Test
  public void testCloseMultipleBlocks() {
    azureBlobOutputStream.write(BYTES, 0, THRESHOLD);
    azureBlobOutputStream.write(BYTES, 0, THRESHOLD);

    int blockNum = 0;
    String blockId = String.format("%05d", blockNum);
    String blockIdEncoded = Base64.getEncoder().encodeToString(blockId.getBytes());

    int blockNum1 = 1;
    String blockId1 = String.format("%05d", blockNum1);
    String blockIdEncoded1 = Base64.getEncoder().encodeToString(blockId1.getBytes());

    PowerMockito.doAnswer(invocation -> {
        ArrayList<String> blockListArg = (ArrayList<String>) invocation.getArguments()[0];
        Assert.assertEquals(Arrays.asList(blockIdEncoded, blockIdEncoded1), blockListArg);
        Map<String, String> blobMetadata = (Map<String, String>) invocation.getArguments()[2];
        Assert.assertEquals(blobMetadata.get(AzureBlobOutputStream.BLOB_RAW_SIZE_BYTES_METADATA), Long.toString(2 * THRESHOLD));
        return Mono.just(new SimpleResponse(null, 200, null, null));
      }).when(mockBlobAsyncClient).commitBlockListWithResponse(anyList(), any(), any(), any(), any());

    azureBlobOutputStream.close();
    verify(mockMetrics).updateAzureCommitMetrics();
  }

  @Test(expected = RuntimeException.class)
  public void testCloseFailed() {
    when(mockBlobAsyncClient.commitBlockListWithResponse(anyList(), any(), any(), any(), any()))
        .thenReturn(Mono.error(new Exception("Test Failed")));

    byte[] b = new byte[100];
    azureBlobOutputStream.write(b, 0, THRESHOLD);
    azureBlobOutputStream.close();
  }

  @Test
  public void testMultipleClose() {
    azureBlobOutputStream.write(BYTES, 0, THRESHOLD);
    azureBlobOutputStream.close();
    azureBlobOutputStream.close();
  }

  @Test
  public void testFlush() throws Exception {
    azureBlobOutputStream.write(BYTES);
    azureBlobOutputStream.flush();
    // azureBlobOutputStream.close waits on the CompletableFuture which does the actual stageBlock in uploadBlockAsync
    azureBlobOutputStream.close();

    int blockNum = 0; // as there is only one block and its id will be 0
    String blockId = String.format("%05d", blockNum);
    String blockIdEncoded = Base64.getEncoder().encodeToString(blockId.getBytes());

    verify(mockCompression).compress(BYTES);
    ArgumentCaptor<Flux> argument = ArgumentCaptor.forClass(Flux.class);
    verify(mockBlobAsyncClient).stageBlock(eq(blockIdEncoded), argument.capture(), eq((long) COMPRESSED_BYTES.length)); // since size of byte[] written is less than threshold
    Assert.assertEquals(ByteBuffer.wrap(COMPRESSED_BYTES), ((Flux) argument.getValue()).blockFirst());
    verify(mockMetrics).updateAzureUploadMetrics();
  }

  @Test (expected = RuntimeException.class)
  public void testFlushFailed() throws IOException {
    azureBlobOutputStream.write(BYTES);
    when(mockBlobAsyncClient.stageBlock(anyString(), any(), anyLong()))
           .thenReturn(Mono.error(new Exception("Test Failed")));

    azureBlobOutputStream.flush();
    // azureBlobOutputStream.close waits on the CompletableFuture which does the actual stageBlock in uploadBlockAsync
    azureBlobOutputStream.close();
    verify(mockMetrics).updateAzureUploadMetrics();
  }


  @Test
  public void testReleaseBuffer() throws Exception {
    azureBlobOutputStream.releaseBuffer();
    verify(mockByteArrayOutputStream).close();
  }

  @Test(expected = IllegalStateException.class)
  public void testWriteAfterReleaseBuffer() throws Exception {
    azureBlobOutputStream.releaseBuffer();
    azureBlobOutputStream.write(new byte[10], 0, 10);
  }

  @Test
  public void testCloseAfterReleaseBuffer() throws Exception {
    azureBlobOutputStream.write(BYTES, 0, 100);
    azureBlobOutputStream.releaseBuffer();
    azureBlobOutputStream.close();
    // mockByteArrayOutputStream.close called only once during releaseBuffer and not during azureBlobOutputStream.close
    verify(mockByteArrayOutputStream).close();
    // azureBlobOutputStream.close still commits the list of blocks.
    verify(mockBlobAsyncClient).commitBlockListWithResponse(any(), any(), any(), any(), any());
  }

  @Test
  public void testFlushAfterReleaseBuffer() throws Exception {
    azureBlobOutputStream.releaseBuffer();
    azureBlobOutputStream.flush(); // becomes no-op after release buffer
    verify(mockBlobAsyncClient, never()).stageBlock(anyString(), any(), anyLong());
  }

  @Test
  public void testGetSize() throws Exception {
    Assert.assertEquals(0, azureBlobOutputStream.getSize());
    azureBlobOutputStream.write(BYTES, 0, BYTES.length);
    Assert.assertEquals(BYTES.length, azureBlobOutputStream.getSize());
  }

  @Test
  public void testGetSizeAfterFlush() throws Exception {
    azureBlobOutputStream.write(BYTES, 0, BYTES.length);
    Assert.assertEquals(BYTES.length, azureBlobOutputStream.getSize());
    azureBlobOutputStream.flush();
    Assert.assertEquals(BYTES.length, azureBlobOutputStream.getSize());
    azureBlobOutputStream.write(BYTES, 0, BYTES.length - 10);
    Assert.assertEquals(BYTES.length + BYTES.length - 10, azureBlobOutputStream.getSize());
  }

  private String blockIdEncoded(int blockNum) {
    String blockId = String.format("%05d", blockNum);
    return Base64.getEncoder().encodeToString(blockId.getBytes());
  }
}
