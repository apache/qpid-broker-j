/*
 *
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
 *
 */
package org.apache.qpid.server.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.ContextProvider;
import org.apache.qpid.server.util.GZIPUtils.GZIPInflationLimitException;
import org.apache.qpid.test.utils.UnitTestBase;

public class GZIPUtilsTest extends UnitTestBase
{
    @Test
    public void testCompressUncompress() throws Exception
    {
        final byte[] data = new byte[1024];
        Arrays.fill(data, (byte)'a');
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(data));
        assertTrue(compressed.length < data.length, "Compression didn't compress");
        final byte[] uncompressed = GZIPUtils.uncompressBufferToArray(ByteBuffer.wrap(compressed), data.length);
        assertArrayEquals(data, uncompressed, "Compression not reversible");
    }

    @Test
    public void testUncompressNonZipReturnsNull() throws Exception
    {
        final byte[] data = new byte[1024];
        Arrays.fill(data, (byte)'a');
        assertNull(GZIPUtils.uncompressBufferToArray(ByteBuffer.wrap(data), data.length),
                   "Non zipped data should not uncompress");
    }

    @Test
    public void testUncompressStreamWithErrorReturnsNull() throws Exception
    {
        final InputStream is = new InputStream()
        {
            @Override
            public int read() throws IOException
            {
                throw new IOException();
            }
        };
        assertNull(GZIPUtils.uncompressStreamToArray(is, 1024), "Stream error should return null");
    }

    @Test
    public void testUncompressNullStreamReturnsNull() throws Exception
    {
        assertNull(GZIPUtils.uncompressStreamToArray(null, 1024), "Null Stream should return null");
    }

    @Test
    public void testUncompressNullBufferReturnsNull() throws Exception
    {
        assertNull(GZIPUtils.uncompressBufferToArray(null, 1024), "Null buffer should return null");
    }

    @Test
    public void testCompressNullArrayReturnsNull()
    {
        assertNull(GZIPUtils.compressBufferToArray(null));
    }

    @Test
    public void testNonHeapBuffers() throws Exception
    {
        final byte[] data = new byte[1024];
        Arrays.fill(data, (byte)'a');
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(1024);
        directBuffer.put(data);
        directBuffer.flip();

        final byte[] compressed = GZIPUtils.compressBufferToArray(directBuffer);

        assertTrue(compressed.length < data.length, "Compression didn't compress");

        directBuffer.clear();
        directBuffer.position(1);
        directBuffer = directBuffer.slice();
        directBuffer.put(compressed);
        directBuffer.flip();

        final byte[] uncompressed = GZIPUtils.uncompressBufferToArray(directBuffer, data.length);

        assertArrayEquals(data, uncompressed, "Compression not reversible");
    }

    @Test
    public void testUncompressRejectsOutputLargerThanLimit()
    {
        final byte[] data = new byte[8192];
        Arrays.fill(data, (byte) 'a');
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(data));

        assertThrows(GZIPInflationLimitException.class, () ->
                GZIPUtils.uncompressBufferToArray(ByteBuffer.wrap(compressed), data.length - 1));
    }

    @Test
    public void testUncompressAcceptsOutputEqualToLimit() throws Exception
    {
        final byte[] data = new byte[8192];
        Arrays.fill(data, (byte) 'a');
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(data));

        assertArrayEquals(data, GZIPUtils.uncompressBufferToArray(ByteBuffer.wrap(compressed), data.length));
    }

    @Test
    public void testLegacyUncompressOverloads()
    {
        final byte[] data = new byte[8192];
        Arrays.fill(data, (byte) 'a');
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(data));

        assertArrayEquals(data, GZIPUtils.uncompressBufferToArray(ByteBuffer.wrap(compressed)));
        assertArrayEquals(data, GZIPUtils.uncompressStreamToArray(new ByteArrayInputStream(compressed)));
    }

    @Test
    public void testUncompressRejectsNegativeLimit()
    {
        assertThrows(IllegalArgumentException.class, () -> GZIPUtils.uncompressStreamToArray(null, -1));
    }

    @Test
    public void testValidateDecompressedSizeRejectsContentExceedingLimit()
    {
        final byte[] data = new byte[8192];
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(data));

        assertThrows(GZIPInflationLimitException.class, () ->
                GZIPUtils.validateDecompressedSize(new ByteArrayInputStream(compressed), 1024));
    }

    @Test
    public void testMaximumMessageDecompressionSizeUsesSmallestApplicableLimit()
    {
        final ContextProvider contextProvider = mock(ContextProvider.class);
        when(contextProvider.getContextValue(Integer.class, Connection.MAX_MESSAGE_DECOMPRESSION_SIZE))
                .thenReturn(4096);
        when(contextProvider.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE)).thenReturn(2048);

        assertEquals(1024, GZIPUtils.getMaximumMessageDecompressionSize(contextProvider, 1024));
    }

    @Test
    public void testMaximumMessageDecompressionSizeUsesDefaultWithoutContextProvider()
    {
        assertEquals(Connection.DEFAULT_MAX_MESSAGE_DECOMPRESSION_SIZE,
                GZIPUtils.getMaximumMessageDecompressionSize((ContextProvider) null));
    }
}
