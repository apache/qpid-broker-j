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
 *
 */

package org.apache.qpid.server.management.plugin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.apache.qpid.test.utils.QpidTestCase;


public class GZIPOutputStreamAdapterTest extends QpidTestCase
{

    public void testDecompressing() throws IOException
    {
        String testText = generateTestText();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStreamAdapter adapter = new GZIPOutputStreamAdapter(outputStream, 128, -1);

        compressAndDecompressWithAdapter(testText, adapter);

        assertEquals("Unexpected content", testText, new String(outputStream.toByteArray()));
    }

    public void testDecompressingLimited() throws IOException
    {
        String testText = generateTestText();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStreamAdapter adapter = new GZIPOutputStreamAdapter(outputStream, 128, testText.length() / 2);

        compressAndDecompressWithAdapter(testText, adapter);

        assertEquals("Unexpected content",
                     testText.substring(0, testText.length() / 2),
                     new String(outputStream.toByteArray()));
    }

    private void compressAndDecompressWithAdapter(final String testText, final GZIPOutputStreamAdapter adapter) throws IOException
    {
        byte[] data = compress(testText);
        byte[] buffer = new byte[256];
        int remaining = data.length;
        int written = 0;
        while (remaining > 0)
        {
            int length = Math.min(remaining, buffer.length);
            System.arraycopy(data, written, buffer, 0, length);
            adapter.write(buffer);
            written += length;
            remaining -= length;
        }
    }

    private byte[] compress(final String testText) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream zipStream = new GZIPOutputStream(baos))
        {
            zipStream.write(testText.getBytes());
        }
        return baos.toByteArray();
    }

    private String generateTestText()
    {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (sb.length() < 5000)
        {
            sb.append(" A simple test text ").append(i++);
        }
        return sb.toString();
    }
}