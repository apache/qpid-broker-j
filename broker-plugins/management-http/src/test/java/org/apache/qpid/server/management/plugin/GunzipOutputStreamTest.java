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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Base64;

import org.junit.Test;

import org.apache.qpid.server.util.GZIPUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class GunzipOutputStreamTest extends UnitTestBase
{
    // base64 encoded content of 'gzip -N test.txt' containing text: This is test
    private static final String GZIP_CONTENT_WITH_EMBEDDED_FILE_NAME =
            "H4sICIAM4FcAA3Rlc3QudHh0AAvJyCxWAKKS1OISANCadxgMAAAA";

    // base64 encoded content of 'gzip -c -N test.txt > test.txt.gz ; gzip -c -N test1.txt >> test.txt.gz'
    // containing texts "This is test" and "Another test text" accordingly
    private static final String GZIP_CONTENT_WITH_MULTIPLE_MEMBERS =
            "H4sICNoV4VcAA3Rlc3QudHh0AAvJyCxWAKKS1OISANCadxgMAAAA"
            + "H4sICOQV4VcAA3Rlc3QxLnR4dABzzMsvyUgtUihJLS4BEhUlAHeK0kERAAAA";
    private static final String TEST_TEXT = "This is test";
    private static final String TEST_TEXT2 = "Another test text";

    @Test
    public void testDecompressing() throws Exception
    {
        final byte[] originalUncompressedInput = generateTestBytes();
        final byte[] compressedBytes = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(originalUncompressedInput));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GunzipOutputStream guos = new GunzipOutputStream(outputStream);
        guos.write(compressedBytes);
        guos.close();

        assertArrayEquals("Unexpected content", originalUncompressedInput, outputStream.toByteArray());
    }

    @Test
    public void testDecompressingWithEmbeddedFileName() throws Exception
    {
        byte[] data = Base64.getDecoder().decode(GZIP_CONTENT_WITH_EMBEDDED_FILE_NAME);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GunzipOutputStream guos = new GunzipOutputStream(outputStream);
        guos.write(data);
        guos.close();

        assertEquals("Unexpected content", TEST_TEXT, new String(outputStream.toByteArray()));
    }

    @Test
    public void testDecompressingMultipleMembers() throws Exception
    {
        byte[] data = Base64.getDecoder().decode(GZIP_CONTENT_WITH_MULTIPLE_MEMBERS);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GunzipOutputStream guos = new GunzipOutputStream(outputStream);
        for (int i = 0; i < data.length; i++)
        {
            guos.write(data[i]);
        }
        guos.close();

        StringBuilder expected = new StringBuilder(TEST_TEXT);
        expected.append(TEST_TEXT2);
        assertEquals("Unexpected content", expected.toString(), new String(outputStream.toByteArray()));
    }

    private byte[] generateTestBytes()
    {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (sb.length() < 5000)
        {
            if (i % 2 == 0)
            {
                sb.append(TEST_TEXT);
            }
            else
            {
                sb.append(TEST_TEXT2);
            }
            sb.append(" ").append(i++);
        }
        return sb.toString().getBytes();
    }
}
