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
package org.apache.qpid.tests.http.compression;

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import org.junit.Test;

import org.apache.qpid.tests.http.HttpTestBase;

public class CompressedResponsesTest extends HttpTestBase
{

    @Test
    public void compressionOffAcceptOff() throws Exception
    {
        doCompressionTest(false, false);
    }

    @Test
    public void compressionOffAcceptOn() throws Exception
    {
        doCompressionTest(false, true);
    }

    @Test
    public void compressionOnAcceptOff() throws Exception
    {
        doCompressionTest(true, false);
    }

    @Test
    public void compressionOnAcceptOn() throws Exception
    {
        doCompressionTest(true, true);

    }

    private void doCompressionTest(final boolean allowCompression,
                                   final boolean acceptCompressed) throws Exception
    {
        final boolean expectCompression = allowCompression && acceptCompressed;

        getHelper().submitRequest("plugin/httpManagement", "POST", Collections.singletonMap("compressResponses", expectCompression), SC_OK);


        HttpURLConnection conn = getHelper().openManagementConnection("/service/metadata", "GET");
        try
        {
            if (acceptCompressed)
            {
                conn.setRequestProperty("Accept-Encoding", "gzip");
            }

            conn.connect();

            String contentEncoding = conn.getHeaderField("Content-Encoding");

            if (expectCompression)
            {
                assertEquals("gzip", contentEncoding);
            }
            else
            {
                if (contentEncoding != null)
                {
                    assertEquals("identity", contentEncoding);
                }
            }

            byte[] bytes;
            try(ByteArrayOutputStream contentBuffer = new ByteArrayOutputStream())
            {
                ByteStreams.copy(conn.getInputStream(), contentBuffer);
                bytes = contentBuffer.toByteArray();
            }
            try (InputStream jsonStream = expectCompression
                    ? new GZIPInputStream(new ByteArrayInputStream(bytes))
                    : new ByteArrayInputStream(bytes))
            {
                ObjectMapper mapper = new ObjectMapper();
                try
                {
                    mapper.readValue(jsonStream, LinkedHashMap.class);
                }
                catch (JsonParseException | JsonMappingException e)
                {
                    fail("Message was not in correct format");
                }
            }
        }
        finally
        {
            conn.disconnect();
        }
    }


}
