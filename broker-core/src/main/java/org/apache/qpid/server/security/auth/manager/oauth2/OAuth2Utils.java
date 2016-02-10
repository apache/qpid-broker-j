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
package org.apache.qpid.server.security.auth.manager.oauth2;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class OAuth2Utils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2Utils.class);

    public static String buildRequestQuery(final Map<String, String> requestBodyParameters)
    {
        try
        {
            final String charset = StandardCharsets.UTF_8.name();
            StringBuilder bodyBuilder = new StringBuilder();
            Iterator<Map.Entry<String, String>> iterator = requestBodyParameters.entrySet().iterator();
            while (iterator.hasNext())
            {
                Map.Entry<String, String> entry = iterator.next();
                bodyBuilder.append(URLEncoder.encode(entry.getKey(), charset));
                bodyBuilder.append("=");
                bodyBuilder.append(URLEncoder.encode(entry.getValue(), charset));
                if (iterator.hasNext())
                {
                    bodyBuilder.append("&");
                }
            }
            return bodyBuilder.toString();
        }
        catch (UnsupportedEncodingException e)
        {
            throw new ServerScopedRuntimeException("Failed to encode as UTF-8", e);
        }
    }

    public static InputStream getResponseStream(final HttpURLConnection connection) throws IOException
    {
        try
        {
            return connection.getInputStream();
        }
        catch (IOException ioe)
        {
            InputStream errorStream = connection.getErrorStream();
            if (errorStream != null)
            {
                return errorStream;
            }
            throw ioe;
        }
    }
}
