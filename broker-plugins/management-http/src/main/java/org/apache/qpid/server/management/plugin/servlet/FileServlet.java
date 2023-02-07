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
package org.apache.qpid.server.management.plugin.servlet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.HttpManagementUtil;

public class FileServlet extends HttpServlet
{
    private static final long serialVersionUID = 1L;

    private static final String RESOURCES_PREFIX = "/resources";
    private static final Map<String, String> CONTENT_TYPES = Map.of("js", "application/javascript",
            "html", "text/html",
            "css",  "text/css",
            "json", "application/json",
            "jpg",  "image/jpg",
            "png",  "image/png",
            "gif",  "image/gif",
            "svg",  "image/svg+xml");

    private final String _resourcePathPrefix;
    private final boolean _usePathInfo;

    public FileServlet()
    {
        this(RESOURCES_PREFIX, false);
    }

    public FileServlet(final String resourcePathPrefix, final boolean usePathInfo)
    {
        _resourcePathPrefix = resourcePathPrefix;
        _usePathInfo = usePathInfo;
    }

    @Override
    protected void doGet(final HttpServletRequest request,
                         final HttpServletResponse response) throws ServletException, IOException
    {
        String filename;
        if (_usePathInfo)
        {
            filename = request.getPathInfo();
        }
        else
        {
            filename = request.getServletPath();
        }

        if (filename.contains("."))
        {
            final String suffix = filename.substring(filename.lastIndexOf('.')+1);
            final String contentType = CONTENT_TYPES.get(suffix);
            if (contentType != null)
            {
                response.setContentType(contentType);
            }
        }

        final URL resourceURL = getClass().getResource(_resourcePathPrefix + filename);
        if (resourceURL != null && !filename.contains(".."))
        {
            response.setStatus(HttpServletResponse.SC_OK);
            try (final InputStream fileInput = resourceURL.openStream();
                 final OutputStream output = HttpManagementUtil.getOutputStream(request, response))
            {
                byte[] buffer = new byte[1024];
                int read;
                while ((read = fileInput.read(buffer)) != -1)
                {
                    output.write(buffer, 0, read);
                }
            }
        }
        else
        {
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "unknown file");
        }
    }
}
