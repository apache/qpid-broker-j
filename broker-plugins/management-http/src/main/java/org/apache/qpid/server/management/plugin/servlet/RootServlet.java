/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.server.management.plugin.servlet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.util.StringUtil;

public class RootServlet extends HttpServlet
{
    private static final long serialVersionUID = 1L;

    private final String _expectedPath;
    private final String _apiDocsPath;
    private final String _filename;


    public RootServlet(final String expectedPath, final String apiDocsPath, final String filename)
    {
        _expectedPath = expectedPath;
        _apiDocsPath = apiDocsPath;
        _filename = filename;
    }

    @Override
    public void init() throws ServletException
    {
        // currently no initialization logic needed
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException
    {
        final String path = request.getServletPath();
        if (_expectedPath == null || _expectedPath.equals(path == null ? "" : path))
        {
            try (final OutputStream output = HttpManagementUtil.getOutputStream(request, response);
                 final InputStream fileInput = getClass().getResourceAsStream("/resources/" + _filename))
            {
                if (fileInput == null)
                {
                    final String fileName = StringUtil.escapeHtml4(_filename);
                    response.sendError(HttpServletResponse.SC_NOT_FOUND, "unknown file: " + fileName);
                    return;
                }

                final byte[] buffer = new byte[1024];
                response.setStatus(HttpServletResponse.SC_OK);
                int read;

                while ((read = fileInput.read(buffer)) > 0)
                {
                    output.write(buffer, 0, read);
                }
            }
        }
        else
        {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            try (final OutputStream output = HttpManagementUtil.getOutputStream(request, response))
            {
                final String servletPath = StringUtil.escapeHtml4(request.getServletPath());
                final String notFoundMessage = "Unknown path \"" + servletPath +
                        "\". Please read the api docs at " + request.getScheme() + "://" + request.getServerName() +
                        ":" + request.getServerPort() + "/" + _apiDocsPath + "\n";
                output.write(notFoundMessage.getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}
