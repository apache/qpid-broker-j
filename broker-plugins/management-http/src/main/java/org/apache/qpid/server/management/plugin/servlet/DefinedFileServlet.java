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

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.HttpManagementUtil;

public class DefinedFileServlet extends HttpServlet
{
    private static final long serialVersionUID = 1L;

    private static final String FILENAME_INIT_PARAMETER = "filename";
    private final String _expectedPath;
    private final String _apiDocsPath;

    private String _filename;

    public DefinedFileServlet()
    {
        this(null);
    }

    public DefinedFileServlet(String filename)
    {
        this(filename, null, null);
    }


    public DefinedFileServlet(String filename, String expectedPath, String apiDocsPath)
    {
        _filename = filename;
        _expectedPath = expectedPath;
        _apiDocsPath = apiDocsPath;
    }

    @Override
    public void init() throws ServletException
    {
        ServletConfig config = getServletConfig();
        String fileName = config.getInitParameter(FILENAME_INIT_PARAMETER);
        if (fileName != null && !"".equals(fileName))
        {
            _filename = fileName;
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        final String path = request.getServletPath();
        if(_expectedPath == null || _expectedPath.equals(path == null ? "" : path))
        {
            try (OutputStream output = HttpManagementUtil.getOutputStream(request, response))
            {
                try (InputStream fileInput = getClass().getResourceAsStream("/resources/" + _filename))
                {
                    if (fileInput != null)
                    {
                        byte[] buffer = new byte[1024];
                        response.setStatus(HttpServletResponse.SC_OK);
                        int read = 0;

                        while ((read = fileInput.read(buffer)) > 0)
                        {
                            output.write(buffer, 0, read);
                        }
                    }
                    else
                    {
                        response.sendError(HttpServletResponse.SC_NOT_FOUND, "unknown file: " + _filename);
                    }
                }
            }
        }
        else
        {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            try (OutputStream output = HttpManagementUtil.getOutputStream(request, response))
            {
                final String notFoundMessage = "Unknown path '"
                                 + request.getServletPath()
                                 + "'. Please read the api docs at "
                                 + request.getScheme()
                                 + "://" + request.getServerName() + ":" + request.getServerPort() + _apiDocsPath + "\n";
                output.write(notFoundMessage.getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}
