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

import static org.apache.qpid.server.management.plugin.HttpManagementUtil.ATTR_MANAGEMENT_CONFIGURATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.io.output.WriterOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;

class RootServletTest
{
    private final HttpManagementConfiguration<?> httpManagementConfiguration = mock(HttpManagementConfiguration.class);
    private final ServletContext servletContext = mock(ServletContext.class);
    private final HttpServletRequest request = mock(HttpServletRequest.class);
    private final HttpServletResponse response = mock(HttpServletResponse.class);

    private final RootServlet _rootServlet = new RootServlet("expectedPath", "apiDocsPath", "file.txt");

    @BeforeEach
    void beforeEach()
    {
        when(servletContext.getAttribute(ATTR_MANAGEMENT_CONFIGURATION)).thenReturn(httpManagementConfiguration);
        when(request.getScheme()).thenReturn("http");
        when(request.getServerName()).thenReturn("localhost");
        when(request.getServerPort()).thenReturn(8080);
        when(request.getServletContext()).thenReturn(servletContext);
    }

    @Test
    void expectedPathUnknownFile() throws Exception
    {
        when(request.getServletPath()).thenReturn("expectedPath");

        try (final StringWriter stringWriter = new StringWriter();
             final OutputStream outputStream = new WriterOutputStream(stringWriter, StandardCharsets.UTF_8))
        {
            final ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
            doAnswer(invocationOnMock ->
            {
                outputStream.write(invocationOnMock.getArgument(0));
                return null;
            }).when(servletOutputStream).write(any(byte[].class));
            when(response.getOutputStream()).thenReturn(servletOutputStream);

            doAnswer(invocationOnMock ->
            {
                final String arg = invocationOnMock.getArgument(1);
                outputStream.write(arg.getBytes(StandardCharsets.UTF_8));
                return null;
            }).when(response).sendError(any(int.class), any(String.class));

            new RootServlet("expectedPath", "apiDocsPath", "unknown-file.txt").doGet(request, response);

            outputStream.flush();
            assertEquals("unknown file: unknown-file.txt", stringWriter.toString());
        }
    }

    @Test
    void expectedPathKnownFile() throws Exception
    {
        when(request.getServletPath()).thenReturn("expectedPath");

        try (final StringWriter stringWriter = new StringWriter();
             final OutputStream outputStream = new WriterOutputStream(stringWriter, StandardCharsets.UTF_8))
        {
            final ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
            doAnswer(invocationOnMock ->
            {
                outputStream.write(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1), invocationOnMock.getArgument(2));
                return null;
            }).when(servletOutputStream).write(any(byte[].class), any(int.class), any(int.class));
            when(response.getOutputStream()).thenReturn(servletOutputStream);

            _rootServlet.doGet(request, response);

            outputStream.flush();
            assertTrue(stringWriter.toString().contains("result"));
        }
    }

    @Test
    void unknownPath () throws Exception
    {
        when(request.getServletPath()).thenReturn("unknown");

        try (final StringWriter stringWriter = new StringWriter();
             final OutputStream outputStream = new WriterOutputStream(stringWriter, StandardCharsets.UTF_8))
        {
            final ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
            doAnswer(invocationOnMock ->
            {
                outputStream.write(invocationOnMock.getArgument(0));
                return null;
            }).when(servletOutputStream).write(any(byte[].class));
            when(response.getOutputStream()).thenReturn(servletOutputStream);

            _rootServlet.doGet(request, response);

            outputStream.flush();
            assertEquals("Unknown path \"unknown\". Please read the api docs at "  +
                    "http://localhost:8080/apiDocsPath\n", stringWriter.toString());
        }
    }

    @Test
    void escapedUnknownPath () throws Exception
    {
        when(request.getServletPath()).thenReturn("<unknown> & \"test\" 'test' ");

        try (final StringWriter stringWriter = new StringWriter();
             final OutputStream outputStream = new WriterOutputStream(stringWriter, StandardCharsets.UTF_8))
        {
            final ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
            doAnswer(invocationOnMock ->
            {
                outputStream.write(invocationOnMock.getArgument(0));
                return null;
            }).when(servletOutputStream).write(any(byte[].class));
            when(response.getOutputStream()).thenReturn(servletOutputStream);

            _rootServlet.doGet(request, response);

            outputStream.flush();
            assertEquals("Unknown path \"&lt;unknown&gt; &amp; &quot;test&quot; &#x27;test&#x27; \". " +
                    "Please read the api docs at http://localhost:8080/apiDocsPath\n", stringWriter.toString());
        }
    }

    @Test
    void escapedUnknownFile () throws Exception
    {
        when(request.getServletPath()).thenReturn("expectedPath");

        try (final StringWriter stringWriter = new StringWriter();
             final OutputStream outputStream = new WriterOutputStream(stringWriter, StandardCharsets.UTF_8))
        {
            final ServletOutputStream servletOutputStream = mock(ServletOutputStream.class);
            doAnswer(invocationOnMock ->
            {
                outputStream.write(invocationOnMock.getArgument(0));
                return null;
            }).when(servletOutputStream).write(any(byte[].class));
            when(response.getOutputStream()).thenReturn(servletOutputStream);

            doAnswer(invocationOnMock ->
            {
                final String arg = invocationOnMock.getArgument(1);
                outputStream.write(arg.getBytes(StandardCharsets.UTF_8));
                return null;
            }).when(response).sendError(any(int.class), any(String.class));

            new RootServlet("expectedPath", "apiDocsPath", "<unknown> & \"test\" 'test'.txt").doGet(request, response);

            outputStream.flush();
            assertEquals("unknown file: &lt;unknown&gt; &amp; &quot;test&quot; &#x27;test&#x27;.txt", stringWriter.toString());
        }
    }
}
