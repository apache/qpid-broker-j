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
package org.apache.qpid.server.management.plugin.servlet.rest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerTestHelper;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.CustomRestHeaders;
import org.apache.qpid.server.model.DecompressionLimitedContent;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.RestContentHeader;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.util.GZIPUtils;
import org.apache.qpid.server.util.GZIPUtils.GZIPInflationLimitException;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class AbstractServletTest extends UnitTestBase
{
    private static final int MAXIMUM_DECOMPRESSED_SIZE = 1024;

    private TestServlet _servlet;
    private HttpServletRequest _request;
    private HttpServletResponse _response;
    private TrackingServletOutputStream _outputStream;
    private HttpManagementConfiguration<?> _managementConfiguration;
    private HttpPort<?> _port;
    private Queue<?> _queue;
    private Content _queueContent;

    @BeforeEach
    public void setUp() throws Exception
    {
        _servlet = new TestServlet();
        _request = mock(HttpServletRequest.class);
        _response = mock(HttpServletResponse.class);
        _outputStream = new TrackingServletOutputStream();

        final ServletConfig servletConfig = mock(ServletConfig.class);
        final ServletContext servletContext = mock(ServletContext.class);
        final Broker<?> broker = mock(Broker.class);
        _managementConfiguration = mock(HttpManagementConfiguration.class);
        _port = mock(HttpPort.class);

        when(servletConfig.getServletContext()).thenReturn(servletContext);
        when(servletContext.getAttribute(HttpManagementUtil.ATTR_BROKER)).thenReturn(broker);
        when(servletContext.getAttribute(HttpManagementUtil.ATTR_MANAGEMENT_CONFIGURATION))
                .thenReturn(_managementConfiguration);
        when(_managementConfiguration.getAllowedResponseHeaders()).thenReturn(Set.of());
        when(_managementConfiguration.isCompressResponses()).thenReturn(false);
        when(_managementConfiguration.getContextValue(Integer.class, Connection.MAX_MESSAGE_DECOMPRESSION_SIZE))
                .thenReturn(MAXIMUM_DECOMPRESSED_SIZE);
        when(_managementConfiguration.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE))
                .thenReturn(MAXIMUM_DECOMPRESSED_SIZE);
        when(_port.getContextValue(Integer.class, Connection.MAX_MESSAGE_DECOMPRESSION_SIZE))
                .thenReturn(MAXIMUM_DECOMPRESSED_SIZE);
        when(_port.getContextValue(Integer.class, Connection.MAX_MESSAGE_SIZE))
                .thenReturn(MAXIMUM_DECOMPRESSED_SIZE);
        when(_request.getAttribute(anyString())).thenReturn(_port);
        when(_response.getOutputStream()).thenReturn(_outputStream);

        _servlet.init(servletConfig);
    }

    @AfterEach
    public void tearDown()
    {
        try
        {
            if (_queueContent != null)
            {
                _queueContent.release();
            }
        }
        finally
        {
            if (_queue != null)
            {
                _queue.close();
            }
        }
    }

    @Test
    public void testCommittedOverLimitResponseIsAborted() throws Exception
    {
        when(_response.isCommitted()).thenReturn(true);

        assertThrows(GZIPInflationLimitException.class, () ->
                _servlet.write(createOverLimitContent(), _request, _response));

        verify(_response, never()).reset();
        verify(_response, never()).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        assertFalse(_outputStream.isClosed());
    }

    @Test
    public void testUncommittedOverLimitResponseIsReplacedWithError() throws Exception
    {
        when(_response.isCommitted()).thenReturn(false);
        doAnswer(invocation ->
        {
            _outputStream.reset();
            return null;
        }).when(_response).reset();

        _servlet.write(createOverLimitContent(), _request, _response);

        verify(_response).reset();
        verify(_response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        assertTrue(_outputStream.toString(StandardCharsets.UTF_8).contains("errorMessage"));
    }

    @Test
    public void testCommittedWriteFailureDoesNotCloseResponse() throws Exception
    {
        when(_response.isCommitted()).thenReturn(true);

        assertThrows(IOException.class, () -> _servlet.write(new FailingContent(), _request, _response));

        verify(_response, never()).reset();
        assertFalse(_outputStream.isClosed());
    }

    @Test
    public void testContentPreparedWithMostRestrictiveHttpLimit() throws Exception
    {
        final int portLimit = MAXIMUM_DECOMPRESSED_SIZE / 2;
        when(_port.getContextValue(Integer.class, Connection.MAX_MESSAGE_DECOMPRESSION_SIZE))
                .thenReturn(portLimit);
        final DecompressionLimitedTestContent content = new DecompressionLimitedTestContent(false);
        when(_response.getOutputStream()).thenAnswer(invocation ->
        {
            assertEquals(portLimit, content.getPreparedMaximumMessageDecompressionSize());
            return _outputStream;
        });

        _servlet.write(content, _request, _response);

        assertTrue(content.isWritten());
    }

    @Test
    public void testGzipPassThroughContentPreparedWithMostRestrictiveHttpLimit() throws Exception
    {
        final int portLimit = MAXIMUM_DECOMPRESSED_SIZE / 2;
        when(_port.getContextValue(Integer.class, Connection.MAX_MESSAGE_DECOMPRESSION_SIZE))
                .thenReturn(portLimit);
        when(_managementConfiguration.isCompressResponses()).thenReturn(true);
        when(_request.getHeaderNames())
                .thenReturn(Collections.enumeration(Set.of(HttpManagementUtil.ACCEPT_ENCODING_HEADER)));
        when(_request.getHeader(HttpManagementUtil.ACCEPT_ENCODING_HEADER))
                .thenReturn(GZIPUtils.GZIP_CONTENT_ENCODING);
        final byte[] compressedContent = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(new byte[]{1}));
        final DecompressionLimitedTestContent content =
                new GzipDecompressionLimitedTestContent(compressedContent);
        when(_response.getOutputStream()).thenAnswer(invocation ->
        {
            assertEquals(portLimit, content.getPreparedMaximumMessageDecompressionSize());
            return _outputStream;
        });

        _servlet.write(content, _request, _response);

        assertTrue(content.isWritten());
        assertArrayEquals(compressedContent, _outputStream.toByteArray());
        verify(_response).setHeader(HttpManagementUtil.CONTENT_ENCODING_HEADER.toUpperCase(),
                GZIPUtils.GZIP_CONTENT_ENCODING);
    }

    @Test
    public void testContentPreparationFailureOccursBeforeResponseOutput() throws Exception
    {
        final DecompressionLimitedTestContent content = new DecompressionLimitedTestContent(true);
        doAnswer(invocation ->
        {
            _outputStream.reset();
            return null;
        }).when(_response).reset();

        _servlet.write(content, _request, _response);

        assertFalse(content.isWritten());
        assertTrue(_outputStream.toString(StandardCharsets.UTF_8).contains("errorMessage"));
        final InOrder responseOrder = inOrder(_response);
        responseOrder.verify(_response).reset();
        responseOrder.verify(_response).getOutputStream();
    }

    @ParameterizedTest
    @CsvSource({"false, false, false", "false, false, true", "false, true, false", "false, true, true",
                "true, false, false", "true, false, true", "true, true, false", "true, true, true"})
    public void testRawQueueContentUsesLocalDecompressionLimit(final boolean preview,
                                                               final boolean decompressBeforeLimiting,
                                                               final boolean inheritedLimit) throws Exception
    {
        final int localLimit = MAXIMUM_DECOMPRESSED_SIZE / 2;
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(new byte[localLimit + 1]));
        final long previewLimit = decompressBeforeLimiting ? MAXIMUM_DECOMPRESSED_SIZE : compressed.length;
        final Content content = createQueueContent(compressed, localLimit, preview ? previewLimit : -1L,
                decompressBeforeLimiting, inheritedLimit);
        when(_response.isCommitted()).thenReturn(true);

        assertThrows(GZIPInflationLimitException.class, () -> _servlet.write(content, _request, _response));

        verify(_response, never()).reset();
        verify(_response, never()).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        assertFalse(_outputStream.isClosed());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRawQueueContentUsesLowerHttpDecompressionLimit(final boolean portLimit) throws Exception
    {
        final int httpLimit = MAXIMUM_DECOMPRESSED_SIZE / 2;
        if (portLimit)
        {
            when(_port.getContextValue(Integer.class, Connection.MAX_MESSAGE_DECOMPRESSION_SIZE))
                    .thenReturn(httpLimit);
        }
        else
        {
            when(_managementConfiguration.getContextValue(Integer.class, Connection.MAX_MESSAGE_DECOMPRESSION_SIZE))
                    .thenReturn(httpLimit);
        }
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(new byte[httpLimit + 1]));
        final Content content = createQueueContent(compressed, MAXIMUM_DECOMPRESSED_SIZE, -1L, false, false);
        when(_response.isCommitted()).thenReturn(true);

        assertThrows(GZIPInflationLimitException.class, () -> _servlet.write(content, _request, _response));

        assertFalse(_outputStream.isClosed());
    }

    @ParameterizedTest
    @CsvSource({"511, false", "512, false", "511, true", "512, true"})
    public void testRawQueueContentWithinLocalDecompressionLimit(final int contentSize, final boolean preview)
            throws Exception
    {
        final byte[] uncompressed = new byte[contentSize];
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(uncompressed));
        final Content content = createQueueContent(compressed, MAXIMUM_DECOMPRESSED_SIZE / 2,
                preview ? MAXIMUM_DECOMPRESSED_SIZE : -1L, true, false);

        _servlet.write(content, _request, _response);

        assertArrayEquals(uncompressed, _outputStream.toByteArray());
        assertTrue(_outputStream.isClosed());
        verify(_response, never()).reset();
    }

    @Test
    public void testRawQueuePreviewStopsBeforeLocalDecompressionLimit() throws Exception
    {
        final int previewLimit = 128;
        final int localLimit = MAXIMUM_DECOMPRESSED_SIZE / 2;
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(new byte[localLimit + 1]));
        final Content content = createQueueContent(compressed, localLimit, previewLimit, true, false);

        _servlet.write(content, _request, _response);

        assertArrayEquals(new byte[previewLimit], _outputStream.toByteArray());
        assertTrue(_outputStream.isClosed());
        verify(_response, never()).reset();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRawQueueContentPassesThroughWithoutDecompression(final boolean decompressBeforeLimiting)
            throws Exception
    {
        final int localLimit = MAXIMUM_DECOMPRESSED_SIZE / 2;
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(new byte[localLimit + 1]));
        final Content content = createQueueContent(compressed, localLimit, -1L, decompressBeforeLimiting, false);
        when(_managementConfiguration.isCompressResponses()).thenReturn(true);
        when(_request.getHeaderNames())
                .thenReturn(Collections.enumeration(Set.of(HttpManagementUtil.ACCEPT_ENCODING_HEADER)));
        when(_request.getHeader(HttpManagementUtil.ACCEPT_ENCODING_HEADER))
                .thenReturn(GZIPUtils.GZIP_CONTENT_ENCODING);

        _servlet.write(content, _request, _response);

        assertArrayEquals(compressed, _outputStream.toByteArray());
        assertTrue(_outputStream.isClosed());
        verify(_response, never()).reset();
        verify(_response).setHeader(HttpManagementUtil.CONTENT_ENCODING_HEADER.toUpperCase(),
                GZIPUtils.GZIP_CONTENT_ENCODING);
    }

    @Test
    public void testUncommittedLocalDecompressionFailureIsReplacedWithError() throws Exception
    {
        final int localLimit = MAXIMUM_DECOMPRESSED_SIZE / 2;
        final byte[] compressed = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(new byte[localLimit + 1]));
        final Content content = createQueueContent(compressed, localLimit, -1L, false, false);
        doAnswer(invocation ->
        {
            _outputStream.reset();
            return null;
        }).when(_response).reset();

        _servlet.write(content, _request, _response);

        verify(_response).reset();
        verify(_response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        assertTrue(_outputStream.toString(StandardCharsets.UTF_8).contains("errorMessage"));
    }

    private Content createQueueContent(final byte[] compressed,
                                       final int localLimit,
                                       final long previewLimit,
                                       final boolean decompressBeforeLimiting,
                                       final boolean inheritedLimit) throws Exception
    {
        final QueueManagingVirtualHost<?> virtualHost = BrokerTestHelper.createVirtualHost(getTestName(), this);
        final Map<String, Integer> context = Map.of(Connection.MAX_MESSAGE_DECOMPRESSION_SIZE, localLimit);
        if (inheritedLimit)
        {
            virtualHost.setAttributes(Map.of(Queue.CONTEXT, context));
        }
        _queue = virtualHost.createChild(Queue.class, Map.of(Queue.NAME, getTestName(),
                                                            Queue.CONTEXT, inheritedLimit ? Map.of() : context));
        final ServerMessage<?> message = BrokerTestHelper.createMessage(1L);
        when(message.getSize()).thenReturn((long) compressed.length);
        when(message.getMessageHeader().getEncoding()).thenReturn(GZIPUtils.GZIP_CONTENT_ENCODING);
        when(message.getMessageHeader().getMimeType()).thenReturn("application/octet-stream");
        when(message.getContent(0, compressed.length)).thenAnswer(invocation -> QpidByteBuffer.wrap(compressed));
        _queue.enqueue(message, null, null);
        _queueContent = _queue.getMessageContent(1L, previewLimit, false, decompressBeforeLimiting);
        return _queueContent;
    }

    private Content createOverLimitContent()
    {
        final byte[] content = new byte[MAXIMUM_DECOMPRESSED_SIZE + 1];
        final byte[] compressedContent = GZIPUtils.compressBufferToArray(ByteBuffer.wrap(content));
        return new GzipContent(compressedContent);
    }

    private static final class TestServlet extends AbstractServlet
    {
        private void write(final Content content,
                           final HttpServletRequest request,
                           final HttpServletResponse response) throws IOException
        {
            writeTypedContent(content, request, response);
        }
    }

    public static final class GzipContent implements Content, CustomRestHeaders
    {
        private final byte[] _content;

        private GzipContent(final byte[] content)
        {
            _content = content;
        }

        @Override
        public void write(final OutputStream outputStream) throws IOException
        {
            outputStream.write(_content);
        }

        @Override
        public void release()
        {
        }

        @RestContentHeader("Content-Encoding")
        public String getContentEncoding()
        {
            return GZIPUtils.GZIP_CONTENT_ENCODING;
        }
    }

    private static final class FailingContent implements Content
    {
        @Override
        public void write(final OutputStream outputStream) throws IOException
        {
            outputStream.write(1);
            throw new IOException("Content write failed");
        }

        @Override
        public void release()
        {
        }
    }

    private static class DecompressionLimitedTestContent implements DecompressionLimitedContent
    {
        private final boolean _failPreparation;
        private final byte[] _content;
        private int _maximumMessageDecompressionSize = -1;
        private boolean _written;

        private DecompressionLimitedTestContent(final boolean failPreparation)
        {
            this(failPreparation, new byte[]{1});
        }

        private DecompressionLimitedTestContent(final boolean failPreparation, final byte[] content)
        {
            _failPreparation = failPreparation;
            _content = content;
        }

        @Override
        public void prepareForWrite(final int maximumMessageDecompressionSize) throws IOException
        {
            _maximumMessageDecompressionSize = maximumMessageDecompressionSize;
            if (_failPreparation)
            {
                throw new GZIPInflationLimitException("Content exceeds decompression limit");
            }
        }

        @Override
        public void write(final OutputStream outputStream) throws IOException
        {
            _written = true;
            outputStream.write(_content);
        }

        @Override
        public void release()
        {
        }

        private int getPreparedMaximumMessageDecompressionSize()
        {
            return _maximumMessageDecompressionSize;
        }

        private boolean isWritten()
        {
            return _written;
        }
    }

    public static final class GzipDecompressionLimitedTestContent extends DecompressionLimitedTestContent
            implements CustomRestHeaders
    {
        private GzipDecompressionLimitedTestContent(final byte[] content)
        {
            super(false, content);
        }

        @RestContentHeader("Content-Encoding")
        public String getContentEncoding()
        {
            return GZIPUtils.GZIP_CONTENT_ENCODING;
        }
    }

    private static final class TrackingServletOutputStream extends ServletOutputStream
    {
        private final ByteArrayOutputStream _delegate = new ByteArrayOutputStream();
        private boolean _closed;

        @Override
        public boolean isReady()
        {
            return true;
        }

        @Override
        public void setWriteListener(final WriteListener writeListener)
        {
        }

        @Override
        public void write(final int value)
        {
            _delegate.write(value);
        }

        @Override
        public void write(final byte[] data, final int offset, final int length)
        {
            _delegate.write(data, offset, length);
        }

        @Override
        public void close() throws IOException
        {
            _closed = true;
            super.close();
        }

        private void reset()
        {
            _delegate.reset();
        }

        private boolean isClosed()
        {
            return _closed;
        }

        private String toString(final Charset charset)
        {
            return _delegate.toString(charset);
        }

        private byte[] toByteArray()
        {
            return _delegate.toByteArray();
        }
    }
}
