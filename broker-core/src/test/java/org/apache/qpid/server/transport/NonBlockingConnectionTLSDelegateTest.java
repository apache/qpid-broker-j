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
 */
package org.apache.qpid.server.transport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;

import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.test.utils.UnitTestBase;

class NonBlockingConnectionTLSDelegateTest extends UnitTestBase
{
    @Test
    void handshakeOutputIsFlushedBeforeWaitingForPeerInput() throws Exception
    {
        final NonBlockingConnection parent = mock(NonBlockingConnection.class);
        final SSLEngine engine = mock(SSLEngine.class);
        final AtomicReference<SSLEngineResult.HandshakeStatus> status =
                new AtomicReference<>(SSLEngineResult.HandshakeStatus.NEED_WRAP);
        when(engine.getHandshakeStatus()).thenAnswer(invocation -> status.get());
        when(engine.wrap(any(ByteBuffer[].class), any(ByteBuffer.class))).thenAnswer(invocation ->
        {
            final ByteBuffer output = invocation.getArgument(1);
            output.put((byte) 1);
            status.set(SSLEngineResult.HandshakeStatus.NEED_UNWRAP);
            return new SSLEngineResult(SSLEngineResult.Status.OK, status.get(), 0, 1);
        });
        final SSLSession session = mock(SSLSession.class);
        when(session.getPacketBufferSize()).thenReturn(1024);
        when(engine.getSession()).thenReturn(session);
        when(engine.getEnabledProtocols()).thenReturn(new String[]{"TLSv1.3"});
        when(engine.getSupportedProtocols()).thenReturn(new String[]{"TLSv1.3"});
        when(engine.getEnabledCipherSuites()).thenReturn(new String[]{"TLS_AES_128_GCM_SHA256"});
        when(engine.getSupportedCipherSuites()).thenReturn(new String[]{"TLS_AES_128_GCM_SHA256"});
        final SSLContext context = mock(SSLContext.class);
        when(context.createSSLEngine()).thenReturn(engine);
        final AmqpPort<?> port = mock(AmqpPort.class);
        when(port.getSSLContext()).thenReturn(context);
        when(port.getNetworkBufferSize()).thenReturn(4096);
        when(port.getContextValue(Boolean.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING)).thenReturn(false);
        when(port.getContextValue(Integer.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING_WARN_THRESHOLD))
                .thenReturn(1000);
        when(port.getContextValue(Integer.class, AmqpPort.PORT_DIAGNOSIS_OF_SSL_ENGINE_LOOPING_BREAK_THRESHOLD))
                .thenReturn(10000);
        final NonBlockingConnectionTLSDelegate delegate = new NonBlockingConnectionTLSDelegate(parent, port);
        try (QpidByteBuffer application = QpidByteBuffer.wrap(new byte[128]))
        {
            final List<QpidByteBuffer> buffers = List.of(application);
            assertFalse(delegate.doWrite(buffers).isComplete());
            assertFalse(delegate.needsReadForWrite(), "Buffered handshake output must still select for writing");

            when(parent.writeToTransport(any())).thenAnswer(invocation ->
            {
                long written = 0;
                final Collection<QpidByteBuffer> encrypted = invocation.getArgument(0);
                for (final QpidByteBuffer buffer : encrypted)
                {
                    written += buffer.remaining();
                    buffer.position(buffer.limit());
                }
                return written;
            });

            assertFalse(delegate.doWrite(buffers).isComplete());
            assertTrue(application.hasRemaining(), "Application bytes must wait for the peer handshake response");
            assertTrue(delegate.needsReadForWrite(), "A writable socket cannot advance NEED_UNWRAP");
        }
        finally
        {
            delegate.shutdownInput();
            delegate.shutdownOutput();
        }
    }
}
