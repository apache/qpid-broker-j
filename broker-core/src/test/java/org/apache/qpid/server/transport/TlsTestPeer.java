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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;

/** A real TLS peer whose encrypted records can be split between socket writes. */
final class TlsTestPeer
{
    private static final int CAPACITY = 64 * 1024;
    private final SSLEngine _engine;
    private final Socket _socket;

    TlsTestPeer(final SSLContext context, final String protocol, final Socket socket) throws Exception
    {
        _socket = socket;
        _engine = context.createSSLEngine("localhost", socket.getPort());
        _engine.setUseClientMode(true);
        _engine.setEnabledProtocols(new String[]{protocol});
        _engine.beginHandshake();
        handshake();
    }

    byte[] wrap(final byte[] plaintext) throws Exception
    {
        final ByteBuffer input = ByteBuffer.wrap(plaintext);
        final ByteBuffer output = ByteBuffer.allocate(CAPACITY);
        final SSLEngineResult result = _engine.wrap(input, output);
        assertEquals(SSLEngineResult.Status.OK, result.getStatus());
        assertEquals(plaintext.length, result.bytesConsumed());
        assertTrue(result.bytesProduced() > 0);
        return Arrays.copyOf(output.array(), output.position());
    }

    void send(final byte[] bytes) throws Exception
    {
        _socket.getOutputStream().write(bytes);
    }

    private void handshake() throws Exception
    {
        final ByteBuffer input = ByteBuffer.allocate(CAPACITY);
        final ByteBuffer output = ByteBuffer.allocate(CAPACITY);
        final ByteBuffer application = ByteBuffer.allocate(CAPACITY);
        for (int step = 0; step < 1000; step++)
        {
            switch (_engine.getHandshakeStatus())
            {
                case NOT_HANDSHAKING:
                    return;
                case NEED_TASK:
                    Runnable task;
                    while ((task = _engine.getDelegatedTask()) != null)
                    {
                        task.run();
                    }
                    break;
                case NEED_WRAP:
                    output.clear();
                    assertEquals(SSLEngineResult.Status.OK,
                                 _engine.wrap(ByteBuffer.allocate(0), output).getStatus());
                    _socket.getOutputStream().write(output.array(), 0, output.position());
                    break;
                case NEED_UNWRAP:
                case NEED_UNWRAP_AGAIN:
                    input.flip();
                    final SSLEngineResult result = _engine.unwrap(input, application);
                    input.compact();
                    if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW)
                    {
                        assertTrue(input.hasRemaining());
                        final int count = _socket.getInputStream().read(input.array(), input.position(), input.remaining());
                        assertTrue(count > 0, "Peer closed during the handshake");
                        input.position(input.position() + count);
                    }
                    else
                    {
                        assertEquals(SSLEngineResult.Status.OK, result.getStatus());
                    }
                    break;
                default:
                    fail("Unexpected handshake status: " + _engine.getHandshakeStatus());
            }
        }
        fail("TLS handshake did not finish");
    }
}
