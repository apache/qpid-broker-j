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

package org.apache.qpid.tests.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;
import org.apache.qpid.server.protocol.v1_0.type.transport.Begin;
import org.apache.qpid.server.protocol.v1_0.type.transport.Flow;
import org.apache.qpid.server.protocol.v1_0.type.transport.Open;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;

/** A bounded, synchronous peer that deliberately leaves AMQP and WebSocket close responses to the test. */
final class WebSocketTestPeer implements AutoCloseable
{
    private static final byte[] AMQP_HEADER = {'A', 'M', 'Q', 'P', 0, 1, 0, 0};
    private static final int MAX_FRAME_SIZE = 16 * 1024 * 1024;

    private final Socket _socket;
    private final DataInputStream _input;
    private final DataOutputStream _output;
    private final SecureRandom _random = new SecureRandom();
    private final AMQPDescribedTypeRegistry _registry = AMQPDescribedTypeRegistry
            .newInstance()
            .registerTransportLayer()
            .registerMessagingLayer();
    private final String _upgradeResponse;

    WebSocketTestPeer(final AmqpPort<?> port,
                      final SSLContext context,
                      final String sni,
                      final String httpHost,
                      final boolean compression) throws Exception
    {
        _socket = port.getTransports().contains(Transport.WSS)
                ? context.getSocketFactory().createSocket()
                : new Socket();
        try
        {
            if (_socket instanceof SSLSocket tls)
            {
                final SSLParameters parameters = tls.getSSLParameters();
                parameters.setServerNames(sni == null ? List.of() : List.of(new SNIHostName(sni)));
                tls.setSSLParameters(parameters);
            }
            _socket.setReceiveBufferSize(1024);
            _socket.connect(new InetSocketAddress("127.0.0.1", port.getBoundPort()), WebSocketTestBase.TIMEOUT_MILLIS);
            _socket.setSoTimeout(WebSocketTestBase.TIMEOUT_MILLIS);
            _input = new DataInputStream(_socket.getInputStream());
            _output = new DataOutputStream(_socket.getOutputStream());
            final byte[] nonce = new byte[16];
            _random.nextBytes(nonce);
            final String key = Base64.getEncoder().encodeToString(nonce);
            final String request = "GET / HTTP/1.1\r\nHost: " + httpHost + ":" + port.getBoundPort() +
                    "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13" +
                    "\r\nSec-WebSocket-Protocol: amqp\r\nSec-WebSocket-Key: " + key + "\r\n" +
                    (compression ? "Sec-WebSocket-Extensions: permessage-deflate\r\n" : "") + "\r\n";
            _output.write(request.getBytes(StandardCharsets.US_ASCII));
            _output.flush();
            final ByteArrayOutputStream response = new ByteArrayOutputStream();
            while (response.size() < 16384)
            {
                response.write(_input.readUnsignedByte());
                if (response.toString(StandardCharsets.US_ASCII).endsWith("\r\n\r\n"))
                {
                    break;
                }
            }
            _upgradeResponse = response.toString(StandardCharsets.US_ASCII);
            assertTrue(_upgradeResponse.startsWith("HTTP/1.1 101 "), _upgradeResponse);
            assertTrue(_upgradeResponse.toLowerCase(Locale.ROOT).contains("sec-websocket-protocol: amqp"),
                    _upgradeResponse);
            final String accept = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1").digest(
                    (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.US_ASCII)));
            assertTrue(_upgradeResponse.contains(accept), _upgradeResponse);
        }
        catch (Exception | AssertionError e)
        {
            _socket.close();
            throw e;
        }
    }

    String getUpgradeResponse()
    {
        return _upgradeResponse;
    }

    SSLSession getTlsSession()
    {
        return ((SSLSocket) _socket).getSession();
    }

    void openAmqp(final String containerId, final String hostname) throws IOException
    {
        sendBinary(AMQP_HEADER);
        final Open open = new Open();
        open.setContainerId(containerId);
        open.setHostname(hostname);
        open.setMaxFrameSize(UnsignedInteger.valueOf(16384));
        sendAmqp(open);
    }

    void sendAmqp(final Object performative) throws IOException
    {
        final ValueWriter<Object> writer = _registry.getValueWriter(performative);
        try (final QpidByteBuffer buffer = QpidByteBuffer.allocate(false, writer.getEncodedSize() + 8))
        {
            buffer.putInt(buffer.capacity());
            buffer.put((byte) 2);
            buffer.put((byte) 0);
            buffer.putShort((short) 0);
            writer.writeToBuffer(buffer);
            buffer.flip();
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            sendBinary(bytes);
        }
    }

    void startReceiver(final String queue) throws IOException
    {
        final UnsignedInteger window = UnsignedInteger.valueOf(100000);
        final Begin begin = new Begin();
        begin.setNextOutgoingId(UnsignedInteger.ZERO);
        begin.setIncomingWindow(window);
        begin.setOutgoingWindow(window);
        sendAmqp(begin);
        final Source source = new Source();
        source.setAddress(queue);
        final Attach attach = new Attach();
        attach.setName("slow-receiver");
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.RECEIVER);
        attach.setSource(source);
        attach.setTarget(new Target());
        sendAmqp(attach);
        final Flow flow = new Flow();
        flow.setNextIncomingId(UnsignedInteger.ZERO);
        flow.setIncomingWindow(window);
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setOutgoingWindow(window);
        flow.setHandle(UnsignedInteger.ZERO);
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setLinkCredit(UnsignedInteger.valueOf(100));
        sendAmqp(flow);
    }

    void heartbeat() throws IOException
    {
        sendBinary(new byte[]{0, 0, 0, 8, 2, 0, 0, 0});
    }

    void sendBinary(final byte[] payload) throws IOException
    {
        sendFrame(2, payload);
    }

    private void sendFrame(final int opcode, final byte[] payload) throws IOException
    {
        assertTrue(payload.length <= MAX_FRAME_SIZE);
        _output.writeByte(0x80 | opcode);
        if (payload.length < 126)
        {
            _output.writeByte(0x80 | payload.length);
        }
        else if (payload.length <= 65535)
        {
            _output.writeByte(0x80 | 126);
            _output.writeShort(payload.length);
        }
        else
        {
            _output.writeByte(0x80 | 127);
            _output.writeLong(payload.length);
        }
        final byte[] mask = new byte[4];
        _random.nextBytes(mask);
        _output.write(mask);
        final byte[] masked = payload.clone();
        for (int i = 0; i < masked.length; i++)
        {
            masked[i] ^= mask[i % 4];
        }
        _output.write(masked);
        _output.flush();
    }

    int awaitClose() throws IOException
    {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WebSocketTestBase.TIMEOUT_MILLIS);
        while (System.nanoTime() < deadline)
        {
            final int header = _input.readUnsignedByte();
            final int lengthByte = _input.readUnsignedByte();
            assertEquals(0, lengthByte & 0x80, "Server frames must not be masked");
            long length = lengthByte & 0x7f;
            if (length == 126)
            {
                length = _input.readUnsignedShort();
            }
            else if (length == 127)
            {
                length = _input.readLong();
            }
            assertTrue(length >= 0 && length <= MAX_FRAME_SIZE, "Unexpected frame size " + length);
            final byte[] payload = _input.readNBytes((int) length);
            if (payload.length != length)
            {
                throw new EOFException("Truncated WebSocket frame");
            }
            if ((header & 0xf) == 8)
            {
                return payload.length >= 2 ? ((payload[0] & 0xff) << 8) | (payload[1] & 0xff) : 1005;
            }
        }
        throw new IOException("WebSocket close frame was not received");
    }

    @Override
    public void close() throws IOException
    {
        _socket.close();
    }
}
