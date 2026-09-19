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
package org.apache.qpid.server.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionSecure;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionSecureOk;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionStartOk;
import org.apache.qpid.server.protocol.v0_10.transport.SegmentType;
import org.apache.qpid.test.utils.UnitTestBase;

class SaslFrameLoggingTest extends UnitTestBase
{
    private static final String REDACTED = "********";
    private static final String SENSITIVE_VALUE = "synthetic-password-token";

    private ListAppender<ILoggingEvent> _appender;
    private ServerConnection _connection;
    private Logger _logger;
    private boolean _originalAdditive;
    private Level _originalLevel;
    private ProtocolEventSender _sender;

    @BeforeEach
    void setUp()
    {
        final Broker<?> broker = mock(Broker.class);
        final AmqpPort<?> port = mock(AmqpPort.class);
        final AMQPConnection_0_10<?> amqpConnection = mock(AMQPConnection_0_10.class);
        _connection = new ServerConnection(1L, broker, port, Transport.TCP, amqpConnection);
        _connection.setConnectionDelegate(mock(ServerConnectionDelegate.class));
        _sender = mock(ProtocolEventSender.class);
        _connection.setSender(_sender);
    }

    @AfterEach
    void tearDown()
    {
        if (_logger != null)
        {
            _logger.detachAppender(_appender);
            _appender.stop();
            _logger.setLevel(_originalLevel);
            _logger.setAdditive(_originalAdditive);
        }
    }

    @Test
    void startOkResponseIsRedactedFromReceivedFrameLog()
    {
        captureLogging(ServerConnection.class);
        final byte[] response = sensitiveBytes();
        final Map<String, Object> clientProperties = Map.of("product", "test-client");
        final ConnectionStartOk startOk = new ConnectionStartOk(clientProperties, "PLAIN", response, "en_US");

        _connection.received(startOk);

        final String logMessage = getSingleLogMessage();
        assertSensitiveFieldRedacted(logMessage, "RECV", "ConnectionStartOk", "response");
        assertAll(() -> assertTrue(logMessage.contains("clientProperties={product=test-client}")),
                () -> assertTrue(logMessage.contains("mechanism=PLAIN")),
                () -> assertTrue(logMessage.contains("locale=en_US")),
                () -> assertSame(response, startOk.getFields().get("response")));
    }

    @Test
    void secureChallengeIsRedactedFromSentFrameLog()
    {
        captureLogging(ServerConnection.class);
        final byte[] challenge = sensitiveBytes();
        final ConnectionSecure secure = new ConnectionSecure(challenge);

        _connection.send(secure);

        assertSensitiveFieldRedacted(getSingleLogMessage(), "SEND", "ConnectionSecure", "challenge");
        assertSame(challenge, secure.getFields().get("challenge"));
        verify(_sender).send(secure);
    }

    @Test
    void secureOkResponseIsRedactedFromReceivedFrameLog()
    {
        captureLogging(ServerConnection.class);
        final byte[] response = sensitiveBytes();
        final ConnectionSecureOk secureOk = new ConnectionSecureOk(response);

        _connection.received(secureOk);

        assertSensitiveFieldRedacted(getSingleLogMessage(), "RECV", "ConnectionSecureOk", "response");
        assertSame(response, secureOk.getFields().get("response"));
    }

    @Test
    void rawFrameBodyIsRedactedFromDiagnostics()
    {
        captureLogging(ServerAssembler.class);
        final ServerConnection ignoredConnection = mock(ServerConnection.class);
        when(ignoredConnection.isIgnoreFutureInput()).thenReturn(true);
        final byte[] bodyBytes = sensitiveBytes();
        final byte flags = (byte) (ServerFrame.FIRST_SEG | ServerFrame.LAST_SEG | ServerFrame.FIRST_FRAME |
                ServerFrame.LAST_FRAME);

        try (final QpidByteBuffer body = QpidByteBuffer.wrap(bodyBytes))
        {
            final ServerFrame frame = new ServerFrame(flags, SegmentType.CONTROL, ServerFrame.L1, 0, body);
            final String frameDescription = frame.toString();

            new ServerAssembler(ignoredConnection).received(List.of(frame));

            final String logMessage = getSingleLogMessage();
            assertAll(() -> assertFalse(frameDescription.contains(SENSITIVE_VALUE)),
                    () -> assertTrue(frameDescription.endsWith(REDACTED)),
                    () -> assertFalse(logMessage.contains(SENSITIVE_VALUE)),
                    () -> assertTrue(logMessage.contains("channel=0")),
                    () -> assertTrue(logMessage.contains("size=" + bodyBytes.length)),
                    () -> assertTrue(logMessage.contains("type=CONTROL")));
        }
    }

    private void captureLogging(final Class<?> loggedClass)
    {
        _logger = (Logger) LoggerFactory.getLogger(loggedClass);
        _originalLevel = _logger.getLevel();
        _originalAdditive = _logger.isAdditive();
        _logger.setAdditive(false);
        _logger.setLevel(Level.DEBUG);

        _appender = new ListAppender<>();
        _appender.setContext(_logger.getLoggerContext());
        _appender.start();
        _logger.addAppender(_appender);
    }

    private String getSingleLogMessage()
    {
        assertEquals(1, _appender.list.size());
        return _appender.list.get(0).getFormattedMessage();
    }

    private void assertSensitiveFieldRedacted(final String logMessage,
                                              final String direction,
                                              final String methodName,
                                              final String fieldName)
    {
        assertAll(() -> assertTrue(logMessage.startsWith(direction + ":")),
                () -> assertTrue(logMessage.contains(methodName)),
                () -> assertTrue(logMessage.contains(fieldName + "=" + REDACTED)),
                () -> assertFalse(logMessage.contains(SENSITIVE_VALUE)));
    }

    private byte[] sensitiveBytes()
    {
        return SENSITIVE_VALUE.getBytes(UTF_8);
    }
}
