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
package org.apache.qpid.server.connection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.messages.ConnectionMessages;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.test.utils.UnitTestBase;

public class ConnectionVersionValidatorTest extends UnitTestBase
{
    private QueueManagingVirtualHost<?> _virtualHostMock;
    private AMQPConnection<?> _connectionMock;
    private EventLogger _eventLoggerMock;
    private ConnectionVersionValidator _connectionValidator;

    @BeforeEach
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void setUp() throws Exception
    {
        _connectionValidator = new ConnectionVersionValidator();
        _virtualHostMock = mock(QueueManagingVirtualHost.class);
        _connectionMock = mock(AMQPConnection.class);
        _eventLoggerMock = mock(EventLogger.class);
        final Broker brokerMock = mock(Broker.class);

        when(_virtualHostMock.getBroker()).thenReturn(brokerMock);
        when(brokerMock.getEventLogger()).thenReturn(_eventLoggerMock);
    }

    @SuppressWarnings("unchecked")
    private void setContextValues(final Map<String, List<String>> values)
    {
        when(_virtualHostMock.getContextKeys(anyBoolean())).thenReturn(values.keySet());
        for (final Map.Entry<String, List<String>> entry : values.entrySet())
        {
            when(_virtualHostMock.getContextValue(any(Class.class), any(Type.class), eq(entry.getKey())))
                    .thenReturn(entry.getValue());
        }
    }

    @Test
    public void testInvalidRegex()
    {
        final Map<String, List<String>> contextValues = Map.of(
                ConnectionVersionValidator.VIRTUALHOST_REJECTED_CONNECTION_VERSION, List.of("${}", "foo"));
        setContextValues(contextValues);
        when(_connectionMock.getClientVersion()).thenReturn("foo");
        assertFalse(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock).message(ConnectionMessages.CLIENT_VERSION_REJECT("foo"));
        // TODO: We should verify that the invalid regex is logged
    }

    @Test
    public void testNullClientDefaultAllowed()
    {
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
    }

    @Test
    public void testClientDefaultAllowed()
    {
        when(_connectionMock.getClientVersion()).thenReturn("foo");
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
    }

    @Test
    public void testEmptyList()
    {
        final Map<String, List<String>> contextValues = Map.of(ConnectionVersionValidator.VIRTUALHOST_REJECTED_CONNECTION_VERSION, List.of());
        setContextValues(contextValues);
        when(_connectionMock.getClientVersion()).thenReturn("foo");
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock, never()).message(any(LogMessage.class));
    }

    @Test
    public void testEmptyString()
    {
        final Map<String, List<String>> contextValues = Map.of(ConnectionVersionValidator.VIRTUALHOST_REJECTED_CONNECTION_VERSION, List.of(""));
        setContextValues(contextValues);
        when(_connectionMock.getClientVersion()).thenReturn("");
        assertFalse(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock).message(ConnectionMessages.CLIENT_VERSION_REJECT(""));
        when(_connectionMock.getClientVersion()).thenReturn(null);
        assertFalse(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock).message(ConnectionMessages.CLIENT_VERSION_REJECT(""));
        verify(_eventLoggerMock).message(ConnectionMessages.CLIENT_VERSION_REJECT(null));
    }

    @Test
    public void testClientRejected()
    {
        when(_connectionMock.getClientVersion()).thenReturn("foo");
        final Map<String, List<String>> contextValues = Map.of(ConnectionVersionValidator.VIRTUALHOST_REJECTED_CONNECTION_VERSION, List.of("foo"));
        setContextValues(contextValues);
        assertFalse(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock).message(ConnectionMessages.CLIENT_VERSION_REJECT("foo"));
    }

    @Test
    public void testClientLogged()
    {
        when(_connectionMock.getClientVersion()).thenReturn("foo");
        final Map<String, List<String>> contextValues = Map.of(ConnectionVersionValidator.VIRTUALHOST_LOGGED_CONNECTION_VERSION, List.of("foo"));
        setContextValues(contextValues);
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock).message(ConnectionMessages.CLIENT_VERSION_LOG("foo"));
    }

    @Test
    public void testAllowedTakesPrecedence()
    {
        when(_connectionMock.getClientVersion()).thenReturn("foo");
        final Map<String, List<String>> contextValues = Map.of(ConnectionVersionValidator.VIRTUALHOST_ALLOWED_CONNECTION_VERSION, List.of("foo"),
                ConnectionVersionValidator.VIRTUALHOST_LOGGED_CONNECTION_VERSION, List.of("foo"),
                ConnectionVersionValidator.VIRTUALHOST_REJECTED_CONNECTION_VERSION, List.of("foo"));
        setContextValues(contextValues);
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock, never()).message(any(LogMessage.class));
    }

    @Test
    public void testLoggedTakesPrecedenceOverRejected()
    {
        when(_connectionMock.getClientVersion()).thenReturn("foo");
        final Map<String, List<String>> contextValues = Map.of(ConnectionVersionValidator.VIRTUALHOST_LOGGED_CONNECTION_VERSION, List.of("foo"),
                ConnectionVersionValidator.VIRTUALHOST_REJECTED_CONNECTION_VERSION, List.of("foo"));
        setContextValues(contextValues);
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock).message(ConnectionMessages.CLIENT_VERSION_LOG("foo"));
    }

    @Test
    public void testRegex()
    {
        final Map<String, List<String>> contextValues = Map.of(ConnectionVersionValidator.VIRTUALHOST_ALLOWED_CONNECTION_VERSION, List.of("foo"),
                ConnectionVersionValidator.VIRTUALHOST_LOGGED_CONNECTION_VERSION, List.of("f.*"));
        setContextValues(contextValues);
        when(_connectionMock.getClientVersion()).thenReturn("foo");
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock, never()).message(any(LogMessage.class));
        when(_connectionMock.getClientVersion()).thenReturn("foo2");
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock).message(ConnectionMessages.CLIENT_VERSION_LOG("foo2"));
        when(_connectionMock.getClientVersion()).thenReturn("baz");
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock, never()).message(ConnectionMessages.CLIENT_VERSION_LOG("baz"));
    }

    @Test
    public void testRegexLists()
    {
        final Map<String, List<String>> contextValues = Map.of(ConnectionVersionValidator.VIRTUALHOST_ALLOWED_CONNECTION_VERSION, List.of("foo"),
                ConnectionVersionValidator.VIRTUALHOST_LOGGED_CONNECTION_VERSION, List.of("f.*", "baz"));
        setContextValues(contextValues);
        when(_connectionMock.getClientVersion()).thenReturn("foo");
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock, never()).message(any(LogMessage.class));
        when(_connectionMock.getClientVersion()).thenReturn("foo2");
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock).message(ConnectionMessages.CLIENT_VERSION_LOG("foo2"));
        when(_connectionMock.getClientVersion()).thenReturn("baz");
        assertTrue(_connectionValidator.validateConnectionCreation(_connectionMock, _virtualHostMock));
        verify(_eventLoggerMock).message(ConnectionMessages.CLIENT_VERSION_LOG("baz"));
    }
}
