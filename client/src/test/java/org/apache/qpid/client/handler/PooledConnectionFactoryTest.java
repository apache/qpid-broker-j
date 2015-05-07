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
package org.apache.qpid.client.handler;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import javax.jms.*;
import javax.jms.IllegalStateException;

import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.qpid.AMQException;
import org.apache.qpid.client.CommonConnection;
import org.apache.qpid.client.PooledConnectionFactory;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.test.utils.QpidTestCase;

public class PooledConnectionFactoryTest extends QpidTestCase
{
    private interface CommonConnectionCreator
    {
        CommonConnection newConnection(ConnectionURL connectionUrl);
    }

    private CommonConnectionCreator _connectionCreator;

    private PooledConnectionFactory _connectionFactory;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _connectionCreator = mock(CommonConnectionCreator.class);

        _connectionFactory = new PooledConnectionFactory()
        {
            @Override
            protected CommonConnection newConnectionInstance(final ConnectionURL connectionDetails) throws AMQException
            {
                return _connectionCreator.newConnection(connectionDetails);
            }
        };
    }


    public void testConnectionCreatedWithUrlUserAndPassword() throws Exception
    {
        _connectionFactory.setConnectionURLString("amqp://user:pass@/?brokerlist='tcp://localhost:5672'");

        final ArgumentCaptor<ConnectionURL> connectionCaptor = ArgumentCaptor.forClass(ConnectionURL.class);
        final List<CommonConnection> createdConnections = new ArrayList<>();

        when(_connectionCreator.newConnection(connectionCaptor.capture())).thenAnswer(new Answer<CommonConnection>()
        {
            @Override
            public CommonConnection answer(final InvocationOnMock invocation) throws Throwable
            {
                ConnectionURL url = connectionCaptor.getValue();
                assertEquals("user", url.getUsername());
                assertEquals("pass", url.getPassword());

                final CommonConnection connection = mock(CommonConnection.class);
                when(connection.isClosed()).thenReturn(false);
                createdConnections.add(connection);
                return connection;
            }
        });

        Connection conn = _connectionFactory.createConnection();
        assertNotNull(conn);
        assertEquals(1, createdConnections.size());
        final String connToString = conn.toString();
        assertEquals(createdConnections.get(0).toString(), connToString);

        conn.close();

        Connection conn2 = _connectionFactory.createConnection();
        assertNotNull(conn2);
        assertEquals(1, createdConnections.size());
        assertEquals(createdConnections.get(0).toString(), conn2.toString());

        assertEquals(connToString, conn2.toString());

        try
        {
            conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Closed connection should not allow sessions to be created");
        }
        catch (IllegalStateException e)
        {
            // pass
        }

        Connection conn3 = _connectionFactory.createConnection();
        assertNotNull(conn3);
        assertEquals(2, createdConnections.size());
        assertEquals(createdConnections.get(1).toString(), conn3.toString());
        assertFalse(conn3.toString().equals(conn2.toString()));

        conn2.close();
        Connection conn4 = _connectionFactory.createConnection();
        assertNotNull(conn4);
        assertEquals(2, createdConnections.size());
        assertEquals(createdConnections.get(0).toString(), conn4.toString());

    }

    public void testConnectionCreatedPassedUserAndPassword() throws Exception
    {

        _connectionFactory.setConnectionURLString("amqp://user:pass@/?brokerlist='tcp://localhost:5672'");

        final ArgumentCaptor<ConnectionURL> connectionCaptor = ArgumentCaptor.forClass(ConnectionURL.class);
        final List<CommonConnection> createdConnections = new ArrayList<>();
        final List<String> usernames = new ArrayList<>();
        final List<String> passwords = new ArrayList<>();

        when(_connectionCreator.newConnection(connectionCaptor.capture())).thenAnswer(new Answer<CommonConnection>()
        {
            @Override
            public CommonConnection answer(final InvocationOnMock invocation) throws Throwable
            {
                ConnectionURL url = connectionCaptor.getValue();
                usernames.add(url.getUsername());
                passwords.add(url.getPassword());

                final CommonConnection connection = mock(CommonConnection.class);
                when(connection.isClosed()).thenReturn(false);
                createdConnections.add(connection);
                return connection;
            }
        });

        Connection conn = _connectionFactory.createConnection("user1", "pass1");
        assertNotNull(conn);
        assertEquals(1, createdConnections.size());
        assertEquals("user1", usernames.get(0));
        assertEquals("pass1", passwords.get(0));
        conn.close();
        Connection conn2 = _connectionFactory.createConnection("user2", "pass2");
        assertNotNull(conn2);
        assertEquals(2, createdConnections.size());
        assertEquals("user2", usernames.get(1));
        assertEquals("pass2", passwords.get(1));
        conn2.close();
        Connection conn3 = _connectionFactory.createConnection("user1", "pass1");
        assertNotNull(conn3);
        assertEquals(2, createdConnections.size());
        assertEquals(createdConnections.get(0).toString(), conn3.toString());


    }

    public void testMaxPoolSize() throws Exception
    {
        _connectionFactory.setConnectionURLString("amqp://user:pass@/?brokerlist='tcp://localhost:5672'");
        _connectionFactory.setMaxPoolSize(3);
        final ArgumentCaptor<ConnectionURL> connectionCaptor = ArgumentCaptor.forClass(ConnectionURL.class);
        final List<CommonConnection> createdConnections = new ArrayList<>();

        when(_connectionCreator.newConnection(connectionCaptor.capture())).thenAnswer(new Answer<CommonConnection>()
        {
            @Override
            public CommonConnection answer(final InvocationOnMock invocation) throws Throwable
            {
                ConnectionURL url = connectionCaptor.getValue();

                final CommonConnection connection = mock(CommonConnection.class);
                when(connection.isClosed()).thenReturn(false);
                createdConnections.add(connection);
                return connection;
            }
        });

        Connection conn1 = _connectionFactory.createQueueConnection();
        Connection conn2 = _connectionFactory.createQueueConnection();
        Connection conn3 = _connectionFactory.createQueueConnection();
        Connection conn4 = _connectionFactory.createQueueConnection();

        assertEquals(4, createdConnections.size());

        conn1.close();
        conn2.close();
        conn3.close();
        conn4.close();

        Connection conn5 = _connectionFactory.createTopicConnection();
        assertEquals(4, createdConnections.size());
        Connection conn6 = _connectionFactory.createTopicConnection();
        assertEquals(4, createdConnections.size());
        Connection conn7 = _connectionFactory.createTopicConnection();
        assertEquals(4, createdConnections.size());
        Connection conn8 = _connectionFactory.createTopicConnection();
        assertEquals(5, createdConnections.size());

    }

    public void testConnectionTimeout() throws Exception
    {
        _connectionFactory.setConnectionURLString("amqp://user:pass@/?brokerlist='tcp://localhost:5672'");
        _connectionFactory.setMaxPoolSize(4);
        _connectionFactory.setConnectionTimeout(100l);
        final ArgumentCaptor<ConnectionURL> connectionCaptor = ArgumentCaptor.forClass(ConnectionURL.class);
        final List<CommonConnection> createdConnections = new ArrayList<>();

        when(_connectionCreator.newConnection(connectionCaptor.capture())).thenAnswer(new Answer<CommonConnection>()
        {
            @Override
            public CommonConnection answer(final InvocationOnMock invocation) throws Throwable
            {
                ConnectionURL url = connectionCaptor.getValue();

                final CommonConnection connection = mock(CommonConnection.class);
                when(connection.isClosed()).thenReturn(false);
                createdConnections.add(connection);
                return connection;
            }
        });

        Connection conn1 = _connectionFactory.createQueueConnection();
        Connection conn2 = _connectionFactory.createQueueConnection();
        Connection conn3 = _connectionFactory.createQueueConnection();
        Connection conn4 = _connectionFactory.createQueueConnection();

        assertEquals(4, createdConnections.size());

        conn1.close();
        conn2.close();
        conn3.close();
        conn4.close();

        Thread.sleep(500l);

        Connection conn5 = _connectionFactory.createTopicConnection();
        assertEquals(5, createdConnections.size());
        Connection conn6 = _connectionFactory.createTopicConnection();
        assertEquals(6, createdConnections.size());
        Connection conn7 = _connectionFactory.createTopicConnection();
        assertEquals(7, createdConnections.size());
        Connection conn8 = _connectionFactory.createTopicConnection();
        assertEquals(8, createdConnections.size());


    }

    public void testSessionsAreClosed() throws Exception
    {
        _connectionFactory.setConnectionURLString("amqp://user:pass@/?brokerlist='tcp://localhost:5672'");

        final ArgumentCaptor<ConnectionURL> connectionCaptor = ArgumentCaptor.forClass(ConnectionURL.class);
        final List<CommonConnection> createdConnections = new ArrayList<>();
        final Session session = mock(Session.class);
        when(_connectionCreator.newConnection(connectionCaptor.capture())).thenAnswer(new Answer<CommonConnection>()
        {
            @Override
            public CommonConnection answer(final InvocationOnMock invocation) throws Throwable
            {
                ConnectionURL url = connectionCaptor.getValue();

                final CommonConnection connection = mock(CommonConnection.class);
                when(connection.isClosed()).thenReturn(false);
                when(connection.createSession(anyBoolean(),anyInt())).thenReturn(session);
                createdConnections.add(connection);
                return connection;
            }
        });

        Connection conn = _connectionFactory.createConnection();
        Session createdSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.close();
        verify(session, times(1)).close();

    }

    public void testConnectionWithExceptionNotPooled() throws Exception
    {
        _connectionFactory.setConnectionURLString("amqp://user:pass@/?brokerlist='tcp://localhost:5672'");

        final ArgumentCaptor<ConnectionURL> connectionCaptor = ArgumentCaptor.forClass(ConnectionURL.class);
        final List<CommonConnection> createdConnections = new ArrayList<>();
        when(_connectionCreator.newConnection(connectionCaptor.capture())).thenAnswer(new Answer<CommonConnection>()
        {
            @Override
            public CommonConnection answer(final InvocationOnMock invocation) throws Throwable
            {
                ConnectionURL url = connectionCaptor.getValue();

                final CommonConnection connection = mock(CommonConnection.class);
                when(connection.isClosed()).thenReturn(false);
                when(connection.createSession(anyBoolean(),anyInt())).thenThrow(new JMSException("foo"));
                createdConnections.add(connection);
                return connection;
            }
        });

        Connection conn = _connectionFactory.createConnection();
        try
        {
            Session createdSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Expected an exception");
        }
        catch(JMSException e)
        {
            assertEquals("foo", e.getMessage());
        }
        conn.close();
        Connection conn2 = _connectionFactory.createConnection();
        assertEquals(2, createdConnections.size());
    }


    public void testConnectionWithExceptionListenerExceptionNotPooled() throws Exception
    {
        _connectionFactory.setConnectionURLString("amqp://user:pass@/?brokerlist='tcp://localhost:5672'");


        final List<CommonConnection> createdConnections = new ArrayList<>();

        when(_connectionCreator.newConnection(any(ConnectionURL.class))).thenAnswer(new Answer<CommonConnection>()
        {
            @Override
            public CommonConnection answer(final InvocationOnMock invocation) throws Throwable
            {
                final CommonConnection connection = mock(CommonConnection.class);
                when(connection.isClosed()).thenReturn(false);
                createdConnections.add(connection);
                final ArgumentCaptor<ExceptionListener> listenerCaptor =
                        ArgumentCaptor.forClass(ExceptionListener.class);

                doAnswer(new Answer()
                {
                    @Override
                    public Object answer(final InvocationOnMock invocation) throws Throwable
                    {
                        when(connection.getExceptionListener()).thenReturn(listenerCaptor.getValue());
                        return null;
                    }
                }).when(connection).setExceptionListener(listenerCaptor.capture());

                return connection;
            }
        });

        Connection conn = _connectionFactory.createConnection();
        assertEquals(1, createdConnections.size());

        ExceptionListener listener = mock(ExceptionListener.class);
        conn.setExceptionListener(listener);
        assertEquals(listener, conn.getExceptionListener());
        verify(listener, never()).onException(any(JMSException.class));
        createdConnections.get(0).getExceptionListener().onException(new JMSException("bar"));
        verify(listener, times(1)).onException(any(JMSException.class));

        conn.close();
        Connection conn2 = _connectionFactory.createConnection();
        assertEquals(2, createdConnections.size());
    }

    public void testSessionCloseException() throws Exception
    {
        _connectionFactory.setConnectionURLString("amqp://user:pass@/?brokerlist='tcp://localhost:5672'");

        final ArgumentCaptor<ConnectionURL> connectionCaptor = ArgumentCaptor.forClass(ConnectionURL.class);
        final List<CommonConnection> createdConnections = new ArrayList<>();
        final Session session = mock(Session.class);
        doThrow(new JMSException("foo")).when(session).close();
        when(_connectionCreator.newConnection(connectionCaptor.capture())).thenAnswer(new Answer<CommonConnection>()
        {
            @Override
            public CommonConnection answer(final InvocationOnMock invocation) throws Throwable
            {
                ConnectionURL url = connectionCaptor.getValue();

                final CommonConnection connection = mock(CommonConnection.class);
                when(connection.isClosed()).thenReturn(false);
                when(connection.createSession(anyBoolean(),anyInt())).thenReturn(session);
                createdConnections.add(connection);
                return connection;
            }
        });

        Connection conn = _connectionFactory.createConnection();
            Session createdSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        try
        {
            conn.close();
            fail("Expected an exception");
        }
        catch(JMSException e)
        {
            assertEquals("foo", e.getMessage());
        }
        Connection conn2 = _connectionFactory.createConnection();
        assertEquals(2, createdConnections.size());
    }


}
