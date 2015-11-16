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
package org.apache.qpid.server.protocol.v1_0;

import static org.apache.qpid.server.logging.subjects.LogSubjectFormat.CONNECTION_FORMAT;

import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.security.auth.Subject;

import org.apache.qpid.amqp_1_0.transport.ConnectionEndpoint;
import org.apache.qpid.amqp_1_0.transport.ConnectionEventListener;
import org.apache.qpid.amqp_1_0.transport.LinkEndpoint;
import org.apache.qpid.amqp_1_0.transport.SessionEndpoint;
import org.apache.qpid.amqp_1_0.transport.SessionEventListener;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.amqp_1_0.type.transport.AmqpError;
import org.apache.qpid.amqp_1_0.type.transport.ConnectionError;
import org.apache.qpid.amqp_1_0.type.transport.End;
import org.apache.qpid.amqp_1_0.type.transport.Error;
import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.VirtualHostImpl;

public class Connection_1_0 implements ConnectionEventListener
{

    private static final long MINIMUM_SUPPORTED_IDLE_TIMEOUT = 1000L;
    private final AmqpPort<?> _port;
    private final SubjectCreator _subjectCreator;
    private AMQPConnection_1_0 _amqpConnection;
    private VirtualHostImpl<?,?,?> _vhost;
    private final Transport _transport;
    private final ConnectionEndpoint _connectionEndpoint;
    private final long _connectionId;
    private final Collection<Session_1_0> _sessions = Collections.synchronizedCollection(new ArrayList<Session_1_0>());
    private final Object _reference = new Object();

    private final LogSubject _logSubject = new LogSubject()
    {
        @Override
        public String toLogString()
        {
            return "[" +
                   MessageFormat.format(CONNECTION_FORMAT,
                                        getConnectionId(),
                                        getClientId(),
                                        getRemoteAddressString(),
                                        _vhost.getName())
                   + "] ";

        }
    };

    private final Queue<Action<? super Connection_1_0>> _asyncTaskList =
            new ConcurrentLinkedQueue<>();


    private boolean _closedOnOpen;


    public Connection_1_0(ConnectionEndpoint connectionEndpoint,
                          long connectionId,
                          AmqpPort<?> port,
                          Transport transport,
                          final SubjectCreator subjectCreator)
    {
        _port = port;
        _transport = transport;
        _connectionEndpoint = connectionEndpoint;
        _connectionId = connectionId;
        _subjectCreator = subjectCreator;
    }

    void setAmqpConnection(final AMQPConnection_1_0 amqpConnection)
    {
        _amqpConnection = amqpConnection;
    }

    public ConnectionEndpoint getConnectionEndpoint()
    {
        return _connectionEndpoint;
    }

    public Object getReference()
    {
        return _reference;
    }

    @Override
    public void openReceived()
    {
        String host = _connectionEndpoint.getLocalHostname();
        Map clientProperties = _connectionEndpoint.getRemoteProperties();
        if(clientProperties != null)
        {
            if(clientProperties.containsKey(Symbol.valueOf("product")))
            {
                _amqpConnection.setClientProduct(clientProperties.get(Symbol.valueOf("product")).toString());
            }
            if(clientProperties.containsKey(Symbol.valueOf("version")))
            {
                _amqpConnection.setClientVersion(clientProperties.get(Symbol.valueOf("version")).toString());
            }
            _amqpConnection.setClientId(_connectionEndpoint.getRemoteContainerId());
        }
        _amqpConnection.getNetwork().setMaxReadIdleMillis(_connectionEndpoint.getDesiredIdleTimeout());
        long idleTimeout = _connectionEndpoint.getIdleTimeout();
        if(idleTimeout != 0L && idleTimeout < MINIMUM_SUPPORTED_IDLE_TIMEOUT)
        {
            _connectionEndpoint.close(new Error(ConnectionError.CONNECTION_FORCED,
                                                "Requested idle timeout of "
                                                  + idleTimeout
                                                  + " is too low. The minimum supported timeout is"
                                                  + MINIMUM_SUPPORTED_IDLE_TIMEOUT));
            _amqpConnection.close();
            _closedOnOpen = true;
        }
        else
        {
            _amqpConnection.getNetwork().setMaxWriteIdleMillis(idleTimeout / 2L);

            _vhost = ((AmqpPort) _port).getVirtualHost(host);
            if (_vhost == null)
            {
                final Error err = new Error();
                err.setCondition(AmqpError.NOT_FOUND);
                err.setDescription("Unknown hostname in connection open: '" + host + "'");
                _connectionEndpoint.close(err);
                _amqpConnection.close();

                _closedOnOpen = true;
            }
            else
            {
                final Principal user = _connectionEndpoint.getUser();
                if (user != null)
                {
                    setUserPrincipal(user);
                }
                _amqpConnection.getSubject().getPrincipals().add(_vhost.getPrincipal());
                _amqpConnection.updateAccessControllerContext();
                if (AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(_amqpConnection.getSubject())
                    == null)
                {
                    final Error err = new Error();
                    err.setCondition(AmqpError.NOT_ALLOWED);
                    err.setDescription("Connection has not been authenticated");
                    _connectionEndpoint.close(err);
                    _amqpConnection.close();
                    _closedOnOpen = true;
                }
                else
                {
                    _amqpConnection.virtualHostAssociated();
                }
            }
        }
    }

    void setUserPrincipal(final Principal user)
    {
        Subject authSubject = _subjectCreator.createSubjectWithGroups(user);
        _amqpConnection.getSubject().getPrincipals().addAll(authSubject.getPrincipals());
        _amqpConnection.getSubject().getPublicCredentials().addAll(authSubject.getPublicCredentials());
        _amqpConnection.getSubject().getPrivateCredentials().addAll(authSubject.getPrivateCredentials());
        _amqpConnection.updateAccessControllerContext();
    }

    public void remoteSessionCreation(SessionEndpoint endpoint)
    {
        if(!_closedOnOpen)
        {
            final Session_1_0 session = new Session_1_0(this, endpoint);
            _sessions.add(session);
            _amqpConnection.sessionAdded(session);
            endpoint.setSessionEventListener(new SessionEventListener()
            {
                @Override
                public void remoteLinkCreation(final LinkEndpoint endpoint)
                {
                    AccessController.doPrivileged(new PrivilegedAction<Object>()
                    {
                        @Override
                        public Object run()
                        {
                            session.remoteLinkCreation(endpoint);
                            return null;
                        }
                    }, session.getAccessControllerContext());
                }

                @Override
                public void remoteEnd(final End end)
                {
                    AccessController.doPrivileged(new PrivilegedAction<Object>()
                    {
                        @Override
                        public Object run()
                        {
                            session.remoteEnd(end);
                            return null;
                        }
                    }, session.getAccessControllerContext());
                }
            });
        }
    }

    void sessionEnded(Session_1_0 session)
    {
        if(!_closedOnOpen)
        {
            _sessions.remove(session);
            _amqpConnection.sessionRemoved(session);
        }
    }

    private void addAsyncTask(final Action<Connection_1_0> action)
    {
        _asyncTaskList.add(action);
        notifyWork();
    }


    public void closeReceived()
    {
        Collection<Session_1_0> sessions = new ArrayList<>(_sessions);

        for(Session_1_0 session : sessions)
        {
            session.remoteEnd(new End());
        }
    }

    void performCloseTasks()
    {
        _amqpConnection.performDeleteTasks();
    }

    public void closed()
    {
        if (_vhost != null)
        {
            _vhost.deregisterConnection(_amqpConnection);
        }
        performCloseTasks();
        closeReceived();
    }


    public void sendConnectionCloseAsync(AMQConstant cause, String message)
    {
        Action<Connection_1_0> action = new Action<Connection_1_0>()
        {
            @Override
            public void performAction(final Connection_1_0 object)
            {
                _connectionEndpoint.close();

            }
        };
        addAsyncTask(action);
    }

    public void block()
    {
        // TODO
    }

    public void unblock()
    {
        // TODO
    }

    public void closeSessionAsync(final Session_1_0 session, final AMQConstant cause, final String message)
    {
        addAsyncTask(new Action<Connection_1_0>()
        {
            @Override
            public void performAction(final Connection_1_0 object)
            {
                session.close(cause, message);
            }
        });
    }

    public long getConnectionId()
    {
        return _connectionId;
    }

    public List<Session_1_0> getSessionModels()
    {
        return new ArrayList<>(_sessions);
    }

    public LogSubject getLogSubject()
    {
        return _logSubject;
    }

    public String getRemoteAddressString()
    {
        return String.valueOf(_connectionEndpoint.getRemoteAddress());
    }

    public String getClientId()
    {
        return _connectionEndpoint.getRemoteContainerId();
    }

    public String getRemoteContainerName()
    {
        return _connectionEndpoint.getRemoteContainerId();
    }

    public Principal getAuthorizedPrincipal()
    {
        Set<AuthenticatedPrincipal> authPrincipals = _amqpConnection.getSubject().getPrincipals(AuthenticatedPrincipal.class);
        return authPrincipals.isEmpty() ? null : authPrincipals.iterator().next();
    }

    public long getSessionCountLimit()
    {
        return 0;  // TODO
    }

    public AmqpPort<?> getPort()
    {
        return _port;
    }

    public AMQPConnection_1_0 getAmqpConnection()
    {
        return _amqpConnection;
    }

    public Transport getTransport()
    {
        return _transport;
    }

    Subject getSubject()
    {
        return _amqpConnection.getSubject();
    }

    public VirtualHostImpl getVirtualHost()
    {
        return _vhost;
    }


    public void transportStateChanged()
    {
        for (Session_1_0 session : _sessions)
        {
            session.transportStateChanged();
        }
    }

    public void notifyWork()
    {
        _amqpConnection.notifyWork();
    }

    public Iterator<Runnable> processPendingIterator()
    {
        return new ProcessPendingIterator();
    }

    private class ProcessPendingIterator implements Iterator<Runnable>
    {
        private final List<? extends AMQSessionModel<?>> _sessionsWithPending;
        private Iterator<? extends AMQSessionModel<?>> _sessionIterator;
        private ProcessPendingIterator()
        {
            _sessionsWithPending = new ArrayList<>(getSessionModels());
            _sessionIterator = _sessionsWithPending.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return !(_sessionsWithPending.isEmpty() && _asyncTaskList.isEmpty());
        }

        @Override
        public Runnable next()
        {
            if(!_sessionsWithPending.isEmpty())
            {
                if(!_sessionIterator.hasNext())
                {
                    _sessionIterator = _sessionsWithPending.iterator();
                }
                final AMQSessionModel<?> session = _sessionIterator.next();
                return new Runnable()
                {
                    @Override
                    public void run()
                    {
                        if(!session.processPending())
                        {
                            _sessionIterator.remove();
                        }
                    }
                };
            }
            else if(!_asyncTaskList.isEmpty())
            {
                final Action<? super Connection_1_0> asyncAction = _asyncTaskList.poll();
                return new Runnable()
                {
                    @Override
                    public void run()
                    {
                        asyncAction.performAction(Connection_1_0.this);
                    }
                };
            }
            else
            {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public String toString()
    {
        return "Connection_1_0["
               +  _connectionId
               + " "
               + _amqpConnection.getAddress()
               + (_vhost == null ? "" : (" vh : " + _vhost.getName()))
               + ']';
    }
}
