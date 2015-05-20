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
package org.apache.qpid.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.*;
import javax.jms.Connection;
import javax.jms.IllegalStateException;
import javax.jms.Session;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.AMQException;
import org.apache.qpid.client.util.JMSExceptionHelper;
import org.apache.qpid.jms.*;
import org.apache.qpid.url.URLSyntaxException;

public class PooledConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PooledConnectionFactory.class);

    private static final ScheduledExecutorService SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactory()
                    {
                        private ThreadGroup _group;
                        {
                            SecurityManager securityManager = System.getSecurityManager();
                            _group = securityManager == null
                                    ? Thread.currentThread().getThreadGroup()
                                    : securityManager.getThreadGroup();

                        }

                        @Override
                        public Thread newThread(Runnable runnable)
                        {
                            Thread thread = new Thread(_group,
                                                       runnable,
                                                       PooledConnectionFactory.class.getSimpleName() + "-Reaper");
                            if (!thread.isDaemon())
                            {
                                thread.setDaemon(true);
                            }
                            return thread;
                        }

                    });

    private final AtomicInteger _maxPoolSize = new AtomicInteger(10);
    private final AtomicLong _connectionTimeout = new AtomicLong(30000l);

    private final AtomicReference<ConnectionURL> _connectionDetails = new AtomicReference<>();

    transient private final byte[] _factoryId = new byte[16];
    transient private final Map<ConnectionDetailsIdentifier, List<ConnectionHolder>> _pool = Collections.synchronizedMap(new HashMap<ConnectionDetailsIdentifier, List<ConnectionHolder>>());
    transient private final Runnable _connectionReaper = new Runnable()
                                                {
                                                    @Override
                                                    public void run()
                                                    {
                                                        _reaperScheduled.set(false);
                                                        if(removeExpiredConnections())
                                                        {
                                                            scheduleReaper();
                                                        }
                                                    }
                                                };


    transient private final AtomicBoolean _reaperScheduled = new AtomicBoolean();

    public PooledConnectionFactory()
    {
        final Random random = new Random();
        random.nextBytes(_factoryId);

    }

    private void scheduleReaper()
    {
        if(_reaperScheduled.compareAndSet(false,true))
        {
            SCHEDULER.schedule(_connectionReaper, _connectionTimeout.get(), TimeUnit.MILLISECONDS);
        }
    }

    private boolean removeExpiredConnections()
    {
        try
        {
            boolean scheduleAgain = false;
            List<List<ConnectionHolder>> pooledConnections;
            synchronized (_pool)
            {
                pooledConnections = new ArrayList<>(_pool.values());
            }
            if(!pooledConnections.isEmpty())
            {
                long now = System.currentTimeMillis();
                for (List<ConnectionHolder> connections : pooledConnections)
                {
                    synchronized (connections)
                    {
                        removeExpiredConnections(connections, now);
                        scheduleAgain = scheduleAgain || !connections.isEmpty();
                    }

                }
            }
            return scheduleAgain;
        }
        catch(RuntimeException e)
        {
            LOGGER.warn("Error encountered in " + PooledConnectionFactory.class.getSimpleName() + " reaper", e);
            return true;
        }
    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException
    {
        return getConnectionFromPool();
    }

    @Override
    public QueueConnection createQueueConnection(final String userName, final String password) throws JMSException
    {
        return getConnectionFromPool(userName, password);
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException
    {
        return getConnectionFromPool();
    }

    @Override
    public TopicConnection createTopicConnection(final String userName, final String password) throws JMSException
    {
        return getConnectionFromPool(userName, password);
    }

    @Override
    public Connection createConnection() throws JMSException
    {
        return getConnectionFromPool();
    }

    @Override
    public Connection createConnection(final String userName, final String password) throws JMSException
    {
        return getConnectionFromPool(userName, password);
    }


    private CommonConnection getConnectionFromPool() throws JMSException
    {
        final ConnectionURL connectionDetails = getConnectionURLOrError();
        ConnectionDetailsIdentifier identity = ConnectionDetailsIdentifier.newInstance(_factoryId,
                                                                                       connectionDetails.getURL(),
                                                                                       connectionDetails.getUsername(),
                                                                                       connectionDetails.getPassword());

        return getConnectionFromPool(connectionDetails, identity);
    }

    private CommonConnection getConnectionFromPool(final ConnectionURL connectionDetails,
                                                   final ConnectionDetailsIdentifier identity)
            throws JMSException
    {
        CommonConnection underlying = null;

        synchronized (_pool)
        {
            List<ConnectionHolder> pooledConnections = _pool.get(identity);
            if(pooledConnections != null)
            {
                synchronized (pooledConnections)
                {
                    if (!pooledConnections.isEmpty())
                    {
                        ConnectionHolder holder = pooledConnections.remove(pooledConnections.size() - 1);
                        underlying = holder._connection;
                    }
                }
            }
        }


        try
        {
            if(underlying == null)
            {
                underlying = newConnectionInstance(connectionDetails);
            }
            return proxyConnection(underlying, identity);
        }
        catch (AMQException e)
        {
            throw JMSExceptionHelper.chainJMSException(new JMSException("Error creating connection: " + e.getMessage()),
                                                       e);
        }
    }

    protected CommonConnection newConnectionInstance(final ConnectionURL connectionDetails) throws AMQException
    {
        return new AMQConnection(connectionDetails);
    }

    private ConnectionURL getConnectionURLOrError() throws IllegalStateException
    {
        final ConnectionURL connectionDetails = _connectionDetails.get();
        if(connectionDetails == null)
        {
            throw new IllegalStateException("Cannot create a connection when the connection URL has not yet been set");
        }
        return connectionDetails;
    }

    private CommonConnection getConnectionFromPool(String user, String password) throws JMSException
    {
        ConnectionURL connectionDetails = getConnectionURLOrError();
        ConnectionDetailsIdentifier identity = ConnectionDetailsIdentifier.newInstance(_factoryId,
                                                                                       connectionDetails.getURL(),
                                                                                       user,
                                                                                       password);
        try
        {
            connectionDetails = new AMQConnectionURL(connectionDetails.getURL());
            connectionDetails.setUsername(user);
            connectionDetails.setPassword(password);
        }
        catch (URLSyntaxException e)
        {
            throw JMSExceptionHelper.chainJMSException(new JMSException("Error creating connection: " + e.getMessage()),
                                                       e);
        }
        return getConnectionFromPool(connectionDetails, identity);
    }



    private synchronized void returnToPool(final CommonConnection connection, final ConnectionDetailsIdentifier identityHash)
            throws JMSException
    {
        if(!connection.isClosed())
        {
            connection.stop();
            List<ConnectionHolder> connections;
            synchronized (_pool)
            {
                connections = _pool.get(identityHash);
                if(connections == null)
                {
                    connections = new ArrayList<>();
                    _pool.put(identityHash, connections);
                    scheduleReaper();
                }
            }
            synchronized (connections)
            {
                if(connections.size()<_maxPoolSize.get())
                {
                    connections.add(new ConnectionHolder(connection, System.currentTimeMillis()));
                }
            }
        }
    }

    private void removeExpiredConnections(final List<ConnectionHolder> connections,
                                          final long now)
    {
        long expiryTime = now - _connectionTimeout.get();
        Iterator<ConnectionHolder> iter = connections.iterator();
        while(iter.hasNext())
        {
            ConnectionHolder ch = iter.next();
            if(ch._lastUse < expiryTime)
            {
                iter.remove();
                try
                {
                    ch._connection.close();
                }
                catch (JMSException | RuntimeException e )
                {
                    LOGGER.warn("Error when closing expired connection in pool", e);
                }
            }
        }
    }

    public int getMaxPoolSize()
    {
        return _maxPoolSize.get();
    }

    public long getConnectionTimeout()
    {
        return _connectionTimeout.get();
    }

    public void setMaxPoolSize(int maxPoolSize)
    {
        _maxPoolSize.set(maxPoolSize);
    }

    public void setConnectionTimeout(long timeout)
    {
        _connectionTimeout.set(timeout);
    }


    public synchronized ConnectionURL getConnectionURL()
    {
        return _connectionDetails.get();
    }

    public synchronized String getConnectionURLString()
    {
        return _connectionDetails.toString();
    }

    //setter necessary to use instances created with the default constructor (which we can't remove)
    public synchronized final void setConnectionURLString(String url) throws URLSyntaxException
    {
        final AMQConnectionURL connectionDetails = new AMQConnectionURL(url);
        if(!_connectionDetails.compareAndSet(null, connectionDetails))
        {
            throw new IllegalArgumentException("Cannot change factory URL after it has already been set");
        }
    }

    public Reference getReference() throws NamingException
    {
        return new Reference(
                PooledConnectionFactory.class.getName(),
                new StringRefAddr(PooledConnectionFactory.class.getName(), _connectionDetails.get().getURL()),
                PooledConnectionFactory.class.getName(), null);          // factory location
    }

    private CommonConnection proxyConnection(CommonConnection underlying, ConnectionDetailsIdentifier identifier) throws JMSException
    {

        final ConnectionInvocationHandler invocationHandler = new ConnectionInvocationHandler(underlying, identifier);
        return (CommonConnection) Proxy.newProxyInstance(getClass().getClassLoader(),
                                                         new Class[] { CommonConnection.class },
                                                         invocationHandler);
    }

    private <X extends Session> X proxySession(X underlying, ConnectionInvocationHandler connectionHandler)
    {
        List<Class<?>> interfaces = new ArrayList<>();
        interfaces.add(Session.class);
        if(underlying instanceof org.apache.qpid.jms.Session)
        {
            interfaces.add(org.apache.qpid.jms.Session.class);
        }
        if(underlying instanceof TopicSession)
        {
            interfaces.add(TopicSession.class);
        }
        if(underlying instanceof QueueSession)
        {
            interfaces.add(QueueSession.class);
        }
        return (X) Proxy.newProxyInstance(getClass().getClassLoader(),
                                          interfaces.toArray(new Class[interfaces.size()]),
                                          new SessionInvocationHandler<X>(underlying, connectionHandler));
    }

    private class ConnectionInvocationHandler  implements InvocationHandler, ExceptionListener
    {
        private final CommonConnection _underlyingConnection;
        private final ConnectionDetailsIdentifier _identityHash;
        private boolean _closed;
        private volatile boolean _exceptionThrown;
        private final List<Session> _openSessions = new ArrayList<>();
        private volatile ExceptionListener _exceptionListener;

        public ConnectionInvocationHandler(final CommonConnection underlying, ConnectionDetailsIdentifier identityHash) throws JMSException
        {
            _underlyingConnection = underlying;
            _underlyingConnection.setExceptionListener(this);
            _identityHash = identityHash;
        }

        @Override
        public synchronized Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable
        {
            if(_closed)
            {
                throw new IllegalStateException("Connection is closed");
            }
            Method underlyingMethod = _underlyingConnection.getClass().getMethod(method.getName(), method.getParameterTypes());
            if(method.getName().equals("getExceptionListener"))
            {
                return _exceptionListener;
            }
            else if(method.getName().equals("setExceptionListener") && method.getParameterTypes().length == 1 && method.getParameterTypes()[0].equals(ExceptionListener.class))
            {
                _exceptionListener = (ExceptionListener) args[0];
                return null;
            }
            else if(method.getName().equals("close") && method.getParameterTypes().length == 0)
            {
                _closed = true;
                _exceptionListener = null;
                List<Session> openSessions = new ArrayList<>(_openSessions);
                for(Session session : openSessions)
                {
                    try
                    {
                        session.close();
                    }
                    catch(JMSException | RuntimeException | Error e)
                    {
                        _exceptionThrown = true;
                        try
                        {
                            _underlyingConnection.close();
                        }
                        finally
                        {
                            throw e;
                        }
                    }
                }
                _openSessions.clear();

                if(!_exceptionThrown)
                {
                    returnToPool(_underlyingConnection, _identityHash);
                }
                else
                {
                    _underlyingConnection.close();
                }

                return null;
            }
            else
            {
                try
                {
                    Object returnVal = underlyingMethod.invoke(_underlyingConnection, args);

                    if(returnVal instanceof Session)
                    {
                        returnVal = proxySession((Session)returnVal, this);
                        _openSessions.add((Session)returnVal);
                    }
                    return returnVal;
                }
                catch (InvocationTargetException e)
                {
                    _exceptionThrown = true;
                    Throwable thrown = e.getCause();
                    throw thrown == null ? e : thrown;
                }
            }
        }


        @Override
        public void onException(final JMSException exception)
        {
            _exceptionThrown = true;
            ExceptionListener exceptionListener = _exceptionListener;
            if(exceptionListener != null)
            {
                exceptionListener.onException(exception);
            }
        }

        public synchronized void removeSession(final Session session)
        {
            _openSessions.remove(session);
        }
    }

    private class SessionInvocationHandler<X extends Session> implements InvocationHandler
    {
        private final X _underlying;
        private final ConnectionInvocationHandler _connectionHandler;

        public SessionInvocationHandler(final X underlying,
                                        final ConnectionInvocationHandler connectionHandler)
        {
            _underlying = underlying;
            _connectionHandler = connectionHandler;
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable
        {
            Method underlyingMethod = _underlying.getClass().getMethod(method.getName(), method.getParameterTypes());
            try
            {
                Object returnVal = underlyingMethod.invoke(_underlying, args);

                if(method.getName().equals("close") && method.getParameterTypes().length == 0)
                {
                    _connectionHandler.removeSession((Session)proxy);
                }

                return returnVal;
            }
            catch (InvocationTargetException e)
            {
                _connectionHandler._exceptionThrown = true;
                Throwable thrown = e.getCause();
                throw thrown == null ? e : thrown;
            }

        }
    }

    private static class ConnectionDetailsIdentifier
    {
        private final byte[] _urlHash;
        private final String _user;
        private final byte[] _userPasswordHash;

        private static ConnectionDetailsIdentifier newInstance(byte[] id, String url, final String user, String password)
        {
            try
            {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                digest.update(id);
                digest.update(url.getBytes(StandardCharsets.UTF_8));
                byte[] urlHash = digest.digest();

                digest.update(id);
                if(user != null)
                {
                    digest.update(user.getBytes(StandardCharsets.UTF_8));
                }
                if(password != null)
                {
                    digest.update(password.getBytes(StandardCharsets.UTF_8));
                }
                byte[] userPasswordHash = digest.digest();

                return new ConnectionDetailsIdentifier(urlHash, user == null ? "" : user, userPasswordHash);

            }
            catch (NoSuchAlgorithmException e)
            {
                throw new RuntimeException("SHA-256 not found, however compliant Java implementations should always provide SHA-256", e);
            }
        }

        private ConnectionDetailsIdentifier(final byte[] urlHash, final String user, final byte[] userPasswordHash)
        {
            _urlHash = urlHash;
            _user = user;
            _userPasswordHash = userPasswordHash;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final ConnectionDetailsIdentifier that = (ConnectionDetailsIdentifier) o;

            if (!Arrays.equals(_urlHash, that._urlHash))
            {
                return false;
            }
            if (!_user.equals(that._user))
            {
                return false;
            }
            return Arrays.equals(_userPasswordHash, that._userPasswordHash);

        }

        @Override
        public int hashCode()
        {
            int result = Arrays.hashCode(_urlHash);
            result = 31 * result + _user.hashCode();
            result = 31 * result + Arrays.hashCode(_userPasswordHash);
            return result;
        }
    }

    private class ConnectionHolder
    {
        private final CommonConnection _connection;
        private final long _lastUse;

        public ConnectionHolder(final CommonConnection connection, final long lastUse)
        {
            _connection = connection;
            _lastUse = lastUse;
        }
    }

}
