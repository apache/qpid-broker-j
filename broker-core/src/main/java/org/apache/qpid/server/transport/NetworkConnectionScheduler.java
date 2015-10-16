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
package org.apache.qpid.server.transport;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.transport.TransportException;

public class NetworkConnectionScheduler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkConnectionScheduler.class);
    private final ThreadFactory _factory;
    private volatile ThreadPoolExecutor _executor;
    private final AtomicInteger _running = new AtomicInteger();
    private final int _poolSizeMinimum;
    private final int _poolSizeMaximum;
    private final long _threadKeepAliveTimeout;
    private final String _name;
    private SelectorThread _selectorThread;

    public NetworkConnectionScheduler(final String name,
                                      int threadPoolSizeMinimum,
                                      int threadPoolSizeMaximum,
                                      long threadKeepAliveTimeout)
    {
        this(name, threadPoolSizeMinimum, threadPoolSizeMaximum, threadKeepAliveTimeout, new ThreadFactory()
                                    {
                                        final AtomicInteger _count = new AtomicInteger();

                                        @Override
                                        public Thread newThread(final Runnable r)
                                        {
                                            Thread t = Executors.defaultThreadFactory().newThread(r);
                                            t.setName("IO-pool-" + name + "-" + _count.incrementAndGet());
                                            return t;
                                        }
                                    });
    }

    public NetworkConnectionScheduler(String name,
                                      int threadPoolSizeMinimum,
                                      int threadPoolSizeMaximum,
                                      long threadKeepAliveTimeout,
                                      ThreadFactory factory)
    {
        _name = name;
        _poolSizeMaximum = threadPoolSizeMaximum;
        _poolSizeMinimum = threadPoolSizeMinimum;
        _threadKeepAliveTimeout = threadKeepAliveTimeout;
        _factory = factory;
    }


    public void start()
    {
        try
        {
            _selectorThread = new SelectorThread(this);
            _executor = new ThreadPoolExecutor(_poolSizeMaximum, _poolSizeMaximum,
                                               _threadKeepAliveTimeout, TimeUnit.MINUTES,
                                               new LinkedBlockingQueue<Runnable>(), _factory);
            _executor.prestartAllCoreThreads();
            _executor.allowCoreThreadTimeOut(true);
            for(int i = 0 ; i < _poolSizeMaximum; i++)
            {
                _executor.execute(_selectorThread);
            }
        }
        catch (IOException e)
        {
            throw new TransportException(e);
        }
    }

    void processConnection(final NonBlockingConnection connection)
    {
        Thread.currentThread().setName(
                SelectorThread.IO_THREAD_NAME_PREFIX + connection.getRemoteAddress().toString());
        try
        {
            _running.incrementAndGet();
            boolean rerun;
            do
            {
                rerun = false;
                boolean closed = connection.doWork();

                if (!closed && connection.getScheduler() == this)
                {

                    if (connection.isStateChanged() || connection.isPartialRead())
                    {
                        if (_running.get() == _poolSizeMaximum)
                        {
                            connection.clearScheduled();
                            schedule(connection);
                        }
                        else
                        {
                            rerun = true;
                        }
                    }
                    else
                    {
                        connection.clearScheduled();
                        if(connection.isStateChanged())
                        {
                            _selectorThread.addToWork(connection);
                        }
                        else
                        {
                            _selectorThread.addConnection(connection);
                        }
                    }
                }
                else if(connection.getScheduler() != this)
                {
                    removeConnection(connection);
                    connection.clearScheduled();
                    connection.getScheduler().addConnection(connection);
                }

            } while (rerun);
        }
        finally
        {
            _running.decrementAndGet();
        }

    }

    public void close()
    {
        if(_selectorThread != null)
        {
            _selectorThread.close();
        }
        if(_executor != null)
        {
            _executor.shutdown();
        }
    }


    public String getName()
    {
        return _name;
    }

    public void addAcceptingSocket(final ServerSocketChannel serverSocket,
                                   final NonBlockingNetworkTransport nonBlockingNetworkTransport)
    {
        _selectorThread.addAcceptingSocket(serverSocket, nonBlockingNetworkTransport);
    }

    public void cancelAcceptingSocket(final ServerSocketChannel serverSocket)
    {
        _selectorThread.cancelAcceptingSocket(serverSocket);
    }

    public void addConnection(final NonBlockingConnection connection)
    {
        _selectorThread.addConnection(connection);
    }

    public void wakeup()
    {
        _selectorThread.wakeup();
    }

    public void removeConnection(final NonBlockingConnection connection)
    {
        _selectorThread.removeConnection(connection);
    }

    int getPoolSizeMaximum()
    {
        return _poolSizeMaximum;
    }

    public void schedule(final NonBlockingConnection connection)
    {
        _selectorThread.addToWork(connection);
    }
}
