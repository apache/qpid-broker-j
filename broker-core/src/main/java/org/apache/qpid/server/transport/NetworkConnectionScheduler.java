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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

public class NetworkConnectionScheduler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkConnectionScheduler.class);
    private final ThreadFactory _factory;
    private final String _selectorThreadName;
    private volatile ThreadPoolExecutor _executor;
    private final AtomicInteger _running = new AtomicInteger();
    private final int _poolSize;
    private final long _threadKeepAliveTimeout;
    private final String _name;
    private final int _numberOfSelectors;
    private SelectorThread _selectorThread;

    public NetworkConnectionScheduler(final String name,
                                      final int numberOfSelectors, int threadPoolSize,
                                      long threadKeepAliveTimeout)
    {
        this(name, numberOfSelectors, threadPoolSize, threadKeepAliveTimeout, new ThreadFactory()
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

    @Override
    public String toString()
    {
        return "NetworkConnectionScheduler{" +
               "_factory=" + _factory +
               ", _executor=" + _executor +
               ", _running=" + _running +
               ", _poolSize=" + _poolSize +
               ", _threadKeepAliveTimeout=" + _threadKeepAliveTimeout +
               ", _name='" + _name + '\'' +
               ", _numberOfSelectors=" + _numberOfSelectors +
               ", _selectorThread=" + _selectorThread +
               '}';
    }

    public NetworkConnectionScheduler(String name,
                                      final int numberOfSelectors, int threadPoolSize,
                                      long threadKeepAliveTimeout,
                                      ThreadFactory factory)
    {
        _name = name;
        _poolSize = threadPoolSize;
        _threadKeepAliveTimeout = threadKeepAliveTimeout;
        _factory = factory;
        _numberOfSelectors = numberOfSelectors;
        _selectorThreadName = "Selector-"+name;
    }


    public void start()
    {
        try
        {
            _selectorThread = new SelectorThread(this, _numberOfSelectors);
            final int corePoolSize = _poolSize;
            final int maximumPoolSize = _poolSize;
            final long keepAliveTime = _threadKeepAliveTimeout;
            final java.util.concurrent.BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
            final ThreadFactory factory = _factory;
            _executor = new ThreadPoolExecutor(corePoolSize,
                                               maximumPoolSize,
                                               keepAliveTime,
                                               TimeUnit.MINUTES,
                                               workQueue,
                                               QpidByteBuffer.createQpidByteBufferTrackingThreadFactory(factory));
            _executor.prestartAllCoreThreads();
            _executor.allowCoreThreadTimeOut(true);
            for(int i = 0 ; i < _poolSize; i++)
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
        Thread.currentThread().setName(connection.getThreadName());
        connection.doPreWork();
        boolean rerun;
        do
        {
            rerun = false;
            boolean closed = connection.doWork();
            if (!closed && connection.getScheduler() == this)
            {

                if (connection.isStateChanged() || connection.isPartialRead())
                {
                    if (_running.get() == _poolSize)
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
                    if (connection.isStateChanged())
                    {
                        _selectorThread.addToWork(connection);
                    }
                    else
                    {
                        _selectorThread.returnConnectionToSelector(connection);
                    }
                }
            }
            else if (connection.getScheduler() != this)
            {
                removeConnection(connection);
                connection.clearScheduled();
                connection.getScheduler().addConnection(connection);
            }

        } while (rerun);

    }

    void decrementRunningCount()
    {
        _running.decrementAndGet();
    }

    void incrementRunningCount()
    {
        _running.incrementAndGet();
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

    public String getSelectorThreadName()
    {
        return _selectorThreadName;
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

    public void removeConnection(final NonBlockingConnection connection)
    {
        _selectorThread.removeConnection(connection);
    }

    int getPoolSize()
    {
        return _poolSize;
    }

    public void schedule(final NonBlockingConnection connection)
    {
        _selectorThread.addToWork(connection);
    }
}
