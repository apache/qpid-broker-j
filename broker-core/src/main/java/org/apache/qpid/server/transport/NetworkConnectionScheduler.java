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

    private volatile SelectorThread _selectorThread;
    private volatile ThreadPoolExecutor _executor;
    private final AtomicInteger _running = new AtomicInteger();
    private final int _poolSizeMinimum;
    private final int _poolSizeMaximum;
    private final String _name;

    public NetworkConnectionScheduler(final String name, int threadPoolSizeMinimum, int threadPoolSizeMaximum)
    {
        this(name, threadPoolSizeMinimum, threadPoolSizeMaximum, new ThreadFactory()
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

    public NetworkConnectionScheduler(String name, int threadPoolSizeMinimum, int threadPoolSizeMaximum, ThreadFactory factory)
    {
        _name = name;
        _poolSizeMaximum = threadPoolSizeMaximum;
        _poolSizeMinimum = threadPoolSizeMinimum;
        _factory = factory;
    }


    public void start()
    {
        try
        {
            _selectorThread = new SelectorThread(this);
            _selectorThread.start();
            _executor = new ThreadPoolExecutor(_poolSizeMinimum, _poolSizeMaximum, 0L, TimeUnit.MILLISECONDS,
                                               new LinkedBlockingQueue<Runnable>(), _factory);
            _executor.prestartAllCoreThreads();
        }
        catch (IOException e)
        {
            throw new TransportException(e);
        }
    }

    public void schedule(final NonBlockingConnection connection)
    {
        _executor.execute(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                String currentName = Thread.currentThread().getName();
                                try
                                {
                                    Thread.currentThread().setName(
                                            SelectorThread.IO_THREAD_NAME_PREFIX + connection.getRemoteAddress().toString());
                                    processConnection(connection);
                                }
                                finally
                                {
                                    Thread.currentThread().setName(currentName);
                                }
                            }
                        });
    }

    private void processConnection(final NonBlockingConnection connection)
    {
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
                            schedule(connection);
                        }
                        else
                        {
                            rerun = true;
                        }
                    }
                    else
                    {
                        _selectorThread.addConnection(connection);
                    }
                }
                else if(connection.getScheduler() != this)
                {
                    removeConnection(connection);
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
}
