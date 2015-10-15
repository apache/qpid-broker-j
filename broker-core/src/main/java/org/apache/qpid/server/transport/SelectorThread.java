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

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class SelectorThread extends Thread
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectorThread.class);

    static final String IO_THREAD_NAME_PREFIX  = "IO-";
    private final Queue<Runnable> _tasks = new ConcurrentLinkedQueue<>();

    /**
     * Queue of connections that are not currently scheduled and not registered with the selector.
     * These need to go back into the Selector.
     */
    private final Queue<NonBlockingConnection> _unregisteredConnections = new ConcurrentLinkedQueue<>();

    /** Set of connections that are currently being selected upon */
    private final Set<NonBlockingConnection> _unscheduledConnections = new HashSet<>();

    private final Selector _selector;
    private final AtomicBoolean _closed = new AtomicBoolean();
    private final AtomicBoolean _selecting = new AtomicBoolean();
    private final NetworkConnectionScheduler _scheduler;
    private long _nextTimeout;

    private final BlockingQueue<Runnable> _workQueue = new LinkedBlockingQueue<>();

    private Runnable _selectionTask = new Runnable()
    {
        @Override
        public void run()
        {
            performSelect();
        }
    };

    SelectorThread(final NetworkConnectionScheduler scheduler) throws IOException
    {
        _selector = Selector.open();
        _scheduler = scheduler;
        _workQueue.add(_selectionTask);
    }

    public void addAcceptingSocket(final ServerSocketChannel socketChannel,
                                   final NonBlockingNetworkTransport nonBlockingNetworkTransport)
    {
        _tasks.add(new Runnable()
        {
            @Override
            public void run()
            {

                try
                {
                    socketChannel.register(_selector, SelectionKey.OP_ACCEPT, nonBlockingNetworkTransport);
                }
                catch (IllegalStateException | ClosedChannelException e)
                {
                    // TODO Communicate condition back to model object to make it go into the ERROR state
                    LOGGER.error("Failed to register selector on accepting port", e);
                }             }
        });
        _selector.wakeup();
    }

    public void cancelAcceptingSocket(final ServerSocketChannel socketChannel)
    {
        _tasks.add(new Runnable()
        {
            @Override
            public void run()
            {
                SelectionKey selectionKey = socketChannel.keyFor(_selector);
                if (selectionKey != null)
                {
                    selectionKey.cancel();
                }
            }
        });
        _selector.wakeup();
    }

    private List<NonBlockingConnection> processUnscheduledConnections()
    {
        List<NonBlockingConnection> toBeScheduled = new ArrayList<>();

        long currentTime = System.currentTimeMillis();
        Iterator<NonBlockingConnection> iterator = _unscheduledConnections.iterator();
        _nextTimeout = Integer.MAX_VALUE;
        while (iterator.hasNext())
        {
            NonBlockingConnection connection = iterator.next();

            int period = connection.getTicker().getTimeToNextTick(currentTime);

            if (period <= 0 || connection.isStateChanged())
            {
                toBeScheduled.add(connection);
                try
                {
                    unregisterConnection(connection);
                }
                catch (ClosedChannelException e)
                {
                    LOGGER.debug("Failed to register with selector for connection " + connection +
                                 ". Connection is probably being closed by peer.", e);
                }
                iterator.remove();
            }
            else
            {
                _nextTimeout = Math.min(period, _nextTimeout);
            }
        }

        return toBeScheduled;
    }

    @Override
    public void run()
    {

        final String name = Thread.currentThread().getName();
        try
        {
            do
            {
                Thread.currentThread().setName(name);
                Runnable task = _workQueue.take();
                task.run();

            } while (!_closed.get());
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }

    }

    private void closeSelector()
    {
        try
        {
            if(_selector.isOpen())
            {
                _selector.close();
            }
        }
        catch (IOException e)

        {
            LOGGER.debug("Failed to close selector", e);
        }
    }

    private static final class ConnectionProcessor implements Runnable
    {

        private final NetworkConnectionScheduler _scheduler;
        private final NonBlockingConnection _connection;
        private AtomicBoolean _running = new AtomicBoolean();

        public ConnectionProcessor(final NetworkConnectionScheduler scheduler, final NonBlockingConnection connection)
        {
            _scheduler = scheduler;
            _connection = connection;
        }

        @Override
        public void run()
        {
            if(_running.compareAndSet(false,true))
            {
                _scheduler.processConnection(_connection);
            }
        }
    }

    private void performSelect()
    {
        while(!_closed.get())
        {
            if(_selecting.compareAndSet(false,true))
            {
                List<ConnectionProcessor> connections = new ArrayList<>();
                try
                {
                    if (!_closed.get())
                    {
                        Thread.currentThread().setName("Selector-" + _scheduler.getName());
                        try
                        {
                             _selector.select(_nextTimeout);
                        }
                        catch (IOException e)
                        {
                            // TODO Inform the model object
                            LOGGER.error("Failed to trying to select()", e);
                            closeSelector();
                            return;
                        }
                        runTasks();
                        for (NonBlockingConnection connection : processSelectionKeys())
                        {
                            if(connection.setScheduled())
                            {
                                connections.add(new ConnectionProcessor(_scheduler, connection));
                            }
                        }
                        for (NonBlockingConnection connection : reregisterUnregisteredConnections())
                        {
                            if(connection.setScheduled())
                            {
                                connections.add(new ConnectionProcessor(_scheduler, connection));
                            }
                        }
                        for (NonBlockingConnection connection : processUnscheduledConnections())
                        {
                            if(connection.setScheduled())
                            {
                                connections.add(new ConnectionProcessor(_scheduler, connection));
                            }
                        }

                    }
                }
                finally
                {
                    _selecting.set(false);
                }
                _workQueue.add(_selectionTask);
                _workQueue.addAll(connections);
                for(ConnectionProcessor connectionProcessor : connections)
                {
                    connectionProcessor.run();
                }

            }
            else
            {
                break;
            }
        }

        if(_closed.get() && _selecting.compareAndSet(false,true))
        {
            closeSelector();
        }
    }

    private void unregisterConnection(final NonBlockingConnection connection) throws ClosedChannelException
    {
        SelectionKey register = connection.getSocketChannel().register(_selector, 0);
        register.cancel();
    }

    private List<NonBlockingConnection> reregisterUnregisteredConnections()
    {
        List<NonBlockingConnection> unregisterableConnections = new ArrayList<>();

        while (_unregisteredConnections.peek() != null)
        {
            NonBlockingConnection unregisteredConnection = _unregisteredConnections.poll();
            _unscheduledConnections.add(unregisteredConnection);


            final int ops = (unregisteredConnection.canRead() ? SelectionKey.OP_READ : 0)
                            | (unregisteredConnection.waitingForWrite() ? SelectionKey.OP_WRITE : 0);
            try
            {
                unregisteredConnection.getSocketChannel().register(_selector, ops, unregisteredConnection);
            }
            catch (ClosedChannelException e)
            {
                unregisterableConnections.add(unregisteredConnection);
            }
        }

        return unregisterableConnections;
    }

    private List<NonBlockingConnection> processSelectionKeys()
    {
        List<NonBlockingConnection> toBeScheduled = new ArrayList<>();

        Set<SelectionKey> selectionKeys = _selector.selectedKeys();
        for (SelectionKey key : selectionKeys)
        {
            if(key.isAcceptable())
            {
                NonBlockingNetworkTransport transport = (NonBlockingNetworkTransport) key.attachment();
                // todo - should we schedule this rather than running in this thread?
                transport.acceptSocketChannel((ServerSocketChannel)key.channel());
            }
            else
            {
                NonBlockingConnection connection = (NonBlockingConnection) key.attachment();

                try
                {
                    key.channel().register(_selector, 0);
                }
                catch (ClosedChannelException e)
                {
                    // Ignore - we will schedule the connection anyway
                }

                toBeScheduled.add(connection);
                _unscheduledConnections.remove(connection);
            }

        }
        selectionKeys.clear();

        return toBeScheduled;
    }

    private void runTasks()
    {
        while(_tasks.peek() != null)
        {
            Runnable task = _tasks.poll();
            task.run();
        }
    }

    private boolean selectionInterestRequiresUpdate(NonBlockingConnection connection)
    {
        final SelectionKey selectionKey = connection.getSocketChannel().keyFor(_selector);
        int expectedOps = connection.canRead() ? SelectionKey.OP_READ : 0;
        if(connection.waitingForWrite())
        {
            expectedOps |= SelectionKey.OP_WRITE;
        }
        try
        {
            return selectionKey == null || !selectionKey.isValid() || selectionKey.interestOps() != expectedOps;
        }
        catch (CancelledKeyException e)
        {
            return true;
        }
    }

    public void addConnection(final NonBlockingConnection connection)
    {
        if(selectionInterestRequiresUpdate(connection))
        {
            _unregisteredConnections.add(connection);
            _selector.wakeup();
        }

    }

    void removeConnection(NonBlockingConnection connection)
    {
        try
        {
            unregisterConnection(connection);
        }
        catch (ClosedChannelException e)
        {
            LOGGER.debug("Failed to unregister with selector for connection " + connection +
                         ". Connection is probably being closed by peer.", e);

        }
    }

    public void wakeup()
    {
        _selector.wakeup();
    }

    public void close()
    {
        Runnable goodNight = new Runnable()
        {
            @Override
            public void run()
            {
                // Make sure take awakes so it can observe _closed
            }
        };
        _closed.set(true);

        int count = _scheduler.getPoolSizeMaximum();
        while(count-- > 0)
        {
            _workQueue.offer(goodNight);
        }

        _selector.wakeup();

    }

     public void addToWork(final NonBlockingConnection connection)
     {
         if(connection.setScheduled())
         {
             _workQueue.add(new ConnectionProcessor(_scheduler, connection));
         }

     }
}
