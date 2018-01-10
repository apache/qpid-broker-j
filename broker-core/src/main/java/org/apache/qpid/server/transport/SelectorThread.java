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
import java.net.SocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.CommonProperties;


class SelectorThread extends Thread
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectorThread.class);
    private static final long ACCEPT_CANCELLATION_TIMEOUT =
            Integer.getInteger(CommonProperties.IO_NETWORK_TRANSPORT_TIMEOUT_PROP_NAME,
                               CommonProperties.IO_NETWORK_TRANSPORT_TIMEOUT_DEFAULT);

    static final String IO_THREAD_NAME_PREFIX  = "IO-";
    private final Queue<Runnable> _tasks = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean _closed = new AtomicBoolean();
    private final NetworkConnectionScheduler _scheduler;

    private final BlockingQueue<Runnable> _workQueue = new LinkedBlockingQueue<>();
    private final  AtomicInteger _nextSelectorTaskIndex = new AtomicInteger();

    public final class SelectionTask implements Runnable
    {
        private final Selector _selector;
        private final AtomicBoolean _selecting = new AtomicBoolean();
        private final AtomicBoolean _inSelect = new AtomicBoolean();
        private final AtomicInteger _wakeups = new AtomicInteger();
        private long _nextTimeout;

        /**
         * Queue of connections that are not currently scheduled and not registered with the selector.
         * These need to go back into the Selector.
         */
        private final Queue<NonBlockingConnection> _unregisteredConnections = new ConcurrentLinkedQueue<>();

        /** Set of connections that are currently being selected upon */
        private final Set<NonBlockingConnection> _unscheduledConnections = new HashSet<>();



        private SelectionTask() throws IOException
        {
            _selector = Selector.open();
        }

        @Override
        public void run()
        {
            performSelect();
        }

        public boolean acquireSelecting()
        {
            return _selecting.compareAndSet(false,true);
        }

        public void clearSelecting()
        {
            _selecting.set(false);
        }

        public Selector getSelector()
        {
            return _selector;
        }

        public Queue<NonBlockingConnection> getUnregisteredConnections()
        {
            return _unregisteredConnections;
        }

        public Set<NonBlockingConnection> getUnscheduledConnections()
        {
            return _unscheduledConnections;
        }

        private List<NonBlockingConnection> processUnscheduledConnections()
        {
            _nextTimeout = Integer.MAX_VALUE;
            if (getUnscheduledConnections().isEmpty())
            {
                return Collections.emptyList();
            }

            List<NonBlockingConnection> toBeScheduled = new ArrayList<>();

            long currentTime = System.currentTimeMillis();
            Iterator<NonBlockingConnection> iterator = getUnscheduledConnections().iterator();
            while (iterator.hasNext())
            {
                NonBlockingConnection connection = iterator.next();

                final AggregateTicker ticker = connection.getTicker();
                int period = ticker.getTimeToNextTick(currentTime);
                ticker.resetModified();

                if (period <= 0 || connection.isStateChanged())
                {
                    toBeScheduled.add(connection);
                    try
                    {
                        connection.getSocketChannel().register(_selector, 0, connection);
                    }
                    catch (ClosedChannelException | CancelledKeyException e)
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

            // QPID-7447: prevent unnecessary allocation of empty iterator
            return toBeScheduled.isEmpty() ? Collections.<NonBlockingConnection>emptyList() : toBeScheduled;
        }

        private List<NonBlockingConnection> processSelectionKeys()
        {
            Set<SelectionKey> selectionKeys = _selector.selectedKeys();
            if (selectionKeys.isEmpty())
            {
                return Collections.emptyList();
            }

            List<NonBlockingConnection> toBeScheduled = new ArrayList<>();
            for (SelectionKey key : selectionKeys)
            {
                if (key.attachment() instanceof NonBlockingNetworkTransport)
                {
                    final NonBlockingNetworkTransport transport = (NonBlockingNetworkTransport) key.attachment();
                    final ServerSocketChannel channel = (ServerSocketChannel) key.channel();
                    final SocketAddress localSocketAddress = channel.socket().getLocalSocketAddress();

                    try
                    {
                        channel.register(_selector, 0, transport);
                    }
                    catch (ClosedChannelException e)
                    {
                        LOGGER.error("Failed to register selector on accepting port {} ",
                                     localSocketAddress, e);
                    }
                    catch (CancelledKeyException e)
                    {
                        LOGGER.info("Failed to register selector on accepting port {}"
                                    + " because selector key is already cancelled", localSocketAddress, e);
                    }

                    _workQueue.add(() -> {
                            try
                            {
                                _scheduler.incrementRunningCount();
                                transport.acceptSocketChannel(channel);
                            }
                            finally
                            {
                                try
                                {
                                    channel.register(_selector, SelectionKey.OP_ACCEPT, transport);
                                    wakeup();
                                }
                                catch (ClosedSelectorException e)
                                {
                                    LOGGER.info(
                                            "Failed to register selector on accepting port {} because selector is"
                                            + " already closed. This is probably a harmless race-condition (QPID-7399)",
                                            localSocketAddress);
                                }
                                catch (ClosedChannelException e)
                                {
                                    LOGGER.error("Failed to register selector on accepting port {}",
                                                 localSocketAddress, e);
                                }
                                catch (CancelledKeyException e)
                                {
                                    LOGGER.info("Failed to register selector on accepting port {}"
                                                + " because selector key is already cancelled", localSocketAddress, e);
                                }
                                finally
                                {
                                    _scheduler.decrementRunningCount();
                                }
                            }
                    });
                }
                else
                {
                    NonBlockingConnection connection = (NonBlockingConnection) key.attachment();
                    if(connection != null)
                    {
                        try
                        {
                            key.channel().register(_selector, 0, connection);
                        }
                        catch (ClosedChannelException | CancelledKeyException e)
                        {
                            // Ignore - we will schedule the connection anyway
                        }

                        toBeScheduled.add(connection);
                        getUnscheduledConnections().remove(connection);
                    }
                }

            }
            selectionKeys.clear();

            return toBeScheduled;
        }

        private List<NonBlockingConnection> reregisterUnregisteredConnections()
        {
            if (getUnregisteredConnections().isEmpty())
            {
                return Collections.emptyList();
            }
            List<NonBlockingConnection> unregisterableConnections = new ArrayList<>();

            NonBlockingConnection unregisteredConnection;
            while ((unregisteredConnection = getUnregisteredConnections().poll()) != null)
            {
                getUnscheduledConnections().add(unregisteredConnection);


                final int ops = (unregisteredConnection.wantsRead() ? SelectionKey.OP_READ : 0)
                                | (unregisteredConnection.wantsWrite() ? SelectionKey.OP_WRITE : 0);
                try
                {
                    unregisteredConnection.getSocketChannel().register(_selector, ops, unregisteredConnection);
                }
                catch (ClosedChannelException | CancelledKeyException e)
                {
                    unregisterableConnections.add(unregisteredConnection);
                }
            }

            // QPID-7447: prevent unnecessary allocation of empty iterator
            return unregisterableConnections.isEmpty() ? Collections.<NonBlockingConnection>emptyList() : unregisterableConnections;
        }

        private void performSelect()
        {
            _scheduler.incrementRunningCount();
            try
            {
                while (!_closed.get())
                {
                    if (acquireSelecting())
                    {
                        List<ConnectionProcessor> connections = new ArrayList<>();
                        try
                        {
                            if (!_closed.get())
                            {
                                Thread.currentThread().setName(_scheduler.getSelectorThreadName());
                                _inSelect.set(true);
                                try
                                {
                                    if (_wakeups.getAndSet(0) > 0)
                                    {
                                        _selector.selectNow();
                                    }
                                    else
                                    {
                                        _selector.select(_nextTimeout);
                                    }
                                }
                                catch (IOException e)
                                {
                                    // TODO Inform the model object
                                    LOGGER.error("Failed to trying to select()", e);
                                    closeSelector();
                                    return;
                                }
                                finally
                                {
                                    _inSelect.set(false);
                                }
                                for (NonBlockingConnection connection : processSelectionKeys())
                                {
                                    if (connection.setScheduled())
                                    {
                                        connections.add(new ConnectionProcessor(_scheduler, connection));
                                    }
                                }
                                for (NonBlockingConnection connection : reregisterUnregisteredConnections())
                                {
                                    if (connection.setScheduled())
                                    {
                                        connections.add(new ConnectionProcessor(_scheduler, connection));
                                    }
                                }
                                for (NonBlockingConnection connection : processUnscheduledConnections())
                                {
                                    if (connection.setScheduled())
                                    {
                                        connections.add(new ConnectionProcessor(_scheduler, connection));
                                    }
                                }
                                runTasks();
                            }
                        }
                        finally
                        {
                            clearSelecting();
                        }

                        if (!connections.isEmpty())
                        {
                            _workQueue.addAll(connections);
                            _workQueue.add(this);
                            for (ConnectionProcessor connectionProcessor : connections)
                            {
                                connectionProcessor.processConnection();
                            }
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                if (_closed.get() && acquireSelecting())
                {
                    closeSelector();
                }
            }
            finally
            {
                _scheduler.decrementRunningCount();
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

        public void wakeup()
        {
            _wakeups.compareAndSet(0, 1);
            if(_inSelect.get() && _wakeups.get() != 0)
            {
                _selector.wakeup();
            }
        }
    }

    private SelectionTask[] _selectionTasks;

    SelectorThread(final NetworkConnectionScheduler scheduler, final int numberOfSelectors) throws IOException
    {
        _scheduler = scheduler;
        _selectionTasks = new SelectionTask[numberOfSelectors];
        for(int i = 0; i < numberOfSelectors; i++)
        {
            _selectionTasks[i] = new SelectionTask();
        }
        for(SelectionTask task : _selectionTasks)
        {
            _workQueue.add(task);
        }
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
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Registering selector on accepting port {} ",
                                     socketChannel.socket().getLocalSocketAddress());
                    }
                    socketChannel.register(_selectionTasks[0].getSelector(), SelectionKey.OP_ACCEPT, nonBlockingNetworkTransport);
                }
                catch (IllegalStateException | ClosedChannelException e)
                {
                    // TODO Communicate condition back to model object to make it go into the ERROR state
                    LOGGER.error("Failed to register selector on accepting port {} ",
                                 socketChannel.socket().getLocalSocketAddress(), e);
                }
            }
        });
        _selectionTasks[0].wakeup();
    }

    public void cancelAcceptingSocket(final ServerSocketChannel socketChannel)
    {
        Future<Void> result = cancelAcceptingSocketAsync(socketChannel);
        try
        {
            result.get(ACCEPT_CANCELLATION_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e)
        {
            LOGGER.warn("Cancellation of accepting socket was interrupted");
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e)
        {
            LOGGER.warn("Cancellation of accepting socket failed", e.getCause());
        }
        catch (TimeoutException e)
        {
            LOGGER.warn("Cancellation of accepting socket timed out");
        }
    }

    private Future<Void> cancelAcceptingSocketAsync(final ServerSocketChannel socketChannel)
    {
        final SettableFuture<Void> cancellationResult = SettableFuture.create();
        _tasks.add(new Runnable()
        {
            @Override
            public void run()
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Cancelling selector on accepting port {} ",
                                 socketChannel.socket().getLocalSocketAddress());
                }

                try
                {
                    SelectionKey selectionKey = null;
                    try
                    {
                        selectionKey = socketChannel.register(_selectionTasks[0].getSelector(), 0);
                    }
                    catch (ClosedChannelException | CancelledKeyException e)
                    {
                        LOGGER.error("Failed to deregister selector on accepting port {}",
                                     socketChannel.socket().getLocalSocketAddress(), e);
                    }

                    if (selectionKey != null)
                    {
                        selectionKey.cancel();
                    }
                }
                finally
                {
                    cancellationResult.set(null);
                }
            }
        });
        _selectionTasks[0].wakeup();
        return cancellationResult;
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
            _scheduler.incrementRunningCount();
            try
            {
                processConnection();
            }
            finally
            {
                _scheduler.decrementRunningCount();
            }
        }

        public void processConnection()
        {
            if (_running.compareAndSet(false, true))
            {
                _scheduler.processConnection(_connection);
            }
        }
    }

    private void unregisterConnection(final NonBlockingConnection connection) throws ClosedChannelException
    {
        SelectionKey register = connection.getSocketChannel().register(connection.getSelectionTask().getSelector(), 0);
        register.cancel();
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
        SelectionTask selectionTask = connection.getSelectionTask();
        if(selectionTask != null)
        {
            final SelectionKey selectionKey = connection.getSocketChannel().keyFor(selectionTask.getSelector());
            int expectedOps = (connection.wantsRead() ? SelectionKey.OP_READ : 0)
                              | (connection.wantsWrite() ? SelectionKey.OP_WRITE : 0);

            try
            {
                return selectionKey == null || !selectionKey.isValid() || selectionKey.interestOps() != expectedOps;
            }
            catch (CancelledKeyException e)
            {
                return true;
            }
        }
        else
        {
            return true;
        }
    }

    public void addConnection(final NonBlockingConnection connection)
    {
        if(selectionInterestRequiresUpdate(connection))
        {
            SelectionTask selectionTask = getNextSelectionTask();
            connection.setSelectionTask(selectionTask);
            selectionTask.getUnregisteredConnections().add(connection);
            selectionTask.wakeup();
        }

    }

    public void returnConnectionToSelector(final NonBlockingConnection connection)
    {
        SelectionTask selectionTask = connection.getSelectionTask();
        if(selectionTask == null)
        {
            throw new IllegalStateException("returnConnectionToSelector should only be called with connections that are currently assigned a selector task");
        }

        if (selectionInterestRequiresUpdate(connection) || connection.getTicker().getModified())
        {
            selectionTask.getUnregisteredConnections().add(connection);
            selectionTask.wakeup();
        }

    }

    private SelectionTask getNextSelectionTask()
    {
        int index;
        do
        {
            index = _nextSelectorTaskIndex.get();
        }
        while(!_nextSelectorTaskIndex.compareAndSet(index, (index + 1) % _selectionTasks.length));
        return _selectionTasks[index];
    }

    void removeConnection(NonBlockingConnection connection)
    {
        try
        {
            unregisterConnection(connection);
        }
        catch (ClosedChannelException e)
        {
            LOGGER.debug("Failed to unregister with selector for connection {}. " +
                         "Connection is probably being closed by peer.", connection);

        }
        catch (ClosedSelectorException | CancelledKeyException e)
        {
            // TODO Port should really not proceed with closing the selector until all of the
            // Connection objects are closed. Connection objects should not be closed until they
            // have closed the underlying socket and removed themselves from the selector. Once
            // this is done, this catch/swallow can be removed.
            LOGGER.debug("Failed to unregister with selector for connection {}. " +
                         "Port has probably already been closed.", connection, e);
        }
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

        int count = _scheduler.getPoolSize();
        while(count-- > 0)
        {
            _workQueue.offer(goodNight);
        }

        for(SelectionTask task : _selectionTasks)
        {
            task.wakeup();
        }

    }

     public void addToWork(final NonBlockingConnection connection)
     {
         if (_closed.get())
         {
             throw new IllegalStateException("Adding connection work " + connection + " to closed selector thread " + _scheduler);
         }
         if(connection.setScheduled())
         {
             _workQueue.add(new ConnectionProcessor(_scheduler, connection));
         }
     }
}
