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
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.security.Principal;
import java.security.cert.Certificate;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.network.Ticker;
import org.apache.qpid.util.SystemUtils;

public abstract class NonBlockingConnection implements SchedulableConnection
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingConnection.class);

    final AtomicLong _usedOutboundMessageSpace = new AtomicLong();
    private final SocketChannel _socketChannel;
    private final Deque<NetworkConnectionScheduler> _schedulerDeque = new ConcurrentLinkedDeque<>();
    private final String _remoteAddressString;
    private final AtomicBoolean _scheduled = new AtomicBoolean();
    private final AtomicLong _maxWriteIdleMillis = new AtomicLong();
    private final AtomicLong _maxReadIdleMillis = new AtomicLong();
    private final List<SchedulingDelayNotificationListener>
            _schedulingDelayNotificationListeners = new CopyOnWriteArrayList<>();
    private final String _threadName;
    private final ConcurrentLinkedQueue<QpidByteBuffer> _buffers = new ConcurrentLinkedQueue<>();
    private NonBlockingConnectionDelegate _delegate;
    private final AtomicBoolean _closed = new AtomicBoolean(false);

    private volatile SelectorThread.SelectionTask _selectionTask;
    private volatile long _scheduledTime;
    private final AtomicBoolean _hasShutdown = new AtomicBoolean();
    private final AtomicBoolean _unexpectedByteBufferSizeReported = new AtomicBoolean();
    private final ProtocolEngine _protocolEngine;
    private volatile Iterator<Runnable> _pendingIterator;
    private volatile boolean _fullyWritten = true;
    private volatile boolean _partialRead = false;

    protected NonBlockingConnection(final SocketChannel socketChannel, ProtocolEngine protocolEngine,
                                    final NetworkConnectionScheduler scheduler,
                                    String remoteAddressString)
    {
        _socketChannel = socketChannel;
        _protocolEngine = protocolEngine;
        _remoteAddressString = remoteAddressString;
        _threadName = SelectorThread.IO_THREAD_NAME_PREFIX + _remoteAddressString;
        pushScheduler(scheduler);
        protocolEngine.setWorkListener(new Action<ProtocolEngine>()
        {
            @Override
            public void performAction(final ProtocolEngine object)
            {
                NetworkConnectionScheduler scheduler = getScheduler();
                if(scheduler != null)
                {
                    scheduler.schedule(NonBlockingConnection.this);
                }
            }
        });


    }

    @Override
    public final String getThreadName()
    {
        return _threadName;
    }


    protected final String getRemoteAddressString()
    {
        return _remoteAddressString;
    }

    protected final boolean isClosed()
    {
        return _closed.get();
    }

    protected final boolean setClosed()
    {
        return _closed.compareAndSet(false,true);
    }

    @Override
    public final SocketChannel getSocketChannel()
    {
        return _socketChannel;
    }


    @Override
    public final SocketAddress getRemoteAddress()
    {
        return getSocketChannel().socket().getRemoteSocketAddress();
    }

    @Override
    public final SocketAddress getLocalAddress()
    {
        return getSocketChannel().socket().getLocalSocketAddress();
    }


    public final void pushScheduler(NetworkConnectionScheduler scheduler)
    {
        _schedulerDeque.addFirst(scheduler);
    }

    public final NetworkConnectionScheduler popScheduler()
    {
        return _schedulerDeque.removeFirst();
    }

    @Override
    public final NetworkConnectionScheduler getScheduler()
    {
        return _schedulerDeque.peekFirst();
    }

    @Override
    public final Principal getPeerPrincipal()
    {
        return _delegate.getPeerPrincipal();
    }

    @Override
    public final Certificate getPeerCertificate()
    {
        return _delegate.getPeerCertificate();
    }

    @Override
    public final String getTransportInfo()
    {
        return _delegate.getTransportInfo();
    }


    @Override
    public final SelectorThread.SelectionTask getSelectionTask()
    {
        return _selectionTask;
    }

    @Override
    public final void setSelectionTask(final SelectorThread.SelectionTask selectionTask)
    {
        _selectionTask = selectionTask;
    }

    @Override
    public final boolean setScheduled()
    {
        final boolean scheduled = _scheduled.compareAndSet(false, true);
        if (scheduled)
        {
            _scheduledTime = System.currentTimeMillis();
        }
        return scheduled;
    }

    @Override
    public final void clearScheduled()
    {
        _scheduled.set(false);
        _scheduledTime = 0;
    }

    @Override
    public final long getScheduledTime()
    {
        return _scheduledTime;
    }

    @Override
    public final void setMaxWriteIdleMillis(final long millis)
    {
        _maxWriteIdleMillis.set(millis);
    }

    @Override
    public final void setMaxReadIdleMillis(final long millis)
    {
        _maxReadIdleMillis.set(millis);
    }

    @Override
    public final long getMaxReadIdleMillis()
    {
        return _maxReadIdleMillis.get();
    }

    @Override
    public final long getMaxWriteIdleMillis()
    {
        return _maxWriteIdleMillis.get();
    }
    @Override
    public final boolean isStateChanged()
    {
        return _protocolEngine.hasWork();
    }

    @Override
    public final boolean wantsRead()
    {
        return _fullyWritten;
    }

    @Override
    public final boolean wantsWrite()
    {
        return !_fullyWritten;
    }

    @Override
    public final boolean isPartialRead()
    {
        return _partialRead;
    }

    @Override
    public boolean doWork()
    {
        _protocolEngine.clearWork();
        try
        {

            if (!isClosed() && (!wantsConnect() || completeConnection()))
            {
                long currentTime = System.currentTimeMillis();
                int tick = getTicker().getTimeToNextTick(currentTime);
                if (tick <= 0)
                {
                    getTicker().tick(currentTime);
                }

                _protocolEngine.setIOThread(Thread.currentThread());
                _protocolEngine.setMessageAssignmentSuspended(true, true);

                boolean processPendingComplete = processPending();

                if (processPendingComplete)
                {
                    _pendingIterator = null;
                    _protocolEngine.setTransportBlockedForWriting(false);
                    boolean dataRead = doRead();
                    _fullyWritten = doWrite();
                    _protocolEngine.setTransportBlockedForWriting(!_fullyWritten);

                    if (!_fullyWritten || dataRead || (_delegate.needsWork()
                                                       && _delegate.getNetInputBuffer().position() != 0))
                    {
                        _protocolEngine.notifyWork();
                    }

                    if (_fullyWritten)
                    {
                        _protocolEngine.setMessageAssignmentSuspended(false, true);
                    }
                }
                else
                {
                    _protocolEngine.notifyWork();
                }
            }
        }
        catch (IOException |
                ConnectionScopedRuntimeException e)
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Exception performing I/O for connection '{}'",
                             getRemoteAddressString(), e);
            }
            else
            {
                LOGGER.info("Exception performing I/O for connection '{}' : {}",
                            getRemoteAddressString(), e.getMessage());
            }

            if(setClosed())
            {
                _protocolEngine.notifyWork();
            }
        }
        finally
        {
            _protocolEngine.setIOThread(null);
        }

        final boolean closed = isClosed();
        if (closed)
        {
            shutdown();
        }

        return closed;

    }

    private boolean completeConnection() throws IOException
    {
        boolean finishConnect = getSocketChannel().finishConnect();
        return finishConnect  && !wantsConnect();
    }


    private boolean processPending() throws IOException
    {
        if(_pendingIterator == null)
        {
            _pendingIterator = _protocolEngine.processPendingIterator();
        }

        final int networkBufferSize = getNetworkBufferSize();

        while(_pendingIterator.hasNext())
        {
            long size = getBufferedSize();
            if(size >= networkBufferSize)
            {
                _fullyWritten = doWrite();
                long bytesWritten = size - getBufferedSize();
                if(bytesWritten < (networkBufferSize / 2))
                {
                    break;
                }
            }
            else
            {
                final Runnable task = _pendingIterator.next();
                task.run();
            }
        }

        boolean complete = !_pendingIterator.hasNext();
        if (getBufferedSize() >= networkBufferSize)
        {
            _fullyWritten = doWrite();
            complete &= getBufferedSize() < networkBufferSize /2;
        }
        return complete;
    }

    protected abstract int getNetworkBufferSize();


    @Override
    public final void doPreWork()
    {
        if (!isClosed())
        {
            long currentTime = System.currentTimeMillis();
            long schedulingDelay = currentTime - getScheduledTime();
            if (!_schedulingDelayNotificationListeners.isEmpty())
            {
                for (SchedulingDelayNotificationListener listener : _schedulingDelayNotificationListeners)
                {
                    listener.notifySchedulingDelay(schedulingDelay);
                }
            }
        }
    }


    /**
     * doRead is not reentrant.
     */
    boolean doRead() throws IOException
    {
        _partialRead = false;
        if(!isClosed() && _delegate.readyForRead())
        {
            int readData = readFromNetwork();

            if (readData > 0)
            {
                return _delegate.processData();
            }
            else
            {
                return false;
            }
        }
        else
        {
            return false;
        }
    }

    @Override
    public long writeToTransport(Collection<QpidByteBuffer> buffers) throws IOException
    {
        long written  = QpidByteBuffer.write(getSocketChannel(), buffers);
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Written " + written + " bytes");
        }
        return written;
    }

    protected int readFromNetwork() throws IOException
    {
        QpidByteBuffer buffer = _delegate.getNetInputBuffer();

        int read = buffer.read(getSocketChannel());
        if (read == -1)
        {
            setClosed();
        }

        _partialRead = read != 0;

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Read " + read + " byte(s)");
        }
        return read;
    }




    @Override
    public final void addSchedulingDelayNotificationListeners(final SchedulingDelayNotificationListener listener)
    {
        _schedulingDelayNotificationListeners.add(listener);
    }

    @Override
    public final void removeSchedulingDelayNotificationListeners(final SchedulingDelayNotificationListener listener)
    {
        _schedulingDelayNotificationListeners.remove(listener);
    }

    @Override
    public final ByteBufferSender getSender()
    {
        return this;
    }

    @Override
    public final boolean isDirectBufferPreferred()
    {
        return true;
    }

    private final long getBufferedSize()
    {
        // Avoids iterator garbage if empty
        if (_buffers.isEmpty())
        {
            return 0L;
        }

        long totalSize = 0L;
        for(QpidByteBuffer buf : _buffers)
        {
            totalSize += buf.remaining();
        }
        return totalSize;
    }

    final boolean doWrite() throws IOException
    {
        boolean fullyWritten = _delegate.doWrite(_buffers);
        while(!_buffers.isEmpty())
        {
            QpidByteBuffer buf = _buffers.peek();
            if(buf.hasRemaining())
            {
                break;
            }
            _buffers.poll();
            buf.dispose();
        }
        if (fullyWritten)
        {
            _usedOutboundMessageSpace.set(0);
        }
        return fullyWritten;

    }

    @Override
    public final void send(final QpidByteBuffer msg)
    {

        if (isClosed())
        {
            LOGGER.warn("Send ignored as the connection is already closed");
        }
        else if (msg.remaining() > 0)
        {
            _buffers.add(msg.duplicate());
        }
        msg.position(msg.limit());
    }


    protected final void shutdown()
    {
        if (_hasShutdown.getAndSet(true))
        {
            return;
        }

        try
        {
            shutdownInput();
            shutdownFinalWrite();

            _protocolEngine.closed();

            shutdownOutput();
        }
        finally
        {
            try
            {
                try
                {
                    NetworkConnectionScheduler scheduler = getScheduler();
                    if (scheduler != null)
                    {
                        scheduler.removeConnection(this);
                    }
                }
                finally
                {
                    getSocketChannel().close();
                }
            }
            catch (IOException e)
            {
                LOGGER.info("Exception closing socket '{}': {}", getRemoteAddressString(), e.getMessage());
            }

            if (SystemUtils.isWindows())
            {
                _delegate.shutdownInput();
                _delegate.shutdownOutput();
            }
        }
    }

    private void shutdownFinalWrite()
    {
        try
        {
            while(!doWrite())
            {
            }
        }
        catch (IOException e)
        {
            LOGGER.info("Exception performing final write/close for '{}': {}", getRemoteAddressString(), e.getMessage());
        }
    }

    private void shutdownOutput()
    {
        if(!SystemUtils.isWindows())
        {
            try
            {
                getSocketChannel().shutdownOutput();
            }
            catch (IOException e)
            {
                LOGGER.info("Exception closing socket '{}': {}", getRemoteAddressString(), e.getMessage());
            }
            finally
            {
                _delegate.shutdownOutput();
            }
        }
    }

    private void shutdownInput()
    {

        if(!SystemUtils.isWindows())
        {
            try
            {
                getSocketChannel().shutdownInput();
            }
            catch (IOException e)
            {
                LOGGER.info("Exception shutting down input for '{}': {}", getRemoteAddressString(), e.getMessage());
            }
            finally
            {
                _delegate.shutdownInput();
            }
        }
    }


    @Override
    public final void start()
    {
    }

    @Override
    public final void flush()
    {
    }


    @Override
    public final void close()
    {
        LOGGER.debug("Closing " + getRemoteAddressString());
        if(setClosed())
        {
            _protocolEngine.notifyWork();
            getSelectionTask().wakeup();
        }
    }


    @Override
    public void processAmqpData(QpidByteBuffer applicationData)
    {
        _protocolEngine.received(applicationData);
    }


    @Override
    public final void reserveOutboundMessageSpace(long size)
    {
        if (_usedOutboundMessageSpace.addAndGet(size) > getOutboundMessageBufferLimit())
        {
            _protocolEngine.setMessageAssignmentSuspended(true, false);
        }
    }


    @Override
    public final Ticker getTicker()
    {
        return _protocolEngine.getAggregateTicker();
    }


    protected abstract long getOutboundMessageBufferLimit();


    public final void reportUnexpectedByteBufferSizeUsage()
    {
        if (_unexpectedByteBufferSizeReported.compareAndSet(false,true))
        {
            LOGGER.info("At least one frame unexpectedly does not fit into default byte buffer size ({}B) on a connection {}.",
                        getNetworkBufferSize(), this.toString());
        }
    }

    protected NonBlockingConnectionDelegate getDelegate()
    {
        return _delegate;
    }

    protected void setDelegate(NonBlockingConnectionDelegate delegate)
    {
        _delegate = delegate;
    }

    public final ProtocolEngine getProtocolEngine()
    {
        return _protocolEngine;
    }
}
