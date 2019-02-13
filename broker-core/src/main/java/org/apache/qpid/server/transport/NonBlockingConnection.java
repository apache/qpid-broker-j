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
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.transport.network.TransportEncryption;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.SystemUtils;

public class NonBlockingConnection implements ServerNetworkConnection, ByteBufferSender
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingConnection.class);

    private final SocketChannel _socketChannel;
    private volatile NonBlockingConnectionDelegate _delegate;
    private final Deque<NetworkConnectionScheduler> _schedulerDeque = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedQueue<QpidByteBuffer> _buffers = new ConcurrentLinkedQueue<>();

    private final String _remoteSocketAddress;
    private final AtomicBoolean _closed = new AtomicBoolean(false);
    private final ProtocolEngine _protocolEngine;
    private final Runnable _onTransportEncryptionAction;

    private volatile boolean _fullyWritten = true;

    private volatile boolean _partialRead = false;

    private final AmqpPort _port;
    private final AtomicBoolean _scheduled = new AtomicBoolean();
    private volatile long _scheduledTime;
    private volatile boolean _unexpectedByteBufferSizeReported;
    private final String _threadName;
    private volatile SelectorThread.SelectionTask _selectionTask;
    private volatile Iterator<Runnable> _pendingIterator;
    private final AtomicLong _maxWriteIdleMillis = new AtomicLong();
    private final AtomicLong _maxReadIdleMillis = new AtomicLong();
    private final List<SchedulingDelayNotificationListener> _schedulingDelayNotificationListeners = new CopyOnWriteArrayList<>();
    private final AtomicBoolean _hasShutdown = new AtomicBoolean();
    private volatile long _bufferedSize;
    private String _selectedHost;

    public NonBlockingConnection(SocketChannel socketChannel,
                                 ProtocolEngine protocolEngine,
                                 final Set<TransportEncryption> encryptionSet,
                                 final Runnable onTransportEncryptionAction,
                                 final NetworkConnectionScheduler scheduler,
                                 final AmqpPort port)
    {
        _socketChannel = socketChannel;
        pushScheduler(scheduler);

        _protocolEngine = protocolEngine;
        _onTransportEncryptionAction = onTransportEncryptionAction;

        _remoteSocketAddress = _socketChannel.socket().getRemoteSocketAddress().toString();
        _port = port;
        _threadName = SelectorThread.IO_THREAD_NAME_PREFIX + _remoteSocketAddress.toString();

        protocolEngine.setWorkListener(new Action<ProtocolEngine>()
        {
            @Override
            public void performAction(final ProtocolEngine object)
            {
                if(!_scheduled.get())
                {
                    getScheduler().schedule(NonBlockingConnection.this);
                }
            }
        });

        if(encryptionSet.size() == 1)
        {
            setTransportEncryption(encryptionSet.iterator().next());
        }
        else
        {
            _delegate = new NonBlockingConnectionUndecidedDelegate(this);
        }

    }

    String getThreadName()
    {
        return _threadName;
    }

    public boolean isPartialRead()
    {
        return _partialRead;
    }

    AggregateTicker getTicker()
    {
        return _protocolEngine.getAggregateTicker();
    }

    SocketChannel getSocketChannel()
    {
        return _socketChannel;
    }

    @Override
    public void start()
    {
    }

    @Override
    public ByteBufferSender getSender()
    {
        return this;
    }

    @Override
    public void close()
    {
        LOGGER.debug("Closing " + _remoteSocketAddress);
        if(_closed.compareAndSet(false,true))
        {
            _protocolEngine.notifyWork();
            _selectionTask.wakeup();
        }
    }

    @Override
    public SocketAddress getRemoteAddress()
    {
        return _socketChannel.socket().getRemoteSocketAddress();
    }

    @Override
    public SocketAddress getLocalAddress()
    {
        return _socketChannel.socket().getLocalSocketAddress();
    }

    @Override
    public void setMaxWriteIdleMillis(final long millis)
    {
        _maxWriteIdleMillis.set(millis);
    }

    @Override
    public void setMaxReadIdleMillis(final long millis)
    {
        _maxReadIdleMillis.set(millis);
    }

    @Override
    public Principal getPeerPrincipal()
    {
        return _delegate.getPeerPrincipal();
    }

    @Override
    public Certificate getPeerCertificate()
    {
        return _delegate.getPeerCertificate();
    }

    @Override
    public long getMaxReadIdleMillis()
    {
        return _maxReadIdleMillis.get();
    }

    @Override
    public long getMaxWriteIdleMillis()
    {
        return _maxWriteIdleMillis.get();
    }

    @Override
    public String getTransportInfo()
    {
        return _delegate.getTransportInfo();
    }

    boolean wantsRead()
    {
        return _fullyWritten;
    }

    boolean wantsWrite()
    {
        return !_fullyWritten;
    }

    public boolean isStateChanged()
    {
        return _protocolEngine.hasWork();
    }

    public void doPreWork()
    {
        if (!_closed.get())
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

    public boolean doWork()
    {
        _protocolEngine.clearWork();
        if (!_closed.get())
        {
            try
            {
                long currentTime = System.currentTimeMillis();
                int tick = getTicker().getTimeToNextTick(currentTime);
                if (tick <= 0)
                {
                    getTicker().tick(currentTime);
                }

                _protocolEngine.setIOThread(Thread.currentThread());

                boolean processPendingComplete = processPending();

                if(processPendingComplete)
                {
                    _pendingIterator = null;
                    _protocolEngine.setTransportBlockedForWriting(false);
                    boolean dataRead = doRead();
                    _protocolEngine.setTransportBlockedForWriting(!doWrite());

                    if (!_fullyWritten || dataRead || (_delegate.needsWork() && _delegate.getNetInputBuffer().position() != 0))
                    {
                        _protocolEngine.notifyWork();
                    }

                }
                else
                {
                    _protocolEngine.notifyWork();
                }

            }
            catch (IOException |
                    ConnectionScopedRuntimeException e)
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Exception performing I/O for connection '{}'",
                                 _remoteSocketAddress, e);
                }
                else
                {
                    LOGGER.info("Exception performing I/O for connection '{}' : {}",
                                _remoteSocketAddress, e.getMessage());
                }

                if(_closed.compareAndSet(false,true))
                {
                    _protocolEngine.notifyWork();
                }
            }
            finally
            {
                _protocolEngine.setIOThread(null);
            }
        }

        final boolean closed = _closed.get();
        if (closed)
        {
            shutdown();
        }

        return closed;

    }

    @Override
    public void addSchedulingDelayNotificationListeners(final SchedulingDelayNotificationListener listener)
    {
        _schedulingDelayNotificationListeners.add(listener);
    }

    @Override
    public void removeSchedulingDelayNotificationListeners(final SchedulingDelayNotificationListener listener)
    {
        _schedulingDelayNotificationListeners.remove(listener);
    }

    private boolean processPending() throws IOException
    {
        if(_pendingIterator == null)
        {
            _pendingIterator = _protocolEngine.processPendingIterator();
        }

        final int networkBufferSize = _port.getNetworkBufferSize();

        while(_pendingIterator.hasNext())
        {
            long size = getBufferedSize();
            if(size >= networkBufferSize)
            {
                doWrite();
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
            doWrite();
            complete &= getBufferedSize() < networkBufferSize /2;
        }
        return complete;
    }

    private long getBufferedSize()
    {
        return _bufferedSize;
    }

    private void shutdown()
    {
        if (!_hasShutdown.compareAndSet(false, true))
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
                    _socketChannel.close();
                }
            }
            catch (IOException e)
            {
                LOGGER.info("Exception closing socket '{}': {}", _remoteSocketAddress, e.getMessage());
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
            LOGGER.info("Exception performing final write/close for '{}': {}", _remoteSocketAddress, e.getMessage());
        }
    }

    private void shutdownOutput()
    {
        try
        {

            if(!SystemUtils.isWindows())
            {
                try
                {
                    _socketChannel.shutdownOutput();
                }
                catch (IOException e)
                {
                    LOGGER.info("Exception closing socket '{}': {}", _remoteSocketAddress, e.getMessage());
                }
                finally
                {
                    _delegate.shutdownOutput();
                }
            }
        }
        finally
        {
            while (!_buffers.isEmpty())
            {
                final QpidByteBuffer buffer = _buffers.poll();
                buffer.dispose();
            }
        }

    }

    private void shutdownInput()
    {

        if(!SystemUtils.isWindows())
        {
            try
            {
                _socketChannel.shutdownInput();
            }
            catch (IOException e)
            {
                LOGGER.info("Exception shutting down input for '{}': {}", _remoteSocketAddress, e.getMessage());
            }
            finally
            {
                _delegate.shutdownInput();
            }
        }
    }

    /**
     * doRead is not reentrant.
     */
    boolean doRead() throws IOException
    {
        _partialRead = false;
        if(!_closed.get() && _delegate.readyForRead())
        {
            long readData = readFromNetwork();

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

    long writeToTransport(Collection<QpidByteBuffer> buffers) throws IOException
    {
        long written  = QpidByteBuffer.write(_socketChannel, buffers);
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Written " + written + " bytes");
        }
        return written;
    }

    private boolean doWrite() throws IOException
    {
        final NonBlockingConnectionDelegate.WriteResult result = _delegate.doWrite(_buffers);
        _bufferedSize -= result.getBytesConsumed();
        _fullyWritten = result.isComplete();
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
        return _fullyWritten;
    }

    protected long readFromNetwork() throws IOException
    {
        QpidByteBuffer buffer = _delegate.getNetInputBuffer();

        long read = buffer.read(_socketChannel);
        if (read == -1)
        {
            _closed.set(true);
            _protocolEngine.notifyWork();
        }

        _partialRead = read != 0;

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Read " + read + " byte(s)");
        }
        return read;
    }

    @Override
    public boolean isDirectBufferPreferred()
    {
        return true;
    }

    @Override
    public void send(final QpidByteBuffer msg)
    {

        if (_closed.get())
        {
            LOGGER.warn("Send ignored as the connection is already closed");
        }
        else
        {
            int remaining = msg.remaining();
            if (remaining > 0)
            {
                _buffers.add(msg.duplicate());
                _bufferedSize += remaining;
            }
        }
        msg.position(msg.limit());
    }

    @Override
    public void flush()
    {
    }

    public final void pushScheduler(NetworkConnectionScheduler scheduler)
    {
        _schedulerDeque.addFirst(scheduler);
    }

    public final NetworkConnectionScheduler popScheduler()
    {
        return _schedulerDeque.removeFirst();
    }

    public final NetworkConnectionScheduler getScheduler()
    {
        return _schedulerDeque.peekFirst();
    }

    @Override
    public String toString()
    {
        return "[NonBlockingConnection " + _remoteSocketAddress + "]";
    }

    public void processAmqpData(QpidByteBuffer applicationData)
    {
        _protocolEngine.received(applicationData);
    }

    public void setTransportEncryption(TransportEncryption transportEncryption)
    {
        NonBlockingConnectionDelegate oldDelegate = _delegate;
        switch (transportEncryption)
        {
            case TLS:
                _onTransportEncryptionAction.run();
                _delegate = new NonBlockingConnectionTLSDelegate(this, _port);
                break;
            case NONE:
                _delegate = new NonBlockingConnectionPlainDelegate(this, _port);
                break;
            default:
                throw new IllegalArgumentException("unknown TransportEncryption " + transportEncryption);
        }
        if (oldDelegate != null)
        {
            try (QpidByteBuffer src = oldDelegate.getNetInputBuffer().duplicate())
            {
                src.flip();
                _delegate.getNetInputBuffer().put(src);
            }
            oldDelegate.shutdownInput();
            oldDelegate.shutdownOutput();
        }
        LOGGER.debug("Identified transport encryption as " + transportEncryption);
    }

    public boolean setScheduled()
    {
        final boolean scheduled = _scheduled.compareAndSet(false, true);
        if (scheduled)
        {
            _scheduledTime = System.currentTimeMillis();
        }
        return scheduled;
    }

    public void clearScheduled()
    {
        _scheduled.set(false);
        _scheduledTime = 0;
    }

    @Override
    public long getScheduledTime()
    {
        return _scheduledTime;
    }

    void reportUnexpectedByteBufferSizeUsage()
    {
        if (!_unexpectedByteBufferSizeReported)
        {
            LOGGER.info("At least one frame unexpectedly does not fit into default byte buffer size ({}B) on a connection {}.",
                    _port.getNetworkBufferSize(), this.toString());
            _unexpectedByteBufferSizeReported = true;
        }
    }

    public SelectorThread.SelectionTask getSelectionTask()
    {
        return _selectionTask;
    }

    public void setSelectionTask(final SelectorThread.SelectionTask selectionTask)
    {
        _selectionTask = selectionTask;
    }

    public void setSelectedHost(final String selectedHost)
    {
        _selectedHost = selectedHost;
    }

    @Override
    public String getSelectedHost()
    {
        return _selectedHost;
    }
}
