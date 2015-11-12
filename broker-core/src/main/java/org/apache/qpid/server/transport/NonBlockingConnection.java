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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.network.Ticker;
import org.apache.qpid.transport.network.TransportEncryption;
import org.apache.qpid.util.SystemUtils;

public class NonBlockingConnection implements ServerNetworkConnection, ByteBufferSender
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingConnection.class);

    private final SocketChannel _socketChannel;
    private NonBlockingConnectionDelegate _delegate;
    private NetworkConnectionScheduler _scheduler;
    private final ConcurrentLinkedQueue<QpidByteBuffer> _buffers = new ConcurrentLinkedQueue<>();

    private final String _remoteSocketAddress;
    private final AtomicBoolean _closed = new AtomicBoolean(false);
    private final ProtocolEngine _protocolEngine;
    private final Runnable _onTransportEncryptionAction;
    private final AtomicLong _usedOutboundMessageSpace = new AtomicLong();
    private final long _outboundMessageBufferLimit;

    private volatile int _maxReadIdle;
    private volatile int _maxWriteIdle;

    private volatile boolean _fullyWritten = true;

    private boolean _partialRead = false;

    private final AmqpPort _port;
    private final AtomicBoolean _scheduled = new AtomicBoolean();
    private volatile boolean _unexpectedByteBufferSizeReported;
    private final String _threadName;
    private volatile SelectorThread.SelectionTask _selectionTask;
    private Iterator<Runnable> _pendingItertor;

    public NonBlockingConnection(SocketChannel socketChannel,
                                 ProtocolEngine protocolEngine,
                                 final Set<TransportEncryption> encryptionSet,
                                 final Runnable onTransportEncryptionAction,
                                 final NetworkConnectionScheduler scheduler,
                                 final AmqpPort port)
    {
        _socketChannel = socketChannel;
        _scheduler = scheduler;

        _protocolEngine = protocolEngine;
        _onTransportEncryptionAction = onTransportEncryptionAction;

        _remoteSocketAddress = _socketChannel.socket().getRemoteSocketAddress().toString();
        _port = port;
        _threadName = SelectorThread.IO_THREAD_NAME_PREFIX + _remoteSocketAddress.toString();

        _outboundMessageBufferLimit = (long) _port.getContextValue(Long.class,
                                                                   AmqpPort.PORT_AMQP_OUTBOUND_MESSAGE_BUFFER_SIZE);

        protocolEngine.setWorkListener(new Action<ProtocolEngine>()
        {
            @Override
            public void performAction(final ProtocolEngine object)
            {
                _scheduler.schedule(NonBlockingConnection.this);
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

    Ticker getTicker()
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
    public void setMaxWriteIdle(int sec)
    {
        _maxWriteIdle = sec;
    }

    @Override
    public void setMaxReadIdle(int sec)
    {
        _maxReadIdle = sec;
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
    public int getMaxReadIdle()
    {
        return _maxReadIdle;
    }

    @Override
    public int getMaxWriteIdle()
    {
        return _maxWriteIdle;
    }

    @Override
    public void reserveOutboundMessageSpace(long size)
    {
        if (_usedOutboundMessageSpace.addAndGet(size) > _outboundMessageBufferLimit)
        {
            _protocolEngine.setMessageAssignmentSuspended(true);
        }
    }

    public boolean wantsRead()
    {
        return _fullyWritten;
    }

    public boolean wantsWrite()
    {
        return !_fullyWritten;
    }

    public boolean isStateChanged()
    {
        return _protocolEngine.hasWork();
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
                _protocolEngine.setMessageAssignmentSuspended(true);

                if(_pendingItertor == null)
                {
                    _pendingItertor = _protocolEngine.processPendingIterator();
                }

                while(_pendingItertor.hasNext())
                {
                    long size = getBufferedSize();
                    if(size >= _port.getNetworkBufferSize())
                    {
                        doWrite();
                        if((size - getBufferedSize()) < (_port.getNetworkBufferSize()/2))
                        {
                            break;
                        }
                    }
                    else
                    {
                        final Runnable task = _pendingItertor.next();
                        task.run();
                    }
                }

                if (!_pendingItertor.hasNext())
                {
                    _pendingItertor = null;
                    _protocolEngine.setTransportBlockedForWriting(false);
                    boolean dataRead = doRead();
                    _protocolEngine.setTransportBlockedForWriting(!doWrite());

                    if (!_fullyWritten || dataRead || (_delegate.needsWork() && _delegate.getNetInputBuffer().position() != 0))
                    {
                        _protocolEngine.notifyWork();
                    }

                    if (_fullyWritten)
                    {
                        _protocolEngine.setMessageAssignmentSuspended(false);
                    }
                }
                else
                {
                    _protocolEngine.notifyWork();
                }

            }
            catch (IOException | ConnectionScopedRuntimeException e)
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

    private long getBufferedSize()
    {
        long totalSize = 0l;
        for(QpidByteBuffer buf : _buffers)
        {
            totalSize += buf.remaining();
        }
        return totalSize;
    }

    private void shutdown()
    {
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
                _socketChannel.close();
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
        _fullyWritten = _delegate.doWrite(_buffers);
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
        if (_fullyWritten)
        {
            _usedOutboundMessageSpace.set(0);
        }
        return _fullyWritten;

    }

    protected int readFromNetwork() throws IOException
    {
        QpidByteBuffer buffer = _delegate.getNetInputBuffer();

        int read = buffer.read(_socketChannel);
        if (read == -1)
        {
            _closed.set(true);
        }

        _partialRead = read != 0;

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Read " + read + " byte(s)");
        }
        return read;
    }

    @Override
    public void send(final QpidByteBuffer msg)
    {

        if (_closed.get())
        {
            LOGGER.warn("Send ignored as the connection is already closed");
        }
        else if (msg.remaining() > 0)
        {
            _buffers.add(msg.duplicate());
        }
        msg.position(msg.limit());
    }

    @Override
    public void flush()
    {
    }

    public void changeScheduler(NetworkConnectionScheduler scheduler)
    {
        _scheduler = scheduler;
    }

    @Override
    public String toString()
    {
        return "[NonBlockingConnection " + _remoteSocketAddress + "]";
    }

    public NetworkConnectionScheduler getScheduler()
    {
        return _scheduler;
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
        if(oldDelegate != null)
        {
            QpidByteBuffer src = oldDelegate.getNetInputBuffer().duplicate();
            src.flip();
            _delegate.getNetInputBuffer().put(src);
            src.dispose();
        }
        LOGGER.debug("Identified transport encryption as " + transportEncryption);
    }

    public boolean setScheduled()
    {
        return _scheduled.compareAndSet(false,true);
    }

    public void clearScheduled()
    {
        _scheduled.set(false);
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
}
