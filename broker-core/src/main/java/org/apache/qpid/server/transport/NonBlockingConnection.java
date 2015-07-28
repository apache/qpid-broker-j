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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.Principal;
import java.security.cert.Certificate;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.server.model.port.AmqpPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.network.NetworkConnection;
import org.apache.qpid.transport.network.Ticker;
import org.apache.qpid.transport.network.TransportEncryption;
import org.apache.qpid.util.SystemUtils;

public class NonBlockingConnection implements NetworkConnection, ByteBufferSender
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingConnection.class);

    private final SocketChannel _socketChannel;
    private NonBlockingConnectionDelegate _delegate;
    private NetworkConnectionScheduler _scheduler;
    private final ConcurrentLinkedQueue<ByteBuffer> _buffers = new ConcurrentLinkedQueue<>();

    private final String _remoteSocketAddress;
    private final AtomicBoolean _closed = new AtomicBoolean(false);
    private final ProtocolEngine _protocolEngine;
    private final Runnable _onTransportEncryptionAction;
    private final int _receiveBufferSize;

    private volatile int _maxReadIdle;
    private volatile int _maxWriteIdle;

    private ByteBuffer _netInputBuffer;

    private volatile boolean _fullyWritten = true;

    private boolean _partialRead = false;
    private final AmqpPort _port;

    public NonBlockingConnection(SocketChannel socketChannel,
                                 ProtocolEngine protocolEngine,
                                 int receiveBufferSize,
                                 final Set<TransportEncryption> encryptionSet,
                                 final Runnable onTransportEncryptionAction,
                                 final NetworkConnectionScheduler scheduler,
                                 final AmqpPort port)
    {
        _socketChannel = socketChannel;
        _scheduler = scheduler;

        _protocolEngine = protocolEngine;
        _onTransportEncryptionAction = onTransportEncryptionAction;

        _receiveBufferSize = receiveBufferSize;

        _netInputBuffer = ByteBuffer.allocateDirect(receiveBufferSize);
        _remoteSocketAddress = _socketChannel.socket().getRemoteSocketAddress().toString();
        _port = port;

        protocolEngine.setWorkListener(new Action<ProtocolEngine>()
        {
            @Override
            public void performAction(final ProtocolEngine object)
            {
                _scheduler.wakeup();
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
            _scheduler.wakeup();
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

    public boolean canRead()
    {
        return true;
    }

    public boolean waitingForWrite()
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

                _protocolEngine.setMessageAssignmentSuspended(true);

                _protocolEngine.processPending();

                _protocolEngine.setTransportBlockedForWriting(!doWrite());
                boolean dataRead = doRead();
                _fullyWritten = doWrite();
                _protocolEngine.setTransportBlockedForWriting(!_fullyWritten);

                if (dataRead || (_delegate.needsWork() && _netInputBuffer.position() != 0))
                {
                    _protocolEngine.notifyWork();
                }

                // tell all consumer targets that it is okay to accept more
                _protocolEngine.setMessageAssignmentSuspended(false);
            }
            catch (IOException | ConnectionScopedRuntimeException e)
            {
                LOGGER.info("Exception performing I/O for thread '" + _remoteSocketAddress + "': " + e);
                LOGGER.debug("Closing " + _remoteSocketAddress);
                if(_closed.compareAndSet(false,true))
                {
                    _protocolEngine.notifyWork();
                }
            }
        }

        final boolean closed = _closed.get();
        if (closed)
        {
            shutdown();
        }

        return closed;

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
                _socketChannel.shutdownOutput();
            }
        }
        catch (IOException e)
        {
            LOGGER.info("Exception closing socket '{}': {}", _remoteSocketAddress, e.getMessage());
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
        }
    }

    /**
     * doRead is not reentrant.
     */
    boolean doRead() throws IOException
    {
        boolean readData = false;
        _partialRead = false;
        if (!_closed.get())
        {
            readData = _delegate.doRead();
        }
        return readData;
    }

    boolean readAndProcessData() throws IOException
    {
        boolean readData = readIntoBuffer(_netInputBuffer);

        ByteBuffer duplicate = _netInputBuffer.duplicate();
        duplicate.flip();

        readData |= processData(duplicate);

        if (_netInputBuffer.hasRemaining())
        {
            // slice but keep unprocessed data
            int amountOfUnprocessedData = duplicate.remaining();
            _netInputBuffer.position(_netInputBuffer.position() - amountOfUnprocessedData);
            _netInputBuffer = _netInputBuffer.slice();
            _netInputBuffer.position(amountOfUnprocessedData);
        }
        else
        {
            // compact into new buffer
            _netInputBuffer = ByteBuffer.allocateDirect(_receiveBufferSize);
            _netInputBuffer.put(duplicate);
        }
        return readData;
    }

    void writeToTransport(ByteBuffer[] buffers) throws IOException
    {
        long written  = _socketChannel.write(buffers);
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Written " + written + " bytes");
        }
    }

    private boolean doWrite() throws IOException
    {
        ByteBuffer[] bufArray = new ByteBuffer[_buffers.size()];
        Iterator<ByteBuffer> bufferIterator = _buffers.iterator();
        for (int i = 0; i < bufArray.length; i++)
        {
            bufArray[i] = bufferIterator.next();
        }

        if (_delegate != null)
        {
            return _delegate.doWrite(bufArray);
        }
        else
        {
            return true;
        }
    }

    boolean processData(ByteBuffer data) throws IOException
    {
        return _delegate.processData(data);
    }

    protected boolean readIntoBuffer(final ByteBuffer buffer) throws IOException
    {
        boolean readData = false;
        int read = _socketChannel.read(buffer);
        if (read > 0)
        {
            readData = true;
        }
        else if (read == -1)
        {
            _closed.set(true);
        }

        _partialRead = !buffer.hasRemaining();

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Read " + read + " byte(s)");
        }
        return readData;
    }

    @Override
    public void send(final ByteBuffer msg)
    {

        if (_closed.get())
        {
            LOGGER.warn("Send ignored as the connection is already closed");
        }
        else if (msg.remaining() > 0)
        {
            _buffers.add(msg);
        }
    }

    public void writeBufferProcessed()
    {
        _buffers.poll();
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

    public void processAmqpData(ByteBuffer data)
    {
        _protocolEngine.received(data);
    }

    public void setTransportEncryption(TransportEncryption transportEncryption)
    {
        switch (transportEncryption)
        {
            case TLS:
                _onTransportEncryptionAction.run();
                _delegate = new NonBlockingConnectionTLSDelegate(this, _port);
                break;
            case NONE:
                _delegate = new NonBlockingConnectionPlainDelegate(this);
                break;
            default:
                throw new IllegalArgumentException("unknown TransportEncryption " + transportEncryption);
        }
        LOGGER.debug("Identified transport encryption as " + transportEncryption);
    }
}
