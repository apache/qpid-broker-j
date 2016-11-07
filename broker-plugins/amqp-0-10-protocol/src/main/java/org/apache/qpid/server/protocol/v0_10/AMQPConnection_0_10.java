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
package org.apache.qpid.server.protocol.v0_10;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.logging.messages.ConnectionMessages;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.ConnectionDelegate;
import org.apache.qpid.transport.Constant;
import org.apache.qpid.server.transport.AggregateTicker;


public class AMQPConnection_0_10 extends AbstractAMQPConnection<AMQPConnection_0_10, ServerConnection>
{
    private static final Logger _logger = LoggerFactory.getLogger(AMQPConnection_0_10.class);
    private final ServerInputHandler _inputHandler;

    private final ServerConnection _connection;

    private volatile boolean _transportBlockedForWriting;

    private final AtomicBoolean _stateChanged = new AtomicBoolean();
    private final AtomicReference<Action<ProtocolEngine>> _workListener = new AtomicReference<>();
    private ServerDisassembler _disassembler;


    public AMQPConnection_0_10(final Broker<?> broker,
                               ServerNetworkConnection network,
                               final AmqpPort<?> port,
                               final Transport transport,
                               final long id,
                               final AggregateTicker aggregateTicker)
    {
        super(broker, network, port, transport, Protocol.AMQP_0_10, id, aggregateTicker);

        _connection = new ServerConnection(id, broker, port, transport, this);
        SocketAddress address = network.getLocalAddress();
        String fqdn = null;

        if (address instanceof InetSocketAddress)
        {
            fqdn = ((InetSocketAddress) address).getHostName();
        }
        SubjectCreator subjectCreator = port.getAuthenticationProvider().getSubjectCreator(transport.isSecure());
        ConnectionDelegate connDelegate = new ServerConnectionDelegate(broker, fqdn, subjectCreator);

        _connection.setConnectionDelegate(connDelegate);
        _connection.setRemoteAddress(network.getRemoteAddress());
        _connection.setLocalAddress(network.getLocalAddress());

        _inputHandler = new ServerInputHandler(new ServerAssembler(_connection));
        _connection.addFrameSizeObserver(_inputHandler);

        AccessController.doPrivileged(new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                _connection.setNetworkConnection(getNetwork());
                _disassembler = new ServerDisassembler(wrapSender(getNetwork().getSender()), Constant.MIN_MAX_FRAME_SIZE);
                _connection.setSender(_disassembler);
                _connection.addFrameSizeObserver(_disassembler);
                return null;
            }
        }, getAccessControllerContext());
    }

    private ByteBufferSender wrapSender(final ByteBufferSender sender)
    {
        return new ByteBufferSender()
        {
            @Override
            public boolean isDirectBufferPreferred()
            {
                return sender.isDirectBufferPreferred();
            }

            @Override
            public void send(final QpidByteBuffer msg)
            {
                updateLastWriteTime();
                sender.send(msg);
            }

            @Override
            public void flush()
            {
                sender.flush();

            }

            @Override
            public void close()
            {
                sender.close();
            }
        };
    }

    public void received(final QpidByteBuffer buf)
    {
        AccessController.doPrivileged(new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                updateLastReadTime();
                try
                {
                    _inputHandler.received(buf);
                    _connection.receivedComplete();
                }
                catch (IllegalArgumentException | IllegalStateException e)
                {
                    throw new ConnectionScopedRuntimeException(e);
                }
                catch (StoreException e)
                {
                    if (getAddressSpace().isActive())
                    {
                        throw new ServerScopedRuntimeException(e);
                    }
                    else
                    {
                        throw new ConnectionScopedRuntimeException(e);
                    }
                }
                return null;
            }
        }, getAccessControllerContext());
    }

    @Override
    public void encryptedTransport()
    {
    }

    public void writerIdle()
    {
        _connection.doHeartBeat();
    }

    public void readerIdle()
    {
        AccessController.doPrivileged(new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                _connection.getEventLogger().message(ConnectionMessages.IDLE_CLOSE("Current connection state: " + _connection.getConnectionDelegate().getState(), true));
                getNetwork().close();
                return null;
            }
        }, getAccessControllerContext());

    }

    public String getAddress()
    {
        return getNetwork().getRemoteAddress().toString();
    }

    @Override
    public void closed()
    {
        try
        {
            AccessController.doPrivileged(new PrivilegedAction<Void>()
            {
                @Override
                public Void run()
                {
                    _inputHandler.closed();
                    if(_disassembler != null)
                    {
                        _disassembler.closed();
                    }
                    return null;
                }
            }, getAccessControllerContext());
        }
        finally
        {
            markTransportClosed();
        }
    }

    @Override
    public boolean isTransportBlockedForWriting()
    {
        return _transportBlockedForWriting;
    }

    @Override
    public void setTransportBlockedForWriting(final boolean blocked)
    {
        if(_transportBlockedForWriting != blocked)
        {
            _transportBlockedForWriting = blocked;
            _connection.transportStateChanged();
        }
    }

    @Override
    public Iterator<Runnable> processPendingIterator()
    {
        if (isIOThread())
        {
            return _connection.processPendingIterator();
        }
        else
        {
            return Collections.emptyIterator();
        }
    }

    @Override
    public boolean hasWork()
    {
        return _stateChanged.get();
    }

    @Override
    public void notifyWork()
    {
        _stateChanged.set(true);

        final Action<ProtocolEngine> listener = _workListener.get();
        if(listener != null)
        {
            listener.performAction(this);
        }
    }

    @Override
    public void notifyWork(final AMQSessionModel<?> sessionModel)
    {
        notifyWork();
    }

    public void clearWork()
    {
        _stateChanged.set(false);
    }

    public void setWorkListener(final Action<ProtocolEngine> listener)
    {
        _workListener.set(listener);
    }

    public boolean hasSessionWithName(final byte[] name)
    {
        return _connection.hasSessionWithName(name);
    }

    public void sendConnectionCloseAsync(final AMQConstant cause, final String message)
    {
        _connection.sendConnectionCloseAsync(cause, message);
    }

    public void closeSessionAsync(final AMQSessionModel<?> session,
                                  final AMQConstant cause, final String message)
    {
        _connection.closeSessionAsync((ServerSession) session, cause, message);
    }

    @Override
    protected void addAsyncTask(final Action<? super ServerConnection> action)
    {
        _connection.addAsyncTask(action);
    }

    public void block()
    {
        _connection.block();
    }

    public String getRemoteContainerName()
    {
        return _connection.getRemoteContainerName();
    }

    public Collection<? extends AMQSessionModel<?>> getSessionModels()
    {
        return _connection.getSessionModels();
    }

    public void unblock()
    {
        _connection.unblock();
    }

    public long getSessionCountLimit()
    {
        return _connection.getSessionCountLimit();
    }

    @Override
    protected boolean isOrderlyClose()
    {
        return !_connection.isConnectionLost();
    }

    @Override
    public void initialiseHeartbeating(final long writerDelay, final long readerDelay)
    {
        super.initialiseHeartbeating(writerDelay, readerDelay);
    }
}
