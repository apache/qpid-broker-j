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

import java.nio.BufferUnderflowException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.messages.ConnectionMessages;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionCloseCode;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.transport.AggregateTicker;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.ServerScopedRuntimeException;


public class AMQPConnection_0_10Impl extends AbstractAMQPConnection<AMQPConnection_0_10Impl, ServerConnection>
        implements
        AMQPConnection_0_10<AMQPConnection_0_10Impl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AMQPConnection_0_10Impl.class);
    private final ServerInputHandler _inputHandler;

    private final ServerConnection _connection;

    private volatile boolean _transportBlockedForWriting;

    private final AtomicBoolean _stateChanged = new AtomicBoolean();
    private final AtomicReference<Action<ProtocolEngine>> _workListener = new AtomicReference<>();
    private ServerDisassembler _disassembler;

    private final Set<AMQPSession<?,?>> _sessionsWithWork =
            Collections.newSetFromMap(new ConcurrentHashMap<AMQPSession<?,?>, Boolean>());

    public AMQPConnection_0_10Impl(final Broker<?> broker,
                                   ServerNetworkConnection network,
                                   final AmqpPort<?> port,
                                   final Transport transport,
                                   final long id,
                                   final AggregateTicker aggregateTicker)
    {
        super(broker, network, port, transport, Protocol.AMQP_0_10, id, aggregateTicker);

        _connection = new ServerConnection(id, broker, port, transport, this);

        ServerConnectionDelegate connDelegate = new ServerConnectionDelegate(port, transport.isSecure(), network.getSelectedHost());

        _connection.setConnectionDelegate(connDelegate);
        _connection.setRemoteAddress(network.getRemoteAddress());

        _inputHandler = new ServerInputHandler(new ServerAssembler(_connection));
        _connection.addFrameSizeObserver(_inputHandler);

        AccessController.doPrivileged((PrivilegedAction<Object>) () ->
        {
            _connection.setNetworkConnection(getNetwork());
            _disassembler = new ServerDisassembler(wrapSender(getNetwork().getSender()), Constant.MIN_MAX_FRAME_SIZE);
            _connection.setSender(_disassembler);
            _connection.addFrameSizeObserver(_disassembler);
            return null;
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

    @Override
    protected void onReceive(final QpidByteBuffer buf)
    {
        try
        {
            _inputHandler.received(buf);
            _connection.receivedComplete();
        }
        catch (IllegalArgumentException | IllegalStateException | BufferUnderflowException e)
        {
            LOGGER.warn("Unexpected exception", e);
            throw new ConnectionScopedRuntimeException(e);
        }
    }

    @Override
    public void encryptedTransport()
    {
    }

    @Override
    public void writerIdle()
    {
        _connection.doHeartBeat();
    }

    @Override
    public void readerIdle()
    {
        AccessController.doPrivileged((PrivilegedAction<Object>) () ->
        {
            _connection.getEventLogger().message(ConnectionMessages.IDLE_CLOSE("Current connection state: " + _connection.getConnectionDelegate().getState(), true));
            getNetwork().close();
            return null;
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
            AccessController.doPrivileged((PrivilegedAction<Void>) () ->
            {
                _inputHandler.closed();
                if(_disassembler != null)
                {
                    _disassembler.closed();
                }
                return null;
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
    public boolean isClosing()
    {
        return _connection.isClosing() || _connection.isConnectionLost();
    }

    @Override
    public int getHeartbeatDelay()
    {
        return _connection.getHeartBeatDelay();
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
            return _connection.processPendingIterator(_sessionsWithWork);
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
    public void notifyWork(final AMQPSession<?,?> sessionModel)
    {
        _sessionsWithWork.add(sessionModel);
        notifyWork();
    }

    @Override
    public void clearWork()
    {
        _stateChanged.set(false);
    }

    @Override
    public void setWorkListener(final Action<ProtocolEngine> listener)
    {
        _workListener.set(listener);
    }

    @Override
    public boolean hasSessionWithName(final byte[] name)
    {
        return _connection.hasSessionWithName(name);
    }

    @Override
    public void sendConnectionCloseAsync(final CloseReason reason, final String description)
    {
        _connection.setConnectionCloseCause(reason, description);
        stopConnection();
        // Best mapping for all reasons is "forced"
        _connection.sendConnectionCloseAsync(ConnectionCloseCode.CONNECTION_FORCED, description);

    }

    @Override
    public void closeSessionAsync(final AMQPSession<?,?> session,
                                  final CloseReason reason, final String message)
    {
        ServerSession s = ((Session_0_10)session).getServerSession();
        _connection.closeSessionAsync(s, reason, message);
    }

    @Override
    protected void addAsyncTask(final Action<? super ServerConnection> action)
    {
        _connection.addAsyncTask(action);
    }

    @Override
    public void block()
    {
        _connection.block();
    }

    @Override
    public String getRemoteContainerName()
    {
        return getClientId();
    }

    @Override
    public Collection<? extends Session_0_10> getSessionModels()
    {
        final Collection<org.apache.qpid.server.model.Session> sessions =
                getChildren(org.apache.qpid.server.model.Session.class);
        final Collection<? extends Session_0_10> session_0_10s = new ArrayList<>((Collection)sessions);
        return session_0_10s;
    }

    @Override
    public void unblock()
    {
        _connection.unblock();
    }

    @Override
    public int getSessionCountLimit()
    {
        return _connection.getSessionCountLimit();
    }

    @Override
    protected boolean isOrderlyClose()
    {
        return !_connection.isConnectionLost();
    }

    @Override
    protected String getCloseCause()
    {
        String connectionCloseMessage = _connection.getConnectionCloseMessage();
        if (connectionCloseMessage == null)
        {
            return null;
        }
        return _connection.getConnectionCloseCode() + " - " + connectionCloseMessage;
    }

    @Override
    public void initialiseHeartbeating(final long writerDelay, final long readerDelay)
    {
        super.initialiseHeartbeating(writerDelay, readerDelay);
    }

    @Override
    protected boolean isOpeningInProgress()
    {
        ServerConnectionDelegate.ConnectionState state = _connection.getConnectionDelegate().getState();
        switch (state)
        {
            case INIT:
            case AWAIT_START_OK:
            case AWAIT_SECURE_OK:
            case AWAIT_TUNE_OK:
            case AWAIT_OPEN:
                return true;
            case OPEN:
                return false;
            default:
                throw new IllegalStateException("Unsupported state " + state);
        }
    }

    @Override
    public Iterator<ServerTransaction> getOpenTransactions()
    {
        return getSessionModels().stream()
                                 .filter(sessionModel -> sessionModel.getServerSession()
                                                                     .getTransaction() instanceof LocalTransaction)
                                 .map(sessionModel -> sessionModel.getServerSession().getTransaction())
                                 .iterator();
    }

}
