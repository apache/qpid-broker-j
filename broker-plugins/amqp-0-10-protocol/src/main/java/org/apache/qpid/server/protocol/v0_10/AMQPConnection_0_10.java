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
import java.nio.ByteBuffer;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.consumer.ConsumerImpl;
import org.apache.qpid.server.logging.messages.ConnectionMessages;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.ConnectionDelegate;
import org.apache.qpid.transport.Constant;
import org.apache.qpid.transport.network.AggregateTicker;
import org.apache.qpid.transport.network.InputHandler;
import org.apache.qpid.transport.network.NetworkConnection;


public class AMQPConnection_0_10 extends AbstractAMQPConnection<AMQPConnection_0_10>
{
    private static final Logger _logger = LoggerFactory.getLogger(AMQPConnection_0_10.class);
    private final InputHandler _inputHandler;


    private final NetworkConnection _network;
    private ServerConnection _connection;

    private long _createTime = System.currentTimeMillis();
    private volatile long _lastReadTime = _createTime;
    private volatile long _lastWriteTime = _createTime;
    private volatile boolean _transportBlockedForWriting;

    private final AtomicReference<Thread> _messageAssignmentSuspended = new AtomicReference<>();

    private final AtomicBoolean _stateChanged = new AtomicBoolean();
    private final AtomicReference<Action<ProtocolEngine>> _workListener = new AtomicReference<>();


    public AMQPConnection_0_10(final Broker<?> broker,
                               NetworkConnection network,
                               final AmqpPort<?> port,
                               final Transport transport,
                               final long id,
                               final AggregateTicker aggregateTicker)
    {
        super(broker, network, port, transport, id, aggregateTicker);

        _connection = new ServerConnection(id, broker, port, transport);
        _connection.setAmqpConnection(this);
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

        _inputHandler = new InputHandler(new ServerAssembler(_connection));
        _network = network;

        Subject.doAs(getSubject(), new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                _connection.getEventLogger().message(ConnectionMessages.OPEN(null, null, null, null, false, false, false, false));

                _connection.setNetworkConnection(_network);
                ServerDisassembler disassembler = new ServerDisassembler(wrapSender(_network.getSender()), Constant.MIN_MAX_FRAME_SIZE);
                _connection.setSender(disassembler);
                _connection.addFrameSizeObserver(disassembler);
                // FIXME Two log messages to maintain compatibility with earlier protocol versions
                _connection.getEventLogger().message(ConnectionMessages.OPEN(null, "0-10", null, null, false, true, false, false));

                return null;
            }
        });
    }

    @Override
    public boolean isMessageAssignmentSuspended()
    {
        Thread lock = _messageAssignmentSuspended.get();
        return lock != null && _messageAssignmentSuspended.get() != Thread.currentThread();
    }

    @Override
    public void setMessageAssignmentSuspended(final boolean messageAssignmentSuspended)
    {
        _messageAssignmentSuspended.set(messageAssignmentSuspended ? Thread.currentThread() : null);

        for(AMQSessionModel<?> session : _connection.getSessionModels())
        {
            for (Consumer<?> consumer : session.getConsumers())
            {
                ConsumerImpl consumerImpl = (ConsumerImpl) consumer;
                if (!messageAssignmentSuspended)
                {
                    consumerImpl.getTarget().notifyCurrentState();
                }
                else
                {
                    // ensure that by the time the method returns, no consumer can be in the process of
                    // delivering a message.
                    consumerImpl.getSendLock();
                    consumerImpl.releaseSendLock();
                }
            }
        }
    }


    private ByteBufferSender wrapSender(final ByteBufferSender sender)
    {
        return new ByteBufferSender()
        {
            @Override
            public void send(ByteBuffer msg)
            {
                _lastWriteTime = System.currentTimeMillis();
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
    public long getLastReadTime()
    {
        return _lastReadTime;
    }

    @Override
    public long getLastWriteTime()
    {
        return _lastWriteTime;
    }

    public void received(final ByteBuffer buf)
    {
        Subject.doAs(_connection.getAuthorizedSubject(), new PrivilegedAction<Object>()
        {
            @Override
            public Object run()
            {
                _lastReadTime = System.currentTimeMillis();
                if (_connection.getAuthorizedPrincipal() == null &&
                    (_lastReadTime - _createTime) > _connection.getPort().getContextValue(Long.class,
                                                                                          Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY))
                {

                    _logger.warn("Connection has taken more than "
                                 + _connection.getPort()
                                         .getContextValue(Long.class, Port.CONNECTION_MAXIMUM_AUTHENTICATION_DELAY)
                                 + "ms to establish identity.  Closing as possible DoS.");
                    _connection.getEventLogger().message(ConnectionMessages.IDLE_CLOSE());
                    _network.close();

                }
                _inputHandler.received(buf);
                _connection.receivedComplete();
                return null;
            }
        });
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
        Subject.doAs(_connection.getAuthorizedSubject(), new PrivilegedAction<Object>()
            {
                @Override
                public Object run()
                {
                    _connection.getEventLogger().message(ConnectionMessages.IDLE_CLOSE());
                    _network.close();
                    return null;
                }
            });

    }

    public String getAddress()
    {
        return _network.getRemoteAddress().toString();
    }

    @Override
    public void closed()
    {
        try
        {
            _inputHandler.closed();
        }
        finally
        {
            markTransportClosed();
        }
    }

    @Override
    protected void performDeleteTasks()
    {
        super.performDeleteTasks();
    }

    @Override
    public boolean isTransportBlockedForWriting()
    {
        return _transportBlockedForWriting;
    }

    @Override
    public void setTransportBlockedForWriting(final boolean blocked)
    {
        _transportBlockedForWriting = blocked;
        _connection.transportStateChanged();
    }

    @Override
    public void processPending()
    {
        _connection.processPending();
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

    public Principal getAuthorizedPrincipal()
    {
        return _connection.getAuthorizedPrincipal();
    }

    public void closeSessionAsync(final AMQSessionModel<?> session,
                                  final AMQConstant cause, final String message)
    {
        _connection.closeSessionAsync((ServerSession)session, cause, message);
    }

    public void block()
    {
        _connection.block();
    }

    public String getRemoteContainerName()
    {
        return _connection.getRemoteContainerName();
    }

    public VirtualHost<?, ?, ?> getVirtualHost()
    {
        return _connection.getVirtualHost();
    }

    public List<ServerSession> getSessionModels()
    {
        return _connection.getSessionModels();
    }

    public void unblock()
    {
        _connection.unblock();
    }

    public LogSubject getLogSubject()
    {
        return _connection.getLogSubject();
    }

    public long getSessionCountLimit()
    {
        return _connection.getSessionCountLimit();
    }

}
