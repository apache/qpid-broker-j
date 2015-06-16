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
package org.apache.qpid.server.protocol.v1_0;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.amqp_1_0.codec.FrameWriter;
import org.apache.qpid.amqp_1_0.codec.ProtocolHandler;
import org.apache.qpid.amqp_1_0.framing.AMQFrame;
import org.apache.qpid.amqp_1_0.framing.FrameHandler;
import org.apache.qpid.amqp_1_0.framing.OversizeFrameException;
import org.apache.qpid.amqp_1_0.framing.SASLFrameHandler;
import org.apache.qpid.amqp_1_0.transport.ConnectionEndpoint;
import org.apache.qpid.amqp_1_0.transport.Container;
import org.apache.qpid.amqp_1_0.transport.FrameOutputHandler;
import org.apache.qpid.amqp_1_0.transport.SaslServerProvider;
import org.apache.qpid.amqp_1_0.type.Binary;
import org.apache.qpid.amqp_1_0.type.FrameBody;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.common.QpidProperties;
import org.apache.qpid.common.ServerPropertyNames;
import org.apache.qpid.server.protocol.ConnectionClosingTicker;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.consumer.ConsumerImpl;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.protocol.AMQSessionModel;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManagerImpl;
import org.apache.qpid.server.transport.NetworkConnectionScheduler;
import org.apache.qpid.server.transport.NonBlockingConnection;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.network.AggregateTicker;
import org.apache.qpid.transport.network.NetworkConnection;

public class ProtocolEngine_1_0_0 implements ProtocolEngine, FrameOutputHandler
{

    public static final long CLOSE_REPONSE_TIMEOUT = 10000l;
    private final AmqpPort<?> _port;
    private final Transport _transport;
    private final AggregateTicker _aggregateTicker;
    private final boolean _useSASL;
    private long _readBytes;
    private long _writtenBytes;

    private volatile long _lastReadTime;
    private volatile long _lastWriteTime;
    private final Broker<?> _broker;
    private long _createTime = System.currentTimeMillis();
    private ConnectionEndpoint _endpoint;
    private long _connectionId;
    private final AtomicBoolean _stateChanged = new AtomicBoolean();
    private final AtomicReference<Action<ProtocolEngine>> _workListener = new AtomicReference<>();


    private static final ByteBuffer SASL_LAYER_HEADER =
           ByteBuffer.wrap(new byte[]
                   {
                       (byte)'A',
                       (byte)'M',
                       (byte)'Q',
                       (byte)'P',
                       (byte) 3,
                       (byte) 1,
                       (byte) 0,
                       (byte) 0
                   });

    private static final ByteBuffer AMQP_LAYER_HEADER =
        ByteBuffer.wrap(new byte[]
                {
                    (byte)'A',
                    (byte)'M',
                    (byte)'Q',
                    (byte)'P',
                    (byte) 0,
                    (byte) 1,
                    (byte) 0,
                    (byte) 0
                });


    private FrameWriter _frameWriter;
    private ProtocolHandler _frameHandler;
    private ByteBuffer _buf = ByteBuffer.allocate(1024 * 1024);
    private Object _sendLock = new Object();
    private byte _major;
    private byte _minor;
    private byte _revision;
    private NetworkConnection _network;
    private ByteBufferSender _sender;
    private Connection_1_0 _connection;
    private volatile boolean _transportBlockedForWriting;


    static enum State {
           A,
           M,
           Q,
           P,
           PROTOCOL,
           MAJOR,
           MINOR,
           REVISION,
           FRAME
       }

    private State _state = State.A;

    private final AtomicReference<Thread> _messageAssignmentSuspended = new AtomicReference<>();




    public ProtocolEngine_1_0_0(final NetworkConnection networkDriver,
                                final Broker<?> broker,
                                long id,
                                AmqpPort<?> port,
                                Transport transport,
                                final AggregateTicker aggregateTicker,
                                final boolean useSASL)
    {
        _connectionId = id;
        _broker = broker;
        _port = port;
        _transport = transport;
        _aggregateTicker = aggregateTicker;
        _useSASL = useSASL;
        if(networkDriver != null)
        {
            setNetworkConnection(networkDriver, networkDriver.getSender());
        }
    }

    @Override
    public AggregateTicker getAggregateTicker()
    {
        return _aggregateTicker;
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

        for(AMQSessionModel<?,?> session : _connection.getSessionModels())
        {
            for(Consumer<?> consumer : session.getConsumers())
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


    public SocketAddress getRemoteAddress()
    {
        return _network.getRemoteAddress();
    }

    public SocketAddress getLocalAddress()
    {
        return _network.getLocalAddress();
    }

    public long getReadBytes()
    {
        return _readBytes;
    }

    public long getWrittenBytes()
    {
        return _writtenBytes;
    }

    public void writerIdle()
    {
        //Todo
    }

    public void readerIdle()
    {
        //Todo
    }

    @Override
    public void encryptedTransport()
    {
    }

    public void setNetworkConnection(final NetworkConnection network, final ByteBufferSender sender)
    {
        _network = network;
        _sender = sender;

        Container container = new Container(_broker.getId().toString());

        SubjectCreator subjectCreator = _port.getAuthenticationProvider().getSubjectCreator(_transport.isSecure());
        _endpoint = new ConnectionEndpoint(container, _useSASL ? asSaslServerProvider(subjectCreator) : null);
        _endpoint.setLogger(new ConnectionEndpoint.FrameReceiptLogger()
        {
            @Override
            public boolean isEnabled()
            {
                return FRAME_LOGGER.isDebugEnabled();
            }

            @Override
            public void received(final SocketAddress remoteAddress, final short channel, final Object frame)
            {
                FRAME_LOGGER.debug("RECV[" + remoteAddress + "|" + channel + "] : " + frame);
            }
        });
        Map<Symbol,Object> serverProperties = new LinkedHashMap<Symbol, Object>();
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.PRODUCT), QpidProperties.getProductName());
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.VERSION), QpidProperties.getReleaseVersion());
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.QPID_BUILD), QpidProperties.getBuildVersion());
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.QPID_INSTANCE_NAME), _broker.getName());

        _endpoint.setProperties(serverProperties);

        _endpoint.setRemoteAddress(getRemoteAddress());
        _connection = new Connection_1_0(_broker, _endpoint, _connectionId, _port, _transport, subjectCreator, this);

        _endpoint.setConnectionEventListener(_connection);
        _endpoint.setFrameOutputHandler(this);
        ByteBuffer headerResponse;
        final List<String> mechanisms = subjectCreator.getMechanisms();
        if(_useSASL)
        {
            _endpoint.setSaslFrameOutput(this);

            _endpoint.setOnSaslComplete(new Runnable()
            {
                public void run()
                {
                    if (_endpoint.isAuthenticated())
                    {
                        _sender.send(AMQP_LAYER_HEADER.duplicate());
                        _sender.flush();
                    }
                    else
                    {
                        _network.close();
                    }
                }
            });
            _frameHandler = new SASLFrameHandler(_endpoint);
            headerResponse = SASL_LAYER_HEADER;
        }
        else
        {
            if(mechanisms.contains(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
               && _network.getPeerPrincipal() != null)
            {
                _connection.setUserPrincipal(new AuthenticatedPrincipal(_network.getPeerPrincipal()));
            }
            else if(mechanisms.contains(AnonymousAuthenticationManager.MECHANISM_NAME))
            {
                _connection.setUserPrincipal(new AuthenticatedPrincipal(AnonymousAuthenticationManager.ANONYMOUS_PRINCIPAL));
            }
            else
            {
                _network.close();
            }

            _frameHandler = new FrameHandler(_endpoint);
            headerResponse = AMQP_LAYER_HEADER;
        }
        _frameWriter =  new FrameWriter(_endpoint.getDescribedTypeRegistry());

        _sender.send(headerResponse.duplicate());
        _sender.flush();

        if(_useSASL)
        {
            _endpoint.initiateSASL(mechanisms.toArray(new String[mechanisms.size()]));
        }

    }

    private SaslServerProvider asSaslServerProvider(final SubjectCreator subjectCreator)
    {
        return new SaslServerProvider()
        {
            @Override
            public SaslServer getSaslServer(String mechanism, String fqdn) throws SaslException
            {
                return subjectCreator.createSaslServer(mechanism, fqdn, _network.getPeerPrincipal());
            }

            @Override
            public Principal getAuthenticatedPrincipal(SaslServer server)
            {
                return new AuthenticatedPrincipal(new UsernamePrincipal(server.getAuthorizationID()));
            }
        };
    }

    public String getAddress()
    {
        return getRemoteAddress().toString();
    }

    public boolean isDurable()
    {
        return false;
    }

    private final Logger RAW_LOGGER = LoggerFactory.getLogger("RAW");


    public synchronized void received(final ByteBuffer msg)
    {
        try
        {
            _lastReadTime = System.currentTimeMillis();
            if(RAW_LOGGER.isDebugEnabled())
            {
                ByteBuffer dup = msg.duplicate();
                byte[] data = new byte[dup.remaining()];
                dup.get(data);
                Binary bin = new Binary(data);
                RAW_LOGGER.debug("RECV[" + getRemoteAddress() + "] : " + bin.toString());
            }
            _readBytes += msg.remaining();
            switch(_state)
            {
                case A:
                    if (msg.hasRemaining())
                    {
                        msg.get();
                    }
                    else
                    {
                        break;
                    }
                case M:
                    if (msg.hasRemaining())
                    {
                        msg.get();
                    }
                    else
                    {
                        _state = State.M;
                        break;
                    }

                case Q:
                    if (msg.hasRemaining())
                    {
                        msg.get();
                    }
                    else
                    {
                        _state = State.Q;
                        break;
                    }
                case P:
                    if (msg.hasRemaining())
                    {
                        msg.get();
                    }
                    else
                    {
                        _state = State.P;
                        break;
                    }
                case PROTOCOL:
                    if (msg.hasRemaining())
                    {
                        msg.get();
                    }
                    else
                    {
                        _state = State.PROTOCOL;
                        break;
                    }
                case MAJOR:
                    if (msg.hasRemaining())
                    {
                        _major = msg.get();
                    }
                    else
                    {
                        _state = State.MAJOR;
                        break;
                    }
                case MINOR:
                    if (msg.hasRemaining())
                    {
                        _minor = msg.get();
                    }
                    else
                    {
                        _state = State.MINOR;
                        break;
                    }
                case REVISION:
                    if (msg.hasRemaining())
                    {
                        _revision = msg.get();

                        _state = State.FRAME;
                    }
                    else
                    {
                        _state = State.REVISION;
                        break;
                    }
                case FRAME:
                    if (msg.hasRemaining())
                    {
                        Subject.doAs(_connection.getSubject(), new PrivilegedAction<Void>()
                        {
                            @Override
                            public Void run()
                            {
                                _frameHandler = _frameHandler.parse(msg);
                                return null;
                            }
                        });

                    }
            }
        }
        catch(RuntimeException e)
        {
            exception(e);
        }
     }

    public void exception(Throwable throwable)
    {
        // noop - exception method is not used by new i/o layer
    }

    public void closed()
    {
        try
        {
            // todo
            try
            {
                _endpoint.inputClosed();
            }
            finally
            {
                if (_endpoint != null && _endpoint.getConnectionEventListener() != null)
                {
                    ((Connection_1_0) _endpoint.getConnectionEventListener()).closed();
                }
            }
        }
        catch(RuntimeException e)
        {
            exception(e);
        }
    }

    void changeScheduler(final NetworkConnectionScheduler networkConnectionScheduler)
    {
        ((NonBlockingConnection)_network).changeScheduler(networkConnectionScheduler);
    }


    public long getCreateTime()
    {
        return _createTime;
    }


    public boolean canSend()
    {
        return true;
    }

    public void send(final AMQFrame amqFrame)
    {
        send(amqFrame, null);
    }

    private static final Logger FRAME_LOGGER = LoggerFactory.getLogger("FRM");


    public void send(final AMQFrame amqFrame, ByteBuffer buf)
    {

        synchronized (_sendLock)
        {
            _lastWriteTime = System.currentTimeMillis();
            if (FRAME_LOGGER.isDebugEnabled())
            {
                FRAME_LOGGER.debug("SEND["
                                   + getRemoteAddress()
                                   + "|"
                                   + amqFrame.getChannel()
                                   + "] : "
                                   + amqFrame.getFrameBody());
            }

            _frameWriter.setValue(amqFrame);

            ByteBuffer dup = ByteBuffer.allocate(_endpoint.getMaxFrameSize());

            int size = _frameWriter.writeToBuffer(dup);
            if (size > _endpoint.getMaxFrameSize())
            {
                throw new OversizeFrameException(amqFrame, size);
            }

            dup.flip();
            _writtenBytes += dup.limit();

            if (RAW_LOGGER.isDebugEnabled())
            {
                ByteBuffer dup2 = dup.duplicate();
                byte[] data = new byte[dup2.remaining()];
                dup2.get(data);
                Binary bin = new Binary(data);
                RAW_LOGGER.debug("SEND[" + getRemoteAddress() + "] : " + bin.toString());
            }

            _sender.send(dup);
            _sender.flush();


        }
    }

    public void send(short channel, FrameBody body)
    {
        AMQFrame frame = AMQFrame.createAMQFrame(channel, body);
        send(frame);

    }



    public void close()
    {
        getAggregateTicker().addTicker(new ConnectionClosingTicker(System.currentTimeMillis()+ CLOSE_REPONSE_TIMEOUT, _network));

    }

    public long getConnectionId()
    {
        return _connectionId;
    }

    @Override
    public Subject getSubject()
    {
        return _connection.getSubject();
    }

    public long getLastReadTime()
    {
        return _lastReadTime;
    }

    public long getLastWriteTime()
    {
        return _lastWriteTime;
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

}
