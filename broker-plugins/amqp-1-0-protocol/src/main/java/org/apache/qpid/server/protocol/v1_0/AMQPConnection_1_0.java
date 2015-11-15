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
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.common.ServerPropertyNames;
import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.protocol.ConnectionClosingTicker;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.model.Broker;
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
import org.apache.qpid.server.transport.ServerNetworkConnection;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.VirtualHostImpl;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.network.AggregateTicker;

public class AMQPConnection_1_0 extends AbstractAMQPConnection<AMQPConnection_1_0>
        implements FrameOutputHandler
{

    public static Logger LOGGER = LoggerFactory.getLogger(AMQPConnection_1_0.class);

    public static final long CLOSE_RESPONSE_TIMEOUT = 10000l;
    private final Broker<?> _broker;

    private ConnectionEndpoint _endpoint;
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
    private Object _sendLock = new Object();
    private byte _major;
    private byte _minor;
    private byte _revision;
    private final Connection_1_0 _connection;
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

    public AMQPConnection_1_0(final Broker<?> broker, final ServerNetworkConnection network,
                              AmqpPort<?> port, Transport transport, long id,
                              final AggregateTicker aggregateTicker,
                              final boolean useSASL)
    {
        super(broker, network, port, transport, Protocol.AMQP_1_0, id, aggregateTicker);
        _broker = broker;
        _connection = createConnection(broker, network, port, transport, id, useSASL);

        _connection.setAmqpConnection(this);
        _endpoint = _connection.getConnectionEndpoint();
        _endpoint.setConnectionEventListener(_connection);
        _endpoint.setFrameOutputHandler(this);
        final List<String> mechanisms = port.getAuthenticationProvider().getSubjectCreator(transport.isSecure()).getMechanisms();
        ByteBuffer headerResponse = useSASL ? initiateSasl() : initiateNonSasl(mechanisms);

        _frameWriter =  new FrameWriter(_endpoint.getDescribedTypeRegistry());

        getSender().send(QpidByteBuffer.wrap(headerResponse.duplicate()));
        getSender().flush();

        if(useSASL)
        {
            _endpoint.initiateSASL(mechanisms.toArray(new String[mechanisms.size()]));
        }


    }

    public static Connection_1_0 createConnection(final Broker<?> broker,
                                                  final ServerNetworkConnection network,
                                                  final AmqpPort<?> port,
                                                  final Transport transport,
                                                  final long id,
                                                  final boolean useSASL)
    {
        Container container = new Container(broker.getId().toString());

        SubjectCreator subjectCreator = port.getAuthenticationProvider().getSubjectCreator(transport.isSecure());
        final ConnectionEndpoint endpoint =
                new ConnectionEndpoint(container, useSASL ? asSaslServerProvider(subjectCreator, network) : null);
        endpoint.setLogger(new ConnectionEndpoint.FrameReceiptLogger()
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
        Map<Symbol,Object> serverProperties = new LinkedHashMap<>();
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.PRODUCT), CommonProperties.getProductName());
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.VERSION), CommonProperties.getReleaseVersion());
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.QPID_BUILD), CommonProperties.getBuildVersion());
        serverProperties.put(Symbol.valueOf(ServerPropertyNames.QPID_INSTANCE_NAME), broker.getName());

        endpoint.setProperties(serverProperties);

        endpoint.setRemoteAddress(network.getRemoteAddress());
        return new Connection_1_0(endpoint, id, port, transport, subjectCreator);
    }


    public ByteBufferSender getSender()
    {
        return getNetwork().getSender();
    }

    public ByteBuffer initiateNonSasl(final List<String> mechanisms)
    {
        final ByteBuffer headerResponse;
        if(mechanisms.contains(ExternalAuthenticationManagerImpl.MECHANISM_NAME)
           && getNetwork().getPeerPrincipal() != null)
        {
            _connection.setUserPrincipal(new AuthenticatedPrincipal(getNetwork().getPeerPrincipal()));
        }
        else if(mechanisms.contains(AnonymousAuthenticationManager.MECHANISM_NAME))
        {
            _connection.setUserPrincipal(new AuthenticatedPrincipal(AnonymousAuthenticationManager.ANONYMOUS_PRINCIPAL));
        }
        else
        {
            getNetwork().close();
        }

        _frameHandler = new FrameHandler(_endpoint);
        headerResponse = AMQP_LAYER_HEADER;
        return headerResponse;
    }

    public ByteBuffer initiateSasl()
    {
        final ByteBuffer headerResponse;
        _endpoint.setSaslFrameOutput(this);

        _endpoint.setOnSaslComplete(new Runnable()
        {
            public void run()
            {
                if (_endpoint.isAuthenticated())
                {
                    getSender().send(QpidByteBuffer.wrap(AMQP_LAYER_HEADER.duplicate()));
                    getSender().flush();
                }
                else
                {
                    getNetwork().close();
                }
            }
        });
        _frameHandler = new SASLFrameHandler(_endpoint);
        headerResponse = SASL_LAYER_HEADER;
        return headerResponse;
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

    private static SaslServerProvider asSaslServerProvider(final SubjectCreator subjectCreator,
                                                           final ServerNetworkConnection network)
    {
        return new SaslServerProvider()
        {
            @Override
            public SaslServer getSaslServer(String mechanism, String fqdn) throws SaslException
            {
                return subjectCreator.createSaslServer(mechanism, fqdn, network.getPeerPrincipal());
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
        return getNetwork().getRemoteAddress().toString();
    }

    private final Logger RAW_LOGGER = LoggerFactory.getLogger("RAW");


    public synchronized void received(final QpidByteBuffer msg)
    {
        try
        {
            updateLastReadTime();
            if(RAW_LOGGER.isDebugEnabled())
            {
                QpidByteBuffer dup = msg.duplicate();
                byte[] data = new byte[dup.remaining()];
                dup.get(data);
                dup.dispose();
                Binary bin = new Binary(data);
                RAW_LOGGER.debug("RECV[" + getNetwork().getRemoteAddress() + "] : " + bin.toString());
            }
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
                        AccessController.doPrivileged(new PrivilegedAction<Void>()
                        {
                            @Override
                            public Void run()
                            {
                                _frameHandler = _frameHandler.parse(msg);
                                return null;
                            }
                        }, getAccessControllerContext());

                    }
            }
        }
        catch(ConnectionScopedRuntimeException e)
        {
            throw e;
        }
        catch(RuntimeException e)
        {
            LOGGER.error("Unexpected exception while processing incoming data", e);
            throw new ConnectionScopedRuntimeException("Unexpected exception while processing incoming data", e);
        }
        finally
        {
            msg.position(msg.limit());
        }
     }


    public void closed()
    {
        try
        {
            _endpoint.inputClosed();
        }
        catch(RuntimeException e)
        {
            LOGGER.error("Exception while closing", e);
        }
        finally
        {
            try
            {
                _connection.closed();
            }
            finally
            {
                markTransportClosed();
            }
        }
    }

    void changeScheduler(final NetworkConnectionScheduler networkConnectionScheduler)
    {
        ((NonBlockingConnection) getNetwork()).changeScheduler(networkConnectionScheduler);
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
            updateLastWriteTime();
            if (FRAME_LOGGER.isDebugEnabled())
            {
                FRAME_LOGGER.debug("SEND["
                                   + getNetwork().getRemoteAddress()
                                   + "|"
                                   + amqFrame.getChannel()
                                   + "] : "
                                   + amqFrame.getFrameBody());
            }

            _frameWriter.setValue(amqFrame);

            QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(_frameWriter.getSize());

            try
            {
                int size = _frameWriter.writeToBuffer(buffer);
                if (size > _endpoint.getMaxFrameSize())
                {
                    throw new OversizeFrameException(amqFrame, size);
                }

                buffer.flip();

                if (RAW_LOGGER.isDebugEnabled())
                {
                    QpidByteBuffer dup = buffer.duplicate();
                    byte[] data = new byte[dup.remaining()];
                    dup.get(data);
                    dup.dispose();
                    Binary bin = new Binary(data);
                    RAW_LOGGER.debug("SEND[" + getNetwork().getRemoteAddress() + "] : " + bin.toString());
                }

                getSender().send(buffer);
                getSender().flush();
            }
            finally
            {
                buffer.dispose();
            }
        }
    }

    public void send(short channel, FrameBody body)
    {
        AMQFrame frame = AMQFrame.createAMQFrame(channel, body);
        send(frame);

    }

    @Override
    protected void performDeleteTasks()
    {
        super.performDeleteTasks();
    }

    public void close()
    {
        getAggregateTicker().addTicker(new ConnectionClosingTicker(System.currentTimeMillis() + CLOSE_RESPONSE_TIMEOUT,
                                                                   getNetwork()));

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
    public void clearWork()
    {
        _stateChanged.set(false);
    }

    @Override
    public void setWorkListener(final Action<ProtocolEngine> listener)
    {
        _workListener.set(listener);
    }

    public boolean hasSessionWithName(final byte[] name)
    {
        return false;
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
        _connection.closeSessionAsync((Session_1_0) session, cause, message);
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

    public List<Session_1_0> getSessionModels()
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
    protected EventLogger getEventLogger()
    {
        final VirtualHostImpl virtualHost = _connection.getVirtualHost();
        if (virtualHost !=  null)
        {
            return virtualHost.getEventLogger();
        }
        else
        {
            return _broker.getEventLogger();
        }
    }
}
