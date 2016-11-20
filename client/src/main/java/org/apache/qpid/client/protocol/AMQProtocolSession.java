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
package org.apache.qpid.client.protocol;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.jms.JMSException;
import javax.security.sasl.SaslClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.AMQException;
import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQProtocolHandler;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.client.ConnectionTuneParameters;
import org.apache.qpid.client.handler.ClientMethodDispatcherImpl;
import org.apache.qpid.client.message.UnprocessedMessage;
import org.apache.qpid.client.message.UnprocessedMessage_0_8;
import org.apache.qpid.client.state.AMQStateManager;
import org.apache.qpid.framing.AMQDataBlock;
import org.apache.qpid.framing.AMQMethodBody;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.ContentBody;
import org.apache.qpid.framing.ContentHeaderBody;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.framing.FrameCreatingMethodProcessor;
import org.apache.qpid.framing.HeartbeatBody;
import org.apache.qpid.framing.MethodDispatcher;
import org.apache.qpid.framing.MethodRegistry;
import org.apache.qpid.framing.ProtocolInitiation;
import org.apache.qpid.framing.ProtocolVersion;
import org.apache.qpid.protocol.AMQVersionAwareProtocolSession;
import org.apache.qpid.transport.ByteBufferSender;
import org.apache.qpid.transport.ConnectionSettings;
import org.apache.qpid.transport.TransportException;
import org.apache.qpid.transport.network.NetworkConnection;

/**
 * Wrapper for protocol session that provides type-safe access to session attributes.
 * <p>
 * The underlying protocol session is still available but clients should not use it to obtain session attributes.
 */
public class AMQProtocolSession implements AMQVersionAwareProtocolSession
{

    protected static final Logger _logger = LoggerFactory.getLogger(AMQProtocolSession.class);

    //Usable channels are numbered 1 to <ChannelMax>
    public static final int MAX_CHANNEL_MAX = 0xFFFF;
    public static final int MIN_USABLE_CHANNEL_NUM = 1;

    private final AMQProtocolHandler _protocolHandler;

    private ConcurrentMap<Integer,AMQSession<?,?>> _closingChannels = new ConcurrentHashMap<>();

    /**
     * Maps from a channel id to an unprocessed message. This is used to tie together the JmsDeliverBody (which arrives
     * first) with the subsequent content header and content bodies.
     */
    private final ConcurrentMap<Integer, UnprocessedMessage> _channelId2UnprocessedMsgMap = new ConcurrentHashMap<>();
    private final UnprocessedMessage[] _channelId2UnprocessedMsgArray = new UnprocessedMessage[16];

    private ProtocolVersion _protocolVersion;

    private final MethodRegistry _methodRegistry =
            new MethodRegistry(ProtocolVersion.getLatestSupportedVersion());

    private final FrameCreatingMethodProcessor _methodProcessor =
            new FrameCreatingMethodProcessor(ProtocolVersion.getLatestSupportedVersion());

    private MethodDispatcher _methodDispatcher;

    private final AMQConnection _connection;

    private ConnectionTuneParameters _connectionTuneParameters;
    private FieldTable _connectionStartServerProperties;

    private SaslClient _saslClient;

    private static final int FAST_CHANNEL_ACCESS_MASK = 0xFFFFFFF0;
    private volatile ByteBufferSender _sender;

    public AMQProtocolSession(AMQProtocolHandler protocolHandler, AMQConnection connection)
    {
        _protocolHandler = protocolHandler;
        _protocolVersion = connection.getProtocolVersion();
        if (_logger.isDebugEnabled())
        {
            _logger.debug("Using ProtocolVersion for Session:" + _protocolVersion);
        }
        _methodDispatcher = ClientMethodDispatcherImpl.newMethodDispatcher(ProtocolVersion.getLatestSupportedVersion(),
                                                                           this);
        _connection = connection;
    }

    public void init(ConnectionSettings settings)
    {
        // start the process of setting up the connection. This is the first place that
        // data is written to the server.
        initialiseTuneParameters(settings);

        _protocolHandler.writeFrame(new ProtocolInitiation(_connection.getProtocolVersion()));
    }

    public ConnectionTuneParameters getConnectionTuneParameters()
    {
        return _connectionTuneParameters;
    }

    private void initialiseTuneParameters(ConnectionSettings settings)
    {
        _connectionTuneParameters = new ConnectionTuneParameters();
        _connectionTuneParameters.setHeartbeat(settings.getHeartbeatInterval08());
        _connectionTuneParameters.setHeartbeatTimeoutFactor(settings.getHeartbeatTimeoutFactor());
    }

    public void tuneConnection(ConnectionTuneParameters params)
    {
        _connectionTuneParameters = params;
        AMQConnection con = getAMQConnection();

        con.setMaximumChannelCount(params.getChannelMax());
        con.setMaximumFrameSize(params.getFrameMax());


        initHeartbeats(params.getHeartbeat(), params.getHeartbeatTimeoutFactor());
    }

    void initHeartbeats(int delay, float timeoutFactor)
    {
        if (delay > 0)
        {
            NetworkConnection network = getProtocolHandler().getNetworkConnection();
            network.setMaxWriteIdleMillis(1000L*delay);
            int readerIdle = (int)(delay * timeoutFactor);
            network.setMaxReadIdleMillis(1000L * readerIdle);
        }
    }

    public String getClientID()
    {
        try
        {
            return getAMQConnection().getClientID();
        }
        catch (JMSException e)
        {
            // we never throw a JMSException here
            return null;
        }
    }

    public void setClientID(String clientID) throws JMSException
    {
        getAMQConnection().setClientID(clientID);
    }

    public AMQStateManager getStateManager()
    {
        return _protocolHandler.getStateManager();
    }

    public String getVirtualHost()
    {
        return getAMQConnection().getVirtualHost();
    }

    public SaslClient getSaslClient()
    {
        return _saslClient;
    }

    /**
     * Store the SASL client currently being used for the authentication handshake
     *
     * @param client if non-null, stores this in the session. if null clears any existing client being stored
     */
    public void setSaslClient(SaslClient client)
    {
        _saslClient = client;
    }

    /**
     * Callback invoked from the BasicDeliverMethodHandler when a message has been received.
     *
     * @throws QpidException if this was not expected
     */
    public void unprocessedMessageReceived(final int channelId, UnprocessedMessage message) throws QpidException
    {
        if ((channelId & FAST_CHANNEL_ACCESS_MASK) == 0)
        {
            _channelId2UnprocessedMsgArray[channelId] = message;
        }
        else
        {
            _channelId2UnprocessedMsgMap.put(channelId, message);
        }
    }

    public void contentHeaderReceived(int channelId, ContentHeaderBody contentHeader) throws QpidException
    {
        final UnprocessedMessage_0_8 msg = (UnprocessedMessage_0_8) ((channelId & FAST_CHANNEL_ACCESS_MASK) == 0 ? _channelId2UnprocessedMsgArray[channelId]
                                               : _channelId2UnprocessedMsgMap.get(channelId));

        if (msg == null)
        {
            throw new QpidException("Error: received content header without having received a BasicDeliver frame first on session:" + this, null);
        }

        if (msg.getContentHeader() != null)
        {
            throw new QpidException("Error: received duplicate content header or did not receive correct number of content body frames on session:" + this, null);
        }

        msg.setContentHeader(contentHeader);
        if (contentHeader.getBodySize() == 0)
        {
            deliverMessageToAMQSession(channelId, msg);
        }
    }

    public void contentBodyReceived(final int channelId, ContentBody contentBody) throws QpidException
    {
        UnprocessedMessage_0_8 msg;
        final boolean fastAccess = (channelId & FAST_CHANNEL_ACCESS_MASK) == 0;
        if (fastAccess)
        {
            msg = (UnprocessedMessage_0_8) _channelId2UnprocessedMsgArray[channelId];
        }
        else
        {
            msg = (UnprocessedMessage_0_8) _channelId2UnprocessedMsgMap.get(channelId);
        }

        if (msg == null)
        {
            throw new QpidException("Error: received content body without having received a JMSDeliver frame first", null);
        }

        if (msg.getContentHeader() == null)
        {
            if (fastAccess)
            {
                _channelId2UnprocessedMsgArray[channelId] = null;
            }
            else
            {
                _channelId2UnprocessedMsgMap.remove(channelId);
            }
            throw new QpidException("Error: received content body without having received a ContentHeader frame first", null);
        }

        msg.receiveBody(contentBody);

        if (msg.isAllBodyDataReceived())
        {
            deliverMessageToAMQSession(channelId, msg);
        }
    }

    public void heartbeatBodyReceived(int channelId, HeartbeatBody body) throws QpidException
    {
        _protocolHandler.heartbeatBodyReceived();
    }

    /**
     * Deliver a message to the appropriate session, removing the unprocessed message from our map
     *
     * @param channelId the channel id the message should be delivered to
     * @param msg       the message
     */
    private void deliverMessageToAMQSession(int channelId, UnprocessedMessage msg)
    {
        AMQSession session = getSession(channelId);
        session.messageReceived(msg);
        if ((channelId & FAST_CHANNEL_ACCESS_MASK) == 0)
        {
            _channelId2UnprocessedMsgArray[channelId] = null;
        }
        else
        {
            _channelId2UnprocessedMsgMap.remove(channelId);
        }
    }

    protected AMQSession getSession(int channelId)
    {
        return _connection.getSession(channelId);
    }

    public void writeFrame(AMQDataBlock frame)
    {
        _protocolHandler.writeFrame(frame);
    }

    /**
     * Starts the process of closing a session
     *
     * @param session the AMQSession being closed
     */
    public void closeSession(AMQSession<?,?> session)
    {
        if (_logger.isDebugEnabled())
        {
            _logger.debug("closeSession called on protocol session for session " + session.getChannelId());
        }
        final int channelId = session.getChannelId();
        if (channelId <= 0)
        {
            throw new IllegalArgumentException("Attempt to close a channel with id < 0");
        }
        // we need to know when a channel is closing so that we can respond
        // with a channel.close frame when we receive any other type of frame
        // on that channel
        _closingChannels.putIfAbsent(channelId, session);
    }

    /**
     * Called from the ChannelClose handler when a channel close frame is received. This method decides whether this is
     * a response or an initiation. The latter case causes the AMQSession to be closed and an exception to be thrown if
     * appropriate.
     *
     * @param channelId the id of the channel (session)
     *
     * @return true if the client must respond to the server, i.e. if the server initiated the channel close, false if
     *         the channel close is just the server responding to the client's earlier request to close the channel.
     */
    public boolean channelClosed(int channelId, int code, String text) throws QpidException
    {

        // if this is not a response to an earlier request to close the channel
        if (_closingChannels.remove(channelId) == null)
        {
            final AMQSession session = getSession(channelId);
            try
            {
                session.closed(new AMQException(code, text, null));
            }
            catch (JMSException e)
            {
                throw new QpidException("JMSException received while closing session", e);
            }

            return true;
        }
        else
        {
            return false;
        }
    }


    public AMQConnection getAMQConnection()
    {
        return _connection;
    }

    public void closeProtocolSession() throws QpidException
    {
        try
        {
            _protocolHandler.getNetworkConnection().close();
        }
        catch(TransportException e)
        {
            //ignore such exceptions, they were already logged
            //and this is a forcible close.
        }
    }

    public ByteBufferSender getSender()
    {
        return _sender;
    }

    public void confirmConsumerCancelled(int channelId, AMQShortString consumerTag)
    {
        final AMQSession session = getSession(channelId);

        session.confirmConsumerCancelled(consumerTag.toString());
    }

    public void setProtocolVersion(final ProtocolVersion pv)
    {
        if (_logger.isDebugEnabled())
        {
            _logger.debug("Setting ProtocolVersion to :" + pv);
        }
        _protocolVersion = pv;
        _methodRegistry.setProtocolVersion(pv);
        _methodProcessor.setProtocolVersion(pv);
        _methodDispatcher = ClientMethodDispatcherImpl.newMethodDispatcher(pv, this);
  }

    public ProtocolVersion getProtocolVersion()
    {
        return _protocolVersion;
    }

    public MethodRegistry getMethodRegistry()
    {
        return _methodRegistry;
    }

    public MethodDispatcher getMethodDispatcher()
    {
        return _methodDispatcher;
    }

    public void setTicket(int ticket, int channelId)
    {
        final AMQSession session = getSession(channelId);
        session.setTicket(ticket);
    }

    public void setFlowControl(final int channelId, final boolean active)
    {
        final AMQSession session = getSession(channelId);
        session.setFlowControl(active);
    }

    public void methodFrameReceived(final int channel, final AMQMethodBody amqMethodBody) throws QpidException
    {
        _protocolHandler.methodBodyReceived(channel, amqMethodBody);
    }

    public void notifyError(Exception error)
    {
        _protocolHandler.propagateExceptionToAllWaiters(error);
    }

    public void setSender(ByteBufferSender sender)
    {
        _sender = sender;
    }


    @Override
    public String toString()
    {
        return "AMQProtocolSession[" + _connection + ']';
    }

    /**
     * The handler from which this session was created and which is used to handle protocol events. We send failover
     * events to the handler.
     */
    protected AMQProtocolHandler getProtocolHandler()
    {
        return _protocolHandler;
    }

    protected AMQConnection getConnection()
    {
        return _connection;
    }

    public void setConnectionStartServerProperties(FieldTable serverProperties)
    {
        _connectionStartServerProperties = serverProperties;
    }

    public FieldTable getConnectionStartServerProperties()
    {
        return _connectionStartServerProperties;
    }

    public void setMaxFrameSize(final long frameMax)
    {
        _protocolHandler.setMaxFrameSize(frameMax);
    }

    public FrameCreatingMethodProcessor getMethodProcessor()
    {
        return _methodProcessor;
    }
}
