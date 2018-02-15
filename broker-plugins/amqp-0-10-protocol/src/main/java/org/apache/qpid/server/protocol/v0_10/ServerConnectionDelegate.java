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

import static org.apache.qpid.server.protocol.v0_10.ServerConnection.State.CLOSE_RCVD;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.ConnectionPropertyEnricher;
import org.apache.qpid.server.properties.ConnectionStartProperties;
import org.apache.qpid.server.protocol.v0_10.transport.*;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticationResult.AuthenticationStatus;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.virtualhost.VirtualHostUnavailableException;

public class ServerConnectionDelegate extends MethodDelegate<ServerConnection> implements ProtocolDelegate<ServerConnection>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerConnectionDelegate.class);
    private final AmqpPort<?> _port;

    private List<Object> _locales;
    private List<Object> _mechanisms;

    private final Broker<?> _broker;
    private int _maxNoOfChannels;
    private Map<String,Object> _clientProperties;
    private final SubjectCreator _subjectCreator;
    private int _maximumFrameSize;

    private boolean _compressionSupported;
    private volatile SaslNegotiator _saslNegotiator;

    enum ConnectionState
    {
        INIT,
        AWAIT_START_OK,
        AWAIT_SECURE_OK,
        AWAIT_TUNE_OK,
        AWAIT_OPEN,
        OPEN
    }

    private volatile ConnectionState _state = ConnectionState.INIT;
    private volatile SubjectAuthenticationResult _successfulAuthenticationResult;


    public ServerConnectionDelegate(AmqpPort<?> port, boolean secure, final String selectedHost)
    {
        _port = port;
        _broker = (Broker<?>) port.getParent();
        _mechanisms = new ArrayList<>(port.getAuthenticationProvider().getAvailableMechanisms(secure));

        _maxNoOfChannels = port.getSessionCountLimit();
        _subjectCreator = port.getSubjectCreator(secure, selectedHost);
        _maximumFrameSize = Math.min(0xffff, _broker.getNetworkBufferSize());
    }

    @Override
    public void control(ServerConnection conn, Method method)
    {
        method.dispatch(conn, this);
    }

    @Override
    public void command(ServerConnection conn, Method method)
    {
        method.dispatch(conn, this);
    }

    @Override
    public void error(ServerConnection conn, ProtocolError error)
    {
        conn.exception(new ConnectionException(error.getMessage()));
    }

    @Override
    public void handle(ServerConnection conn, Method method)
    {
        conn.dispatch(method);
    }

    @Override public void connectionHeartbeat(ServerConnection conn, ConnectionHeartbeat hearbeat)
    {
        // do nothing
    }

    protected void sendConnectionCloseOkAndCloseSender(ServerConnection conn)
    {
        conn.connectionCloseOk();
        conn.getSender().close();
    }

    @Override public void connectionCloseOk(ServerConnection conn, ConnectionCloseOk ok)
    {
        conn.getSender().close();
    }

    @Override public void sessionDetached(ServerConnection conn, SessionDetached dtc)
    {
        ServerSession ssn = conn.getSession(dtc.getChannel());
        if (ssn != null)
        {
            ssn.setDetachCode(dtc.getCode());
            conn.unmap(ssn);
            ssn.closed();
        }
    }

    public final ConnectionState getState()
    {
        return _state;
    }

    private void assertState(final ServerConnection conn, final ConnectionState requiredState)
    {
        if(_state != requiredState)
        {
            String replyText = "Command Invalid, expected " + requiredState + " but was " + _state;
            conn.sendConnectionClose(ConnectionCloseCode.FRAMING_ERROR, replyText);
            conn.closeAndIgnoreFutureInput();
            throw new ConnectionScopedRuntimeException(replyText);
        }
    }

    @Override
    public void init(final ServerConnection serverConnection, final ProtocolHeader hdr)
    {
        assertState(serverConnection, ConnectionState.INIT);
        serverConnection.send(new ProtocolHeader(1, 0, 10));
        Map<String,Object> props = Collections.emptyMap();
        for(ConnectionPropertyEnricher enricher : _port.getConnectionPropertyEnrichers())
        {
            props = enricher.addConnectionProperties(serverConnection.getAmqpConnection(), props);
        }
        serverConnection.sendConnectionStart(props, _mechanisms, Collections.singletonList((Object)"en_US"));
        _state = ConnectionState.AWAIT_START_OK;
    }

    @Override
    public void connectionSecureOk(final ServerConnection serverConnection, final ConnectionSecureOk ok)
    {
        assertState(serverConnection, ConnectionState.AWAIT_SECURE_OK);
        secure(serverConnection, ok.getResponse());
    }

    protected void secure(final ServerConnection sconn, final byte[] response)
    {
        SubjectAuthenticationResult authResult = _successfulAuthenticationResult;
        byte[] challenge = null;
        if (authResult == null)
        {
            authResult = _subjectCreator.authenticate(_saslNegotiator, response);
            challenge = authResult.getChallenge();
        }

        if (AuthenticationStatus.SUCCESS.equals(authResult.getStatus()))
        {
            _successfulAuthenticationResult = authResult;
            if (challenge == null || challenge.length == 0)
            {
                sconn.sendConnectionTune(getChannelMax(), getFrameMax(), 0, getHeartbeatMax());
                sconn.setAuthorizedSubject(authResult.getSubject());
                _state = ConnectionState.AWAIT_TUNE_OK;
                disposeSaslNegotiator();
            }
            else
            {
                sconn.sendConnectionSecure(authResult.getChallenge());
                _state = ConnectionState.AWAIT_SECURE_OK;
            }
        }
        else if (AuthenticationStatus.CONTINUE.equals(authResult.getStatus()))
        {
            sconn.sendConnectionSecure(authResult.getChallenge());
            _state = ConnectionState.AWAIT_SECURE_OK;
        }
        else
        {
            connectionAuthFailed(sconn, authResult.getCause());
        }
    }

    @Override
    public void connectionClose(ServerConnection sconn, ConnectionClose close)
    {
        sconn.closeCode(close);
        sconn.setState(CLOSE_RCVD);
        sendConnectionCloseOkAndCloseSender(sconn);
    }

    @Override
    public void connectionOpen(ServerConnection sconn, ConnectionOpen open)
    {
        assertState(sconn, ConnectionState.AWAIT_OPEN);
        NamedAddressSpace addressSpace;
        String vhostName;
        if(open.hasVirtualHost())
        {
            vhostName = open.getVirtualHost();
        }
        else
        {
            vhostName = "";
        }

        AmqpPort port = sconn.getPort();
        addressSpace = port.getAddressSpace(vhostName);

        if(addressSpace != null)
        {
            if (!addressSpace.isActive())
            {
                sconn.setState(ServerConnection.State.CLOSING);
                final String redirectHost = addressSpace.getRedirectHost(port);
                if(redirectHost == null)
                {
                    sconn.sendConnectionClose(ConnectionCloseCode.CONNECTION_FORCED,
                                                     "Virtual host '" + vhostName + "' is not active");
                }
                else
                {
                    sconn.invoke(new ConnectionRedirect(redirectHost, new ArrayList<Object>()));
                }
                return;
            }

            try
            {
                sconn.setVirtualHost(addressSpace);
                if(!addressSpace.authoriseCreateConnection(sconn.getAmqpConnection()))
                {
                    sconn.setState(ServerConnection.State.CLOSING);
                    sconn.sendConnectionClose(ConnectionCloseCode.CONNECTION_FORCED, "Connection not authorized");
                    return;
                }
            }
            catch (AccessControlException | VirtualHostUnavailableException e)
            {
                sconn.setState(ServerConnection.State.CLOSING);
                sconn.sendConnectionClose(ConnectionCloseCode.CONNECTION_FORCED, e.getMessage());
                return;
            }

            sconn.setState(ServerConnection.State.OPEN);
            _state = ConnectionState.OPEN;
            sconn.invoke(new ConnectionOpenOk(Collections.emptyList()));
        }
        else
        {
            sconn.setState(ServerConnection.State.CLOSING);
            sconn.sendConnectionClose(ConnectionCloseCode.INVALID_PATH,
                                             "Unknown virtualhost '" + vhostName + "'");
        }

    }

    @Override
    public void connectionTuneOk(final ServerConnection sconn, final ConnectionTuneOk ok)
    {
        assertState(sconn, ConnectionState.AWAIT_TUNE_OK);
        int okChannelMax = ok.getChannelMax();
        int okMaxFrameSize = ok.getMaxFrameSize();

        if (okChannelMax > getChannelMax())
        {
            LOGGER.error("Connection '" + sconn.getConnectionId() + "' being severed, " +
                    "client connectionTuneOk returned a channelMax (" + okChannelMax +
                    ") above the server's offered limit (" + getChannelMax() +")");

            //Due to the error we must forcefully close the connection without negotiation
            sconn.closeAndIgnoreFutureInput();
            return;
        }

        if(okMaxFrameSize > getFrameMax())
        {
            LOGGER.error("Connection '" + sconn.getConnectionId() + "' being severed, " +
                         "client connectionTuneOk returned a frameMax (" + okMaxFrameSize +
                         ") above the server's offered limit (" + getFrameMax() +")");

            //Due to the error we must forcefully close the connection without negotiation
            sconn.closeAndIgnoreFutureInput();

            return;
        }
        else if(okMaxFrameSize > 0 && okMaxFrameSize < Constant.MIN_MAX_FRAME_SIZE)
        {
            LOGGER.error("Connection '" + sconn.getConnectionId() + "' being severed, " +
                         "client connectionTuneOk returned a frameMax (" + okMaxFrameSize +
                         ") below the minimum permitted size (" + Constant.MIN_MAX_FRAME_SIZE +")");

            //Due to the error we must forcefully close the connection without negotiation
            sconn.closeAndIgnoreFutureInput();
            return;
        }
        else if(okMaxFrameSize == 0)
        {
            okMaxFrameSize = getFrameMax();
        }

        if(ok.hasHeartbeat() && ok.getHeartbeat() > 0)
        {
            int heartbeat = ok.getHeartbeat();
            sconn.setHeartBeatDelay(heartbeat);
            long readerIdle = 2000L * heartbeat;
            long writerIdle = 1000L * heartbeat;
            sconn.getAmqpConnection().initialiseHeartbeating(writerIdle, readerIdle);
        }

        //0 means no implied limit, except available server resources
        //(or that forced by protocol limitations [0xFFFF])
        sconn.setChannelMax(okChannelMax == 0 ? getChannelMax() : okChannelMax);
        sconn.setMaxFrameSize(okMaxFrameSize);
        _state = ConnectionState.AWAIT_OPEN;
    }

    private int getChannelMax()
    {
        return _maxNoOfChannels;
    }

    private int getFrameMax()
    {
        return _maximumFrameSize;
    }

    @Override
    public void sessionDetach(ServerConnection conn, SessionDetach dtc)
    {
        int channel = dtc.getChannel();
        ServerSession ssn = conn.getSession(channel);
        if (ssn != null)
        {
            stopAllSubscriptions(ssn);
            ssn.setClose(true);
            ssn.sessionDetached(dtc.getName(),
                                ssn.getDetachCode() == null ? SessionDetachCode.NORMAL : ssn.getDetachCode());
            conn.unmap(ssn);
            ssn.closed();
        }
        else
        {
            if(LOGGER.isDebugEnabled())
            {
                LOGGER.debug("SessionDetach received on unattached channel : {}", channel);
            }
            SessionDetached sessionDetached = new SessionDetached(dtc.getName(), SessionDetachCode.NOT_ATTACHED);
            sessionDetached.setChannel(channel);
            conn.invoke(sessionDetached);
        }
    }

    private void stopAllSubscriptions(final ServerSession ssn)
    {
        final Collection<ConsumerTarget_0_10> subs = ssn.getSubscriptions();
        for (ConsumerTarget_0_10 subscription_0_10 : subs)
        {
            subscription_0_10.stop();
        }
    }


    @Override
    public void sessionAttach(final ServerConnection serverConnection, final SessionAttach atc)
    {
        assertState(serverConnection, ConnectionState.OPEN);

        // We ignore the force flag

        if(isSessionNameUnique(atc.getName(), serverConnection))
        {
            ServerSessionDelegate serverSessionDelegate = new ServerSessionDelegate();

            final ServerSession serverSession =
                    new ServerSession(serverConnection, serverSessionDelegate, new Binary(atc.getName()), 0);
            final Session_0_10 session = new Session_0_10(serverConnection.getAmqpConnection(), atc.getChannel(),
                                                          serverSession);
            session.create();
            serverSession.setModelObject(session);

            serverConnection.map(serverSession, atc.getChannel());
            serverConnection.registerSession(serverSession);
            serverSession.sendSessionAttached(atc.getName());
            serverSession.setState(ServerSession.State.OPEN);
        }
        else
        {
            final SessionDetached detached = new SessionDetached(atc.getName(), SessionDetachCode.SESSION_BUSY);
            detached.setChannel(atc.getChannel());
            serverConnection.invoke(detached);
        }
    }

    private boolean isSessionNameUnique(final byte[] name, final ServerConnection conn)
    {
        final Principal authorizedPrincipal = conn.getAuthorizedPrincipal();
        final String userId = authorizedPrincipal == null ? "" : authorizedPrincipal.getName();

        final Iterator<? extends org.apache.qpid.server.model.Connection<?>> connections =
                        conn.getAddressSpace().getConnections().iterator();
        while(connections.hasNext())
        {
            final AMQPConnection<?> amqConnectionModel = (AMQPConnection<?>) connections.next();

            final String userName = amqConnectionModel.getAuthorizedPrincipal() == null
                    ? ""
                    : amqConnectionModel.getAuthorizedPrincipal().getName();
            if (userId.equals(userName) && amqConnectionModel.hasSessionWithName(name))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void connectionStartOk(ServerConnection serverConnection, ConnectionStartOk ok)
    {
        assertState(serverConnection, ConnectionState.AWAIT_START_OK);
        _clientProperties = ok.getClientProperties();
        if(_clientProperties != null)
        {
            Object compressionSupported =
                    _clientProperties.get(ConnectionStartProperties.QPID_MESSAGE_COMPRESSION_SUPPORTED);
            if (compressionSupported != null)
            {
                _compressionSupported = Boolean.parseBoolean(String.valueOf(compressionSupported));

            }
            final AMQPConnection_0_10 protocolEngine = serverConnection.getAmqpConnection();
            protocolEngine.setClientId(getStringClientProperty(ConnectionStartProperties.CLIENT_ID_0_10));
            protocolEngine.setClientProduct(getStringClientProperty(ConnectionStartProperties.PRODUCT));
            protocolEngine.setClientVersion(getStringClientProperty(ConnectionStartProperties.VERSION_0_10));
            protocolEngine.setRemoteProcessPid(getStringClientProperty(ConnectionStartProperties.PID));
        }


        serverConnection.setLocale(ok.getLocale());
        String mechanism = ok.getMechanism();

        if (mechanism == null || mechanism.length() == 0)
        {
            serverConnection.sendConnectionClose(ConnectionCloseCode.CONNECTION_FORCED,
                                                 "No Sasl mechanism was specified");
            return;
        }

        _saslNegotiator = _subjectCreator.createSaslNegotiator(mechanism,
                                                               (SaslSettings) serverConnection.getAmqpConnection());
        if (_saslNegotiator == null)
        {
            serverConnection.sendConnectionClose(ConnectionCloseCode.CONNECTION_FORCED,
                                                 "No SaslServer could be created for mechanism: " + mechanism);
        }
        else
        {
            secure(serverConnection, ok.getResponse());
        }
    }

    private String getStringClientProperty(final String name)
    {
        return (_clientProperties == null || _clientProperties.get(name) == null) ? null : String.valueOf(_clientProperties.get(name));
    }


    protected int getHeartbeatMax()
    {
        int delay = _port.getHeartbeatDelay();
        return delay == 0 ? 0xFFFF : delay;
    }

    public boolean isCompressionSupported()
    {
        return _compressionSupported && _broker.isMessageCompressionEnabled();
    }

    private void connectionAuthFailed(final ServerConnection serverConnection, Exception e)
    {
        if (e != null)
        {
            serverConnection.exception(e);
        }
        serverConnection.sendConnectionClose(ConnectionCloseCode.CONNECTION_FORCED, e == null ? "Authentication failed" : e.getMessage());
        disposeSaslNegotiator();
    }

    private void disposeSaslNegotiator()
    {
        _saslNegotiator.dispose();
        _saslNegotiator = null;
    }
}
