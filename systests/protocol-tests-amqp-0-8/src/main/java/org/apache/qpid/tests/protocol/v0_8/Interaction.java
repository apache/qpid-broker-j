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
package org.apache.qpid.tests.protocol.v0_8;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.server.security.auth.manager.AbstractScramAuthenticationManager.PLAIN;

import java.util.Arrays;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.security.auth.manager.AnonymousAuthenticationManager;
import org.apache.qpid.tests.protocol.AbstractInteraction;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class Interaction extends AbstractInteraction<Interaction>
{
    private final BrokerAdmin _brokerAdmin;
    private final BrokerAdmin.PortType _portType;
    private byte[] _protocolHeader;
    private int _channelId;
    private int _maximumPayloadSize = 512;
    private ConnectionInteraction _connectionInteraction;
    private ChannelInteraction _channelInteraction;
    private QueueInteraction _queueInteraction;
    private BasicInteraction _basicInteraction;
    private TxInteraction _txInteraction;
    private ExchangeInteraction _exchangeInteraction;

    Interaction(final FrameTransport transport, final BrokerAdmin brokerAdmin, final BrokerAdmin.PortType portType)
    {
        super(transport);
        _connectionInteraction = new ConnectionInteraction(this);
        _channelInteraction = new ChannelInteraction(this);
        _queueInteraction = new QueueInteraction(this);
        _basicInteraction = new BasicInteraction(this);
        _txInteraction = new TxInteraction(this);
        _exchangeInteraction = new ExchangeInteraction(this);
        _protocolHeader = getTransport().getProtocolHeader();
        _brokerAdmin = brokerAdmin;
        _portType = portType;
    }

    @Override
    public Interaction protocolHeader(final byte[] header)
    {
        _protocolHeader = header;
        return this;
    }

    @Override
    protected byte[] getProtocolHeader()
    {
        return _protocolHeader;
    }

    public ProtocolVersion getProtocolVersion()
    {
        return ((FrameTransport) getTransport()).getProtocolVersion();
    }

    public Interaction sendPerformative(final AMQBody amqBody) throws Exception
    {
        return sendPerformative(_channelId, amqBody);
    }

    public Interaction sendPerformative(int channel, final AMQBody amqBody) throws Exception
    {
        final AMQFrame frameBody = new AMQFrame(channel, amqBody);
        sendPerformativeAndChainFuture(frameBody);
        return this;
    }

    public Interaction sendPerformative(final AMQDataBlock dataBlock) throws Exception
    {
        sendPerformativeAndChainFuture(dataBlock);
        return this;
    }

    public Interaction negotiateOpen() throws Exception
    {
        authenticateConnection().connection().tuneOk()
                                .connection().open().consumeResponse(ConnectionOpenOkBody.class);
        return this;
    }

    public Interaction authenticateConnection() throws Exception
    {
        if (_portType == BrokerAdmin.PortType.ANONYMOUS_AMQP || _portType == BrokerAdmin.PortType.ANONYMOUS_AMQPWS)
        {
            authenticateAnonymous();
        }
        else
        {
            final ConnectionStartBody start = negotiateProtocol().consumeResponse()
                                                                 .getLatestResponse(ConnectionStartBody.class);
            final String mechanisms = start.getMechanisms() == null ? "" : new String(start.getMechanisms(), US_ASCII);
            final List<String> supportedMechanisms = Arrays.asList(mechanisms.split(" "));

            if (supportedMechanisms.stream().noneMatch(m -> m.equalsIgnoreCase(PLAIN)))
            {
                if (supportedMechanisms.stream()
                                       .noneMatch(m -> m.equalsIgnoreCase(AnonymousAuthenticationManager.MECHANISM_NAME)))
                {
                    throw new IllegalStateException(String.format(
                            "PLAIN or ANONYMOUS SASL mechanism is not listed among supported '%s'", mechanisms));
                }
                else
                {
                    authenticateAnonymous();
                }
            }
            else
            {
                final byte[] initialResponse = String.format("\0%s\0%s",
                                                             _brokerAdmin.getValidUsername(),
                                                             _brokerAdmin.getValidPassword())
                                                     .getBytes(US_ASCII);
                this.connection().startOkMechanism(PLAIN).startOk().consumeResponse(ConnectionSecureBody.class)
                    .connection().secureOk(initialResponse).consumeResponse(ConnectionTuneBody.class);
            }
        }
        return this;
    }

    private void authenticateAnonymous() throws Exception
    {
        this.negotiateProtocol().consumeResponse(ConnectionStartBody.class)
                           .connection().startOkMechanism(AnonymousAuthenticationManager.MECHANISM_NAME).startOk()
                           .consumeResponse(ConnectionTuneBody.class);

    }

    public ConnectionInteraction connection()
    {
        return _connectionInteraction;
    }

    public ChannelInteraction channel()
    {
        return _channelInteraction;
    }

    public QueueInteraction queue()
    {
        return _queueInteraction;
    }

    public int getChannelId()
    {
        return _channelId;
    }

    public Interaction channelId(final int channelId)
    {
        _channelId = channelId;
        return this;
    }

    public int getMaximumFrameSize()
    {
        return _maximumPayloadSize;
    }

    public BasicInteraction basic()
    {
        return _basicInteraction;
    }

    public TxInteraction tx()
    {
        return _txInteraction;
    }

    public ExchangeInteraction exchange()
    {
        return _exchangeInteraction;
    }

    public String getLatestResponseContentBodyAsString() throws Exception
    {
        ContentBody content = getLatestResponse(ContentBody.class);
        QpidByteBuffer payload = content.getPayload();
        byte[] contentData = new byte[payload.remaining()];
        payload.get(contentData);
        payload.dispose();
        return new String(contentData, UTF_8);
    }
}
