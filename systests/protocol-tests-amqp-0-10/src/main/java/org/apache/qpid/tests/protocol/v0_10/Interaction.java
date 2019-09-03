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
package org.apache.qpid.tests.protocol.v0_10;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.qpid.server.protocol.v0_10.transport.BBDecoder;
import org.apache.qpid.server.protocol.v0_10.transport.BBEncoder;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionOpenOk;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionSecure;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionStart;
import org.apache.qpid.server.protocol.v0_10.transport.ConnectionTune;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Method;
import org.apache.qpid.server.protocol.v0_10.transport.SessionAttached;
import org.apache.qpid.tests.protocol.AbstractInteraction;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class Interaction extends AbstractInteraction<Interaction>
{
    private final BrokerAdmin _brokerAdmin;
    private final BrokerAdmin.PortType _portType;
    private byte[] _protocolHeader;
    private ConnectionInteraction _connectionInteraction;
    private SessionInteraction _sessionInteraction;
    private MessageInteraction _messageInteraction;
    private ExecutionInteraction _executionInteraction;
    private QueueInteraction _queueInteraction;
    private ExchangeInteraction _exchangeInteraction;
    private int _channelId;
    private TxInteraction _txInteraction;

    public Interaction(final FrameTransport frameTransport,
                       final BrokerAdmin brokerAdmin,
                       final BrokerAdmin.PortType portType)
    {
        super(frameTransport);
        _connectionInteraction = new ConnectionInteraction(this);
        _sessionInteraction = new SessionInteraction(this);
        _messageInteraction = new MessageInteraction(this);
        _executionInteraction = new ExecutionInteraction(this);
        _txInteraction = new TxInteraction(this);
        _queueInteraction = new QueueInteraction(this);
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

    public <T extends Method> Interaction sendPerformative(final T performative) throws Exception
    {
        performative.setChannel(_channelId);
        sendPerformativeAndChainFuture(copyPerformative(performative));
        return this;
    }

    public <T extends Method> Interaction sendPerformativeWithoutCopying(final T performative) throws Exception
    {
        performative.setChannel(_channelId);
        sendPerformativeAndChainFuture(performative);
        return this;
    }

    public ConnectionInteraction connection()
    {
        return _connectionInteraction;
    }

    private <T extends Method> T copyPerformative(final T src)
    {
        T dst = (T) Method.create(src.getStructType());
        final BBEncoder encoder = new BBEncoder(4096);
        encoder.init();
        src.write(encoder);
        ByteBuffer buffer = encoder.buffer();

        final BBDecoder decoder = new BBDecoder();
        decoder.init(buffer);
        dst.read(decoder);
        dst.setChannel(src.getChannel());

        if (src.getHeader() != null)
        {
            Header srcHeader = src.getHeader();
            MessageProperties dstMessageProperties = null;
            DeliveryProperties dstDeliveryProperties = null;

            if (srcHeader.getMessageProperties() != null)
            {
                MessageProperties properties = srcHeader.getMessageProperties();
                dstMessageProperties = new MessageProperties();

                encoder.init();
                properties.write(encoder);

                decoder.init(encoder.buffer());
                dstMessageProperties.read(decoder);
            }

            if (srcHeader.getDeliveryProperties() != null)
            {
                DeliveryProperties properties = srcHeader.getDeliveryProperties();
                dstDeliveryProperties = new DeliveryProperties();

                encoder.init();
                properties.write(encoder);

                decoder.init(encoder.buffer());
                dstDeliveryProperties.read(decoder);
            }

            if (dstMessageProperties != null || dstDeliveryProperties != null)
            {
                dst.setHeader(new Header(dstDeliveryProperties, dstMessageProperties));
            }
        }

        if (src.getBody() != null)
        {
            dst.setBody(src.getBody());
        }

        return dst;
    }

    public Interaction negotiateOpen() throws Exception
    {
        authenticateConnection().connection().tuneOk()
                                .connection().open()
                                .consumeResponse(ConnectionOpenOk.class);
        return this;
    }

    public Interaction authenticateConnection() throws Exception
    {
        if (_portType == BrokerAdmin.PortType.ANONYMOUS_AMQP || _portType == BrokerAdmin.PortType.ANONYMOUS_AMQPWS)
        {
            openAnonymous();
        }
        else
        {
            final ConnectionStart start = this.negotiateProtocol().consumeResponse()
                                              .consumeResponse().getLatestResponse(ConnectionStart.class);
            final List<Object> supportedMechanisms =
                    start.getMechanisms() == null ? Collections.emptyList() : start.getMechanisms();

            if (supportedMechanisms.stream().noneMatch(m -> String.valueOf(m).equalsIgnoreCase(ConnectionInteraction.SASL_MECHANISM_PLAIN)))
            {
                if (supportedMechanisms.stream()
                                       .noneMatch(m -> String.valueOf(m).equalsIgnoreCase(ConnectionInteraction.SASL_MECHANISM_ANONYMOUS)))
                {
                    throw new IllegalStateException(String.format(
                            "PLAIN or ANONYMOUS SASL mechanism is not listed among supported '%s'", supportedMechanisms.stream().map(
                                    String::valueOf).collect(
                                    Collectors.joining(","))));
                }
                else
                {
                    openAnonymous();
                }
            }
            else
            {
                final byte[] initialResponse = String.format("\0%s\0%s",
                                                             _brokerAdmin.getValidUsername(),
                                                             _brokerAdmin.getValidPassword())
                                                     .getBytes(UTF_8);
                this.connection().startOkMechanism(ConnectionInteraction.SASL_MECHANISM_PLAIN).startOk()
                    .consumeResponse(ConnectionSecure.class)
                    .connection().secureOk(initialResponse).consumeResponse(ConnectionTune.class);
            }
        }
        return this;
    }

    private void openAnonymous() throws Exception
    {
        this.negotiateProtocol().consumeResponse()
            .consumeResponse(ConnectionStart.class)
            .connection().startOkMechanism(ConnectionInteraction.SASL_MECHANISM_ANONYMOUS).startOk()
            .consumeResponse(ConnectionTune.class);
    }

    public SessionInteraction session()
    {
        return _sessionInteraction;
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

    public Interaction attachSession(final byte[] sessionName) throws Exception
    {
        this.session()
            .attachName(sessionName)
            .attach()
            .consumeResponse(SessionAttached.class)
            .session().commandPointCommandId(0).commandPoint();
        return this;
    }

    public MessageInteraction message()
    {
        return _messageInteraction;
    }

    public ExecutionInteraction execution()
    {
        return _executionInteraction;
    }

    public TxInteraction tx()
    {
        return _txInteraction;
    }

    public QueueInteraction queue()
    {
        return _queueInteraction;
    }

    public ExchangeInteraction exchange()
    {
        return _exchangeInteraction;
    }

}
