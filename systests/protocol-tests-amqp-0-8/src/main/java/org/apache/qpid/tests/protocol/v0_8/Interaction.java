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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.tests.protocol.AbstractInteraction;
import org.apache.qpid.tests.protocol.Response;

public class Interaction extends AbstractInteraction<Interaction>
{
    private byte[] _protocolHeader;
    private int _channelId;
    private int _maximumPayloadSize = 512;
    private ConnectionInteraction _connectionInteraction;
    private ChannelInteraction _channelInteraction;
    private QueueInteraction _queueInteraction;
    private BasicInteraction _basicInteraction;
    private TxInteraction _txInteraction;
    private ExchangeInteraction _exchangeInteraction;

    Interaction(final FrameTransport transport)
    {
        super(transport);
        _connectionInteraction = new ConnectionInteraction(this);
        _channelInteraction = new ChannelInteraction(this);
        _queueInteraction = new QueueInteraction(this);
        _basicInteraction = new BasicInteraction(this);
        _txInteraction = new TxInteraction(this);
        _exchangeInteraction = new ExchangeInteraction(this);
        _protocolHeader = getTransport().getProtocolHeader();
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

    public Interaction openAnonymousConnection() throws Exception
    {
        return this.negotiateProtocol().consumeResponse(ConnectionStartBody.class)
                   .connection().startOkMechanism("ANONYMOUS").startOk().consumeResponse(ConnectionTuneBody.class)
                   .connection().tuneOk()
                   .connection().open().consumeResponse(ConnectionOpenOkBody.class);

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

    @SafeVarargs
    public final <T extends AMQBody> T consume(final Class<T> expected,
                                               final Class<? extends AMQBody>... ignore)
            throws Exception
    {
        final Class<? extends AMQBody>[] expectedResponses = Arrays.copyOf(ignore, ignore.length + 1);
        expectedResponses[ignore.length] = expected;

        T completed = null;
        do
        {
            Response<?> response = consumeResponse(expectedResponses).getLatestResponse();
            if (expected.isAssignableFrom(response.getBody().getClass()))
            {
                completed = (T) response.getBody();
            }
        }
        while (completed == null);
        return completed;
    }

    public String getLatestResponseContentBodyAsString() throws Exception
    {
        ContentBody content = getLatestResponse(ContentBody.class);
        QpidByteBuffer payload = content.getPayload();
        byte[] contentData = new byte[payload.remaining()];
        payload.get(contentData);
        payload.dispose();
        return new String(contentData, StandardCharsets.UTF_8);
    }
}
