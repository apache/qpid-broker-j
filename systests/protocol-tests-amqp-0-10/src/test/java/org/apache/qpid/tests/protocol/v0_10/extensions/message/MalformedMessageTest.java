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
package org.apache.qpid.tests.protocol.v0_10.extensions.message;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.tests.utils.BrokerAdmin.KIND_BROKER_J;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.Decoder;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Encoder;
import org.apache.qpid.server.protocol.v0_10.transport.Frame;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcceptMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageAcquireMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.Method;
import org.apache.qpid.server.protocol.v0_10.transport.MethodDelegate;
import org.apache.qpid.server.protocol.v0_10.transport.Option;
import org.apache.qpid.tests.protocol.ChannelClosedResponse;
import org.apache.qpid.tests.protocol.v0_10.FrameTransport;
import org.apache.qpid.tests.protocol.v0_10.Interaction;
import org.apache.qpid.tests.utils.BrokerAdmin;
import org.apache.qpid.tests.utils.BrokerAdminUsingTestBase;
import org.apache.qpid.tests.utils.BrokerSpecific;
import org.apache.qpid.tests.utils.ConfigItem;

@BrokerSpecific(kind = KIND_BROKER_J)
@ConfigItem(name = "broker.flowToDiskThreshold", value = "1")
@ConfigItem(name = "connection.maxUncommittedInMemorySize", value = "1")
public class MalformedMessageTest extends BrokerAdminUsingTestBase
{
    private static final String CONTENT_TEXT = "Test";

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(BrokerAdmin.TEST_QUEUE_NAME);
    }

    @Test
    public void malformedMessage() throws Exception
    {
        try (FrameTransport transport = new FrameTransport(getBrokerAdmin()).connect())
        {
            final Interaction interaction = transport.newInteraction();
            byte[] sessionName = "test".getBytes(UTF_8);

            byte[] contentBytes = CONTENT_TEXT.getBytes(StandardCharsets.UTF_8);

            DeliveryProperties deliveryProps = new DeliveryProperties();
            MessageProperties messageProps = new MessageProperties();

            deliveryProps.setRoutingKey(BrokerAdmin.TEST_QUEUE_NAME);
            deliveryProps.setTimestamp(System.currentTimeMillis());
            messageProps.setContentLength(contentBytes.length);
            messageProps.setContentType("plain/text");
            messageProps.setMessageId(UUID.randomUUID());

            final Header header = new Header(deliveryProps, messageProps, null);

            final TestMessageTransfer malformedTransfer = new TestMessageTransfer(BrokerAdmin.TEST_QUEUE_NAME,
                                                                                  MessageAcceptMode.EXPLICIT,
                                                                                  MessageAcquireMode.PRE_ACQUIRED,
                                                                                  header,
                                                                                  QpidByteBuffer.wrap(contentBytes))
            {
                @Override
                public void write(final Encoder enc)
                {
                    // write flags  without writing anything else
                    enc.writeUint16(packingFlags);
                }
            };

            interaction.negotiateOpen()
                       .channelId(1)
                       .attachSession(sessionName)
                       .sendPerformativeWithoutCopying(malformedTransfer)
                       .session()
                       .flushCompleted()
                       .flush()
                       .consumeResponse()
                       .getLatestResponse(ChannelClosedResponse.class);
        }
    }

    private class TestMessageTransfer extends Method
    {
        short packingFlags;
        private String destination;
        private MessageAcceptMode acceptMode;
        private MessageAcquireMode acquireMode;
        private Header header;
        private QpidByteBuffer _body;
        private int _bodySize;


        TestMessageTransfer(String destination,
                            MessageAcceptMode acceptMode,
                            MessageAcquireMode acquireMode,
                            Header header,
                            QpidByteBuffer body,
                            Option... options)
        {
            if (destination != null)
            {
                setDestination(destination);
            }
            if (acceptMode != null)
            {
                setAcceptMode(acceptMode);
            }
            if (acquireMode != null)
            {
                setAcquireMode(acquireMode);
            }
            setHeader(header);
            setBody(body);

            for (final Option option : options)
            {
                switch (option)
                {
                    case SYNC:
                        this.setSync(true);
                        break;
                    case NONE:
                        break;
                    default:
                        throw new IllegalArgumentException("invalid option: " + option);
                }
            }
        }

        @Override
        public final int getStructType()
        {
            return 1025;
        }

        @Override
        public final int getSizeWidth()
        {
            return 0;
        }

        @Override
        public final int getPackWidth()
        {
            return 2;
        }

        @Override
        public final boolean hasPayload()
        {
            return true;
        }

        @Override
        public final byte getEncodedTrack()
        {
            return Frame.L4;
        }

        @Override
        public final boolean isConnectionControl()
        {
            return false;
        }

        @Override
        public <C> void dispatch(C context, MethodDelegate<C> delegate)
        {
            delegate.handle(context, this);
        }

        @Override
        public final Header getHeader()
        {
            return this.header;
        }

        @Override
        public final void setHeader(Header header)
        {
            this.header = header;
        }

        @Override
        public final QpidByteBuffer getBody()
        {
            return _body;
        }

        @Override
        public final void setBody(QpidByteBuffer body)
        {
            if (body == null)
            {
                _bodySize = 0;
                if (_body != null)
                {
                    _body.dispose();
                }
                _body = null;
            }
            else
            {
                _body = body.duplicate();
                _bodySize = _body.remaining();
            }
        }

        @Override
        public int getBodySize()
        {
            return _bodySize;
        }

        @Override
        public void write(Encoder enc)
        {
            enc.writeUint16(packingFlags);
            if ((packingFlags & 256) != 0)
            {
                enc.writeStr8(this.destination);
            }
            if ((packingFlags & 512) != 0)
            {
                enc.writeUint8(this.acceptMode.getValue());
            }
            if ((packingFlags & 1024) != 0)
            {
                enc.writeUint8(this.acquireMode.getValue());
            }
        }

        @Override
        public void read(Decoder dec)
        {
            packingFlags = (short) dec.readUint16();
            if ((packingFlags & 256) != 0)
            {
                this.destination = dec.readStr8();
            }
            if ((packingFlags & 512) != 0)
            {
                this.acceptMode = MessageAcceptMode.get(dec.readUint8());
            }
            if ((packingFlags & 1024) != 0)
            {
                this.acquireMode = MessageAcquireMode.get(dec.readUint8());
            }
        }

        @Override
        public Map<String, Object> getFields()
        {
            Map<String, Object> result = new LinkedHashMap<>();

            if ((packingFlags & 256) != 0)
            {
                result.put("destination", getDestination());
            }
            if ((packingFlags & 512) != 0)
            {
                result.put("acceptMode", getAcceptMode());
            }
            if ((packingFlags & 1024) != 0)
            {
                result.put("acquireMode", getAcquireMode());
            }
            return result;
        }

        final String getDestination()
        {
            return destination;
        }

        final void setDestination(String value)
        {
            this.destination = value;
            packingFlags |= 256;
            setDirty(true);
        }

        final MessageAcceptMode getAcceptMode()
        {
            return acceptMode;
        }

        final void setAcceptMode(MessageAcceptMode value)
        {
            this.acceptMode = value;
            packingFlags |= 512;
            setDirty(true);
        }

        final MessageAcquireMode getAcquireMode()
        {
            return acquireMode;
        }

        final void setAcquireMode(MessageAcquireMode value)
        {
            this.acquireMode = value;
            packingFlags |= 1024;
            setDirty(true);
        }
    }

}
