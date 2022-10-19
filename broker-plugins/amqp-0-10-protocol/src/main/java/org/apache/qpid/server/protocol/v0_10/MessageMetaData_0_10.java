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

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.protocol.v0_10.transport.DeliveryProperties;
import org.apache.qpid.server.protocol.v0_10.transport.EncoderUtils;
import org.apache.qpid.server.protocol.v0_10.transport.Header;
import org.apache.qpid.server.protocol.v0_10.transport.MessageDeliveryMode;
import org.apache.qpid.server.protocol.v0_10.transport.MessageProperties;
import org.apache.qpid.server.protocol.v0_10.transport.MessageTransfer;
import org.apache.qpid.server.protocol.v0_10.transport.Struct;
import org.apache.qpid.server.store.StorableMessageMetaData;

public class MessageMetaData_0_10 implements StorableMessageMetaData
{
    private static final int ENCODER_SIZE = 1 << 10;

    private static final MessageMetaDataType_0_10 TYPE = new MessageMetaDataType_0_10();

    public static final MessageMetaDataType.Factory<MessageMetaData_0_10> FACTORY = new MetaDataFactory();

    private final Header _header;
    private final DeliveryProperties _deliveryProps;
    private final MessageProperties _messageProps;
    private final MessageTransferHeader _messageHeader;
    private final int _bodySize;

    private volatile QpidByteBuffer _encoded;

    public MessageMetaData_0_10(MessageTransfer xfr)
    {
        this(xfr.getHeader(), xfr.getBodySize(), System.currentTimeMillis());
    }

    public MessageMetaData_0_10(Header header, int bodySize, long arrivalTime)
    {
        _header = header;
        if(_header != null)
        {
            _deliveryProps = _header.getDeliveryProperties();
            _messageProps = _header.getMessageProperties();
        }
        else
        {
            _deliveryProps = null;
            _messageProps = null;
        }
        _messageHeader = new MessageTransferHeader(_deliveryProps, _messageProps, arrivalTime);
        _bodySize = bodySize;

    }



    @Override
    public MessageMetaDataType getType()
    {
        return TYPE;
    }


    @Override
    public int getStorableSize()
    {
        int len = 0;

        len += 8; // arrival time
        len += 4; // body size
        len += 4; // headers length

        if (_header != null)
        {
            if(_header.getDeliveryProperties() != null)
            {
                len += EncoderUtils.getStruct32Length(_header.getDeliveryProperties());
            }
            if(_header.getMessageProperties() != null)
            {
                len += EncoderUtils.getStruct32Length(_header.getMessageProperties());
            }
            if(_header.getNonStandardProperties() != null)
            {
                for(Struct header : _header.getNonStandardProperties())
                {
                    len += EncoderUtils.getStruct32Length(header);
                }
            }
        }
        return len;
    }

    private QpidByteBuffer encodeAsBuffer()
    {
        ServerEncoder encoder = new ServerEncoder(ENCODER_SIZE, false);

        encoder.writeInt64(_messageHeader.getArrivalTime());
        encoder.writeInt32(_bodySize);
        int headersLength = 0;
        if (_header != null)
        {
            if (_header.getDeliveryProperties() != null)
            {
                headersLength++;
            }
            if (_header.getMessageProperties() != null)
            {
                headersLength++;
            }
            if (_header.getNonStandardProperties() != null)
            {
                headersLength += _header.getNonStandardProperties().size();
            }
        }

        encoder.writeInt32(headersLength);

        if (_header != null)
        {
            if (_header.getDeliveryProperties() != null)
            {
                encoder.writeStruct32(_header.getDeliveryProperties());
            }
            if (_header.getMessageProperties() != null)
            {
                encoder.writeStruct32(_header.getMessageProperties());
            }
            if (_header.getNonStandardProperties() != null)
            {

                for (Struct header : _header.getNonStandardProperties())
                {
                    encoder.writeStruct32(header);
                }
            }
        }
        QpidByteBuffer buf = encoder.getBuffer();
        encoder.close();
        return buf;
    }

    @Override
    public synchronized void writeToBuffer(QpidByteBuffer dest)
    {
        if (_encoded == null)
        {
            _encoded = encodeAsBuffer();
        }
        dest.put(_encoded);
        // We have special knowledge that we no longer need the encoded form after this call
        // to reduce memory usage associated with the metadata free the encoded form here (QPID-7465)
        clearEncodedForm();
    }

    @Override
    public int getContentSize()
    {
        return _bodySize;
    }

    @Override
    public boolean isPersistent()
    {
        return _deliveryProps != null && _deliveryProps.getDeliveryMode() == MessageDeliveryMode.PERSISTENT;
    }

    @Override
    public void dispose()
    {
        clearEncodedForm();
    }

    @Override
    public synchronized void clearEncodedForm()
    {
        if (_encoded != null)
        {
            _encoded.dispose();
            _encoded = null;
        }
    }

    @Override
    public synchronized void reallocate()
    {
        _encoded = QpidByteBuffer.reallocateIfNecessary(_encoded);
    }

    public String getRoutingKey()
    {
        return _deliveryProps == null ? null : _deliveryProps.getRoutingKey();
    }

    public String getExchange()
    {
        return _deliveryProps == null ? null : _deliveryProps.getExchange();
    }

    public AMQMessageHeader getMessageHeader()
    {
        return _messageHeader;
    }

    public long getSize()
    {

        return _bodySize;
    }

    public boolean isImmediate()
    {
        return _deliveryProps != null && _deliveryProps.getImmediate();
    }

    public long getExpiration()
    {
        return _messageHeader.getExpiration();
    }

    public long getArrivalTime()
    {
        return _messageHeader.getArrivalTime();
    }

    public Header getHeader()
    {
        return _header;
    }

    public DeliveryProperties getDeliveryProperties()
    {
        return _deliveryProps;
    }

    public MessageProperties getMessageProperties()
    {
        return _messageProps;
    }

    private static class MetaDataFactory implements MessageMetaDataType.Factory<MessageMetaData_0_10>
    {
        @Override
        public MessageMetaData_0_10 createMetaData(QpidByteBuffer buf)
        {
            ServerDecoder decoder = new ServerDecoder(buf);

            long arrivalTime = decoder.readInt64();
            int bodySize = decoder.readInt32();
            int headerCount = decoder.readInt32();

            DeliveryProperties deliveryProperties = null;
            MessageProperties messageProperties = null;
            List<Struct> otherProps = null;

            for(int i = 0 ; i < headerCount; i++)
            {
                Struct struct = decoder.readStruct32();
                if(struct instanceof DeliveryProperties && deliveryProperties == null)
                {
                    deliveryProperties = (DeliveryProperties) struct;
                }
                else if(struct instanceof MessageProperties && messageProperties == null)
                {
                    messageProperties = (MessageProperties) struct;
                }
                else
                {
                    if(otherProps == null)
                    {
                        otherProps = new ArrayList<Struct>();

                    }
                    otherProps.add(struct);
                }
            }
            Header header = new Header(deliveryProperties,messageProperties,otherProps);

            return new MessageMetaData_0_10(header, bodySize, arrivalTime);

        }
    }


}
