/*
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

package org.apache.qpid.tests.protocol.v1_0;

import static org.apache.qpid.server.protocol.v1_0.MessageConverter_from_1_0.convertValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.SectionDecoderRegistry;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpSequenceSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValueSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationPropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DataSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.DeliveryAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.FooterSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.HeaderSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.MessageAnnotationsSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.protocol.v1_0.type.transport.Transfer;

public class MessageDecoder
{
    private static final AMQPDescribedTypeRegistry AMQP_DESCRIBED_TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
                                                                                                          .registerTransportLayer()
                                                                                                          .registerMessagingLayer();
    private static final SectionDecoderRegistry SECTION_DECODER_REGISTRY = AMQP_DESCRIBED_TYPE_REGISTRY
                                                                                                    .getSectionDecoderRegistry();
    private List<QpidByteBuffer> _fragments = new LinkedList<>();
    private SectionDecoder _sectionDecoder = new SectionDecoderImpl(SECTION_DECODER_REGISTRY);
    private HeaderSection _headerSection = null;
    private PropertiesSection _propertiesSection = null;
    private DeliveryAnnotationsSection _deliveryAnnotationsSection = null;
    private MessageAnnotationsSection _messageAnnotationsSection = null;
    private ApplicationPropertiesSection _applicationPropertiesSection = null;
    private FooterSection _footerSection = null;
    private List<EncodingRetainingSection<?>> _dataSections = new ArrayList<>();
    private long _contentSize;
    private boolean _parsed;

    public void addTransfer(final Transfer transfer)
    {
        if (_parsed)
        {
            throw new IllegalStateException("The section fragments have already been parsed");
        }
        _fragments.add(transfer.getPayload());
    }

    public void parse() throws AmqpErrorException
    {
        if (!_parsed)
        {
            List<EncodingRetainingSection<?>> sections;
            try (QpidByteBuffer combined = QpidByteBuffer.concatenate(_fragments))
            {
                sections = _sectionDecoder.parseAll(combined);
            }
            _fragments.forEach(QpidByteBuffer::dispose);

            Iterator<EncodingRetainingSection<?>> iter = sections.iterator();
            EncodingRetainingSection<?> s = iter.hasNext() ? iter.next() : null;
            if (s instanceof HeaderSection)
            {
                _headerSection = (HeaderSection) s;
                s = iter.hasNext() ? iter.next() : null;
            }

            if (s instanceof DeliveryAnnotationsSection)
            {
                _deliveryAnnotationsSection = (DeliveryAnnotationsSection) s;
                s = iter.hasNext() ? iter.next() : null;
            }

            if (s instanceof MessageAnnotationsSection)
            {
                _messageAnnotationsSection = (MessageAnnotationsSection) s;
                s = iter.hasNext() ? iter.next() : null;
            }

            if (s instanceof PropertiesSection)
            {
                _propertiesSection = (PropertiesSection) s;
                s = iter.hasNext() ? iter.next() : null;
            }

            if (s instanceof ApplicationPropertiesSection)
            {
                _applicationPropertiesSection = (ApplicationPropertiesSection) s;
                s = iter.hasNext() ? iter.next() : null;
            }

            if (s instanceof AmqpValueSection)
            {
                _contentSize = s.getEncodedSize();
                _dataSections.add(s);
                s = iter.hasNext() ? iter.next() : null;
            }
            else if (s instanceof DataSection)
            {
                do
                {
                    _contentSize += s.getEncodedSize();
                    _dataSections.add(s);
                    s = iter.hasNext() ? iter.next() : null;
                }
                while (s instanceof DataSection);
            }
            else if (s instanceof AmqpSequenceSection)
            {
                do
                {
                    _contentSize += s.getEncodedSize();
                    _dataSections.add(s);
                    s = iter.hasNext() ? iter.next() : null;
                }
                while (s instanceof AmqpSequenceSection);
            }
            else
            {
                throw new IllegalStateException("Application data sections are not found");
            }

            if (s instanceof FooterSection)
            {
                _footerSection = (FooterSection) s;
                s = iter.hasNext() ? iter.next() : null;
            }

            if (s != null)
            {
                throw new IllegalStateException(String.format("Encountered unexpected section '%s'",
                                                              s.getClass().getSimpleName()));
            }
            _parsed = true;
        }
    }


    public Object getData() throws AmqpErrorException
    {
        parse();


        Object bodyObject = null;
        EncodingRetainingSection<?> firstBodySection = _dataSections.get(0);
        if(firstBodySection instanceof AmqpValueSection)
        {
            bodyObject = convertValue(firstBodySection.getValue());
        }
        else if(firstBodySection instanceof DataSection)
        {
            int totalSize = 0;
            for(EncodingRetainingSection<?> section : _dataSections)
            {
                totalSize += ((DataSection)section).getValue().getArray().length;
            }
            byte[] bodyData = new byte[totalSize];
            ByteBuffer buf = ByteBuffer.wrap(bodyData);
            for(EncodingRetainingSection<?> section : _dataSections)
            {
                buf.put(((DataSection) section).getValue().asByteBuffer());
            }
            bodyObject = bodyData;
        }
        else
        {
            ArrayList<Object> totalSequence = new ArrayList<>();
            for(EncodingRetainingSection<?> section : _dataSections)
            {
                totalSequence.addAll(((AmqpSequenceSection)section).getValue());
            }
            bodyObject = convertValue(totalSequence);
        }
        return bodyObject;
    }

    public Map<String, Object> getApplicationProperties() throws AmqpErrorException
    {
        parse();
        if (_applicationPropertiesSection != null)
        {
            return _applicationPropertiesSection.getValue();
        }
        return Collections.emptyMap();
    }
}
