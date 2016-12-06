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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.protocol.v1_0.codec.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoder;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.*;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public class MessageMetaData_1_0 implements StorableMessageMetaData
{
    private static final Logger _logger = LoggerFactory.getLogger(MessageMetaData_1_0.class);
    private static final MessageMetaDataType_1_0 TYPE = new MessageMetaDataType_1_0();
    public static final MessageMetaDataType.Factory<MessageMetaData_1_0> FACTORY = new MetaDataFactory();
    private static final byte VERSION_BYTE = 1;

    private long _contentSize;

    // TODO move to somewhere more useful
    private static final Symbol JMS_TYPE = Symbol.valueOf("x-opt-jms-type");
    private static final Symbol DELIVERY_TIME = Symbol.valueOf("x-opt-delivery-time");
    private static final Symbol NOT_VALID_BEFORE = Symbol.valueOf("x-qpid-not-valid-before");

    private HeaderSection _headerSection;
    private PropertiesSection _propertiesSection;
    private DeliveryAnnotationsSection _deliveryAnnotationsSection;
    private MessageAnnotationsSection _messageAnnotationsSection;
    private ApplicationPropertiesSection _applicationPropertiesSection;
    private FooterSection _footerSection;


    private MessageHeader_1_0 _messageHeader;
    private final int _version;
    private final long _arrivalTime;

    public MessageMetaData_1_0(List<NonEncodingRetainingSection<?>> sections,
                               SectionEncoder encoder,
                               final List<EncodingRetainingSection<?>> bodySections,
                               final long arrivalTime)
    {
        _version = VERSION_BYTE;
        _arrivalTime = arrivalTime;
        Iterator<NonEncodingRetainingSection<?>> iter = sections.iterator();
        NonEncodingRetainingSection<?> s = iter.hasNext() ? iter.next() : null;
        long contentSize = 0L;
        if(s instanceof Header)
        {
            encoder.reset();
            encoder.encodeObject(s);
            Binary encodedOutput = encoder.getEncoding();
            final QpidByteBuffer buf = QpidByteBuffer.wrap(encodedOutput.asByteBuffer());
            _headerSection = new HeaderSection((Header)s, Collections.singletonList(buf), encoder.getRegistry());
            s = iter.hasNext() ? iter.next() : null;
        }

        if(s instanceof DeliveryAnnotations)
        {
            encoder.reset();
            encoder.encodeObject(s);
            Binary encodedOutput = encoder.getEncoding();
            final QpidByteBuffer buf = QpidByteBuffer.wrap(encodedOutput.asByteBuffer());
            _deliveryAnnotationsSection = new DeliveryAnnotationsSection((DeliveryAnnotations)s, Collections.singletonList(buf), encoder.getRegistry());
            s = iter.hasNext() ? iter.next() : null;
        }

        if(s instanceof MessageAnnotations)
        {
            encoder.reset();
            encoder.encodeObject(s);
            Binary encodedOutput = encoder.getEncoding();
            final QpidByteBuffer buf = QpidByteBuffer.wrap(encodedOutput.asByteBuffer());
            _messageAnnotationsSection = new MessageAnnotationsSection((MessageAnnotations)s, Collections.singletonList(buf), encoder.getRegistry());
            s = iter.hasNext() ? iter.next() : null;
        }

        if(s instanceof Properties)
        {
            encoder.reset();
            encoder.encodeObject(s);
            Binary encodedOutput = encoder.getEncoding();
            final QpidByteBuffer buf = QpidByteBuffer.wrap(encodedOutput.asByteBuffer());
            _propertiesSection = new PropertiesSection((Properties)s, Collections.singletonList(buf), encoder.getRegistry());
            s = iter.hasNext() ? iter.next() : null;
        }

        if(s instanceof ApplicationProperties)
        {
            encoder.reset();
            encoder.encodeObject(s);
            Binary encodedOutput = encoder.getEncoding();
            final QpidByteBuffer buf = QpidByteBuffer.wrap(encodedOutput.asByteBuffer());
            _applicationPropertiesSection = new ApplicationPropertiesSection((ApplicationProperties)s, Collections.singletonList(buf), encoder.getRegistry());
            s = iter.hasNext() ? iter.next() : null;
        }

        if(s instanceof AmqpValue)
        {
            encoder.reset();
            encoder.encodeObject(s);
            Binary encodedOutput = encoder.getEncoding();
            final QpidByteBuffer buf = QpidByteBuffer.wrap(encodedOutput.asByteBuffer());
            bodySections.add(new AmqpValueSection((AmqpValue)s, Collections.singletonList(buf), encoder.getRegistry()));

            contentSize = buf.remaining();
            s = iter.hasNext() ? iter.next() : null;
        }
        else if(s instanceof Data)
        {
            do
            {
                encoder.reset();
                encoder.encodeObject(s);
                Binary encodedOutput = encoder.getEncoding();
                final QpidByteBuffer buf = QpidByteBuffer.wrap(encodedOutput.asByteBuffer());
                bodySections.add(new DataSection((Data)s, Collections.singletonList(buf), encoder.getRegistry()));

                contentSize += buf.remaining();

                s = iter.hasNext() ? iter.next() : null;
            } while(s instanceof Data);
        }
        else if(s instanceof AmqpSequence)
        {
            do
            {
                encoder.reset();
                encoder.encodeObject(s);
                Binary encodedOutput = encoder.getEncoding();
                final QpidByteBuffer buf = QpidByteBuffer.wrap(encodedOutput.asByteBuffer());
                bodySections.add(new AmqpSequenceSection((AmqpSequence)s, Collections.singletonList(buf), encoder.getRegistry()));

                contentSize += buf.remaining();
                s = iter.hasNext() ? iter.next() : null;
            }
            while(s instanceof AmqpSequence);
        }

        if(s instanceof Footer)
        {
            encoder.reset();
            encoder.encodeObject(s);
            Binary encodedOutput = encoder.getEncoding();
            final QpidByteBuffer buf = QpidByteBuffer.wrap(encodedOutput.asByteBuffer());
            _footerSection = new FooterSection((Footer)s, Collections.singletonList(buf), encoder.getRegistry());
        }
        _contentSize = contentSize;

    }

    public Properties getProperties()
    {
        return _propertiesSection == null ? null : _propertiesSection.getValue();
    }


    public PropertiesSection getPropertiesSection()
    {
        return _propertiesSection;
    }

    public MessageMetaData_1_0(QpidByteBuffer[] fragments, SectionDecoder decoder, List<EncodingRetainingSection<?>> dataSections, long arrivalTime)
    {
        _version = VERSION_BYTE;
        _arrivalTime = arrivalTime;
        List<QpidByteBuffer> src = new ArrayList<>(fragments.length);
        for(QpidByteBuffer buf : fragments)
        {
            src.add(buf.duplicate());
        }

        try
        {
            EncodingRetainingSection<?> s = decoder.readSection(src);
            long contentSize = 0L;
            if(s instanceof HeaderSection)
            {
                _headerSection = (HeaderSection) s;
                s = QpidByteBufferUtils.hasRemaining(src) ? decoder.readSection(src) : null;
            }

            if(s instanceof DeliveryAnnotationsSection)
            {
                _deliveryAnnotationsSection = (DeliveryAnnotationsSection) s;
                s = QpidByteBufferUtils.hasRemaining(src) ? decoder.readSection(src) : null;
            }

            if(s instanceof MessageAnnotationsSection)
            {
                _messageAnnotationsSection = (MessageAnnotationsSection) s;
                s = QpidByteBufferUtils.hasRemaining(src) ? decoder.readSection(src) : null;
            }

            if(s instanceof PropertiesSection)
            {
                _propertiesSection = (PropertiesSection) s;
                s = QpidByteBufferUtils.hasRemaining(src) ? decoder.readSection(src) : null;
            }

            if(s instanceof ApplicationPropertiesSection)
            {
                _applicationPropertiesSection = (ApplicationPropertiesSection) s;
                s = QpidByteBufferUtils.hasRemaining(src) ? decoder.readSection(src) : null;
            }

            if(s instanceof AmqpValueSection)
            {
                contentSize = s.getEncodedSize();
                dataSections.add(s);
                s = QpidByteBufferUtils.hasRemaining(src) ? decoder.readSection(src) : null;
            }
            else if(s instanceof DataSection)
            {
                do
                {
                    contentSize += s.getEncodedSize();
                    dataSections.add(s);
                    s = QpidByteBufferUtils.hasRemaining(src) ? decoder.readSection(src) : null;
                } while(s instanceof DataSection);
            }
            else if(s instanceof AmqpSequenceSection)
            {
                do
                {
                    contentSize += s.getEncodedSize();
                    dataSections.add(s);
                    s = QpidByteBufferUtils.hasRemaining(src) ? decoder.readSection(src) : null;
                }
                while(s instanceof AmqpSequenceSection);
            }

            if(s instanceof FooterSection)
            {
                _footerSection = (FooterSection) s;
            }
            _contentSize = contentSize;
        }
        catch (AmqpErrorException e)
        {
            _logger.error("Decoding read section error", e);
            // TODO - fix error handling
            throw new IllegalArgumentException(e);
        }
        finally
        {
            for(QpidByteBuffer buf : src)
            {
                buf.dispose();
            }
        }

        _messageHeader = new MessageHeader_1_0();

    }

    private MessageMetaData_1_0(List<EncodingRetainingSection<?>> sections, long contentSize, int version, long arrivalTime)
    {
        _contentSize = contentSize;
        _version = version;
        _arrivalTime = arrivalTime;

        Iterator<EncodingRetainingSection<?>> sectIter = sections.iterator();

        EncodingRetainingSection<?> section = sectIter.hasNext() ? sectIter.next() : null;
        if(section instanceof HeaderSection)
        {
            _headerSection = (HeaderSection) section;
            section = sectIter.hasNext() ? sectIter.next() : null;
        }

        if(section instanceof DeliveryAnnotationsSection)
        {
            _deliveryAnnotationsSection = (DeliveryAnnotationsSection) section;
            section = sectIter.hasNext() ? sectIter.next() : null;
        }

        if(section instanceof MessageAnnotationsSection)
        {
            _messageAnnotationsSection = (MessageAnnotationsSection) section;
            section = sectIter.hasNext() ? sectIter.next() : null;
        }

        if(section instanceof PropertiesSection)
        {
            _propertiesSection = ((PropertiesSection) section);
            section = sectIter.hasNext() ? sectIter.next() : null;
        }

        if(section instanceof ApplicationPropertiesSection)
        {
            _applicationPropertiesSection = (ApplicationPropertiesSection) section;
            section = sectIter.hasNext() ? sectIter.next() : null;
        }

        if(section instanceof FooterSection)
        {
            _footerSection = (FooterSection) section;
            section = sectIter.hasNext() ? sectIter.next() : null;
        }

        _messageHeader = new MessageHeader_1_0();

    }


    public MessageMetaDataType getType()
    {
        return TYPE;
    }


    public int getStorableSize()
    {

        long size = 17L;
        if(_headerSection != null)
        {
            size += _headerSection.getEncodedSize();
        }
        if(_deliveryAnnotationsSection != null)
        {
            size += _deliveryAnnotationsSection.getEncodedSize();
        }
        if(_messageAnnotationsSection != null)
        {
            size += _messageAnnotationsSection.getEncodedSize();
        }
        if(_propertiesSection != null)
        {
            size += _propertiesSection.getEncodedSize();
        }
        if(_applicationPropertiesSection != null)
        {
            size += _applicationPropertiesSection.getEncodedSize();
        }
        if(_footerSection != null)
        {
            size += _footerSection.getEncodedSize();
        }

        return (int) size;
    }

    public void writeToBuffer(QpidByteBuffer dest)
    {
        dest.put(VERSION_BYTE);
        dest.putLong(_arrivalTime);
        dest.putLong(_contentSize);
        if(_headerSection != null)
        {
            _headerSection.writeTo(dest);
        }
        if(_deliveryAnnotationsSection != null)
        {
            _deliveryAnnotationsSection.writeTo(dest);
        }
        if(_messageAnnotationsSection != null)
        {
            _messageAnnotationsSection.writeTo(dest);
        }
        if(_propertiesSection != null)
        {
            _propertiesSection.writeTo(dest);
        }
        if(_applicationPropertiesSection != null)
        {
            _applicationPropertiesSection.writeTo(dest);
        }
        if(_footerSection != null)
        {
            _footerSection.writeTo(dest);
        }

    }

    public int getContentSize()
    {
        return (int) _contentSize;
    }

    public boolean isPersistent()
    {
        return _headerSection != null && Boolean.TRUE.equals(_headerSection.getValue().getDurable());
    }

    public MessageHeader_1_0 getMessageHeader()
    {
        return _messageHeader;
    }

    @Override
    public synchronized void  dispose()
    {
        if(_headerSection != null)
        {
            _headerSection.dispose();
            _headerSection = null;
        }
        if(_deliveryAnnotationsSection != null)
        {
            _deliveryAnnotationsSection.dispose();
            _deliveryAnnotationsSection = null;
        }
        if(_messageAnnotationsSection != null)
        {
            _messageAnnotationsSection.dispose();
            _deliveryAnnotationsSection = null;
        }
        if(_propertiesSection != null)
        {
            _propertiesSection.dispose();
            _propertiesSection = null;
        }
        if(_applicationPropertiesSection != null)
        {
            _applicationPropertiesSection.dispose();
            _applicationPropertiesSection = null;
        }

    }

    @Override
    public void clearEncodedForm()
    {
        dispose();
    }

    public HeaderSection getHeaderSection()
    {
        return _headerSection;
    }

    public DeliveryAnnotationsSection getDeliveryAnnotationsSection()
    {
        return _deliveryAnnotationsSection;
    }

    public MessageAnnotationsSection getMessageAnnotationsSection()
    {
        return _messageAnnotationsSection;
    }

    public ApplicationPropertiesSection getApplicationPropertiesSection()
    {
        return _applicationPropertiesSection;
    }

    public FooterSection getFooterSection()
    {
        return _footerSection;
    }

    public int getVersion()
    {
        return _version;
    }

    public long getArrivalTime()
    {
        return _arrivalTime;
    }

    private static class MetaDataFactory implements MessageMetaDataType.Factory<MessageMetaData_1_0>
    {
        private final AMQPDescribedTypeRegistry _typeRegistry = AMQPDescribedTypeRegistry.newInstance();

        private MetaDataFactory()
        {
            _typeRegistry.registerTransportLayer();
            _typeRegistry.registerMessagingLayer();
            _typeRegistry.registerTransactionLayer();
            _typeRegistry.registerSecurityLayer();
        }

        public MessageMetaData_1_0 createMetaData(QpidByteBuffer buf)
        {
            try
            {
                byte versionByte = buf.get(0);
                long arrivalTime;
                long contentSize = 0;
                if (versionByte == 1)
                {
                    // we can discard the first byte
                    buf.get();
                    arrivalTime = buf.getLong();
                    contentSize = buf.getLong();

                }
                else
                {
                    arrivalTime = System.currentTimeMillis();
                }

                SectionDecoder sectionDecoder = new SectionDecoderImpl(_typeRegistry.getSectionDecoderRegistry());

                List<EncodingRetainingSection<?>> sections = sectionDecoder.parseAll(Collections.singletonList(buf));

                if(versionByte == 0)
                {
                    Iterator<EncodingRetainingSection<?>> iter = sections.iterator();
                    while(iter.hasNext())
                    {
                        final EncodingRetainingSection<?> section = iter.next();
                        if(section instanceof DataSection || section instanceof AmqpValueSection || section instanceof AmqpSequenceSection)
                        {
                            contentSize += section.getEncodedSize();
                            iter.remove();
                            section.dispose();
                        }
                    }

                }

                return new MessageMetaData_1_0(sections, contentSize, (int) versionByte & 0xff, arrivalTime);

            }
            catch (AmqpErrorException e)
            {
                //TODO
                throw new ConnectionScopedRuntimeException(e);
            }

        }
    }

    public class MessageHeader_1_0 implements AMQMessageHeader
    {

        public String getCorrelationId()
        {
            if(_propertiesSection == null || _propertiesSection.getValue().getCorrelationId() == null)
            {
                return null;
            }
            else
            {
                return _propertiesSection.getValue().getCorrelationId().toString();
            }
        }

        @Override
        public long getExpiration()
        {
            final Date absoluteExpiryTime = _propertiesSection == null ? null : _propertiesSection.getValue().getAbsoluteExpiryTime();
            if(absoluteExpiryTime != null)
            {
                return absoluteExpiryTime.getTime();
            }
            else
            {
                final Date creationTime = _propertiesSection == null ? null : _propertiesSection.getValue().getCreationTime();
                final UnsignedInteger ttl = _headerSection == null ? null : _headerSection.getValue().getTtl();
                return ttl == null || creationTime == null ? 0L : ttl.longValue() + creationTime.getTime();
            }
        }

        public String getMessageId()
        {
            if(_propertiesSection == null || _propertiesSection.getValue().getMessageId() == null)
            {
                return null;
            }
            else
            {
                return _propertiesSection.getValue().getMessageId().toString();
            }
        }

        public String getMimeType()
        {

            if(_propertiesSection == null || _propertiesSection.getValue().getContentType() == null)
            {
                return null;
            }
            else
            {
                return _propertiesSection.getValue().getContentType().toString();
            }
        }

        public String getEncoding()
        {
            return null;  //TODO
        }

        public byte getPriority()
        {
            if(_headerSection == null || _headerSection.getValue().getPriority() == null)
            {
                return 4; //javax.jms.Message.DEFAULT_PRIORITY;
            }
            else
            {
                return _headerSection.getValue().getPriority().byteValue();
            }
        }

        public long getTimestamp()
        {
            if(_propertiesSection == null || _propertiesSection.getValue().getCreationTime() == null)
            {
                return 0L;
            }
            else
            {
                return _propertiesSection.getValue().getCreationTime().getTime();
            }

        }


        @Override
        public long getNotValidBefore()
        {
            long notValidBefore;
            Object annotation;

            if(_messageAnnotationsSection != null && (annotation = _messageAnnotationsSection.getValue().get(DELIVERY_TIME)) instanceof Number)
            {
                notValidBefore = ((Number)annotation).longValue();
            }
            else if(_messageAnnotationsSection != null && (annotation = _messageAnnotationsSection.getValue().get(NOT_VALID_BEFORE)) instanceof Number)
            {
                notValidBefore = ((Number)annotation).longValue();
            }
            else
            {
                notValidBefore = 0L;
            }
            return notValidBefore;
        }

        public String getType()
        {
            String subject = getSubject();
            if(subject != null)
            {
                return subject;
            }

            // Use legacy annotation if present and there was no subject
            if(_messageAnnotationsSection == null || _messageAnnotationsSection.getValue().get(JMS_TYPE) == null)
            {
                return null;
            }
            else
            {
                return _messageAnnotationsSection.getValue().get(JMS_TYPE).toString();
            }
        }

        public String getReplyTo()
        {
            if(_propertiesSection == null || _propertiesSection.getValue().getReplyTo() == null)
            {
                return null;
            }
            else
            {
                return _propertiesSection.getValue().getReplyTo();
            }
        }

        public String getAppId()
        {
            //TODO
            return null;
        }

        public String getUserId()
        {
            // TODO
            return null;
        }

        public Object getHeader(final String name)
        {
            return _applicationPropertiesSection == null ? null : _applicationPropertiesSection.getValue().get(name);
        }

        public boolean containsHeaders(final Set<String> names)
        {
            if(_applicationPropertiesSection == null)
            {
                return false;
            }

            for(String key : names)
            {
                if(!_applicationPropertiesSection.getValue().containsKey(key))
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Collection<String> getHeaderNames()
        {
            if(_applicationPropertiesSection == null)
            {
                return Collections.emptySet();
            }
            return Collections.unmodifiableCollection(_applicationPropertiesSection.getValue().keySet());
        }

        public boolean containsHeader(final String name)
        {
            return _applicationPropertiesSection != null && _applicationPropertiesSection.getValue().containsKey(name);
        }

        public String getSubject()
        {
            return _propertiesSection == null ? null : _propertiesSection.getValue().getSubject();
        }

        public String getTo()
        {
            return _propertiesSection == null ? null : _propertiesSection.getValue().getTo();
        }

        public Map<String, Object> getHeadersAsMap()
        {
            return _applicationPropertiesSection == null ? new HashMap<String,Object>() : new HashMap<>(
                    _applicationPropertiesSection.getValue());
        }
    }

}
