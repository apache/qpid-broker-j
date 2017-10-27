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

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.plugin.MessageMetaDataType;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionDecoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
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
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.PropertiesSection;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public class MessageMetaData_1_0 implements StorableMessageMetaData
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageMetaData_1_0.class);
    private static final MessageMetaDataType_1_0 TYPE = new MessageMetaDataType_1_0();
    public static final MessageMetaDataType.Factory<MessageMetaData_1_0> FACTORY = new MetaDataFactory();
    private static final byte VERSION_BYTE = 1;

    private final long _contentSize;

    // TODO move to somewhere more useful
    private static final Symbol DELIVERY_TIME = Symbol.valueOf("x-opt-delivery-time");
    private static final Symbol NOT_VALID_BEFORE = Symbol.valueOf("x-qpid-not-valid-before");

    private HeaderSection _headerSection;
    private PropertiesSection _propertiesSection;
    private DeliveryAnnotationsSection _deliveryAnnotationsSection;
    private MessageAnnotationsSection _messageAnnotationsSection;
    private ApplicationPropertiesSection _applicationPropertiesSection;
    private FooterSection _footerSection;


    private final MessageHeader_1_0 _messageHeader = new MessageHeader_1_0();
    private final int _version;
    private final long _arrivalTime;

    public MessageMetaData_1_0(final HeaderSection headerSection,
                               final DeliveryAnnotationsSection deliveryAnnotationsSection,
                               final MessageAnnotationsSection messageAnnotationsSection,
                               final PropertiesSection propertiesSection,
                               final ApplicationPropertiesSection applicationPropertiesSection,
                               final FooterSection footerSection,
                               final long arrivalTime,
                               final long contentSize)
    {
        this(headerSection,
             deliveryAnnotationsSection,
             messageAnnotationsSection,
             propertiesSection,
             applicationPropertiesSection,
             footerSection,
             arrivalTime,
             contentSize,
             VERSION_BYTE);
    }

    public MessageMetaData_1_0(final HeaderSection headerSection,
                               final DeliveryAnnotationsSection deliveryAnnotationsSection,
                               final MessageAnnotationsSection messageAnnotationsSection,
                               final PropertiesSection propertiesSection,
                               final ApplicationPropertiesSection applicationPropertiesSection,
                               final FooterSection footerSection,
                               final long arrivalTime,
                               final long contentSize,
                               final int version)
    {
        _headerSection = headerSection;
        _deliveryAnnotationsSection = deliveryAnnotationsSection;
        _messageAnnotationsSection = messageAnnotationsSection;
        _propertiesSection = propertiesSection;
        _applicationPropertiesSection = applicationPropertiesSection;
        _footerSection = footerSection;
        _arrivalTime = arrivalTime;
        _contentSize = contentSize;
        _version = version;
    }

    private MessageMetaData_1_0(List<EncodingRetainingSection<?>> sections,
                                long contentSize,
                                int version,
                                long arrivalTime)
    {
        _contentSize = contentSize;
        _version = version;
        _arrivalTime = arrivalTime;

        Iterator<EncodingRetainingSection<?>> sectIter = sections.iterator();

        EncodingRetainingSection<?> section = sectIter.hasNext() ? sectIter.next() : null;
        if (section instanceof HeaderSection)
        {
            _headerSection = (HeaderSection) section;
            section = sectIter.hasNext() ? sectIter.next() : null;
        }

        if (section instanceof DeliveryAnnotationsSection)
        {
            _deliveryAnnotationsSection = (DeliveryAnnotationsSection) section;
            section = sectIter.hasNext() ? sectIter.next() : null;
        }

        if (section instanceof MessageAnnotationsSection)
        {
            _messageAnnotationsSection = (MessageAnnotationsSection) section;
            section = sectIter.hasNext() ? sectIter.next() : null;
        }

        if (section instanceof PropertiesSection)
        {
            _propertiesSection = ((PropertiesSection) section);
            section = sectIter.hasNext() ? sectIter.next() : null;
        }

        if (section instanceof ApplicationPropertiesSection)
        {
            _applicationPropertiesSection = (ApplicationPropertiesSection) section;
            section = sectIter.hasNext() ? sectIter.next() : null;
        }

        if (section instanceof FooterSection)
        {
            _footerSection = (FooterSection) section;
            section = sectIter.hasNext() ? sectIter.next() : null;
        }
    }


    public Properties getProperties()
    {
        return _propertiesSection == null ? null : _propertiesSection.getValue();
    }

    public PropertiesSection getPropertiesSection()
    {
        return _propertiesSection == null ? null : _propertiesSection.copy();
    }


    @Override
    public MessageMetaDataType getType()
    {
        return TYPE;
    }


    @Override
    public int getStorableSize()
    {

        long size = 17L;
        if (_headerSection != null)
        {
            size += _headerSection.getEncodedSize();
        }
        if (_deliveryAnnotationsSection != null)
        {
            size += _deliveryAnnotationsSection.getEncodedSize();
        }
        if (_messageAnnotationsSection != null)
        {
            size += _messageAnnotationsSection.getEncodedSize();
        }
        if (_propertiesSection != null)
        {
            size += _propertiesSection.getEncodedSize();
        }
        if (_applicationPropertiesSection != null)
        {
            size += _applicationPropertiesSection.getEncodedSize();
        }
        if (_footerSection != null)
        {
            size += _footerSection.getEncodedSize();
        }

        return (int) size;
    }

    @Override
    public void writeToBuffer(QpidByteBuffer dest)
    {
        dest.put(VERSION_BYTE);
        dest.putLong(_arrivalTime);
        dest.putLong(_contentSize);
        if (_headerSection != null)
        {
            _headerSection.writeTo(dest);
        }
        if (_deliveryAnnotationsSection != null)
        {
            _deliveryAnnotationsSection.writeTo(dest);
        }
        if (_messageAnnotationsSection != null)
        {
            _messageAnnotationsSection.writeTo(dest);
        }
        if (_propertiesSection != null)
        {
            _propertiesSection.writeTo(dest);
        }
        if (_applicationPropertiesSection != null)
        {
            _applicationPropertiesSection.writeTo(dest);
        }
        if (_footerSection != null)
        {
            _footerSection.writeTo(dest);
        }
    }

    @Override
    public int getContentSize()
    {
        return (int) _contentSize;
    }

    @Override
    public boolean isPersistent()
    {
        return _headerSection != null && Boolean.TRUE.equals(_headerSection.getValue().getDurable());
    }

    public MessageHeader_1_0 getMessageHeader()
    {
        return _messageHeader;
    }

    @Override
    public synchronized void dispose()
    {
        if (_headerSection != null)
        {
            _headerSection.dispose();
            _headerSection = null;
        }
        if (_deliveryAnnotationsSection != null)
        {
            _deliveryAnnotationsSection.dispose();
            _deliveryAnnotationsSection = null;
        }
        if (_messageAnnotationsSection != null)
        {
            _messageAnnotationsSection.dispose();
            _messageAnnotationsSection = null;
        }
        if (_propertiesSection != null)
        {
            _propertiesSection.dispose();
            _propertiesSection = null;
        }
        if (_applicationPropertiesSection != null)
        {
            _applicationPropertiesSection.dispose();
            _applicationPropertiesSection = null;
        }
        if (_footerSection != null)
        {
            _footerSection.dispose();
            _footerSection = null;
        }
    }

    @Override
    public void reallocate()
    {
        if (_headerSection != null)
        {
            _headerSection.reallocate();
        }
        if (_deliveryAnnotationsSection != null)
        {
            _deliveryAnnotationsSection.reallocate();
        }
        if (_messageAnnotationsSection != null)
        {
            _messageAnnotationsSection.reallocate();
        }
        if (_propertiesSection != null)
        {
            _propertiesSection.reallocate();
        }
        if (_applicationPropertiesSection != null)
        {
            _applicationPropertiesSection.reallocate();
        }
        if (_footerSection != null)
        {
            _footerSection.reallocate();
        }
    }

    @Override
    public void clearEncodedForm()
    {
        if (_headerSection != null)
        {
            _headerSection.clearEncodedForm();
        }
        if (_deliveryAnnotationsSection != null)
        {
            _deliveryAnnotationsSection.clearEncodedForm();
        }
        if (_messageAnnotationsSection != null)
        {
            _messageAnnotationsSection.clearEncodedForm();
        }
        if (_propertiesSection != null)
        {
            _propertiesSection.clearEncodedForm();
        }
        if (_applicationPropertiesSection != null)
        {
            _applicationPropertiesSection.clearEncodedForm();
        }
        if (_footerSection != null)
        {
            _footerSection.clearEncodedForm();
        }
    }

    public HeaderSection getHeaderSection()
    {
        return _headerSection == null ? null : _headerSection.copy();
    }

    public DeliveryAnnotationsSection getDeliveryAnnotationsSection()
    {
        return _deliveryAnnotationsSection == null ? null : _deliveryAnnotationsSection.copy();
    }

    public MessageAnnotationsSection getMessageAnnotationsSection()
    {
        return _messageAnnotationsSection == null ? null : _messageAnnotationsSection.copy();
    }

    public ApplicationPropertiesSection getApplicationPropertiesSection()
    {
        return _applicationPropertiesSection == null ? null : _applicationPropertiesSection.copy();
    }

    public FooterSection getFooterSection()
    {
        return _footerSection == null ? null : _footerSection.copy();
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

        @Override
        public MessageMetaData_1_0 createMetaData(QpidByteBuffer buf)
        {
            try
            {
                if (!buf.hasRemaining())
                {
                    throw new ConnectionScopedRuntimeException("No metadata found");
                }

                byte versionByte = buf.get(buf.position());

                long arrivalTime;
                long contentSize = 0;
                if (versionByte == 1)
                {
                    if (!buf.hasRemaining(17))
                    {
                        throw new ConnectionScopedRuntimeException("Cannot decode stored message meta data.");
                    }
                    // we can discard the first byte
                    buf.get();
                    arrivalTime = buf.getLong();
                    contentSize = buf.getLong();
                }
                else if (versionByte == 0)
                {
                    arrivalTime = System.currentTimeMillis();
                }
                else
                {
                    throw new ConnectionScopedRuntimeException(String.format(
                            "Unexpected version byte %d encountered at head of metadata. Unable to decode message.",
                            versionByte));
                }

                SectionDecoder sectionDecoder = new SectionDecoderImpl(_typeRegistry.getSectionDecoderRegistry());

                List<EncodingRetainingSection<?>> sections = sectionDecoder.parseAll(buf);

                if (versionByte == 0)
                {
                    Iterator<EncodingRetainingSection<?>> iter = sections.iterator();
                    while (iter.hasNext())
                    {
                        final EncodingRetainingSection<?> section = iter.next();
                        if (section instanceof DataSection
                            || section instanceof AmqpValueSection
                            || section instanceof AmqpSequenceSection)
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
                throw new ConnectionScopedRuntimeException(e);
            }
        }
    }

    public class MessageHeader_1_0 implements AMQMessageHeader
    {

        private final AtomicReference<String> _decodedUserId = new AtomicReference<>();

        @Override
        public String getCorrelationId()
        {
            if (_propertiesSection == null || _propertiesSection.getValue().getCorrelationId() == null)
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
            final UnsignedInteger ttl = _headerSection == null ? null : _headerSection.getValue().getTtl();
            return ttl == null ? 0L : ttl.longValue() + getArrivalTime();
        }

        @Override
        public String getMessageId()
        {
            if (_propertiesSection == null || _propertiesSection.getValue().getMessageId() == null)
            {
                return null;
            }
            else
            {
                return _propertiesSection.getValue().getMessageId().toString();
            }
        }

        @Override
        public String getMimeType()
        {
            if (_propertiesSection == null || _propertiesSection.getValue().getContentType() == null)
            {
                return null;
            }
            else
            {
                return _propertiesSection.getValue().getContentType().toString();
            }
        }

        @Override
        public String getEncoding()
        {
            if (_propertiesSection == null || _propertiesSection.getValue().getContentEncoding() == null)
            {
                return null;
            }
            else
            {
                return _propertiesSection.getValue().getContentEncoding().toString();
            }
        }

        @Override
        public byte getPriority()
        {
            if (_headerSection == null || _headerSection.getValue().getPriority() == null)
            {
                return 4; //javax.jms.Message.DEFAULT_PRIORITY;
            }
            else
            {
                return _headerSection.getValue().getPriority().byteValue();
            }
        }

        @Override
        public long getTimestamp()
        {
            if (_propertiesSection == null || _propertiesSection.getValue().getCreationTime() == null)
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

            if (_messageAnnotationsSection != null && (annotation =
                    _messageAnnotationsSection.getValue().get(DELIVERY_TIME)) instanceof Number)
            {
                notValidBefore = ((Number) annotation).longValue();
            }
            else if (_messageAnnotationsSection != null && (annotation =
                    _messageAnnotationsSection.getValue().get(NOT_VALID_BEFORE)) instanceof Number)
            {
                notValidBefore = ((Number) annotation).longValue();
            }
            else
            {
                notValidBefore = 0L;
            }
            return notValidBefore;
        }

        @Override
        public String getType()
        {
            return getSubject();
        }

        @Override
        public String getReplyTo()
        {
            if (_propertiesSection == null || _propertiesSection.getValue().getReplyTo() == null)
            {
                return null;
            }
            else
            {
                return _propertiesSection.getValue().getReplyTo();
            }
        }

        @Override
        public String getAppId()
        {
            //TODO
            return null;
        }

        @Override
        public String getGroupId()
        {
            if (_propertiesSection == null || _propertiesSection.getValue().getGroupId() == null)
            {
                return null;
            }
            else
            {
                return _propertiesSection.getValue().getGroupId();
            }
        }

        @Override
        public String getUserId()
        {
            if (_propertiesSection == null || _propertiesSection.getValue().getUserId() == null)
            {
                return null;
            }
            else
            {
                if (_decodedUserId.get() == null)
                {
                    Binary encodededUserId = _propertiesSection.getValue().getUserId();
                    _decodedUserId.set(new String(encodededUserId.getArray(), StandardCharsets.UTF_8));
                }
                return _decodedUserId.get();
            }
        }

        @Override
        public Object getHeader(final String name)
        {
            return _applicationPropertiesSection == null ? null : _applicationPropertiesSection.getValue().get(name);
        }

        @Override
        public boolean containsHeaders(final Set<String> names)
        {
            if (_applicationPropertiesSection == null)
            {
                return false;
            }

            for (String key : names)
            {
                if (!_applicationPropertiesSection.getValue().containsKey(key))
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Collection<String> getHeaderNames()
        {
            if (_applicationPropertiesSection == null)
            {
                return Collections.emptySet();
            }
            return Collections.unmodifiableCollection(_applicationPropertiesSection.getValue().keySet());
        }

        @Override
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
            return _applicationPropertiesSection == null ? new HashMap<>() : new HashMap<>(
                    _applicationPropertiesSection.getValue());
        }
    }
}
