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
package org.apache.qpid.server.protocol.v0_8.transport;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.EncodingUtils;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.transport.ByteBufferSender;

public class BasicContentHeaderProperties
{
    //persistent & non-persistent constants, values as per JMS DeliveryMode
    public static final byte NON_PERSISTENT = 1;
    public static final byte PERSISTENT = 2;

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicContentHeaderProperties.class);

    private static final AMQShortString ZERO_STRING = null;

    private AMQShortString _contentType;

    private AMQShortString _encoding;

    private FieldTable _headers;

    private byte _deliveryMode;

    private byte _priority;

    private AMQShortString _correlationId;

    private AMQShortString _replyTo;

    private long _expiration;

    private AMQShortString _messageId;

    private long _timestamp;

    private AMQShortString _type;

    private AMQShortString _userId;

    private AMQShortString _appId;

    private AMQShortString _clusterId;

    private int _propertyFlags = 0;
    private static final int CONTENT_TYPE_MASK = 1 << 15;
    private static final int ENCODING_MASK = 1 << 14;
    private static final int HEADERS_MASK = 1 << 13;
    private static final int DELIVERY_MODE_MASK = 1 << 12;
    private static final int PRIORITY_MASK = 1 << 11;
    private static final int CORRELATION_ID_MASK = 1 << 10;
    private static final int REPLY_TO_MASK = 1 << 9;
    private static final int EXPIRATION_MASK = 1 << 8;
    private static final int MESSAGE_ID_MASK = 1 << 7;
    private static final int TIMESTAMP_MASK = 1 << 6;
    private static final int TYPE_MASK = 1 << 5;
    private static final int USER_ID_MASK = 1 << 4;
    private static final int APPLICATION_ID_MASK = 1 << 3;
    private static final int CLUSTER_ID_MASK = 1 << 2;

    private volatile QpidByteBuffer _encodedForm;


    public BasicContentHeaderProperties(BasicContentHeaderProperties other)
    {
        _headers = FieldTableFactory.createFieldTable(other.getHeadersAsMap());

        _contentType = other._contentType;

        _encoding = other._encoding;

        _deliveryMode = other._deliveryMode;

        _priority = other._priority;

        _correlationId = other._correlationId;

        _replyTo = other._replyTo;

        _expiration = other._expiration;

        _messageId = other._messageId;

        _timestamp = other._timestamp;

        _type = other._type;

        _userId = other._userId;

        _appId = other._appId;

        _clusterId = other._clusterId;
        
        _propertyFlags = other._propertyFlags;
    }

    public BasicContentHeaderProperties()
    { 
    }

    public synchronized int getPropertyListSize()
    {
        if(useEncodedForm())
        {
            return _encodedForm.remaining();
        }
        else
        {
            int size = 0;

            if ((_propertyFlags & (CONTENT_TYPE_MASK)) != 0)
            {
                size += EncodingUtils.encodedShortStringLength(_contentType);
            }

            if ((_propertyFlags & ENCODING_MASK) != 0)
            {
                size += EncodingUtils.encodedShortStringLength(_encoding);
            }

            if ((_propertyFlags & HEADERS_MASK) != 0)
            {
                size += EncodingUtils.encodedFieldTableLength(_headers);
            }

            if ((_propertyFlags & DELIVERY_MODE_MASK) != 0)
            {
                size += 1;
            }

            if ((_propertyFlags & PRIORITY_MASK) != 0)
            {
                size += 1;
            }

            if ((_propertyFlags & CORRELATION_ID_MASK) != 0)
            {
                size += EncodingUtils.encodedShortStringLength(_correlationId);
            }

            if ((_propertyFlags & REPLY_TO_MASK) != 0)
            {
                size += EncodingUtils.encodedShortStringLength(_replyTo);
            }

            if ((_propertyFlags & EXPIRATION_MASK) != 0)
            {
                if (_expiration == 0L)
                {
                    size += EncodingUtils.encodedShortStringLength(ZERO_STRING);
                }
                else
                {
                    size += EncodingUtils.encodedShortStringLength(_expiration);
                }
            }

            if ((_propertyFlags & MESSAGE_ID_MASK) != 0)
            {
                size += EncodingUtils.encodedShortStringLength(_messageId);
            }

            if ((_propertyFlags & TIMESTAMP_MASK) != 0)
            {
                size += 8;
            }

            if ((_propertyFlags & TYPE_MASK) != 0)
            {
                size += EncodingUtils.encodedShortStringLength(_type);
            }

            if ((_propertyFlags & USER_ID_MASK) != 0)
            {
                size += EncodingUtils.encodedShortStringLength(_userId);
            }

            if ((_propertyFlags & APPLICATION_ID_MASK) != 0)
            {
                size += EncodingUtils.encodedShortStringLength(_appId);
            }

            if ((_propertyFlags & CLUSTER_ID_MASK) != 0)
            {
                size += EncodingUtils.encodedShortStringLength(_clusterId);
            }

            return size;
        }
    }

    public void setPropertyFlags(int propertyFlags)
    {
        _propertyFlags = propertyFlags;
    }

    public int getPropertyFlags()
    {
        return _propertyFlags;
    }

    public synchronized long writePropertyListPayload(QpidByteBuffer buffer)
    {
        if(useEncodedForm())
        {
            buffer.putCopyOf(_encodedForm);
            return _encodedForm.remaining();

        }
        else
        {
            int propertyListSize = getPropertyListSize();

            if ((_propertyFlags & (CONTENT_TYPE_MASK)) != 0)
            {
                EncodingUtils.writeShortStringBytes(buffer, _contentType);
            }

            if ((_propertyFlags & ENCODING_MASK) != 0)
            {
                EncodingUtils.writeShortStringBytes(buffer, _encoding);
            }

            if ((_propertyFlags & HEADERS_MASK) != 0)
            {
                EncodingUtils.writeFieldTableBytes(buffer, _headers);
            }

            if ((_propertyFlags & DELIVERY_MODE_MASK) != 0)
            {
                buffer.put(_deliveryMode);
            }

            if ((_propertyFlags & PRIORITY_MASK) != 0)
            {
                buffer.put(_priority);
            }

            if ((_propertyFlags & CORRELATION_ID_MASK) != 0)
            {
                EncodingUtils.writeShortStringBytes(buffer, _correlationId);
            }

            if ((_propertyFlags & REPLY_TO_MASK) != 0)
            {
                EncodingUtils.writeShortStringBytes(buffer, _replyTo);
            }

            if ((_propertyFlags & EXPIRATION_MASK) != 0)
            {
                if (_expiration == 0L)
                {
                    EncodingUtils.writeShortStringBytes(buffer, ZERO_STRING);
                }
                else
                {
                    EncodingUtils.writeLongAsShortString(buffer, _expiration);
                }
            }

            if ((_propertyFlags & MESSAGE_ID_MASK) != 0)
            {
                EncodingUtils.writeShortStringBytes(buffer, _messageId);
            }

            if ((_propertyFlags & TIMESTAMP_MASK) != 0)
            {
                buffer.putLong(_timestamp);
            }

            if ((_propertyFlags & TYPE_MASK) != 0)
            {
                EncodingUtils.writeShortStringBytes(buffer, _type);
            }

            if ((_propertyFlags & USER_ID_MASK) != 0)
            {
                EncodingUtils.writeShortStringBytes(buffer, _userId);
            }

            if ((_propertyFlags & APPLICATION_ID_MASK) != 0)
            {
                EncodingUtils.writeShortStringBytes(buffer, _appId);
            }

            if ((_propertyFlags & CLUSTER_ID_MASK) != 0)
            {
                EncodingUtils.writeShortStringBytes(buffer, _clusterId);
            }

            return propertyListSize;
        }
    }

    public synchronized long writePropertyListPayload(final ByteBufferSender sender)
    {
        if(useEncodedForm())
        {
            try (QpidByteBuffer duplicate = _encodedForm.duplicate())
            {
                sender.send(duplicate);
            }
            return _encodedForm.remaining();
        }
        else
        {
            int propertyListSize = getPropertyListSize();
            try (QpidByteBuffer buf = QpidByteBuffer.allocate(sender.isDirectBufferPreferred(), propertyListSize))
            {
                writePropertyListPayload(buf);
                buf.flip();
                sender.send(buf);
            }
            return propertyListSize;
        }

    }

    public BasicContentHeaderProperties(QpidByteBuffer buffer, int propertyFlags, int size) throws AMQFrameDecodingException
    {
        _propertyFlags = propertyFlags;

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Property flags: " + _propertyFlags);
        }
        _encodedForm = buffer.view(0,size);

        try (QpidByteBuffer byteBuffer = _encodedForm.slice())
        {
            decode(byteBuffer);
        }
        buffer.position(buffer.position()+size);

    }

    private void decode(QpidByteBuffer buffer) throws AMQFrameDecodingException
    {
        if ((_propertyFlags & (CONTENT_TYPE_MASK)) != 0)
        {
            _contentType = AMQShortString.readAMQShortString(buffer);
        }

        if ((_propertyFlags & ENCODING_MASK) != 0)
        {
            _encoding = AMQShortString.readAMQShortString(buffer);
        }

        if ((_propertyFlags & HEADERS_MASK) != 0)
        {
            _headers = EncodingUtils.readFieldTable(buffer);
        }

        if ((_propertyFlags & DELIVERY_MODE_MASK) != 0)
        {
            _deliveryMode = buffer.get();
        }

        if ((_propertyFlags & PRIORITY_MASK) != 0)
        {
            _priority = buffer.get();
        }

        if ((_propertyFlags & CORRELATION_ID_MASK) != 0)
        {
            _correlationId = AMQShortString.readAMQShortString(buffer);
        }

        if ((_propertyFlags & REPLY_TO_MASK) != 0)
        {
            _replyTo = AMQShortString.readAMQShortString(buffer);
        }

        if ((_propertyFlags & EXPIRATION_MASK) != 0)
        {
            _expiration = EncodingUtils.readLongAsShortString(buffer);
        }

        if ((_propertyFlags & MESSAGE_ID_MASK) != 0)
        {
            _messageId = AMQShortString.readAMQShortString(buffer);
        }

        if ((_propertyFlags & TIMESTAMP_MASK) != 0)
        {
            _timestamp = buffer.getLong();
        }

        if ((_propertyFlags & TYPE_MASK) != 0)
        {
            _type = AMQShortString.readAMQShortString(buffer);
        }

        if ((_propertyFlags & USER_ID_MASK) != 0)
        {
            _userId = AMQShortString.readAMQShortString(buffer);
        }

        if ((_propertyFlags & APPLICATION_ID_MASK) != 0)
        {
            _appId = AMQShortString.readAMQShortString(buffer);
        }

        if ((_propertyFlags & CLUSTER_ID_MASK) != 0)
        {
            _clusterId = AMQShortString.readAMQShortString(buffer);
        }

    }


    public AMQShortString getContentType()
    {
        return _contentType;
    }

    public String getContentTypeAsString()
    {
        return (_contentType == null) ? null : _contentType.toString();
    }

    public synchronized void setContentType(AMQShortString contentType)
    {

        if(contentType == null)
        {
            _propertyFlags &= (~CONTENT_TYPE_MASK);
        }
        else
        {
            _propertyFlags |= CONTENT_TYPE_MASK;
        }
        _contentType = contentType;
        nullEncodedForm();
    }

    public void setContentType(String contentType)
    {
        setContentType((contentType == null) ? null : AMQShortString.valueOf(contentType));
    }

    public String getEncodingAsString()
    {

        return (getEncoding() == null) ? null : getEncoding().toString();
    }

    public AMQShortString getEncoding()
    {
        return _encoding;
    }

    public void setEncoding(String encoding)
    {
        setEncoding(encoding == null ? null : AMQShortString.valueOf(encoding));
    }

    public synchronized void setEncoding(AMQShortString encoding)
    {
        if(encoding == null)
        {
            _propertyFlags &= (~ENCODING_MASK);
        }
        else
        {
            _propertyFlags |= ENCODING_MASK;
        }
        _encoding = encoding;
        nullEncodedForm();
    }

    public synchronized Object getHeader(String name)
    {
        return _headers == null ? null : _headers.get(name);
    }

    public synchronized Collection<String> getHeaderNames()
    {
        return getHeadersAsMap().keySet();
    }

    public synchronized boolean containsHeader(String name)
    {
        return _headers != null && _headers.containsKey(name);
    }

    public synchronized Map<String, Object> getHeadersAsMap()
    {
        return FieldTable.convertToMap(_headers);
    }

    public synchronized void setHeaders(FieldTable headers)
    {
        if(headers == null)
        {
            _propertyFlags &= (~HEADERS_MASK);
        }
        else
        {
            _propertyFlags |= HEADERS_MASK;
        }
        _headers = headers;
        nullEncodedForm();
    }

    public byte getDeliveryMode()
    {
        return _deliveryMode;
    }

    public synchronized void setDeliveryMode(byte deliveryMode)
    {
        _propertyFlags |= DELIVERY_MODE_MASK;
        _deliveryMode = deliveryMode;
        nullEncodedForm();
    }

    public byte getPriority()
    {
        return _priority;
    }

    public synchronized void setPriority(byte priority)
    {
        _propertyFlags |= PRIORITY_MASK;
        _priority = priority;
        nullEncodedForm();
    }

    public AMQShortString getCorrelationId()
    {
        return _correlationId;
    }

    public String getCorrelationIdAsString()
    {
        return (_correlationId == null) ? null : _correlationId.toString();
    }

    public void setCorrelationId(String correlationId)
    {
        setCorrelationId((correlationId == null) ? null : AMQShortString.valueOf(correlationId));
    }

    public synchronized void setCorrelationId(AMQShortString correlationId)
    {
        if(correlationId == null)
        {
            _propertyFlags &= (~CORRELATION_ID_MASK);
        }
        else
        {
            _propertyFlags |= CORRELATION_ID_MASK;
        }
        _correlationId = correlationId;
        nullEncodedForm();
    }

    public String getReplyToAsString()
    {
        return (_replyTo == null) ? null : _replyTo.toString();
    }

    public AMQShortString getReplyTo()
    {
        return _replyTo;
    }

    public void setReplyTo(String replyTo)
    {
        setReplyTo((replyTo == null) ? null : AMQShortString.valueOf(replyTo));
    }

    public synchronized void setReplyTo(AMQShortString replyTo)
    {
        if(replyTo == null)
        {
            _propertyFlags &= (~REPLY_TO_MASK);
        }
        else
        {
            _propertyFlags |= REPLY_TO_MASK;
        }
        _replyTo = replyTo;
        nullEncodedForm();
    }

    public long getExpiration()
    {
        return _expiration;
    }

    public synchronized void setExpiration(long expiration)
    {
        if(expiration == 0l)
        {
            _propertyFlags &= (~EXPIRATION_MASK);
        }
        else
        {
            _propertyFlags |= EXPIRATION_MASK;
        }
        _expiration = expiration;
        nullEncodedForm();
    }

    public boolean hasExpiration()
    {
        return (_propertyFlags & EXPIRATION_MASK) != 0;
    }

    public AMQShortString getMessageId()
    {
        return _messageId;
    }

    public String getMessageIdAsString()
    {
        return (_messageId == null) ? null : _messageId.toString();
    }

    public void setMessageId(String messageId)
    {
        setMessageId(messageId == null ? null : AMQShortString.createAMQShortString(messageId));
    }

    public synchronized void setMessageId(AMQShortString messageId)
    {
        if(messageId == null)
        {
            _propertyFlags &= (~MESSAGE_ID_MASK);
        }
        else
        {
            _propertyFlags |= MESSAGE_ID_MASK;
        }
        _messageId = messageId;
        nullEncodedForm();
    }

    public long getTimestamp()
    {
        return _timestamp;
    }

    public synchronized void setTimestamp(long timestamp)
    {
        _propertyFlags |= TIMESTAMP_MASK;
        _timestamp = timestamp;
        nullEncodedForm();
    }


    public boolean hasTimestamp()
    {
        return (_propertyFlags & TIMESTAMP_MASK) != 0;
    }

    public String getTypeAsString()
    {
        return (_type == null) ? null : _type.toString();
    }

    public AMQShortString getType()
    {
        return _type;
    }

    public void setType(String type)
    {
        setType((type == null) ? null : AMQShortString.valueOf(type));
    }

    public synchronized void setType(AMQShortString type)
    {
        if(type == null)
        {
            _propertyFlags &= (~TYPE_MASK);
        }
        else
        {
            _propertyFlags |= TYPE_MASK;
        }
        _type = type;
        nullEncodedForm();
    }

    public String getUserIdAsString()
    {
        return (_userId == null) ? null : _userId.toString();
    }

    public AMQShortString getUserId()
    {
        return _userId;
    }

    public void setUserId(String userId)
    {
        setUserId((userId == null) ? null : AMQShortString.valueOf(userId));
    }

    public synchronized void setUserId(AMQShortString userId)
    {
        if(userId == null)
        {
            _propertyFlags &= (~USER_ID_MASK);
        }
        else
        {
            _propertyFlags |= USER_ID_MASK;
        }
        _userId = userId;
        nullEncodedForm();
    }

    public String getAppIdAsString()
    {
        return (_appId == null) ? null : _appId.toString();
    }

    public AMQShortString getAppId()
    {
        return _appId;
    }

    public void setAppId(String appId)
    {
        setAppId((appId == null) ? null : AMQShortString.valueOf(appId));
    }

    public synchronized void setAppId(AMQShortString appId)
    {
        if(appId == null)
        {
            _propertyFlags &= (~APPLICATION_ID_MASK);
        }
        else
        {
            _propertyFlags |= APPLICATION_ID_MASK;
        }
        _appId = appId;
        nullEncodedForm();
    }

    public String getClusterIdAsString()
    {
        return (_clusterId == null) ? null : _clusterId.toString();
    }

    public AMQShortString getClusterId()
    {
        return _clusterId;
    }

    public void setClusterId(String clusterId)
    {
        setClusterId((clusterId == null) ? null : AMQShortString.valueOf(clusterId));
    }

    public synchronized void setClusterId(AMQShortString clusterId)
    {
        if(clusterId == null)
        {
            _propertyFlags &= (~CLUSTER_ID_MASK);
        }
        else
        {
            _propertyFlags |= CLUSTER_ID_MASK;
        }
        _clusterId = clusterId;
        nullEncodedForm();
    }

    @Override
    public String toString()
    {
        return "reply-to = " + _replyTo + ",propertyFlags = " + _propertyFlags + ",ApplicationID = " + _appId
            + ",ClusterID = " + _clusterId + ",UserId = " + _userId + ",JMSMessageID = " + _messageId
            + ",JMSCorrelationID = " + _correlationId + ",JMSDeliveryMode = " + _deliveryMode + ",JMSExpiration = "
            + _expiration + ",JMSPriority = " + _priority + ",JMSTimestamp = " + _timestamp + ",JMSType = " + _type
            + ",contentType = " + _contentType;
    }

    private synchronized boolean useEncodedForm()
    {
        return _encodedForm != null;
    }


    public synchronized void dispose()
    {
        nullEncodedForm();
        if (_headers != null)
        {
            _headers.dispose();
        }
    }

    public synchronized void clearEncodedForm()
    {
        nullEncodedForm();
        if (_headers != null)
        {
            final FieldTable headers = FieldTable.convertToDecodedFieldTable(_headers);
            _headers.dispose();
            _headers = headers;
        }
    }

    private synchronized void nullEncodedForm()
    {
        if(_encodedForm != null)
        {
            _encodedForm.dispose();
            _encodedForm = null;
        }
    }

    synchronized void reallocate()
    {
        if (_encodedForm != null)
        {
            _encodedForm = QpidByteBuffer.reallocateIfNecessary(_encodedForm);

            if (_headers != null)
            {
                _headers.dispose();
                _headers = null;
            }
            rebuildHeadersIfHeadersMaskIsSet();
        }
    }

    private void rebuildHeadersIfHeadersMaskIsSet()
    {
        try (QpidByteBuffer byteBuffer = _encodedForm.slice())
        {
            if ((_propertyFlags & (CONTENT_TYPE_MASK)) != 0)
            {
                int contentTypeSize = byteBuffer.getUnsignedByte();
                byteBuffer.position(byteBuffer.position() + contentTypeSize);
            }

            if ((_propertyFlags & ENCODING_MASK) != 0)
            {
                int encodingSize = byteBuffer.getUnsignedByte();
                byteBuffer.position(byteBuffer.position() + encodingSize);
            }

            if ((_propertyFlags & HEADERS_MASK) != 0)
            {
                _headers = EncodingUtils.readFieldTable(byteBuffer);
            }
        }
    }

    public synchronized void validate()
    {
        if (_headers != null)
        {
            _headers.validate();
        }
    }

    public boolean checkValid()
    {
        try
        {
            validate();
            return true;
        }
        catch (RuntimeException e)
        {
            return false;
        }
    }
}
