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
package org.apache.qpid.server.message;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.server.consumer.ConsumerImpl;

public class MessageInfoFacade implements MessageInfo
{
    private final MessageInstance _instance;
    private final boolean _includeHeaders;

    public MessageInfoFacade(final MessageInstance instance, final boolean includeHeaders)
    {
        _instance = instance;
        _includeHeaders = includeHeaders;
    }


    @Override
    public long getId()
    {
        return _instance.getMessage().getMessageNumber();
    }

    @Override
    public long getSize()
    {
        return _instance.getMessage().getSize();
    }

    @Override
    public int getDeliveryCount()
    {
        return _instance.getDeliveryCount();
    }

    @Override
    public String getState()
    {
        return _instance.isAvailable()
                ? "Available"
                : _instance.isAcquired()
                        ? "Acquired"
                        : "";
    }

    @Override
    public String getDeliveredTo()
    {
        final ConsumerImpl deliveredConsumer = _instance.getDeliveredConsumer();
        return deliveredConsumer == null ? null : String.valueOf(deliveredConsumer.getConsumerNumber());
    }

    @Override
    public long getArrivalTime()
    {
        return _instance.getMessage().getArrivalTime();
    }

    @Override
    public boolean isPersistent()
    {
        return _instance.getMessage().isPersistent();
    }

    @Override
    public String getMessageId()
    {
        return _instance.getMessage().getMessageHeader().getMessageId();
    }

    @Override
    public Long getExpirationTime()
    {
        long expiration = _instance.getMessage().getMessageHeader().getExpiration();
        return expiration == 0l ? null : expiration;
    }

    @Override
    public String getApplicationId()
    {
        return _instance.getMessage().getMessageHeader().getAppId();
    }

    @Override
    public String getCorrelationId()
    {
        return _instance.getMessage().getMessageHeader().getCorrelationId();
    }

    @Override
    public String getEncoding()
    {
        return _instance.getMessage().getMessageHeader().getEncoding();
    }

    @Override
    public String getMimeType()
    {
        return _instance.getMessage().getMessageHeader().getMimeType();
    }

    @Override
    public int getPriority()
    {
        return _instance.getMessage().getMessageHeader().getPriority();
    }

    @Override
    public String getReplyTo()
    {
        return _instance.getMessage().getMessageHeader().getReplyTo();
    }

    @Override
    public Long getTimestamp()
    {
        long timestamp = _instance.getMessage().getMessageHeader().getTimestamp();
        return timestamp == 0l ? null : timestamp;
    }

    @Override
    public String getType()
    {
        return _instance.getMessage().getMessageHeader().getType();
    }

    @Override
    public String getUserId()
    {
        return _instance.getMessage().getMessageHeader().getUserId();
    }

    @Override
    public Map<String, Object> getHeaders()
    {
        if(_includeHeaders)
        {
            final Collection<String> headerNames =
                    _instance.getMessage().getMessageHeader().getHeaderNames();
            return new AbstractMap<String, Object>()
            {
                @Override
                public Set<Entry<String, Object>> entrySet()
                {
                    return new AbstractSet<Entry<String, Object>>()
                    {
                        @Override
                        public Iterator<Entry<String, Object>> iterator()
                        {
                            final Iterator<String> nameIterator = headerNames.iterator();

                            return new Iterator<Entry<String, Object>>()
                            {
                                @Override
                                public boolean hasNext()
                                {
                                    return nameIterator.hasNext();
                                }

                                @Override
                                public Entry<String, Object> next()
                                {
                                    final String headerName = nameIterator.next();
                                    final Object value =
                                            _instance.getMessage().getMessageHeader().getHeader(headerName);
                                    return new Entry<String, Object>()
                                    {
                                        @Override
                                        public String getKey()
                                        {
                                            return headerName;
                                        }

                                        @Override
                                        public Object getValue()
                                        {
                                            return value;
                                        }

                                        @Override
                                        public Object setValue(final Object value)
                                        {
                                            throw new UnsupportedOperationException();
                                        }
                                    };
                                }

                                @Override
                                public void remove()
                                {
                                    nameIterator.remove();
                                }
                            };
                        }

                        @Override
                        public int size()
                        {
                            return headerNames.size();
                        }
                    };
                }
            };
        }
        else
        {
            return null;
        }
    }
}
