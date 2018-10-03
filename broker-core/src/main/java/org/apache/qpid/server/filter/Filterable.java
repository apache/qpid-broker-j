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
package org.apache.qpid.server.filter;

import org.apache.qpid.server.filter.FilterableMessage;
import org.apache.qpid.server.message.AMQMessageHeader;
import org.apache.qpid.server.message.InstanceProperties;
import org.apache.qpid.server.message.ServerMessage;

public interface Filterable extends FilterableMessage
{
    AMQMessageHeader getMessageHeader();

    @Override
    boolean isPersistent();

    @Override
    boolean isRedelivered();

    Object getConnectionReference();

    long getMessageNumber();

    long getArrivalTime();

    class Factory
    {

        public static Filterable newInstance(final ServerMessage message, final InstanceProperties properties)
        {
            return new Filterable()
            {

                private final AMQMessageHeader _messageHeader = message.getMessageHeader();

                @Override
                public AMQMessageHeader getMessageHeader()
                {
                    return _messageHeader;
                }

                @Override
                public boolean isPersistent()
                {
                    return Boolean.TRUE.equals(properties.getProperty(InstanceProperties.Property.PERSISTENT));
                }

                @Override
                public boolean isRedelivered()
                {
                    return Boolean.TRUE.equals(properties.getProperty(InstanceProperties.Property.REDELIVERED));
                }

                @Override
                public Object getConnectionReference()
                {
                    return message.getConnectionReference();
                }

                @Override
                public long getMessageNumber()
                {
                    return message.getMessageNumber();
                }

                @Override
                public long getArrivalTime()
                {
                    return message.getArrivalTime();
                }

                @Override
                public String getReplyTo()
                {
                    return _messageHeader.getReplyTo();
                }

                @Override
                public String getType()
                {
                    return _messageHeader.getType();
                }

                @Override
                public byte getPriority()
                {
                    return _messageHeader.getPriority();
                }

                @Override
                public String getMessageId()
                {
                    return _messageHeader.getMessageId();
                }

                @Override
                public long getTimestamp()
                {
                    return _messageHeader.getTimestamp();
                }

                @Override
                public String getCorrelationId()
                {
                    return _messageHeader.getCorrelationId();
                }

                @Override
                public long getExpiration()
                {
                    return _messageHeader.getExpiration();
                }

                @Override
                public Object getHeader(String name)
                {
                    return _messageHeader.getHeader(name);
                }

                @Override
                public String toString()
                {
                    StringBuilder builder = new StringBuilder();
                    builder.append(getClass().getName());
                    builder.append(" [messageNumber=");
                    builder.append(getMessageNumber());
                    if (getMessageId() != null)
                    {
                        builder.append(", id=");
                        builder.append(getMessageId());
                    }
                    builder.append("]");


                    return builder.toString();
                }
            };
        }
    }
}
