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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.ApplicationProperties;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Header;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Properties;

public class MessageEncoder
{
    private Properties _properties;
    private Header _header;
    private List<String> _data = new LinkedList<>();
    private Map<String, Object> _applicationProperties;

    public void addData(final String data)
    {
        _data.add(data);
    }

    public void setHeader(Header header)
    {
        _header = header;
    }

    public void setProperties(final Properties properties)
    {
        _properties = properties;
    }

    public void setApplicationProperties(Map<String, Object> applicationProperties)
    {
        _applicationProperties = applicationProperties;
    }

    public QpidByteBuffer getPayload()
    {
        List<QpidByteBuffer> payload = new ArrayList<>();
        if (_header != null)
        {
            payload.add(_header.createEncodingRetainingSection().getEncodedForm());
        }

        if (_properties != null)
        {
            payload.add(_properties.createEncodingRetainingSection().getEncodedForm());
        }

        if (_applicationProperties != null)
        {
            payload.add(new ApplicationProperties(_applicationProperties).createEncodingRetainingSection().getEncodedForm());
        }

        if (_data.isEmpty())
        {
            throw new IllegalStateException("Message should have at least one data section");
        }

        List<EncodingRetainingSection<?>> dataSections = new ArrayList<>();
        if (_data.size() == 1)
        {
            AmqpValue amqpValue = new AmqpValue(_data.get(0));
            dataSections.add(amqpValue.createEncodingRetainingSection());
        }
        else
        {
            throw new UnsupportedOperationException("Unsupported yet");
        }

        for (EncodingRetainingSection<?> section: dataSections)
        {
            payload.add(section.getEncodedForm());
            section.dispose();
        }

        QpidByteBuffer combined = QpidByteBuffer.concatenate(payload);
        payload.forEach(QpidByteBuffer::dispose);
        return combined;
    }
}
