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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoder;
import org.apache.qpid.server.protocol.v1_0.messaging.SectionEncoderImpl;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.messaging.AmqpValue;
import org.apache.qpid.server.protocol.v1_0.type.messaging.EncodingRetainingSection;

public class MessageEncoder
{
    private static final AMQPDescribedTypeRegistry
            AMQP_DESCRIBED_TYPE_REGISTRY = AMQPDescribedTypeRegistry.newInstance()
                                                                    .registerTransportLayer()
                                                                    .registerMessagingLayer();
    private List<String> _data = new LinkedList<>();
    private SectionEncoder _encoder = new SectionEncoderImpl(AMQP_DESCRIBED_TYPE_REGISTRY);

    public void addData(final String data)
    {
        _data.add(data);
    }

    public List<QpidByteBuffer> getPayload()
    {
        List<QpidByteBuffer> payload = new ArrayList<>();
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
            payload.addAll(section.getEncodedForm());
            section.dispose();
        }

        return payload;
    }
}
