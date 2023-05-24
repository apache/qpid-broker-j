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

package org.apache.qpid.server.protocol.v0_8;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.test.utils.UnitTestBase;

class MessageMetaDataFactoryTest extends UnitTestBase
{
    private static final String CONTENT_TYPE = "content/type";

    private final long _arrivalTime = System.currentTimeMillis();
    private final AMQShortString _routingKey = AMQShortString.valueOf("routingkey");
    private final AMQShortString _exchange = AMQShortString.valueOf("exch");

    private MessageMetaData _mmd;

    @BeforeEach
    void setUp() throws Exception
    {
        _mmd = createTestMessageMetaData();
    }

    @AfterEach
    void tearDown()
    {
        if (_mmd != null)
        {
            _mmd.dispose();
        }
    }

    @Test
    void unmarshalFromSingleBuffer()
    {
        try (final QpidByteBuffer qpidByteBuffer = QpidByteBuffer.allocateDirect(_mmd.getStorableSize()))
        {
            _mmd.writeToBuffer(qpidByteBuffer);
            qpidByteBuffer.flip();

            final MessageMetaData recreated = MessageMetaData.FACTORY.createMetaData(qpidByteBuffer);

            assertEquals(_arrivalTime, recreated.getArrivalTime(), "Unexpected arrival time");
            assertEquals(_routingKey, recreated.getMessagePublishInfo().getRoutingKey(), "Unexpected routing key");

            assertEquals(CONTENT_TYPE, recreated.getContentHeaderBody().getProperties().getContentTypeAsString(),
                    "Unexpected content type");
            recreated.dispose();
        }
    }

    private MessageMetaData createTestMessageMetaData()
    {
        final MessagePublishInfo publishBody = new MessagePublishInfo(_exchange, false, false, _routingKey);
        final BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        props.setContentType(CONTENT_TYPE);
        final ContentHeaderBody contentHeaderBody = new ContentHeaderBody(props);

        return new MessageMetaData(publishBody, contentHeaderBody, _arrivalTime);
    }
}