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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.bytebuffer.QpidByteBufferInputStream;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.test.utils.QpidTestCase;

public class MessageMetaDataFactoryTest extends QpidTestCase
{
    private static final String CONTENT_TYPE = "content/type";
    private final long _arrivalTime = System.currentTimeMillis();
    private final AMQShortString _routingKey = AMQShortString.valueOf("routingkey");
    private final AMQShortString _exchange = AMQShortString.valueOf("exch");
    private MessageMetaData _mmd;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mmd = createTestMessageMetaData();
    }

    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();
        if (_mmd != null)
        {
            _mmd.dispose();
        }
    }

    public void testUnmarshalFromSingleBuffer() throws Exception
    {
        try(QpidByteBuffer qpidByteBuffer = QpidByteBuffer.allocateDirect(_mmd.getStorableSize()))
        {
            _mmd.writeToBuffer(qpidByteBuffer);
            qpidByteBuffer.flip();

            MessageMetaData recreated = MessageMetaData.FACTORY.createMetaData(Collections.singletonList(qpidByteBuffer));

            assertEquals("Unexpected arrival time", _arrivalTime, recreated.getArrivalTime());
            assertEquals("Unexpected routing key", _routingKey, recreated.getMessagePublishInfo().getRoutingKey());
            assertEquals("Unexpected content type", CONTENT_TYPE, recreated.getContentHeaderBody().getProperties()
                                                                               .getContentTypeAsString());
            recreated.dispose();
        }
    }

    public void testUnmarshalFromMultipleBuffers() throws Exception
    {
        List<QpidByteBuffer> bufs = Collections.emptyList();
        try (QpidByteBuffer qpidByteBuffer = QpidByteBuffer.allocateDirect(_mmd.getStorableSize()))
        {
            _mmd.writeToBuffer(qpidByteBuffer);
            qpidByteBuffer.flip();

            bufs = splitIntoSmallerBuffers(qpidByteBuffer);

            final MessageMetaData recreated = MessageMetaData.FACTORY.createMetaData(bufs);
            assertEquals("Unexpected arrival time", _arrivalTime, recreated.getArrivalTime());
            assertEquals("Unexpected routing key", _routingKey, recreated.getMessagePublishInfo().getRoutingKey());
            assertEquals("Unexpected content type", CONTENT_TYPE, recreated.getContentHeaderBody().getProperties().getContentTypeAsString());
            recreated.dispose();
        }
        finally
        {
            for (final QpidByteBuffer buf : bufs)
            {
                buf.dispose();
            }
        }
    }

    private MessageMetaData createTestMessageMetaData()
    {
        final MessagePublishInfo publishBody = new MessagePublishInfo(_exchange,
                                                                      false,
                                                                      false,
                                                                      _routingKey);
        final BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        props.setContentType(CONTENT_TYPE);
        final ContentHeaderBody contentHeaderBody = new ContentHeaderBody(props);

        return new MessageMetaData(publishBody, contentHeaderBody, _arrivalTime);
    }

    private List<QpidByteBuffer> splitIntoSmallerBuffers(final QpidByteBuffer qpidByteBuffer) throws IOException
    {
        List<QpidByteBuffer> bufs = new ArrayList<>();
        try (InputStream stream = new QpidByteBufferInputStream(Collections.singletonList(qpidByteBuffer)))
        {
            byte[] transferBuf = new byte[3];
            int read = stream.read(transferBuf);
            while (read != -1)
            {
                QpidByteBuffer buf = QpidByteBuffer.allocateDirect(read);
                buf.put(transferBuf, 0, read);
                buf.flip();
                bufs.add(buf);
                read = stream.read(transferBuf);
            }
        }
        return bufs;
    }

}