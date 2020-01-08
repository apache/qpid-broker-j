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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.EncodingUtils;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.test.utils.UnitTestBase;

public class BasicContentHeaderPropertiesTest extends UnitTestBase
{

    private BasicContentHeaderProperties _testProperties;
    private FieldTable _testTable;
    private String _testString = "This is a test string";

    /**
     * Currently only test setting/getting String, int and boolean props
     */
    public BasicContentHeaderPropertiesTest()
    {
        _testProperties = new BasicContentHeaderProperties();
    }

    @Before
    public void setUp() throws Exception
    {
        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put("TestString", _testString);
        headers.put("Testint", Integer.MAX_VALUE);
        _testTable = FieldTableFactory.createFieldTable(headers);

        _testProperties = new BasicContentHeaderProperties();
        _testProperties.setHeaders(_testTable);
    }

    @Test
    public void testGetPropertyListSize()
    {
        //needs a better test but at least we're exercising the code !
         // FT length is encoded in an int
        int expectedSize = EncodingUtils.encodedIntegerLength();

        expectedSize += EncodingUtils.encodedShortStringLength("TestInt");
        // 1 is for the Encoding Letter. here an 'i'
        expectedSize += 1 + EncodingUtils.encodedIntegerLength();

        expectedSize += EncodingUtils.encodedShortStringLength("TestString");
        // 1 is for the Encoding Letter. here an 'S'
        expectedSize += 1 + EncodingUtils.encodedLongStringLength(_testString);


        int size = _testProperties.getPropertyListSize();

        assertEquals((long) expectedSize, (long) size);
    }

    @Test
    public void testGetSetPropertyFlags()
    {
        _testProperties.setPropertyFlags(99);
        assertEquals((long) 99, (long) _testProperties.getPropertyFlags());
    }

    @Test
    public void testPopulatePropertiesFromBuffer() throws Exception
    {
        QpidByteBuffer buf = QpidByteBuffer.wrap(new byte[300]);
        _testProperties.dispose();
        _testProperties = new BasicContentHeaderProperties(buf, 99, 99);
    }

    @Test
    public void testSetGetContentType()
    {
        String contentType = "contentType";
        _testProperties.setContentType(contentType);
        assertEquals(contentType, _testProperties.getContentTypeAsString());
    }

    @Test
    public void testSetGetEncoding()
    {
        String encoding = "encoding";
        _testProperties.setEncoding(encoding);
        assertEquals(encoding, _testProperties.getEncodingAsString());
    }

    @Test
    public void testSetGetHeaders()
    {
        _testProperties.setHeaders(_testTable);
        assertEquals(FieldTable.convertToMap(_testTable), _testProperties.getHeadersAsMap());
    }

    @Test
    public void testSetGetDeliveryMode()
    {
        byte deliveryMode = 1;
        _testProperties.setDeliveryMode(deliveryMode);
        assertEquals((long) deliveryMode, (long) _testProperties.getDeliveryMode());
    }

    @Test
    public void testSetGetPriority()
    {
        byte priority = 1;
        _testProperties.setPriority(priority);
        assertEquals((long) priority, (long) _testProperties.getPriority());
    }

    @Test
    public void testSetGetCorrelationId()
    {
        String correlationId = "correlationId";
        _testProperties.setCorrelationId(correlationId);
        assertEquals(correlationId, _testProperties.getCorrelationIdAsString());
    }

    @Test
    public void testSetGetReplyTo()
    {
        String replyTo = "replyTo";
        _testProperties.setReplyTo(replyTo);
        assertEquals(replyTo, _testProperties.getReplyToAsString());
    }

    @Test
    public void testSetGetExpiration()
    {
        long expiration = 999999999;
        _testProperties.setExpiration(expiration);
        assertEquals(expiration, _testProperties.getExpiration());
        expiration = 0l;
        _testProperties.setExpiration(expiration);
        assertEquals(expiration, _testProperties.getExpiration());
    }

    @Test
    public void testSetGetMessageId()
    {
        String messageId = "messageId";
        _testProperties.setMessageId(messageId);
        assertEquals(messageId, _testProperties.getMessageIdAsString());
    }

    @Test
    public void testSetGetTimestamp()
    {
        long timestamp = System.currentTimeMillis();
        _testProperties.setTimestamp(timestamp);
        assertEquals(timestamp, _testProperties.getTimestamp());
    }

    @Test
    public void testSetGetType()
    {
        String type = "type";
        _testProperties.setType(type);
        assertEquals(type, _testProperties.getTypeAsString());
    }

    @Test
    public void testSetGetUserId()
    {
        String userId = "userId";
        _testProperties.setUserId(userId);
        assertEquals(userId, _testProperties.getUserIdAsString());
    }

    @Test
    public void testSetGetAppId()
    {
        String appId = "appId";
        _testProperties.setAppId(appId);
        assertEquals(appId, _testProperties.getAppIdAsString());
    }

    @Test
    public void testSetGetClusterId()
    {
        String clusterId = "clusterId";
        _testProperties.setClusterId(clusterId);
        assertEquals(clusterId, _testProperties.getClusterIdAsString());
    }

    private static final int BUFFER_SIZE = 1024 * 10;
    private static final int POOL_SIZE = 20;
    private static final double SPARSITY_FRACTION = 0.5;

    @Test
    public void testReallocate() throws Exception
    {
        try
        {
            QpidByteBuffer.deinitialisePool();
            QpidByteBuffer.initialisePool(BUFFER_SIZE, POOL_SIZE, SPARSITY_FRACTION);
            try (QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(BUFFER_SIZE))
            {
                // set some test fields
                _testProperties.setContentType("text/plain");
                _testProperties.setUserId("test");
                final Map<String, Object> headers = _testProperties.getHeadersAsMap();
                final int propertyListSize = _testProperties.getPropertyListSize();
                final int flags = _testProperties.getPropertyFlags();

                // write at the buffer end
                final int pos = BUFFER_SIZE - propertyListSize * 2;
                buffer.position(pos);

                try (QpidByteBuffer propertiesBuffer = buffer.view(0, propertyListSize))
                {
                    _testProperties.writePropertyListPayload(propertiesBuffer);
                    propertiesBuffer.flip();

                    BasicContentHeaderProperties testProperties = new BasicContentHeaderProperties(propertiesBuffer, flags, propertyListSize);
                    final Map<String, Object> headersBeforeReallocation = testProperties.getHeadersAsMap();
                    assertEquals("Unexpected headers", headers, headersBeforeReallocation);

                    buffer.dispose();

                    assertTrue("Properties buffer should be sparse", propertiesBuffer.isSparse());
                    testProperties.reallocate();

                    final Map<String, Object> headersAfterReallocation = testProperties.getHeadersAsMap();
                    assertEquals("Unexpected headers after re-allocation", headers, headersAfterReallocation);
                }
            }
        }
        finally
        {
            QpidByteBuffer.deinitialisePool();
        }
    }
}
