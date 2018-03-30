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
package org.apache.qpid.server.protocol.v1_0.framing;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.ConnectionHandler;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedShort;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.test.utils.UnitTestBase;

public class FrameHandlerTest extends UnitTestBase
{
    private static final int MAX_FRAME_SIZE = 4096;
    private ValueHandler _valueHandler;

    @Before
    public void setUp() throws Exception
    {
        _valueHandler = new ValueHandler(AMQPDescribedTypeRegistry.newInstance());
    }

    @Test
    public void testSaslHeartbeat()
    {
        ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
        when(connectionHandler.getMaxFrameSize()).thenReturn(MAX_FRAME_SIZE);
        FrameHandler handler = new  FrameHandler(_valueHandler, connectionHandler, true);

        QpidByteBuffer body = QpidByteBuffer.allocate(false, 8);
        body.putInt(8);    // size
        body.put((byte)2); // DOFF
        body.put((byte)1); // AMQP Frame Type
        body.putShort(UnsignedShort.ZERO.shortValue());  // channel
        body.flip();

        handler.parse(body);

        ArgumentCaptor<Error> errorCaptor = ArgumentCaptor.forClass(Error.class);
        verify(connectionHandler).handleError(errorCaptor.capture());

        Error error = errorCaptor.getValue();
        assertNotNull(error);
        assertEquals(ConnectionError.FRAMING_ERROR, error.getCondition());
        assertEquals("Empty (heartbeat) frames are not permitted during SASL negotiation",
                            error.getDescription());
    }

    @Test
    public void testOversizedFrame()
    {
        ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
        when(connectionHandler.getMaxFrameSize()).thenReturn(MAX_FRAME_SIZE);
        FrameHandler handler = new FrameHandler(_valueHandler, connectionHandler, true);

        QpidByteBuffer body = QpidByteBuffer.allocate(false, MAX_FRAME_SIZE + 8);
        body.putInt(body.capacity()); // size
        body.put((byte) 2); // DOFF
        body.put((byte) 1); // AMQP Frame Type
        body.putShort(UnsignedShort.ZERO.shortValue()); // channel
        body.position(body.capacity());
        body.flip();

        handler.parse(body);

        ArgumentCaptor<Error> errorCaptor = ArgumentCaptor.forClass(Error.class);
        verify(connectionHandler).handleError(errorCaptor.capture());

        Error error = errorCaptor.getValue();
        assertNotNull(error);
        assertEquals(ConnectionError.FRAMING_ERROR, error.getCondition());
        assertEquals(String.format("specified frame size %s larger than maximum frame header size %s",
                                          body.capacity(),
                                          MAX_FRAME_SIZE), error.getDescription());
    }
}
