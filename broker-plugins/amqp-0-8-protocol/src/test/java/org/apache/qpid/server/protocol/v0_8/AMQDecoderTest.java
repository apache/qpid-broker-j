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
package org.apache.qpid.server.protocol.v0_8;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQProtocolVersionException;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.FrameCreatingMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.HeartbeatBody;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.test.utils.UnitTestBase;

public class AMQDecoderTest extends UnitTestBase
{

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
    private ClientDecoder _decoder;
    private FrameCreatingMethodProcessor _methodProcessor;


    @Before
    public void setUp() throws Exception
    {
        _methodProcessor = new FrameCreatingMethodProcessor(ProtocolVersion.v0_91);
        _decoder = new ClientDecoder(_methodProcessor);
    }


    private ByteBuffer getHeartbeatBodyBuffer() throws IOException
    {
        TestSender sender = new TestSender();
        HeartbeatBody.FRAME.writePayload(sender);
        return combine(sender.getSentBuffers());
    }

    @Test
    public void testSingleFrameDecode() throws AMQProtocolVersionException, AMQFrameDecodingException, IOException
    {
        ByteBuffer msg = getHeartbeatBodyBuffer();
        _decoder.decodeBuffer(msg);
        List<AMQDataBlock> frames = _methodProcessor.getProcessedMethods();
        if (frames.get(0) instanceof AMQFrame)
        {
            assertEquals((long) HeartbeatBody.FRAME.getBodyFrame().getFrameType(),
                                (long) ((AMQFrame) frames.get(0)).getBodyFrame().getFrameType());

        }
        else
        {
            fail("decode was not a frame");
        }
    }


    @Test
    public void testContentHeaderPropertiesFrame() throws AMQProtocolVersionException, AMQFrameDecodingException, IOException
    {
        final BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        Map<String, Object> headersMap = new LinkedHashMap<>();
        headersMap.put("hello","world");
        headersMap.put("1+1=",2);
        final FieldTable table = FieldTableFactory.createFieldTable(headersMap);
        props.setHeaders(table);
        final AMQBody body = new ContentHeaderBody(props);
        AMQFrame frame = new AMQFrame(1, body);
        TestSender sender = new TestSender();
        frame.writePayload(sender);
        ByteBuffer msg = combine(sender.getSentBuffers());

        _decoder.decodeBuffer(msg);
        List<AMQDataBlock> frames = _methodProcessor.getProcessedMethods();
        AMQDataBlock firstFrame = frames.get(0);
        if (firstFrame instanceof AMQFrame)
        {
            assertEquals((long) ContentHeaderBody.TYPE,
                                (long) ((AMQFrame) firstFrame).getBodyFrame().getFrameType());
            BasicContentHeaderProperties decodedProps = ((ContentHeaderBody)((AMQFrame)firstFrame).getBodyFrame()).getProperties();
            final Map<String, Object> headers = decodedProps.getHeadersAsMap();
            assertEquals("world", headers.get("hello"));
        }
        else
        {
            fail("decode was not a frame");
        }
    }


    @Test
    public void testDecodeWithManyBuffers() throws AMQProtocolVersionException, AMQFrameDecodingException, IOException
    {
        Random random = new Random();
        final byte[] payload = new byte[2048];
        random.nextBytes(payload);
        final AMQBody body = new ContentBody(ByteBuffer.wrap(payload));
        AMQFrame frame = new AMQFrame(1, body);
        TestSender sender = new TestSender();
        frame.writePayload(sender);
        ByteBuffer allData = combine(sender.getSentBuffers());


        for(int i = 0 ; i < allData.remaining(); i++)
        {
            byte[] minibuf = new byte[1];
            minibuf[0] = allData.get(i);
            _decoder.decodeBuffer(ByteBuffer.wrap(minibuf));
        }

        List<AMQDataBlock> frames = _methodProcessor.getProcessedMethods();
        if (frames.get(0) instanceof AMQFrame)
        {
            assertEquals((long) ContentBody.TYPE,
                                (long) ((AMQFrame) frames.get(0)).getBodyFrame().getFrameType());
            ContentBody decodedBody = (ContentBody) ((AMQFrame) frames.get(0)).getBodyFrame();
            byte[] bodyBytes;
            try (QpidByteBuffer payloadBuffer = decodedBody.getPayload())
            {
                bodyBytes = new byte[payloadBuffer.remaining()];
                payloadBuffer.get(bodyBytes);
            }
            assertTrue("Body was corrupted", Arrays.equals(payload, bodyBytes));
        }
        else
        {
            fail("decode was not a frame");
        }
    }

    @Test
    public void testPartialFrameDecode() throws AMQProtocolVersionException, AMQFrameDecodingException, IOException
    {
        ByteBuffer msg = getHeartbeatBodyBuffer();
        ByteBuffer msgA = msg.slice();
        int msgbPos = msg.remaining() / 2;
        int msgaLimit = msg.remaining() - msgbPos;
        msgA.limit(msgaLimit);
        msg.position(msgbPos);
        ByteBuffer msgB = msg.slice();

        _decoder.decodeBuffer(msgA);
        List<AMQDataBlock> frames = _methodProcessor.getProcessedMethods();
        assertEquals((long) 0, (long) frames.size());

        _decoder.decodeBuffer(msgB);
        assertEquals((long) 1, (long) frames.size());
        if (frames.get(0) instanceof AMQFrame)
        {
            assertEquals((long) HeartbeatBody.FRAME.getBodyFrame().getFrameType(), (long) ((AMQFrame) frames.get
                    (0)).getBodyFrame().getFrameType());
        }
        else
        {
            fail("decode was not a frame");
        }
    }

    @Test
    public void testMultipleFrameDecode() throws AMQProtocolVersionException, AMQFrameDecodingException, IOException
    {
        ByteBuffer msgA = getHeartbeatBodyBuffer();
        ByteBuffer msgB = getHeartbeatBodyBuffer();
        ByteBuffer msg = ByteBuffer.allocate(msgA.remaining() + msgB.remaining());
        msg.put(msgA);
        msg.put(msgB);
        msg.flip();
        _decoder.decodeBuffer(msg);
        List<AMQDataBlock> frames = _methodProcessor.getProcessedMethods();
        assertEquals((long) 2, (long) frames.size());
        for (AMQDataBlock frame : frames)
        {
            if (frame instanceof AMQFrame)
            {
                assertEquals((long) HeartbeatBody.FRAME.getBodyFrame().getFrameType(),
                                    (long) ((AMQFrame) frame).getBodyFrame().getFrameType());
            }
            else
            {
                fail("decode was not a frame");
            }
        }
    }

    @Test
    public void testMultiplePartialFrameDecode() throws AMQProtocolVersionException, AMQFrameDecodingException, IOException
    {
        ByteBuffer msgA = getHeartbeatBodyBuffer();
        ByteBuffer msgB = getHeartbeatBodyBuffer();
        ByteBuffer msgC = getHeartbeatBodyBuffer();

        ByteBuffer sliceA = ByteBuffer.allocate(msgA.remaining() + msgB.remaining() / 2);
        sliceA.put(msgA);
        int limit = msgB.limit();
        int pos = msgB.remaining() / 2;
        msgB.limit(pos);
        sliceA.put(msgB);
        sliceA.flip();
        msgB.limit(limit);
        msgB.position(pos);

        ByteBuffer sliceB = ByteBuffer.allocate(msgB.remaining() + pos);
        sliceB.put(msgB);
        msgC.limit(pos);
        sliceB.put(msgC);
        sliceB.flip();
        msgC.limit(limit);

        _decoder.decodeBuffer(sliceA);
        List<AMQDataBlock> frames = _methodProcessor.getProcessedMethods();
        assertEquals((long) 1, (long) frames.size());
        frames.clear();
        _decoder.decodeBuffer(sliceB);
        assertEquals((long) 1, (long) frames.size());
        frames.clear();
        _decoder.decodeBuffer(msgC);
        assertEquals((long) 1, (long) frames.size());
        for (AMQDataBlock frame : frames)
        {
            if (frame instanceof AMQFrame)
            {
                assertEquals((long) HeartbeatBody.FRAME.getBodyFrame().getFrameType(),
                                    (long) ((AMQFrame) frame).getBodyFrame().getFrameType());
            }
            else
            {
                fail("decode was not a frame");
            }
        }
    }

    private static class TestSender implements ByteBufferSender
    {

        private final Collection<QpidByteBuffer> _sentBuffers = new ArrayList<>();
        @Override
        public boolean isDirectBufferPreferred()
        {
            return false;
        }

        @Override
        public void send(final QpidByteBuffer msg)
        {
            _sentBuffers.add(msg.duplicate());
            msg.position(msg.limit());
        }

        @Override
        public void flush()
        {

        }

        @Override
        public void close()
        {

        }

        public Collection<QpidByteBuffer> getSentBuffers()
        {
            return _sentBuffers;
        }

    }

    private static ByteBuffer combine(Collection<QpidByteBuffer> bufs)
    {
        if(bufs == null || bufs.isEmpty())
        {
            return EMPTY_BYTE_BUFFER;
        }
        else
        {
            int size = 0;
            boolean isDirect = false;
            for(QpidByteBuffer buf : bufs)
            {
                size += buf.remaining();
                isDirect = isDirect || buf.isDirect();
            }
            ByteBuffer combined = isDirect ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);

            for(QpidByteBuffer buf : bufs)
            {
                buf.copyTo(combined);
            }
            combined.flip();
            return combined;
        }
    }
}
