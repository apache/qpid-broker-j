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

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.transport.ByteBufferSender;

public class HeartbeatBody implements AMQBody
{
    public static final byte TYPE = 8;
    public static final AMQFrame FRAME = new HeartbeatBody().toFrame();

    public HeartbeatBody()
    {

    }

    public HeartbeatBody(DataInputStream buffer, long size) throws IOException
    {
        if(size > 0)
        {
            //allow other implementations to have a payload, but ignore it:
            buffer.skip(size);
        }
    }

    @Override
    public byte getFrameType()
    {
        return TYPE;
    }

    @Override
    public int getSize()
    {
        return 0;//heartbeats we generate have no payload
    }

    @Override
    public long writePayload(final ByteBufferSender sender)
    {
        return 0l;
    }

    @Override
    public void handle(final int channelId, final AMQVersionAwareProtocolSession session)
            throws QpidException
    {
        session.heartbeatBodyReceived(channelId, this);
    }

    protected void populateFromBuffer(DataInputStream buffer, long size) throws AMQFrameDecodingException, IOException
    {
        if(size > 0)
        {
            //allow other implementations to have a payload, but ignore it:
            buffer.skip(size);
        }
    }

    public AMQFrame toFrame()
    {
        return new AMQFrame(0, this);
    }

    public static void process(final int channel,
                            final QpidByteBuffer in,
                            final MethodProcessor processor,
                            final long bodySize)
    {

        if(bodySize > 0)
        {
            in.position(in.position()+(int)bodySize);
        }
        processor.receiveHeartbeat();
    }
}
