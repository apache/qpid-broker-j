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
package org.apache.qpid.tests.protocol.v0_8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.FrameCreatingMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.tests.protocol.HeaderResponse;
import org.apache.qpid.tests.protocol.InputDecoder;
import org.apache.qpid.tests.protocol.Response;

public class FrameDecoder implements InputDecoder
{
    private final ClientDecoder _clientDecoder;
    private final FrameCreatingMethodProcessor _methodProcessor;

    FrameDecoder(ProtocolVersion protocolVersion)
    {
        _methodProcessor = new FrameCreatingMethodProcessor(protocolVersion);
        _clientDecoder = new ClientDecoder(_methodProcessor);
    }

    @Override
    public Collection<Response<?>> decode(final ByteBuffer inputBuffer) throws Exception
    {
        _clientDecoder.decodeBuffer(inputBuffer);

        List<AMQDataBlock> receivedFrames = new ArrayList<>(_methodProcessor.getProcessedMethods());
        List<Response<?>> result = new ArrayList<>();

        for (AMQDataBlock frame : receivedFrames)
        {
            if (frame instanceof AMQFrame)
            {
                AMQFrame amqFrame = (AMQFrame) frame;
                if (amqFrame.getBodyFrame() instanceof ConnectionTuneBody)
                {
                    ConnectionTuneBody tuneBody = (ConnectionTuneBody) amqFrame.getBodyFrame();
                    _clientDecoder.setMaxFrameSize((int) tuneBody.getFrameMax());
                }

                result.add(new PerformativeResponse(amqFrame.getChannel(), amqFrame.getSize(), amqFrame.getBodyFrame()));
            }
            else if (frame instanceof ProtocolInitiation)
            {
                byte[] data =  new byte[(int) frame.getSize()];
                frame.writePayload(new ByteBufferSender()
                {
                    @Override
                    public boolean isDirectBufferPreferred()
                    {
                        return false;
                    }

                    @Override
                    public void send(final QpidByteBuffer msg)
                    {
                        msg.copyTo(data);
                    }

                    @Override
                    public void flush()
                    {

                    }

                    @Override
                    public void close()
                    {

                    }
                });

                result.add(new HeaderResponse(data));
            }
            else
            {
                throw new IllegalArgumentException(String.format("Unexpected data block received %s", frame));
            }
        }
        _methodProcessor.getProcessedMethods().removeAll(receivedFrames);
        return result;
    }

    ProtocolVersion getVersion()
    {
        return _methodProcessor.getProtocolVersion();
    }

}
