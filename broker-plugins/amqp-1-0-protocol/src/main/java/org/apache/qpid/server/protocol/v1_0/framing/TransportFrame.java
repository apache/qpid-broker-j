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
 */
package org.apache.qpid.server.protocol.v1_0.framing;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.FrameWriter;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;

public final class TransportFrame extends AMQFrame<FrameBody>
{
    public static final TransportFrame HEARTBEAT = new TransportFrame(0, null);

    private final int _channel;

    /**
     * Base Transport frame.
     * @param channel channel
     * @param frameBody frame body (may be null for heartbeat-like frames)
     */
    public TransportFrame(int channel, FrameBody frameBody)
    {
        super(frameBody);
        _channel = channel;
    }

    /**
     * Base Transport frame.
     * @param channel channel
     * @param frameBody frame body (may be null for heartbeat-like frames)
     * @param frameBodyWriter  optional cached writer for {@code frameBody}.
     * Use only when the writer was resolved for this exact instance/state of {@code frameBody}
     * and the body will not be mutated afterwards; otherwise pass null and let {@link FrameWriter}
     * resolve the writer from the registry.
     */
    public TransportFrame(int channel, FrameBody frameBody, ValueWriter<FrameBody> frameBodyWriter)
    {
        super(frameBody, null, frameBodyWriter);
        _channel = channel;
    }

    /**
     * Base Transport frame.
     * @param channel channel
     * @param frameBody frame body (may be null for heartbeat-like frames)
     * @param payload optional payload; remaining bytes will be written
     */
    public TransportFrame(int channel, FrameBody frameBody, QpidByteBuffer payload)
    {
        super(frameBody, payload);
        _channel = channel;
    }

    /**
     * Base Transport frame.
     * @param channel channel
     * @param frameBody frame body (may be null for heartbeat-like frames)
     * @param payload optional payload; remaining bytes will be written
     * @param frameBodyWriter  optional cached writer for {@code frameBody}.
     * Use only when the writer was resolved for this exact instance/state of {@code frameBody}
     * and the body will not be mutated afterwards; otherwise pass null and let {@link FrameWriter}
     * resolve the writer from the registry.
     */
    public TransportFrame(int channel, FrameBody frameBody, QpidByteBuffer payload, ValueWriter<FrameBody> frameBodyWriter)
    {
        super(frameBody, payload, frameBodyWriter);
        _channel = channel;
    }

    @Override public int getChannel()
    {
        return _channel;
    }

    @Override public byte getFrameType()
    {
        return (byte)0;
    }
}
