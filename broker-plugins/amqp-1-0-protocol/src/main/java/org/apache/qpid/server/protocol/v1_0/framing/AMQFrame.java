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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.FrameWriter;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;

public abstract class AMQFrame<T>
{
    private final T _frameBody;
    private final QpidByteBuffer _payload;
    private final ValueWriter<T> _frameBodyWriter;

    /**
     * Base AMQP frame.
     * @param frameBody frame body (may be null for heartbeat-like frames)
     */
    AMQFrame(T frameBody)
    {
        this(frameBody, null, null);
    }

    /**
     * Base AMQP frame.
     * <br>
     * {@code payload} (if present) is sent as-is using its current {@code position/limit}.
     * Do not mutate the payload buffer concurrently. Ensure it remains valid/alive until the frame
     * has been written to the transport.
     * <br>
     * @param frameBody frame body (may be null for heartbeat-like frames)
     * @param payload optional payload; remaining bytes will be written
     */
    protected AMQFrame(T frameBody, QpidByteBuffer payload)
    {
        this(frameBody, payload, null);
    }

    /**
     * Base AMQP frame.
     * <br>
     * IMPORTANT: Frames are expected to be effectively immutable after construction.
     * {@link FrameWriter} may use a cached {@link ValueWriter} supplied at construction time and will
     * not re-resolve it from the registry. If {@code frameBody} is mutated after the writer is cached,
     * the encoded size / encoding may become inconsistent.
     *<br>
     * {@code payload} (if present) is sent as-is using its current {@code position/limit}.
     * Do not mutate the payload buffer concurrently. Ensure it remains valid/alive until the frame
     * has been written to the transport.
     * <br>
     * @param frameBody frame body (may be null for heartbeat-like frames)
     * @param payload optional payload; remaining bytes will be written
     * @param frameBodyWriter optional cache hint; must match {@code frameBody} in its current state
     */
    protected AMQFrame(T frameBody, QpidByteBuffer payload, ValueWriter<T> frameBodyWriter)
    {
        _frameBody = frameBody;
        _payload = payload;
        _frameBodyWriter = frameBodyWriter;
    }

    public QpidByteBuffer getPayload()
    {
        return _payload;
    }

    abstract public int getChannel();

    abstract public byte getFrameType();

    public T getFrameBody()
    {
        return _frameBody;
    }

    public ValueWriter<T> getFrameBodyWriter()
    {
        return _frameBodyWriter;
    }

    @Override
    public String toString()
    {
        return "AMQFrame{" +
               "frameBody=" + _frameBody +
               '}';
    }
}
