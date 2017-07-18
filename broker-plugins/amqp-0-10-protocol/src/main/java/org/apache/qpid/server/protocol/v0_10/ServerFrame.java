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
package org.apache.qpid.server.protocol.v0_10;

import static org.apache.qpid.server.transport.util.Functions.str;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_10.transport.SegmentType;


public final class ServerFrame
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerFrame.class);

    public static final int HEADER_SIZE = 12;

    // XXX: enums?
    public static final byte L1 = 0;
    public static final byte L2 = 1;
    public static final byte L3 = 2;
    public static final byte L4 = 3;

    public static final byte FIRST_SEG = 0x8;
    public static final byte LAST_SEG = 0x4;
    public static final byte FIRST_FRAME = 0x2;
    public static final byte LAST_FRAME = 0x1;

    final private byte flags;
    final private SegmentType type;
    final private byte track;
    final private int channel;
    final private QpidByteBuffer body;

    public ServerFrame(byte flags, SegmentType type, byte track, int channel,
                       QpidByteBuffer body)
    {
        this.flags = flags;
        this.type = type;
        this.track = track;
        this.channel = channel;
        this.body = body;
    }

    public QpidByteBuffer getBody()
    {
        return body;
    }

    public byte getFlags()
    {
        return flags;
    }

    public int getChannel()
    {
        return channel;
    }

    public int getSize()
    {
        return body.remaining();
    }

    public SegmentType getType()
    {
        return type;
    }

    public byte getTrack()
    {
        return track;
    }

    private boolean flag(byte mask)
    {
        return (flags & mask) != 0;
    }

    public boolean isFirstSegment()
    {
        return flag(FIRST_SEG);
    }

    public boolean isLastSegment()
    {
        return flag(LAST_SEG);
    }

    public boolean isFirstFrame()
    {
        return flag(FIRST_FRAME);
    }

    public boolean isLastFrame()
    {
        return flag(LAST_FRAME);
    }

    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder();

        str.append(String.format
                   ("[%05d %05d %1d %s %d%d%d%d] ", getChannel(), getSize(),
                    getTrack(), getType(),
                    isFirstSegment() ? 1 : 0, isLastSegment() ? 1 : 0,
                    isFirstFrame() ? 1 : 0, isLastFrame() ? 1 : 0));

        str.append(str(body));

        return str.toString();
    }

}
