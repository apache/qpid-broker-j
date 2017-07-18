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

package org.apache.qpid.server.protocol.v0_10.transport;


import java.util.LinkedHashMap;
import java.util.Map;


public final class ConnectionTune extends Method {

    public static final int TYPE = 261;

    @Override
    public final int getStructType() {
        return TYPE;
    }

    @Override
    public final int getSizeWidth() {
        return 0;
    }

    @Override
    public final int getPackWidth() {
        return 2;
    }

    @Override
    public final boolean hasPayload() {
        return false;
    }

    @Override
    public final byte getEncodedTrack() {
        return Frame.L1;
    }

    @Override
    public final boolean isConnectionControl()
    {
        return true;
    }

    private short packing_flags = 0;
    private int channelMax;
    private int maxFrameSize;
    private int heartbeatMin;
    private int heartbeatMax;


    public ConnectionTune() {}


    public ConnectionTune(int channelMax, int maxFrameSize, int heartbeatMin, int heartbeatMax, Option ... _options) {
        setChannelMax(channelMax);
        setMaxFrameSize(maxFrameSize);
        setHeartbeatMin(heartbeatMin);
        setHeartbeatMax(heartbeatMax);

        for (int i=0; i < _options.length; i++) {
            switch (_options[i]) {
            case SYNC: this.setSync(true); break;
            case BATCH: this.setBatch(true); break;
            case UNRELIABLE: this.setUnreliable(true); break;
            case NONE: break;
            default: throw new IllegalArgumentException("invalid option: " + _options[i]);
            }
        }

    }

    @Override
    public <C> void dispatch(C context, MethodDelegate<C> delegate) {
        delegate.connectionTune(context, this);
    }


    public final boolean hasChannelMax() {
        return (packing_flags & 256) != 0;
    }

    public final ConnectionTune clearChannelMax() {
        packing_flags &= ~256;
        this.channelMax = 0;
        setDirty(true);
        return this;
    }

    public final int getChannelMax() {
        return channelMax;
    }

    public final ConnectionTune setChannelMax(int value) {
        this.channelMax = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public final ConnectionTune channelMax(int value) {
        return setChannelMax(value);
    }

    public final boolean hasMaxFrameSize() {
        return (packing_flags & 512) != 0;
    }

    public final ConnectionTune clearMaxFrameSize() {
        packing_flags &= ~512;
        this.maxFrameSize = 0;
        setDirty(true);
        return this;
    }

    public final int getMaxFrameSize() {
        return maxFrameSize;
    }

    public final ConnectionTune setMaxFrameSize(int value) {
        this.maxFrameSize = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public final ConnectionTune maxFrameSize(int value) {
        return setMaxFrameSize(value);
    }

    public final boolean hasHeartbeatMin() {
        return (packing_flags & 1024) != 0;
    }

    public final ConnectionTune clearHeartbeatMin() {
        packing_flags &= ~1024;
        this.heartbeatMin = 0;
        setDirty(true);
        return this;
    }

    public final int getHeartbeatMin() {
        return heartbeatMin;
    }

    public final ConnectionTune setHeartbeatMin(int value) {
        this.heartbeatMin = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public final ConnectionTune heartbeatMin(int value) {
        return setHeartbeatMin(value);
    }

    public final boolean hasHeartbeatMax() {
        return (packing_flags & 2048) != 0;
    }

    public final ConnectionTune clearHeartbeatMax() {
        packing_flags &= ~2048;
        this.heartbeatMax = 0;
        setDirty(true);
        return this;
    }

    public final int getHeartbeatMax() {
        return heartbeatMax;
    }

    public final ConnectionTune setHeartbeatMax(int value) {
        this.heartbeatMax = value;
        packing_flags |= 2048;
        setDirty(true);
        return this;
    }

    public final ConnectionTune heartbeatMax(int value) {
        return setHeartbeatMax(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeUint16(this.channelMax);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeUint16(this.maxFrameSize);
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeUint16(this.heartbeatMin);
        }
        if ((packing_flags & 2048) != 0)
        {
            enc.writeUint16(this.heartbeatMax);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.channelMax = dec.readUint16();
        }
        if ((packing_flags & 512) != 0)
        {
            this.maxFrameSize = dec.readUint16();
        }
        if ((packing_flags & 1024) != 0)
        {
            this.heartbeatMin = dec.readUint16();
        }
        if ((packing_flags & 2048) != 0)
        {
            this.heartbeatMax = dec.readUint16();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<String,Object>();

        if ((packing_flags & 256) != 0)
        {
            result.put("channelMax", getChannelMax());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("maxFrameSize", getMaxFrameSize());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("heartbeatMin", getHeartbeatMin());
        }
        if ((packing_flags & 2048) != 0)
        {
            result.put("heartbeatMax", getHeartbeatMax());
        }


        return result;
    }


}
