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


public final class SessionKnownCompleted extends Method {

    public static final int TYPE = 523;

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
        return Frame.L3;
    }

    @Override
    public final boolean isConnectionControl()
    {
        return false;
    }

    private short packing_flags = 0;
    private RangeSet commands;


    public SessionKnownCompleted() {}


    public SessionKnownCompleted(RangeSet commands, Option ... _options) {
        if(commands != null) {
            setCommands(commands);
        }

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
        delegate.sessionKnownCompleted(context, this);
    }


    public final boolean hasCommands() {
        return (packing_flags & 256) != 0;
    }

    public final SessionKnownCompleted clearCommands() {
        packing_flags &= ~256;
        this.commands = null;
        setDirty(true);
        return this;
    }

    public final RangeSet getCommands() {
        return commands;
    }

    public final SessionKnownCompleted setCommands(RangeSet value) {
        this.commands = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public final SessionKnownCompleted commands(RangeSet value) {
        return setCommands(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeSequenceSet(this.commands);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.commands = dec.readSequenceSet();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("commands", getCommands());
        }
        return result;
    }
}
