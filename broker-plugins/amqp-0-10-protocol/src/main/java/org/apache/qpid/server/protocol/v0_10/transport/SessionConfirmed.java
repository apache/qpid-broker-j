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


public final class SessionConfirmed extends Method {

    public static final int TYPE = 521;

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
    private java.util.List<Object> fragments;


    public SessionConfirmed() {}


    public SessionConfirmed(RangeSet commands, java.util.List<Object> fragments, Option ... _options) {
        if(commands != null) {
            setCommands(commands);
        }
        if(fragments != null) {
            setFragments(fragments);
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
        delegate.sessionConfirmed(context, this);
    }


    public final boolean hasCommands() {
        return (packing_flags & 256) != 0;
    }

    public final SessionConfirmed clearCommands() {
        packing_flags &= ~256;
        this.commands = null;
        setDirty(true);
        return this;
    }

    public final RangeSet getCommands() {
        return commands;
    }

    public final SessionConfirmed setCommands(RangeSet value) {
        this.commands = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public final SessionConfirmed commands(RangeSet value) {
        return setCommands(value);
    }

    public final boolean hasFragments() {
        return (packing_flags & 512) != 0;
    }

    public final SessionConfirmed clearFragments() {
        packing_flags &= ~512;
        this.fragments = null;
        setDirty(true);
        return this;
    }

    public final java.util.List<Object> getFragments() {
        return fragments;
    }

    public final SessionConfirmed setFragments(java.util.List<Object> value) {
        this.fragments = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public final SessionConfirmed fragments(java.util.List<Object> value) {
        return setFragments(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeSequenceSet(this.commands);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeArray(this.fragments);
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
        if ((packing_flags & 512) != 0)
        {
            this.fragments = dec.readArray();
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
        if ((packing_flags & 512) != 0)
        {
            result.put("fragments", getFragments());
        }
        return result;
    }
}
