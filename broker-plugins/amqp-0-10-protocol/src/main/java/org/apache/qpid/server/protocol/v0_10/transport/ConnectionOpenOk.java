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


public final class ConnectionOpenOk extends Method {

    public static final int TYPE = 264;

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
    private java.util.List<Object> knownHosts;


    public ConnectionOpenOk() {}


    public ConnectionOpenOk(java.util.List<Object> knownHosts, Option ... _options) {
        if(knownHosts != null) {
            setKnownHosts(knownHosts);
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
        delegate.connectionOpenOk(context, this);
    }


    public final boolean hasKnownHosts() {
        return (packing_flags & 256) != 0;
    }

    public final ConnectionOpenOk clearKnownHosts() {
        packing_flags &= ~256;
        this.knownHosts = null;
        setDirty(true);
        return this;
    }

    public final java.util.List<Object> getKnownHosts() {
        return knownHosts;
    }

    public final ConnectionOpenOk setKnownHosts(java.util.List<Object> value) {
        this.knownHosts = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public final ConnectionOpenOk knownHosts(java.util.List<Object> value) {
        return setKnownHosts(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeArray(this.knownHosts);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.knownHosts = dec.readArray();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();
        if ((packing_flags & 256) != 0)
        {
            result.put("knownHosts", getKnownHosts());
        }
        return result;
    }
}
