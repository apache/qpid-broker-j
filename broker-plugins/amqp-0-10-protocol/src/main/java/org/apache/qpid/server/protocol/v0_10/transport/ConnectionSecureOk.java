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


public final class ConnectionSecureOk extends Method {

    public static final int TYPE = 260;

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
    private byte[] response;


    public ConnectionSecureOk() {}


    public ConnectionSecureOk(byte[] response, Option ... _options) {
        if(response != null) {
            setResponse(response);
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
        delegate.connectionSecureOk(context, this);
    }


    public final boolean hasResponse() {
        return (packing_flags & 256) != 0;
    }

    public final ConnectionSecureOk clearResponse() {
        packing_flags &= ~256;
        this.response = null;
        setDirty(true);
        return this;
    }

    public final byte[] getResponse() {
        return response;
    }

    public final ConnectionSecureOk setResponse(byte[] value) {
        this.response = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public final ConnectionSecureOk response(byte[] value) {
        return setResponse(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeVbin32(this.response);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.response = dec.readVbin32();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<String,Object>();

        if ((packing_flags & 256) != 0)
        {
            result.put("response", getResponse());
        }


        return result;
    }


}
