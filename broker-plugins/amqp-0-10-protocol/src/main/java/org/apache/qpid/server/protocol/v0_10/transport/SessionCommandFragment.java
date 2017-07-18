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


public final class SessionCommandFragment extends Struct {

    public static final int TYPE = -2;

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
        return 0;
    }

    public final boolean hasPayload() {
        return false;
    }

    public final byte getEncodedTrack() {
        return -1;
    }

    public final boolean isConnectionControl()
    {
        return false;
    }

    private int commandId;
    private RangeSet byteRanges;


    public SessionCommandFragment() {}


    public SessionCommandFragment(int commandId, RangeSet byteRanges) {
        setCommandId(commandId);
        if(byteRanges != null) {
            setByteRanges(byteRanges);
        }

    }




    public final int getCommandId() {
        return commandId;
    }

    public final SessionCommandFragment setCommandId(int value) {
        this.commandId = value;

        setDirty(true);
        return this;
    }

    public final SessionCommandFragment commandId(int value) {
        return setCommandId(value);
    }

    public final RangeSet getByteRanges() {
        return byteRanges;
    }

    public final SessionCommandFragment setByteRanges(RangeSet value) {
        this.byteRanges = value;

        setDirty(true);
        return this;
    }

    public final SessionCommandFragment byteRanges(RangeSet value) {
        return setByteRanges(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeSequenceNo(this.commandId);
        enc.writeByteRanges(this.byteRanges);

    }

    @Override
    public void read(Decoder dec)
    {
        this.commandId = dec.readSequenceNo();
        this.byteRanges = dec.readByteRanges();

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<String,Object>();

        result.put("commandId", getCommandId());
        result.put("byteRanges", getByteRanges());


        return result;
    }

    @Override
    public int getEncodedLength()
    {
        throw new UnsupportedOperationException();
    }

}
