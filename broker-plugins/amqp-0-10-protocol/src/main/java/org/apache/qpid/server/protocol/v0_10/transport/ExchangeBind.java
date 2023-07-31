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


public final class ExchangeBind extends Method {

    public static final int TYPE = 1796;

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
        return Frame.L4;
    }

    @Override
    public final boolean isConnectionControl()
    {
        return false;
    }

    private short packing_flags = 0;
    private String queue;
    private String exchange;
    private String bindingKey;
    private Map<String,Object> arguments;


    public ExchangeBind() {}


    public ExchangeBind(String queue, String exchange, String bindingKey, Map<String,Object> arguments, Option ... _options) {
        if(queue != null) {
            setQueue(queue);
        }
        if(exchange != null) {
            setExchange(exchange);
        }
        if(bindingKey != null) {
            setBindingKey(bindingKey);
        }
        if(arguments != null) {
            setArguments(arguments);
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
        delegate.exchangeBind(context, this);
    }


    public final boolean hasQueue() {
        return (packing_flags & 256) != 0;
    }

    public final ExchangeBind clearQueue() {
        packing_flags &= ~256;
        this.queue = null;
        setDirty(true);
        return this;
    }

    public final String getQueue() {
        return queue;
    }

    public final ExchangeBind setQueue(String value) {
        this.queue = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public final ExchangeBind queue(String value) {
        return setQueue(value);
    }

    public final boolean hasExchange() {
        return (packing_flags & 512) != 0;
    }

    public final ExchangeBind clearExchange() {
        packing_flags &= ~512;
        this.exchange = null;
        setDirty(true);
        return this;
    }

    public final String getExchange() {
        return exchange;
    }

    public final ExchangeBind setExchange(String value) {
        this.exchange = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public final ExchangeBind exchange(String value) {
        return setExchange(value);
    }

    public final boolean hasBindingKey() {
        return (packing_flags & 1024) != 0;
    }

    public final ExchangeBind clearBindingKey() {
        packing_flags &= ~1024;
        this.bindingKey = null;
        setDirty(true);
        return this;
    }

    public final String getBindingKey() {
        return bindingKey;
    }

    public final ExchangeBind setBindingKey(String value) {
        this.bindingKey = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public final ExchangeBind bindingKey(String value) {
        return setBindingKey(value);
    }

    public final boolean hasArguments() {
        return (packing_flags & 2048) != 0;
    }

    public final ExchangeBind clearArguments() {
        packing_flags &= ~2048;
        this.arguments = null;
        setDirty(true);
        return this;
    }

    public final Map<String,Object> getArguments() {
        return arguments;
    }

    public final ExchangeBind setArguments(Map<String,Object> value) {
        this.arguments = value;
        packing_flags |= 2048;
        setDirty(true);
        return this;
    }

    public final ExchangeBind arguments(Map<String,Object> value) {
        return setArguments(value);
    }




    @Override
    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeStr8(this.queue);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeStr8(this.exchange);
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeStr8(this.bindingKey);
        }
        if ((packing_flags & 2048) != 0)
        {
            enc.writeMap(this.arguments);
        }

    }

    @Override
    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.queue = dec.readStr8();
        }
        if ((packing_flags & 512) != 0)
        {
            this.exchange = dec.readStr8();
        }
        if ((packing_flags & 1024) != 0)
        {
            this.bindingKey = dec.readStr8();
        }
        if ((packing_flags & 2048) != 0)
        {
            this.arguments = dec.readMap();
        }

    }

    @Override
    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<>();

        if ((packing_flags & 256) != 0)
        {
            result.put("queue", getQueue());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("exchange", getExchange());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("bindingKey", getBindingKey());
        }
        if ((packing_flags & 2048) != 0)
        {
            result.put("arguments", getArguments());
        }
        return result;
    }
}
