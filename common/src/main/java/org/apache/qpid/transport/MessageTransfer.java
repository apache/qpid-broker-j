package org.apache.qpid.transport;
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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.transport.codec.Decoder;
import org.apache.qpid.transport.codec.Encoder;

import org.apache.qpid.util.Strings;
import org.apache.qpid.transport.network.Frame;


public final class MessageTransfer extends Method {

    public static final int TYPE = 1025;
    private int _bodySize;

    public final int getStructType() {
        return TYPE;
    }

    public final int getSizeWidth() {
        return 0;
    }

    public final int getPackWidth() {
        return 2;
    }

    public final boolean hasPayload() {
        return true;
    }

    public final byte getEncodedTrack() {
        return Frame.L4;
    }

    public final boolean isConnectionControl()
    {
        return false;
    }

    private short packing_flags = 0;
    private String destination;
    private MessageAcceptMode acceptMode;
    private MessageAcquireMode acquireMode;
    private Header header;
    private Collection<QpidByteBuffer> _body;


    public MessageTransfer() {}

    public MessageTransfer(String destination, MessageAcceptMode acceptMode, MessageAcquireMode acquireMode, Header header, java.nio.ByteBuffer body, Option ... options)
    {
        this(destination,
             acceptMode,
             acquireMode,
             header,
             Collections.singletonList(QpidByteBuffer.wrap(body)),
             options);
    }

    public MessageTransfer(String destination, MessageAcceptMode acceptMode, MessageAcquireMode acquireMode, Header header, Collection<QpidByteBuffer> body, Option ... _options) {
        if(destination != null) {
            setDestination(destination);
        }
        if(acceptMode != null) {
            setAcceptMode(acceptMode);
        }
        if(acquireMode != null) {
            setAcquireMode(acquireMode);
        }
        setHeader(header);
        setBody(body);

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

    public <C> void dispatch(C context, MethodDelegate<C> delegate) {
        delegate.messageTransfer(context, this);
    }


    public final boolean hasDestination() {
        return (packing_flags & 256) != 0;
    }

    public final MessageTransfer clearDestination() {
        packing_flags &= ~256;
        this.destination = null;
        setDirty(true);
        return this;
    }

    public final String getDestination() {
        return destination;
    }

    public final MessageTransfer setDestination(String value) {
        this.destination = value;
        packing_flags |= 256;
        setDirty(true);
        return this;
    }

    public final MessageTransfer destination(String value) {
        return setDestination(value);
    }

    public final boolean hasAcceptMode() {
        return (packing_flags & 512) != 0;
    }

    public final MessageTransfer clearAcceptMode() {
        packing_flags &= ~512;
        this.acceptMode = null;
        setDirty(true);
        return this;
    }

    public final MessageAcceptMode getAcceptMode() {
        return acceptMode;
    }

    public final MessageTransfer setAcceptMode(MessageAcceptMode value) {
        this.acceptMode = value;
        packing_flags |= 512;
        setDirty(true);
        return this;
    }

    public final MessageTransfer acceptMode(MessageAcceptMode value) {
        return setAcceptMode(value);
    }

    public final boolean hasAcquireMode() {
        return (packing_flags & 1024) != 0;
    }

    public final MessageTransfer clearAcquireMode() {
        packing_flags &= ~1024;
        this.acquireMode = null;
        setDirty(true);
        return this;
    }

    public final MessageAcquireMode getAcquireMode() {
        return acquireMode;
    }

    public final MessageTransfer setAcquireMode(MessageAcquireMode value) {
        this.acquireMode = value;
        packing_flags |= 1024;
        setDirty(true);
        return this;
    }

    public final MessageTransfer acquireMode(MessageAcquireMode value) {
        return setAcquireMode(value);
    }


    public final Header getHeader() {
        return this.header;
    }

    public final void setHeader(Header header) {
        this.header = header;
    }

    public final MessageTransfer header(Header header) {
        setHeader(header);
        return this;
    }

    @Override
    public final Collection<QpidByteBuffer> getBody() {
        if (this._body == null)
        {
            return null;
        }
        else
        {
            return Collections.unmodifiableCollection(_body);
        }
    }

    @Override
    public final void setBody(Collection<QpidByteBuffer> body)
    {
        if (body == null)
        {
            _bodySize = 0;
            _body = null;
        }
        else
        {
            _body = new ArrayList<>(body.size());
            int size = 0;
            for (QpidByteBuffer buf : body)
            {
                size += buf.remaining();
                _body.add(buf.duplicate());
            }
            _bodySize = size;
        }
    }

    @Override
    public int getBodySize()
    {
        return _bodySize;
    }

    public final MessageTransfer body(List<QpidByteBuffer> body)
    {
        setBody(body);
        return this;
    }

    public final byte[] getBodyBytes() {
        Collection<QpidByteBuffer> body = getBody();
        byte[] bytes = new byte[getBodySize()];
        for(QpidByteBuffer buf : body)
        {
            buf.duplicate().get(bytes);
        }
        return bytes;
    }

    public final void setBody(byte[] body)
    {
        setBody(Collections.singletonList(QpidByteBuffer.wrap(body)));
    }

    public final String getBodyString() {
        return Strings.fromUTF8(getBodyBytes());
    }

    public final void setBody(String body) {
        setBody(Strings.toUTF8(body));
    }


    public void write(Encoder enc)
    {
        enc.writeUint16(packing_flags);
        if ((packing_flags & 256) != 0)
        {
            enc.writeStr8(this.destination);
        }
        if ((packing_flags & 512) != 0)
        {
            enc.writeUint8(this.acceptMode.getValue());
        }
        if ((packing_flags & 1024) != 0)
        {
            enc.writeUint8(this.acquireMode.getValue());
        }

    }

    public void read(Decoder dec)
    {
        packing_flags = (short) dec.readUint16();
        if ((packing_flags & 256) != 0)
        {
            this.destination = dec.readStr8();
        }
        if ((packing_flags & 512) != 0)
        {
            this.acceptMode = MessageAcceptMode.get(dec.readUint8());
        }
        if ((packing_flags & 1024) != 0)
        {
            this.acquireMode = MessageAcquireMode.get(dec.readUint8());
        }

    }

    public Map<String,Object> getFields()
    {
        Map<String,Object> result = new LinkedHashMap<String,Object>();

        if ((packing_flags & 256) != 0)
        {
            result.put("destination", getDestination());
        }
        if ((packing_flags & 512) != 0)
        {
            result.put("acceptMode", getAcceptMode());
        }
        if ((packing_flags & 1024) != 0)
        {
            result.put("acquireMode", getAcquireMode());
        }


        return result;
    }

    public void dispose()
    {
        if (_body != null)
        {
            for (QpidByteBuffer buf : _body)
            {
                buf.dispose();
            }
        }
    }
}
