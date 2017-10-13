/*
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

import static org.apache.qpid.server.transport.util.Functions.str;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

/**
 * Method
 *
 * @author Rafael H. Schloming
 */

public abstract class Method extends Struct implements ProtocolEvent
{


    public static final Method create(int type)
    {
        // XXX: should generate separate factories for separate
        // namespaces
        return (Method) StructFactory.createInstruction(type);
    }

    // XXX: command subclass?
    public static interface CompletionListener
    {
        public void onComplete(Method method);
    }

    private int id;
    private int channel;
    private boolean idSet = false;
    private boolean sync = false;
    private boolean batch = false;
    private boolean unreliable = false;
    private CompletionListener completionListener;

    public final int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
        this.idSet = true;
    }

    boolean idSet()
    {
        return idSet;
    }

    @Override
    public final int getChannel()
    {
        return channel;
    }

    @Override
    public final void setChannel(int channel)
    {
        this.channel = channel;
    }

    public final boolean isSync()
    {
        return sync;
    }

    public final void setSync(boolean value)
    {
        this.sync = value;
    }

    public final boolean isBatch()
    {
        return batch;
    }

    final void setBatch(boolean value)
    {
        this.batch = value;
    }

    public final boolean isUnreliable()
    {
        return unreliable;
    }

    final void setUnreliable(boolean value)
    {
        this.unreliable = value;
    }

    public abstract boolean hasPayload();

    public Header getHeader()
    {
        return null;
    }

    public void setHeader(Header header)
    {
        throw new UnsupportedOperationException();
    }

    public QpidByteBuffer getBody()
    {
        return null;
    }

    public void setBody(QpidByteBuffer body)
    {
        throw new UnsupportedOperationException();
    }

    public int getBodySize()
    {
        return 0;

    }

    @Override
    public abstract byte getEncodedTrack();

    public abstract <C> void dispatch(C context, MethodDelegate<C> delegate);

    @Override
    public <C> void delegate(C context, ProtocolDelegate<C> delegate)
    {
        if (getEncodedTrack() == Frame.L4)
        {
            delegate.command(context, this);
        }
        else
        {
            delegate.control(context, this);
        }
    }


    public void setCompletionListener(CompletionListener completionListener)
    {
        this.completionListener = completionListener;
    }

    public void complete()
    {
        if(completionListener!= null)
        {
            completionListener.onComplete(this);
            completionListener = null;            
        }
    }

    public boolean hasCompletionListener()
    {
        return completionListener != null;
    }

    @Override
    public final int getEncodedLength()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder();

        str.append("ch=");
        str.append(channel);

        if (getEncodedTrack() == Frame.L4 && idSet)
        {
            str.append(" id=");
            str.append(id);
        }

        if (sync || batch)
        {
            str.append(" ");
            str.append("[");
            if (sync)
            {
                str.append("S");
            }
            if (batch)
            {
                str.append("B");
            }
            str.append("]");
        }

        str.append(" ");
        str.append(super.toString());
        Header hdr = getHeader();
        if (hdr != null)
        {
            for (Struct st : hdr.getStructs())
            {
                str.append("\n  ");
                str.append(st);
            }
        }
        QpidByteBuffer body = getBody();
        if (body != null)
        {
            str.append("\n  body=");
            str.append(str(body, 64));
        }

        return str.toString();
    }

}
