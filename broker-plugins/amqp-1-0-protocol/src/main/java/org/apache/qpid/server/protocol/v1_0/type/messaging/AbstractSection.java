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
package org.apache.qpid.server.protocol.v1_0.type.messaging;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.QpidByteBufferUtils;
import org.apache.qpid.server.protocol.v1_0.type.Section;
import org.apache.qpid.server.protocol.v1_0.type.messaging.codec.EncodingRetaining;

public abstract class AbstractSection<T> implements Section<T>, EncodingRetaining
{
    private List<QpidByteBuffer> _encodedForm;

    @Override
    public final void setEncodedForm(final List<QpidByteBuffer> encodedForm)
    {
        _encodedForm = encodedForm;
    }

    @Override
    public final List<QpidByteBuffer> getEncodedForm()
    {
        List<QpidByteBuffer> returnVal = new ArrayList<>(_encodedForm.size());
        for(int i = 0; i < _encodedForm.size(); i++)
        {
            returnVal.add(_encodedForm.get(i).duplicate());
        }
        return returnVal;
    }

    @Override
    public final void dispose()
    {
        for(int i = 0; i < _encodedForm.size(); i++)
        {
            _encodedForm.get(i).dispose();
        }
        _encodedForm = null;

    }

    @Override
    public final long getEncodedSize()
    {
        return QpidByteBufferUtils.remaining(_encodedForm);
    }

    @Override
    public void writeTo(final QpidByteBuffer dest)
    {
        for(QpidByteBuffer buf : _encodedForm)
        {
            dest.putCopyOf(buf);
        }
    }
}
