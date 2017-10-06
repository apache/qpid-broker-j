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
 */

package org.apache.qpid.server.protocol.v1_0.messaging;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructorRegistry;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;

public class SectionEncoderImpl implements SectionEncoder
{
    private final AMQPDescribedTypeRegistry _registry;

    public SectionEncoderImpl(final AMQPDescribedTypeRegistry describedTypeRegistry)
    {
        _registry = describedTypeRegistry;
    }

    @Override
    public QpidByteBuffer encodeObject(Object obj)
    {
        final ValueWriter<Object> valueWriter = _registry.getValueWriter(obj);
        int size = valueWriter.getEncodedSize();
        final QpidByteBuffer buf = QpidByteBuffer.allocateDirect(size);
        valueWriter.writeToBuffer(buf);
        buf.flip();
        return buf;
    }

    @Override
    public DescribedTypeConstructorRegistry getRegistry()
    {
        return _registry;
    }
}
