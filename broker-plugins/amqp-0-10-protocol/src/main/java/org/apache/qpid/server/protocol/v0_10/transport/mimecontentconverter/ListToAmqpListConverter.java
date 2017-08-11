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
package org.apache.qpid.server.protocol.v0_10.transport.mimecontentconverter;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.qpid.server.message.mimecontentconverter.ObjectToMimeContentConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.protocol.v0_10.transport.BBEncoder;
import org.apache.qpid.server.protocol.v0_10.transport.EncoderUtils;

@PluggableService
public class ListToAmqpListConverter implements ObjectToMimeContentConverter<List>
{
    @Override
    public String getType()
    {
        return getMimeType();
    }

    @Override
    public Class<List> getObjectClass()
    {
        return List.class;
    }

    @Override
    public int getRank()
    {
        return 0;
    }

    @Override
    public String getMimeType()
    {
        return "amqp/list";
    }

    @Override
    public boolean isAcceptable(final List list)
    {
        return EncoderUtils.isEncodable(list);
    }

    @Override
    public byte[] toMimeContent(final List list)
    {
        BBEncoder encoder = new BBEncoder(1024);
        encoder.writeList(list);
        final ByteBuffer buf = encoder.buffer();
        int remaining = buf.remaining();
        byte[] data = new byte[remaining];
        buf.get(data);
        return data;
    }
}
