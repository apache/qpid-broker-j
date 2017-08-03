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
package org.apache.qpid.server.typedmessage.mimecontentconverter;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.qpid.server.message.mimecontentconverter.ObjectToMimeContentConverter;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.typedmessage.TypedBytesContentWriter;
import org.apache.qpid.server.typedmessage.TypedBytesFormatException;

@PluggableService
public class MapToJmsMapMessage implements ObjectToMimeContentConverter<Map>
{
    @Override
    public String getType()
    {
        return getMimeType();
    }

    @Override
    public String getMimeType()
    {
        return "jms/map-message";
    }

    @Override
    public Class<Map> getObjectClass()
    {
        return Map.class;
    }

    @Override
    public int getRank()
    {
        return 10;
    }

    @Override
    public boolean isAcceptable(final Map map)
    {
        if (map != null)
        {
            for (Entry entry : (Set<Entry>) map.entrySet())
            {
                Object key = entry.getKey();
                if (!(key instanceof String))
                {
                    return false;
                }
                Object value = entry.getValue();

                if (value != null
                    && !(value instanceof String
                      || value instanceof Integer
                      || value instanceof Long
                      || value instanceof Double
                      || value instanceof Float
                      || value instanceof Byte
                      || value instanceof Short
                      || value instanceof Character
                      || value instanceof Boolean
                      || value instanceof byte[]))
                {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public byte[] toMimeContent(final Map map)
    {
        TypedBytesContentWriter writer = new TypedBytesContentWriter();
        writer.writeIntImpl(map == null ? 0 : map.size());

        if (map != null)
        {
            try
            {
                for (Entry entry : (Set<Entry>) map.entrySet())
                {
                    writer.writeNullTerminatedStringImpl((String) entry.getKey());
                    writer.writeObject(entry.getValue());
                }
            }
            catch (TypedBytesFormatException e)
            {
                throw new IllegalArgumentException(e);
            }
        }

        final ByteBuffer buf = writer.getData();
        int remaining = buf.remaining();
        byte[] data = new byte[remaining];
        buf.get(data);
        return data;
    }
}
