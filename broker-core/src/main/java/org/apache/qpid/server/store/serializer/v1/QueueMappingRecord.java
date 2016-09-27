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
package org.apache.qpid.server.store.serializer.v1;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

class QueueMappingRecord implements Record
{
    private final UUID _id;
    private final String _name;

    QueueMappingRecord(final UUID id, final String name)
    {
        _id = id;
        _name = name;
    }

    @Override
    public RecordType getType()
    {
        return RecordType.QUEUE_MAPPING;
    }


    public UUID getId()
    {
        return _id;
    }

    public String getName()
    {
        return _name;
    }

    @Override
    public void writeData(final Serializer output) throws IOException
    {
        byte[] nameBytes = _name.getBytes(StandardCharsets.UTF_8);
        output.writeLong(_id.getMostSignificantBits());
        output.writeLong(_id.getLeastSignificantBits());
        output.writeInt(nameBytes.length);
        output.write(nameBytes);
    }

    public static QueueMappingRecord read(final Deserializer deserializer) throws IOException
    {
        UUID id = deserializer.readUUID();
        int nameLength = deserializer.readInt();
        byte[] nameBytes = deserializer.readBytes(nameLength);
        return new QueueMappingRecord(id, new String(nameBytes, StandardCharsets.UTF_8));
    }
}
