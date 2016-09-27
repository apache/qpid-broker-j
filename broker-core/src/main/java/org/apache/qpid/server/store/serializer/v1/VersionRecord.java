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

class VersionRecord implements Record
{

    private final byte[] _versionBytes = MessageStoreSerializer_v1.VERSION.getBytes(StandardCharsets.UTF_8);

    @Override
    public RecordType getType()
    {
        return RecordType.VERSION;
    }

    @Override
    public void writeData(final Serializer output) throws IOException
    {
        output.writeInt(_versionBytes.length);
        output.write(_versionBytes);
    }

    public static VersionRecord read(final Deserializer deserializer) throws IOException
    {
        byte[] data = deserializer.readBytes(deserializer.readInt());
        String version = new String(data, StandardCharsets.UTF_8);
        if(!version.equals(MessageStoreSerializer_v1.VERSION))
        {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }

        return new VersionRecord();
    }

}
