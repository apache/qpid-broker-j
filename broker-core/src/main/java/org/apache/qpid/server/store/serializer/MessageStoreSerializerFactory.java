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
package org.apache.qpid.server.store.serializer;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.qpid.server.plugin.QpidServiceLoader;

class MessageStoreSerializerFactory implements MessageStoreSerializer.Factory
{
    @Override
    public MessageStoreSerializer newInstance()
    {
        return new QpidServiceLoader().getInstancesByType(MessageStoreSerializer.class).get(MessageStoreSerializer.LATEST);
    }

    @Override
    public MessageStoreSerializer newInstance(final DataInputStream data) throws IOException
    {

        // All encodings should start 0x00 << int length of the version string>> << version string in UTF-8 >>
        data.mark(50);
        if (data.read() != 0)
        {
            throw new IllegalArgumentException("Invalid format for upload");
        }
        int stringLength = data.readInt();
        byte[] stringBytes = new byte[stringLength];
        data.readFully(stringBytes);

        String version = new String(stringBytes, StandardCharsets.UTF_8);

        data.reset();

        Map<String, MessageStoreSerializer> serializerMap =
                new QpidServiceLoader().getInstancesByType(MessageStoreSerializer.class);

        MessageStoreSerializer serializer = serializerMap.get(version);

        if(serializer == null)
        {
            throw new IllegalArgumentException("Message store import uses version '"
                                               + version + "' which is not supported");
        }
        else
        {
            return serializer;
        }
    }
}
