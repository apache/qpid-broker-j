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

enum RecordType
{
    VERSION
            {
                @Override
                public VersionRecord read(Deserializer deserializer) throws IOException
                {
                    return VersionRecord.read(deserializer);
                }
            },
    MESSAGE
            {
                @Override
                public MessageRecord read(Deserializer deserializer) throws IOException
                {
                    return MessageRecord.read(deserializer);
                }
            },
    QUEUE_MAPPING
            {
                @Override
                public QueueMappingRecord read(Deserializer deserializer) throws IOException
                {
                    return QueueMappingRecord.read(deserializer);
                }
            },
    MESSAGE_INSTANCE
            {
                @Override
                public MessageInstanceRecord read(Deserializer deserializer) throws IOException
                {
                    return MessageInstanceRecord.read(deserializer);
                }
            },
    DTX
            {
                @Override
                public DTXRecord read(Deserializer deserializer) throws IOException
                {
                    return DTXRecord.read(deserializer);
                }
            },
    DIGEST
            {
                @Override
                public Record read(Deserializer deserializer) throws IOException
                {
                    return deserializer.validateDigest();
                }
            };

    abstract public Record read(Deserializer reader) throws IOException;

}
