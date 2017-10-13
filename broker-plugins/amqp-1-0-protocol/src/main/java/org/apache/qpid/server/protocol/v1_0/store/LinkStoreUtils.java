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

package org.apache.qpid.server.protocol.v1_0.store;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.ValueHandler;
import org.apache.qpid.server.protocol.v1_0.codec.ValueWriter;
import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.store.StoreException;

public class LinkStoreUtils
{
    private static final AMQPDescribedTypeRegistry DESCRIBED_TYPE_REGISTRY =
            AMQPDescribedTypeRegistry.newInstance().registerTransportLayer().registerMessagingLayer();

    public static Object amqpBytesToObject(final byte[] bytes)
    {
        ValueHandler valueHandler = new ValueHandler(DESCRIBED_TYPE_REGISTRY);
        try (QpidByteBuffer qpidByteBuffer = QpidByteBuffer.wrap(bytes))
        {
            return valueHandler.parse(qpidByteBuffer);
        }
        catch (AmqpErrorException e)
        {
            throw new StoreException("Unexpected serialized data", e);
        }
    }

    public static byte[] objectToAmqpBytes(final Object object)
    {
        ValueWriter valueWriter = DESCRIBED_TYPE_REGISTRY.getValueWriter(object);
        int encodedSize = valueWriter.getEncodedSize();
        try (QpidByteBuffer qpidByteBuffer = QpidByteBuffer.allocate(encodedSize))
        {
            valueWriter.writeToBuffer(qpidByteBuffer);
            return qpidByteBuffer.array();
        }
    }
}
