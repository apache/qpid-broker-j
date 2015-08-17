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

package org.apache.qpid.server.util;

import java.util.Map;

import org.apache.qpid.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;

public class QpidByteBufferUtils
{
    public static final int DEFAULT_MAX_POOLSIZE = 1024;

    public static void createPool(ConfiguredObject object, int bufferSize)
    {
        Map<String, Integer> bufferPoolSizes =
                (Map<String, Integer>) object.getContextValue(Map.class, Broker.BROKER_DIRECT_BYTE_BUFFER_POOL_SIZES);

        int fallbackMaxPoolSize = bufferPoolSizes != null && bufferPoolSizes.containsKey("") ? bufferPoolSizes.get("") : DEFAULT_MAX_POOLSIZE;
        final String bufferSizeKey = String.valueOf(bufferSize);
        int maxPoolSize = bufferPoolSizes != null && bufferPoolSizes.containsKey(bufferSizeKey) ? bufferPoolSizes.get(bufferSizeKey) : fallbackMaxPoolSize;

        QpidByteBuffer.createPool(bufferSize, maxPoolSize);

    }

}
