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

package org.apache.qpid.server.protocol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.plugin.MessageFormat;
import org.apache.qpid.server.plugin.QpidServiceLoader;

public class MessageFormatRegistry
{
    private static Map<Integer, MessageFormat<? extends ServerMessage<?>>> INPUT_CONVERTERS =
            new HashMap<>();

    private static Map<Class<? extends ServerMessage<?>>, MessageFormat<? extends ServerMessage<?>>> OUTPUT_CONVERTERS =
            new HashMap<>();


    static
    {
        for(MessageFormat<? extends ServerMessage<?>> converter : (new QpidServiceLoader()).instancesOf(MessageFormat.class))
        {
            INPUT_CONVERTERS.put(converter.getSupportedFormat(), converter);
            OUTPUT_CONVERTERS.put(converter.getMessageClass(), converter);
        }
    }

    public static MessageFormat<? extends ServerMessage<?>> getFormat(int format)
    {
        return INPUT_CONVERTERS.get(format);
    }

    public  static MessageFormat<? extends ServerMessage<?>> getFormat(ServerMessage<?> message)
    {
        return OUTPUT_CONVERTERS.get(message.getClass());
    }

}
