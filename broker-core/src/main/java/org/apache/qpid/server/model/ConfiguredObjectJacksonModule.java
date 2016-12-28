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
package org.apache.qpid.server.model;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class ConfiguredObjectJacksonModule extends SimpleModule
{
    private static final long serialVersionUID = 1L;

    private static final ConfiguredObjectJacksonModule INSTANCE = new ConfiguredObjectJacksonModule(false);
    private static final ConfiguredObjectJacksonModule PERSISTENCE_INSTANCE = new ConfiguredObjectJacksonModule(true);


    private static final Set<String> OBJECT_METHOD_NAMES = Collections.synchronizedSet(new HashSet<String>());

    static
    {
        for(Method method : Object.class.getMethods())
        {
            OBJECT_METHOD_NAMES.add(method.getName());
        }
    }

    private  ConfiguredObjectJacksonModule(final boolean forPersistence)
    {
        super("ConfiguredObjectSerializer", new Version(1,0,0,null, "org.apache.qpid", "broker-core"));
        for(final ConfiguredObjectCustomSerialization.Converter converter :
                ConfiguredObjectCustomSerialization.getConverters(forPersistence))
        {
            addSerializer(converter.getConversionClass(), new JsonSerializer()
            {
                @Override
                public void serialize(final Object value, final JsonGenerator gen, final SerializerProvider serializers)
                        throws IOException, JsonProcessingException
                {
                    gen.writeObject(converter.convert(value));
                }
            });
        }

    }

    public static ObjectMapper newObjectMapper(final boolean forPersistence)
    {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(forPersistence ? PERSISTENCE_INSTANCE : INSTANCE);
        return objectMapper;
    }

}
