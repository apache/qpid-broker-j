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
package org.apache.qpid.server.store.berkeleydb.tuple;

import java.util.Map;
import java.util.UUID;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.berkeleydb.BDBConfiguredObjectRecord;

public class ConfiguredObjectBinding extends TupleBinding<ConfiguredObjectRecord>
{
    private static final ConfiguredObjectBinding INSTANCE = new ConfiguredObjectBinding(null);
    private static final TypeReference<Map<String,Object>> MAP_TYPE_REFERENCE = new TypeReference<>() { };
    private static final ObjectMapper OBJECT_MAPPER = ConfiguredObjectJacksonModule.newObjectMapper(true);

    private final UUID _uuid;

    public static ConfiguredObjectBinding getInstance()
    {
        return INSTANCE;
    }

    public ConfiguredObjectBinding(UUID uuid)
    {
        _uuid = uuid;
    }

    @Override
    public BDBConfiguredObjectRecord entryToObject(final TupleInput tupleInput)
    {
        final String type = tupleInput.readString();
        final String json = tupleInput.readString();
        try
        {
            final Map<String,Object> value = OBJECT_MAPPER.readValue(json, MAP_TYPE_REFERENCE);
            return new BDBConfiguredObjectRecord(_uuid, type, value);
        }
        catch (final JacksonException e)
        {
            // should never happen
            throw new StoreException(e);
        }
    }

    @Override
    public void objectToEntry(final ConfiguredObjectRecord object, final TupleOutput tupleOutput)
    {
        try
        {
            tupleOutput.writeString(object.getType());
            tupleOutput.writeString(OBJECT_MAPPER.writeValueAsString(object.getAttributes()));
        }
        catch (final JacksonException e)
        {
            throw new StoreException(e);
        }
    }
}
