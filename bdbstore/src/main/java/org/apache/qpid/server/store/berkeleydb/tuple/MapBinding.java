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

package org.apache.qpid.server.store.berkeleydb.tuple;

import java.util.Map;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;

import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.store.StoreException;

public class MapBinding extends TupleBinding<Map<String, Object>>
{
    private static final MapBinding INSTANCE = new MapBinding();
    private static final ObjectMapper MAPPER = ConfiguredObjectJacksonModule.newObjectMapper(true);
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() { };
    private static final ObjectReader MAP_READER = MAPPER.readerFor(MAP_TYPE_REFERENCE);
    private static final ObjectWriter MAP_WRITER = MAPPER.writerFor(MAP_TYPE_REFERENCE);

    public static MapBinding getInstance()
    {
        return INSTANCE;
    }

    @Override
    public Map<String, Object> entryToObject(final TupleInput input)
    {
        try
        {
            return MAP_READER.readValue(input.readString());
        }
        catch (final JacksonException e)
        {
            //should never happen
            throw new StoreException(e);
        }
    }

    @Override
    public void objectToEntry(final Map<String, Object> map, final TupleOutput output)
    {
        try
        {
            output.writeString(MAP_WRITER.writeValueAsString(map));
        }
        catch (final JacksonException e)
        {
            throw new StoreException(e);
        }
    }
}
