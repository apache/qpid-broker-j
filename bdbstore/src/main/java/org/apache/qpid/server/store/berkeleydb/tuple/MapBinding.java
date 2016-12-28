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

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.store.StoreException;

public class MapBinding extends TupleBinding<Map<String, Object>>
{
    private static final MapBinding INSTANCE = new MapBinding();

    public static MapBinding getInstance()
    {
        return INSTANCE;
    }

    @Override
    public Map<String, Object> entryToObject(final TupleInput input)
    {
        String json = input.readString();
        ObjectMapper mapper = ConfiguredObjectJacksonModule.newObjectMapper(true);
        try
        {
            Map<String, Object> value = mapper.readValue(json, Map.class);
            return value;
        }
        catch (IOException e)
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
            StringWriter writer = new StringWriter();
            final ObjectMapper objectMapper = ConfiguredObjectJacksonModule.newObjectMapper(true);
            objectMapper.writeValue(writer, map);
            output.writeString(writer.toString());
        }
        catch (IOException e)
        {
            throw new StoreException(e);
        }
    }
}
