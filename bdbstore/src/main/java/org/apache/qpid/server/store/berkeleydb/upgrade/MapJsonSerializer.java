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
package org.apache.qpid.server.store.berkeleydb.upgrade;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


public class MapJsonSerializer
{
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>()
    {
    };

    private final ObjectMapper _mapper;

    public MapJsonSerializer()
    {
        _mapper = new ObjectMapper();
    }

    public String serialize(Map<String, Object> attributeMap) throws IOException
    {
        StringWriter stringWriter = new StringWriter();
        _mapper.writeValue(stringWriter, attributeMap);
        return stringWriter.toString();
    }

    public Map<String, Object> deserialize(String json) throws IOException
    {
        Map<String, Object> attributesMap = _mapper.readValue(json, MAP_TYPE_REFERENCE);
        return attributesMap;
    }
}
