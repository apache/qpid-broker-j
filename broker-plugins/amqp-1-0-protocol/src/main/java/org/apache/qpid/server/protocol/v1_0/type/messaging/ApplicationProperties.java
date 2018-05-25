
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


package org.apache.qpid.server.protocol.v1_0.type.messaging;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.server.protocol.v1_0.type.Binary;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;

public class ApplicationProperties implements NonEncodingRetainingSection<Map<String,Object>>
{

    private final Map<String,Object> _value;

    public ApplicationProperties(Map<String,Object> value)
    {
        if (value == null)
        {
            throw new IllegalArgumentException("Value must not be null");
        }
        for(Map.Entry<String,Object> entry: value.entrySet())
        {
            if (entry.getKey() == null)
            {
                throw new IllegalArgumentException("Application properties do not allow null keys");
            }
            if (!isSimpleType(entry.getValue()))
            {
                throw new IllegalArgumentException("Application properties do not allow non-primitive values");
            }
        }
        _value = value;
    }

    @Override
    public Map<String,Object> getValue()
    {
        return _value;
    }

    @Override
    public ApplicationPropertiesSection createEncodingRetainingSection()
    {
        return new ApplicationPropertiesSection(this);
    }

    private boolean isSimpleType(final Object value)
    {
        return value == null
               || value instanceof String
               || value instanceof Number
               || value instanceof Boolean
               || value instanceof Symbol
               || value instanceof Date
               || value instanceof Character
               || value instanceof UUID
               || value instanceof Binary
               || value instanceof byte[];
    }

}
