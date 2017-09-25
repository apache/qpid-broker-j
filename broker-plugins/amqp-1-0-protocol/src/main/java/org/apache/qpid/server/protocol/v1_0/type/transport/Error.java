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


package org.apache.qpid.server.protocol.v1_0.type.transport;


import java.util.Map;

import org.apache.qpid.server.protocol.v1_0.CompositeType;
import org.apache.qpid.server.protocol.v1_0.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.ErrorCondition;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;

@CompositeType( symbolicDescriptor = "amqp:error:list", numericDescriptor = 0x000000000000001DL)
public class Error
{
    @CompositeTypeField(index = 0, mandatory = true,
            deserializationConverter = "org.apache.qpid.server.protocol.v1_0.DeserializationFactories.convertToErrorCondition")
    private ErrorCondition _condition;

    @CompositeTypeField(index = 1)
    private String _description;

    @CompositeTypeField(index = 2)
    private Map<Symbol, Object> _info;

    public Error()
    {
    }

    public Error(final ErrorCondition condition, final String description)
    {
        _condition = condition;
        _description = description;
    }

    public ErrorCondition getCondition()
    {
        return _condition;
    }

    public void setCondition(ErrorCondition condition)
    {
        _condition = condition;
    }

    public String getDescription()
    {
        return _description;
    }

    public void setDescription(String description)
    {
        _description = description;
    }

    public Map<Symbol, Object> getInfo()
    {
        return _info;
    }

    public void setInfo(Map<Symbol, Object> info)
    {
        _info = info;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Error{");
        final int origLength = builder.length();

        if (_condition != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("condition=").append(_condition);
        }

        if (_description != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("description=").append(_description);
        }

        if (_info != null)
        {
            if (builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("info=").append(_info);
        }

        builder.append('}');
        return builder.toString();
    }
}
