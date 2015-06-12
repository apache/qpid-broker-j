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

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OperationParameter
{
    private final Param _param;
    private final Class<?> _type;
    private final Type _genericType;

    public OperationParameter(final Param param, final Class<?> type, final Type genericType)
    {
        _param = param;
        _type = type;
        _genericType = genericType;
    }

    public String getName()
    {
        return _param.name();
    }

    public String getDefaultValue()
    {
        return _param.defaultValue();
    }

    public String getDescription()
    {
        return _param.description();
    }

    public List<String> getValidValues()
    {
        return Collections.unmodifiableList(Arrays.asList(_param.validValues()));
    }

    public Class<?> getType()
    {
        return _type;
    }

    public Type getGenericType()
    {
        return _genericType;
    }


    public boolean isCompatible(final OperationParameter that)
    {
        if (!_param.name().equals(that._param.name()))
        {
            return false;
        }
        if (getType() != null ? !getType().equals(that.getType()) : that.getType() != null)
        {
            return false;
        }
        return !(getGenericType() != null
                ? !getGenericType().equals(that.getGenericType())
                : that.getGenericType() != null);

    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final OperationParameter that = (OperationParameter) o;

        if (_param != null ? !_param.equals(that._param) : that._param != null)
        {
            return false;
        }
        if (getType() != null ? !getType().equals(that.getType()) : that.getType() != null)
        {
            return false;
        }
        return !(getGenericType() != null
                ? !getGenericType().equals(that.getGenericType())
                : that.getGenericType() != null);

    }

    @Override
    public int hashCode()
    {
        int result = _param != null ? _param.hashCode() : 0;
        result = 31 * result + (getType() != null ? getType().hashCode() : 0);
        result = 31 * result + (getGenericType() != null ? getGenericType().hashCode() : 0);
        return result;
    }
}
