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

package org.apache.qpid.systests.end_to_end_conversion.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SerializableTestClass implements Serializable
{
    private final HashMap<String, String> _mapField;
    private final ArrayList<Integer> _listField;

    public SerializableTestClass(final Map<String, String> map,
                                 final List<Integer> list)
    {
        _mapField = new HashMap<>(map);
        _listField = new ArrayList<>(list);
    }

    public HashMap<String, String> getMapField()
    {
        return _mapField;
    }

    public ArrayList<Integer> getListField()
    {
        return _listField;
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

        final SerializableTestClass that = (SerializableTestClass) o;

        if (_mapField != null ? !_mapField.equals(that._mapField) : that._mapField != null)
        {
            return false;
        }
        return _listField != null ? _listField.equals(that._listField) : that._listField == null;
    }

    @Override
    public int hashCode()
    {
        int result = _mapField != null ? _mapField.hashCode() : 0;
        result = 31 * result + (_listField != null ? _listField.hashCode() : 0);
        return result;
    }
}
