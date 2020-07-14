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

package org.apache.qpid.server.model.validator;

import org.apache.qpid.server.model.ConfiguredObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class Regex implements ValueValidator
{
    private static final Map<String, ValueValidator> MAP = new ConcurrentHashMap<>();

    private final Pattern _pattern;

    public static ValueValidator validator(String pattern)
    {
        return MAP.computeIfAbsent(pattern, Regex::new);
    }

    protected Regex(String pattern)
    {
        _pattern = Pattern.compile(pattern);
    }

    @Override
    public boolean test(Object value)
    {
        return value != null && _pattern.matcher(value.toString()).matches();
    }

    @Override
    public String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
    {
        return "Attribute '" + attribute
                + "' instance of " + object.getClass().getName()
                + " named '" + object.getName() + "'"
                + " cannot have value '" + value + "'."
                + " Valid value pattern is: "
                + _pattern.pattern();
    }
}
