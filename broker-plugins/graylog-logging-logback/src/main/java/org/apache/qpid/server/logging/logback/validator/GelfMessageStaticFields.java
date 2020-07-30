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

package org.apache.qpid.server.logging.logback.validator;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public final class GelfMessageStaticFields implements Validator<Map<String, Object>>
{
    private static class Key implements Validator<String>, Predicate<String>
    {
        private static final Pattern PATTERN = Pattern.compile("[\\w\\.\\-]+");

        Key()
        {
            super();
        }

        @Override
        public boolean test(String value)
        {
            return value != null && PATTERN.matcher(value).matches();
        }

        @Override
        public void validate(String value, ConfiguredObject<?> object, String attribute)
        {
            if (!test(value))
            {
                throw new IllegalConfigurationException(errorMessage(value, object, attribute));
            }
        }

        private String errorMessage(String value, ConfiguredObject<?> object, String attribute)
        {
            return "Key of '" + attribute + " attribute"
                    + " instance of " + object.getClass().getName()
                    + " named '" + object.getName() + "'"
                    + " cannot be '" + value + "'."
                    + " Key pattern is: " + PATTERN.pattern();
        }
    }

    private static final class Value implements Validator<Object>, Predicate<Object>
    {
        Value()
        {
            super();
        }

        @Override
        public boolean test(Object value)
        {
            return value instanceof String || value instanceof Number;
        }

        @Override
        public void validate(Object value, ConfiguredObject<?> object, String attribute)
        {
            if (!test(value))
            {
                throw new IllegalConfigurationException(errorMessage(value, object, attribute));
            }
        }

        private String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
        {
            return "Value of '" + attribute + " attribute"
                    + " instance of " + object.getClass().getName()
                    + " named '" + object.getName() + "'"
                    + " cannot be '" + value + "',"
                    + " as it has to be a string or number";
        }
    }

    private static final Key KEY = new Key();

    private static final Value VALUE = new Value();

    private static final GelfMessageStaticFields VALIDATOR = new GelfMessageStaticFields();

    public static Validator<Map<String, Object>> validator()
    {
        return VALIDATOR;
    }

    public static void validateStaticFields(Map<String, Object> value, ConfiguredObject<?> object, String attribute)
    {
        validator().validate(value, object, attribute);
    }

    private GelfMessageStaticFields()
    {
        super();
    }

    @Override
    public void validate(Map<String, Object> map, final ConfiguredObject<?> object, final String attribute)
    {
        if (map == null) {
            throw new IllegalConfigurationException(nullErrorMessage(object, attribute));
        }
        map.entrySet().forEach(entry -> validateMapEntry(entry, object, attribute));
    }

    private String nullErrorMessage(ConfiguredObject<?> object, String attribute)
    {
        return "Attribute '" + attribute
                + " instance of " + object.getClass().getName()
                + " named '" + object.getName() + "'"
                + " cannot be 'null'";
    }

    private void validateMapEntry(Entry<String, Object> entry, ConfiguredObject<?> object, String attribute)
    {
        KEY.validate(entry.getKey(), object, attribute);
        VALUE.validate(entry.getValue(), object, attribute);
    }
}
