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

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.validator.Map;
import org.apache.qpid.server.model.validator.Regex;
import org.apache.qpid.server.model.validator.ValueValidator;

public final class GelfMessageStaticFieldsValidator extends Map
{
    private static final class RegexKey extends Regex
    {
        private static final ValueValidator VALIDATOR = new RegexKey();

        private static final String KEY_PATTERN = "[\\w\\.\\-]+";

        private RegexKey()
        {
            super(KEY_PATTERN);
        }

        @Override
        public String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
        {
            return "Key of '" + attribute + " attribute"
                    + " instance of " + object.getClass().getName()
                    + " named '" + object.getName() + "'"
                    + " cannot be '" + value + "'."
                    + " Key pattern is: " + KEY_PATTERN;
        }
    }

    private static final class StringKey implements ValueValidator
    {
        private static final ValueValidator VALIDATOR = new StringKey();

        private StringKey()
        {
            super();
        }

        @Override
        public boolean test(Object value)
        {
            return value instanceof String;
        }

        @Override
        public String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
        {
            return "Key of '" + attribute + " attribute"
                    + " instance of " + object.getClass().getName()
                    + " named '" + object.getName() + "'"
                    + " cannot be '" + value + "',"
                    + " as it has to be a string";
        }
    }

    private static final class StringOrNumber implements ValueValidator
    {
        private static final ValueValidator VALIDATOR = new StringOrNumber();

        private StringOrNumber()
        {
            super();
        }

        @Override
        public boolean test(Object value)
        {
            return value instanceof String || value instanceof Number;
        }

        @Override
        public String errorMessage(Object value, ConfiguredObject<?> object, String attribute)
        {
            return "Value of '" + attribute + " attribute"
                    + " instance of " + object.getClass().getName()
                    + " named '" + object.getName() + "'"
                    + " cannot be '" + value + "',"
                    + " as it has to be a string or number";
        }
    }

    private static final ValueValidator VALIDATOR = new GelfMessageStaticFieldsValidator();

    public static ValueValidator validator()
    {
        return VALIDATOR;
    }

    private GelfMessageStaticFieldsValidator()
    {
        super(StringKey.VALIDATOR.andThen(RegexKey.VALIDATOR), StringOrNumber.VALIDATOR);
    }
}
