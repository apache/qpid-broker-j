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
package org.apache.qpid.server.management.plugin.controller;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.ManagementException;

public class ConverterHelper
{

    private static final Pattern CONTEXT_VARIABLE_PATTERN = Pattern.compile("\\$\\{[\\w+.\\-:]+}");

    private ConverterHelper()
    {
        super();
    }

    public static long toLong(final Object attributeValue)
    {
        long value;
        if (attributeValue instanceof Number)
        {
            value = ((Number) attributeValue).longValue();
        }
        else if (attributeValue instanceof String)
        {
            try
            {
                value = Long.parseLong((String) attributeValue);
            }
            catch (Exception e)
            {
                value = 0;
            }
        }
        else
        {
            value = 0;
        }
        return value;
    }

    public static boolean toBoolean(final Object attributeValue)
    {
        boolean value;
        if (attributeValue instanceof Boolean)
        {
            value = (Boolean) attributeValue;
        }
        else if (attributeValue instanceof String)
        {
            return Boolean.parseBoolean((String)attributeValue);

        }
        else
        {
            value = false;
        }
        return value;
    }

    public static int toInt(final Object value)
    {
        int result;
        if (value instanceof Number)
        {
            result = ((Number) value).intValue();
        }
        else if (value instanceof String)
        {
            try
            {
                result = Integer.parseInt(String.valueOf(value));
            }
            catch (RuntimeException e)
            {
                result = 0;
            }
        }
        else
        {
            result = 0;
        }
        return result;
    }

    public static  boolean isContextVariable(final Object value)
    {
        return value != null && CONTEXT_VARIABLE_PATTERN.matcher(String.valueOf(value)).matches();
    }

    public static String encode (String value)
    {
        try
        {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.displayName());
        }
        catch (UnsupportedEncodingException e)
        {
            throw ManagementException.createInternalServerErrorManagementException("UTF8 encoding is unsupported", e);
        }
    }

    public static String getParameter(String name, Map<String, List<String>> parameters)
    {
        List<String> values = parameters.get(name);
        return values == null || values.isEmpty() ? null : values.get(0);
    }

    public static int getIntParameterFromRequest(final Map<String, List<String>> parameters,
                                                 final String parameterName,
                                                 final int defaultValue)
    {
        int intValue = defaultValue;
        final String stringValue = getParameter(parameterName, parameters);
        if (stringValue != null)
        {
            try
            {
                intValue = Integer.parseInt(stringValue);
            }
            catch (NumberFormatException e)
            {
                // noop
            }
        }
        return intValue;
    }
}
