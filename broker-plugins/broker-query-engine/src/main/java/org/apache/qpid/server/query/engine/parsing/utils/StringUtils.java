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
package org.apache.qpid.server.query.engine.parsing.utils;

/**
 * Utility class containing string handling methods
 */
public final class StringUtils
{
    /**
     * Shouldn't be instantiated directly
     */
    private StringUtils()
    {

    }

    /**
     * Returns object class name
     *
     * @param object Object to evaluate
     *
     * @return Object class name or "null"
     */
    public static String getClassName(final Object object)
    {
        if (object == null)
        {
            return "null";
        }
        return object.getClass().getSimpleName();
    }

    /**
     * Strips any of a set of characters from the start of a string.
     *
     * @param str String to remove characters from, may be null
     * @param stripChars Characters to remove, null treated as whitespace
     *
     * @return Stripped String
     */
    public static String stripStart(final String str, final String stripChars)
    {
        final int strLen = str == null ? 0 : str.length();
        if (strLen == 0)
        {
            return str;
        }
        int start = 0;
        if (stripChars == null)
        {
            while (start != strLen && Character.isWhitespace(str.charAt(start)))
            {
                start++;
            }
        }
        else
        {
            if (stripChars.isEmpty())
            {
                return str;
            }
            else
            {
                while (start != strLen && stripChars.indexOf(str.charAt(start)) != -1)
                {
                    start++;
                }
            }
        }
        return str.substring(start);
    }

    /**
     * Strips any of a set of characters from the end of a string.
     *
     * @param str String to remove characters from, may be null
     * @param stripChars The set of characters to remove, null treated as whitespace
     *
     * @return Stripped String
     */
    public static String stripEnd(final String str, final String stripChars)
    {
        int end = str == null ? 0 : str.length();
        if (end == 0)
        {
            return str;
        }
        if (stripChars == null)
        {
            while (end != 0 && Character.isWhitespace(str.charAt(end - 1)))
            {
                end--;
            }
        }
        else if (stripChars.isEmpty())
        {
            return str;
        }
        else
        {
            while (end != 0 && stripChars.indexOf(str.charAt(end - 1)) != -1)
            {
                end--;
            }
        }
        return str.substring(0, end);
    }
}
