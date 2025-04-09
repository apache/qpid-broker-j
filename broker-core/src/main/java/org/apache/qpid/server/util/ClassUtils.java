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

package org.apache.qpid.server.util;

import java.util.Objects;

public final class ClassUtils
{
    private ClassUtils()
    {

    }

    private static final Double DOUBLE_DEFAULT = 0d;
    private static final Float FLOAT_DEFAULT = 0f;

    public static <T> T defaultValue(final Class<T> type)
    {
        Objects.requireNonNull(type);
        if (type.isPrimitive())
        {
            if (boolean.class == type)
            {
                return (T) Boolean.FALSE;
            }
            else if (char.class == type)
            {
                return (T) Character.valueOf('\0');
            }
            else if (byte.class == type)
            {
                return (T) Byte.valueOf((byte) 0);
            }
            else if (short.class == type)
            {
                return (T) Short.valueOf((short) 0);
            }
            else if (int.class == type)
            {
                return (T) Integer.valueOf(0);
            }
            else if (long.class == type)
            {
                return (T) Long.valueOf(0L);
            }
            else if (float.class == type)
            {
                return (T) FLOAT_DEFAULT;
            }
            else if (double.class == type)
            {
                return (T) DOUBLE_DEFAULT;
            }
        }
        return null;
    }
}
