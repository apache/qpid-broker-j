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
package org.apache.qpid.server.management.amqp;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

class ManagementInputConverter
{
    private final ManagementNode _managementNode;

    ManagementInputConverter(final ManagementNode managementNode)
    {
        _managementNode = managementNode;
    }

    <T> T convert(Class<T> clazz, Object input)
    {
        if (input == null)
        {
            return null;
        }
        else
        {
            if (clazz.isAssignableFrom(input.getClass()))
            {
                return (T) input;
            }
            else
            {
                if (Number.class.isAssignableFrom(clazz))
                {
                    if (input instanceof Number)
                    {
                        return convertToNumberClass(clazz, (Number) input);
                    }
                    else if (input instanceof String)
                    {
                        return convertToNumberClass(clazz, (String) input);
                    }
                    else
                    {
                        throw new IllegalArgumentException("Do not know how to convert type " + input.getClass().getName() + " to a number");
                    }
                }
                else if(String.class == clazz)
                {
                    return (T) input.toString();
                }
                else if(Collection.class.isAssignableFrom(clazz) || Map.class.isAssignableFrom(clazz))
                {
                    ObjectMapper objectMapper = new ObjectMapper();
                    try
                    {
                        return objectMapper.readValue(input.toString(), clazz);
                    }
                    catch (IOException e)
                    {
                        throw new IllegalArgumentException("Cannot convert String '"
                                                           + input.toString() + "' to a " + clazz.getSimpleName());
                    }
                }
                else
                {
                    throw new IllegalArgumentException("Do not know how to convert to type " + clazz.getSimpleName());
                }
            }
        }
    }

    private <N> N convertToNumberClass(final Class<N> clazz, final Number number)
    {
        if (clazz == Byte.class || clazz == Byte.TYPE)
        {
            return (N) new Byte(number.byteValue());
        }
        else if (clazz == Short.class || clazz == Short.TYPE)
        {
            return (N) new Short(number.shortValue());
        }
        else if (clazz == Integer.class || clazz == Integer.TYPE)
        {
            return (N) new Integer(number.intValue());
        }
        else if (clazz == Long.class || clazz == Long.TYPE)
        {
            return (N) new Long(number.longValue());
        }
        else if (clazz == Float.class || clazz == Float.TYPE)
        {
            return (N) new Float(number.floatValue());
        }
        else if (clazz == Double.class || clazz == Double.TYPE)
        {
            return (N) new Double(number.doubleValue());
        }
        else
        {
            throw new IllegalArgumentException("Do not know how to convert to type " + clazz.getSimpleName());
        }
    }

    private <N> N convertToNumberClass(final Class<N> clazz, final String number)
    {
        if (clazz == Byte.class || clazz == Byte.TYPE)
        {
            return (N) Byte.valueOf(number);
        }
        else if (clazz == Short.class || clazz == Short.TYPE)
        {
            return (N) Short.valueOf(number);
        }
        else if (clazz == Integer.class || clazz == Integer.TYPE)
        {
            return (N) Integer.valueOf(number);
        }
        else if (clazz == Long.class || clazz == Long.TYPE)
        {
            return (N) Long.valueOf(number);
        }
        else if (clazz == Float.class || clazz == Float.TYPE)
        {
            return (N) Float.valueOf(number);
        }
        else if (clazz == Double.class || clazz == Double.TYPE)
        {
            return (N) Double.valueOf(number);
        }
        else
        {
            throw new IllegalArgumentException("Do not know how to convert to type " + clazz.getSimpleName());
        }
    }
}
