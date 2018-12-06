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
package org.apache.qpid.server.protocol.v1_0.codec;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public interface SpecializedDescribedType
{
    static <X extends SpecializedDescribedType> X getInvalidValue(Class<X> clazz, DescribedType value) {
        for(Method method : clazz.getMethods())
        {
            if(method.getName().equals("getInvalidValue")
               && method.getParameterCount() == 1
               && method.getParameterTypes()[0] == DescribedType.class
               && method.getReturnType() == clazz
               && (method.getModifiers() & (Modifier.STATIC | Modifier.PUBLIC)) == (Modifier.STATIC | Modifier.PUBLIC))
            {
                try
                {
                    return (X) method.invoke(null, value);
                }
                catch (IllegalAccessException | InvocationTargetException e)
                {
                    return null;
                }
            }
        }

        return null;
    }

    static <X extends SpecializedDescribedType> boolean hasInvalidValue(Class<X> clazz)
    {
        for(Method method : clazz.getMethods())
        {
            if(method.getName().equals("getInvalidValue")
               && method.getParameterCount() == 1
               && method.getParameterTypes()[0] == DescribedType.class
               && method.getReturnType() == clazz
               && (method.getModifiers() & (Modifier.STATIC | Modifier.PUBLIC)) == (Modifier.STATIC | Modifier.PUBLIC))
            {
                return true;
            }
        }
        return false;
    }
}
