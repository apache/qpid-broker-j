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

package org.apache.qpid.server.logging;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class CallerDataFilter
{
    private static final Set<String> METHOD_NAMES = buildMethodNames();

    private final ClassLoader _classLoader = Thread.currentThread().getContextClassLoader();

    public StackTraceElement[] filter(StackTraceElement[] elements)
    {
        if (elements == null)
        {
            return new StackTraceElement[0];
        }

        for (int depth = elements.length - 1; depth >= 0; --depth)
        {
            final StackTraceElement element = elements[depth];
            if (isMessageMethod(element.getMethodName()) && isMessageLogger(element.getClassName()))
            {
                final int length = elements.length - (depth + 1);
                if (length > 0)
                {
                    final StackTraceElement[] stackTrace = new StackTraceElement[length];
                    System.arraycopy(elements, depth + 1, stackTrace, 0, length);
                    return stackTrace;
                }
                return elements;
            }
        }
        return elements;
    }

    private boolean isMessageMethod(String method)
    {
        return METHOD_NAMES.contains(method);
    }

    private boolean isMessageLogger(String className)
    {
        try
        {
            return MessageLogger.class.isAssignableFrom(Class.forName(className, false, _classLoader));
        }
        catch (ClassNotFoundException ignored)
        {
            return false;
        }
    }

    private static Set<String> buildMethodNames()
    {
        return Arrays.stream(MessageLogger.class.getDeclaredMethods())
                .filter(method -> Void.TYPE.equals(method.getReturnType()))
                .map(Method::getName)
                .collect(Collectors.toSet());
    }
}
