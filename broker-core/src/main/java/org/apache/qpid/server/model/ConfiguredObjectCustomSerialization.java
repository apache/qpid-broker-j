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
package org.apache.qpid.server.model;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.qpid.server.model.preferences.GenericPrincipal;
import org.apache.qpid.server.security.QpidPrincipal;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class ConfiguredObjectCustomSerialization
{

    private static final Set<String> OBJECT_METHOD_NAMES = Collections.synchronizedSet(new HashSet<String>());

    static
    {
        for(Method method : Object.class.getMethods())
        {
            OBJECT_METHOD_NAMES.add(method.getName());
        }
    }

    public interface Converter<T>
    {
        Class<T> getConversionClass();

        Object convert(T value);
    }

    private static final Map<Class<?>, Converter<?>> REGISTERED_CONVERTERS = new ConcurrentHashMap<>();

    private abstract static class AbstractConverter<T> implements Converter<T>
    {
        private final Class<T> _conversionClass;

        public AbstractConverter(Class<T> conversionClass)
        {
            REGISTERED_CONVERTERS.put(conversionClass, this);
            _conversionClass = conversionClass;
        }

        @Override
        public final Class<T> getConversionClass()
        {
            return _conversionClass;
        }
    }

    public static Collection<Converter<?>> getConverters()
    {
        return REGISTERED_CONVERTERS.values();
    }

    @SuppressWarnings("unused")
    private static final Converter<Principal> PRINCIPAL_CONVERTER =
            new AbstractConverter<Principal>(Principal.class)
            {
                @Override
                public Object convert(final Principal value)
                {
                    if (value instanceof QpidPrincipal)
                    {
                        return new GenericPrincipal((QpidPrincipal) value).toExternalForm();
                    }
                    else if (value instanceof GenericPrincipal)
                    {
                        return ((GenericPrincipal) value).toExternalForm();
                    }
                    else
                    {
                        return value.getName();
                    }
                }
            };


    @SuppressWarnings("unused")
    private static final Converter<Certificate> CERTIFICATE_CONVERTER =
            new AbstractConverter<Certificate>(Certificate.class)
            {
                @Override
                public Object convert(final Certificate value)
                {
                    try
                    {
                        return value.getEncoded();
                    }
                    catch (CertificateEncodingException e)
                    {
                        throw new IllegalArgumentException(e);
                    }
                }
            };


    @SuppressWarnings("unused")
    private static final Converter<ConfiguredObject> CONFIGURED_OBJECT_CONVERTER =
            new AbstractConverter<ConfiguredObject>(ConfiguredObject.class)
            {
                @Override
                public Object convert(final ConfiguredObject value)
                {
                    return value.getId().toString();
                }
            };


    @SuppressWarnings("unused")
    private static final Converter<ManagedAttributeValue> MANAGED_ATTRIBUTE_VALUE_CONVERTER =
            new AbstractConverter<ManagedAttributeValue>(ManagedAttributeValue.class)
            {
                @Override
                public Object convert(final ManagedAttributeValue value)
                {

                    Map<String, Object> valueAsMap = new LinkedHashMap<>();
                    for (Method method : value.getClass().getMethods())
                    {
                        final String methodName = method.getName();
                        if (method.getParameterTypes().length == 0
                            && !OBJECT_METHOD_NAMES.contains(methodName)
                            && (methodName.startsWith("is")
                                || methodName.startsWith("has")
                                || methodName.startsWith("get")))
                        {
                            String propertyName =
                                    methodName.startsWith("is") ? methodName.substring(2) : methodName.substring(3);
                            propertyName = Character.toLowerCase(propertyName.charAt(0)) + propertyName.substring(1);
                            try
                            {
                                final Object attrValue = method.invoke(value);
                                if (attrValue != null)
                                {
                                    valueAsMap.put(propertyName, attrValue);
                                }
                            }
                            catch (IllegalAccessException | InvocationTargetException e)
                            {
                                throw new ServerScopedRuntimeException(e);
                            }
                        }
                    }
                    return valueAsMap;
                }
            };

}
