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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
    private static final Map<Class<?>, Converter<?>> REGISTERED_PERSISTENCE_CONVERTERS = new ConcurrentHashMap<>();

    private abstract static class AbstractConverter<T> implements Converter<T>
    {
        private final Class<T> _conversionClass;


        public AbstractConverter(Class<T> conversionClass)
        {
            this(conversionClass, true, true);
        }

        public AbstractConverter(Class<T> conversionClass, boolean nonPersistenceConverter, boolean persistenceConverted)
        {
            if(nonPersistenceConverter)
            {
                REGISTERED_CONVERTERS.put(conversionClass, this);
            }
            if(persistenceConverted)
            {
                REGISTERED_PERSISTENCE_CONVERTERS.put(conversionClass, this);
            }
            _conversionClass = conversionClass;
        }

        @Override
        public final Class<T> getConversionClass()
        {
            return _conversionClass;
        }
    }

    public static Collection<Converter<?>> getConverters(final boolean forPersistence)
    {
        return forPersistence ? REGISTERED_PERSISTENCE_CONVERTERS.values() : REGISTERED_CONVERTERS.values();
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
            new ManagedAttributeValueAbstractConverter(true);


    @SuppressWarnings("unused")
    private static final Converter<ManagedAttributeValue> MANAGED_ATTRIBUTE_VALUE_PERSISTENCE_CONVERTER =
            new ManagedAttributeValueAbstractConverter(false);

    private static class ManagedAttributeValueAbstractConverter extends AbstractConverter<ManagedAttributeValue>
    {
        private final boolean _includeDerivedAttributes;

        public ManagedAttributeValueAbstractConverter(final boolean includeDerivedAttributes)
        {
            super(ManagedAttributeValue.class, includeDerivedAttributes, !includeDerivedAttributes);
            _includeDerivedAttributes = includeDerivedAttributes;
        }

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
                    if(_includeDerivedAttributes || !isDerivedMethod(method))
                    {
                        String propertyName =
                                methodName.startsWith("is") ? methodName.substring(2) : methodName.substring(3);
                        propertyName = Character.toLowerCase(propertyName.charAt(0)) + propertyName.substring(1);
                        final boolean originalAccessible = method.isAccessible();
                        try
                        {
                            if (!originalAccessible)
                            {
                                method.setAccessible(true);
                            }
                            final Object attrValue = method.invoke(value);
                            if (attrValue != null)
                            {
                                valueAsMap.put(propertyName, attrValue);
                            }
                        }
                        catch (IllegalAccessException | InvocationTargetException e)
                        {
                            throw new ServerScopedRuntimeException(String.format("Failed to access %s", propertyName), e);
                        }
                        finally
                        {
                            if (!originalAccessible)
                            {
                                method.setAccessible(originalAccessible);
                            }
                        }
                    }
                }
            }
            return valueAsMap;
        }

        private boolean isDerivedMethod(final Method method)
        {
            final boolean annotationPresent = method.isAnnotationPresent(ManagedAttributeValueTypeDerivedMethod.class);
            if(!annotationPresent)
            {

                final Class<?> clazz = method.getDeclaringClass();
                final String methodName = method.getName();
                if (isDerivedMethod(clazz, methodName))
                {
                    return true;
                }
            }
            return annotationPresent;
        }

        private boolean isDerivedMethod(final Class<?> clazz, final String methodName)
        {
            for(Method method : clazz.getDeclaredMethods())
            {
                if(method.getName().equals(methodName)
                   && method.getParameterTypes().length==0)
                {
                    if(method.isAnnotationPresent(ManagedAttributeValueTypeDerivedMethod.class))
                    {
                        return true;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            for(Class<?> iface : clazz.getInterfaces())
            {
                if(ManagedAttributeValue.class.isAssignableFrom(iface))
                {
                    if(isDerivedMethod(iface, methodName))
                    {
                        return true;
                    }
                }
            }
            if(clazz.getSuperclass() != null && ManagedAttributeValue.class.isAssignableFrom(clazz.getSuperclass()))
            {
                if(isDerivedMethod(clazz.getSuperclass(), methodName))
                {
                    return true;
                }
            }
            return false;
        }
    }

    @SuppressWarnings("unused")
    private static final Converter<Content> CONTENT_CONVERTER = new ContentConverter();

    private static class ContentConverter extends AbstractConverter<Content>
    {

        public ContentConverter()
        {
            super(Content.class, true, false);
        }

        @Override
        public Object convert(final Content content)
        {
            final byte[] resultBytes;
            try(ByteArrayOutputStream baos = new ByteArrayOutputStream())
            {
                content.write(baos);
                return baos.toByteArray();
            }
            catch (IOException e)
            {
                throw new RuntimeException(String.format(
                        "Unexpected failure whilst streaming operation content from content : %s", content));
            }
            finally
            {
                content.release();
            }
        }
    }
}
