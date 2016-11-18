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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.qpid.server.model.preferences.GenericPrincipal;
import org.apache.qpid.server.security.QpidPrincipal;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

public class ConfiguredObjectJacksonModule extends SimpleModule
{
    private static final long serialVersionUID = 1L;

    private static final ConfiguredObjectJacksonModule INSTANCE = new ConfiguredObjectJacksonModule();

    private static final Set<String> OBJECT_METHOD_NAMES = Collections.synchronizedSet(new HashSet<String>());

    static
    {
        for(Method method : Object.class.getMethods())
        {
            OBJECT_METHOD_NAMES.add(method.getName());
        }
    }

    private  ConfiguredObjectJacksonModule()
    {
        super("ConfiguredObjectSerializer", new Version(1,0,0,null, "org.apache.qpid", "broker-core"));
        addConfiguredObjectSerializer();
        addManageableAttributeTypeSerializer();
        addCertificateSerializer();
        addPrincipalSerializer();
    }

    private void addPrincipalSerializer()
    {
        final JsonSerializer<Principal> serializer = new JsonSerializer<Principal>()
        {
            @Override
            public void serialize(final Principal value, final JsonGenerator jgen, final SerializerProvider provider)
                    throws IOException, JsonGenerationException
            {
                if (value == null)
                {
                    jgen.writeNull();
                }
                else if (value instanceof QpidPrincipal)
                {
                    jgen.writeString(new GenericPrincipal((QpidPrincipal) value).toExternalForm());
                }
                else if (value instanceof GenericPrincipal)
                {
                    jgen.writeString(((GenericPrincipal) value).toExternalForm());
                }
                else
                {
                    jgen.writeString(value.getName());
                }
            }
        };
        addSerializer(Principal.class, serializer);
    }

    private void addCertificateSerializer()
    {
        final JsonSerializer<Certificate> serializer = new JsonSerializer<Certificate>()
        {
            @Override
            public void serialize(final Certificate value,
                                  final JsonGenerator jgen,
                                  final SerializerProvider provider)
                    throws IOException
            {
                try
                {
                    jgen.writeBinary(value.getEncoded());
                }
                catch (CertificateEncodingException e)
                {
                    throw new IllegalArgumentException(e);
                }
            }
        };
        addSerializer(Certificate.class, serializer);
    }

    private void addManageableAttributeTypeSerializer()
    {
        final JsonSerializer<ManagedAttributeValue> serializer = new JsonSerializer<ManagedAttributeValue>()
        {
            @Override
            public void serialize(final ManagedAttributeValue value,
                                  final JsonGenerator jgen,
                                  final SerializerProvider provider)
                    throws IOException
            {
                Map<String,Object> valueAsMap = new LinkedHashMap<>();
                for(Method method : value.getClass().getMethods())
                {
                    final String methodName = method.getName();
                    if(method.getParameterTypes().length == 0
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
                            if(attrValue != null)
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
                jgen.writeObject(valueAsMap);
            }
        };
        addSerializer(ManagedAttributeValue.class, serializer);
    }

    private void addConfiguredObjectSerializer()
    {
        final JsonSerializer<ConfiguredObject> serializer = new JsonSerializer<ConfiguredObject>()
        {
            @Override
            public void serialize(final ConfiguredObject value,
                                  final JsonGenerator jgen,
                                  final SerializerProvider provider)
                    throws IOException
            {
                jgen.writeString(value.getId().toString());
            }
        };
        addSerializer(ConfiguredObject.class, serializer);
    }

    public static ObjectMapper newObjectMapper()
    {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(INSTANCE);
        return objectMapper;
    }

}
