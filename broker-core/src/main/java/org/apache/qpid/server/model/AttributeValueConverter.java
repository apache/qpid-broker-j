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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Defaults;

import org.apache.qpid.server.model.preferences.GenericPrincipal;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.util.Strings;

abstract class AttributeValueConverter<T>
{
    static final AttributeValueConverter<String> STRING_CONVERTER = new AttributeValueConverter<String>()
    {
        @Override
        public String convert(final Object value, final ConfiguredObject object)
        {
            return value == null ? null : AbstractConfiguredObject.interpolate(object, value.toString());
        }
    };

    private static final DateTimeFormatter ISO_DATE_TIME_FORMAT =  new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .optionalStart()
                .appendLiteral('T')
                .append(DateTimeFormatter.ISO_LOCAL_TIME)
                .optionalStart()
                .appendOffsetId()
                .optionalStart()
                .appendLiteral('[')
                .parseCaseSensitive()
                .appendZoneRegionId()
                .appendLiteral(']')
                .toFormatter()
                .withChronology(IsoChronology.INSTANCE);

    static final AttributeValueConverter<Object> OBJECT_CONVERTER = new AttributeValueConverter<Object>()
    {
        @Override
        public Object convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof String)
            {
                return AbstractConfiguredObject.interpolate(object, (String) value);
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                return value;
            }
        }
    };
    static final AttributeValueConverter<UUID> UUID_CONVERTER = new AttributeValueConverter<UUID>()
    {
        @Override
        public UUID convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof UUID)
            {
                return (UUID) value;
            }
            else if(value instanceof String)
            {
                return UUID.fromString(AbstractConfiguredObject.interpolate(object, (String) value));
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a UUID");
            }
        }
    };

    static final AttributeValueConverter<URI> URI_CONVERTER = new AttributeValueConverter<URI>()
    {
        @Override
        URI convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof URI)
            {
                return (URI) value;
            }
            else if(value instanceof String)
            {
                return URI.create(AbstractConfiguredObject.interpolate(object, (String) value));
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a URI");
            }
        }
    };

    static final AttributeValueConverter<byte[]> BINARY_CONVERTER = new AttributeValueConverter<byte[]>()
    {
        @Override
        byte[] convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof byte[])
            {
                return (byte[]) value;
            }
            else if(value == null)
            {
                return null;
            }
            else if(value instanceof String)
            {
                String interpolated = AbstractConfiguredObject.interpolate(object,
                                                                          (String) value);
                return Strings.decodeBase64(interpolated);

            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a byte[]");
            }
        }
    };

    public static final Pattern BASE64_PATTERN = Pattern.compile("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$");

    static final AttributeValueConverter<Certificate> CERTIFICATE_CONVERTER = new AttributeValueConverter<Certificate>()
    {
        @Override
        public Certificate convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Certificate)
            {
                return (Certificate) value;
            }
            else if(value instanceof byte[])
            {
                try(ByteArrayInputStream is = new ByteArrayInputStream((byte[])value))
                {
                    return SSLUtil.getCertificateFactory().generateCertificate(is);
                }
                catch (IOException | CertificateException e)
                {
                    throw new IllegalArgumentException(e);
                }
            }
            else if(value instanceof String)
            {
                String strValue = AbstractConfiguredObject.interpolate(object, (String) value);
                if (BASE64_PATTERN.matcher(strValue).matches())
                {
                    byte[] certificateBytes = BINARY_CONVERTER.convert(strValue, object);
                    return convert(certificateBytes, object);
                }
                else
                {
                    return convert(strValue.getBytes(StandardCharsets.UTF_8), object);
                }
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a Certificate");
            }
        }
    };

    static final AttributeValueConverter<Long> LONG_CONVERTER = new AttributeValueConverter<Long>()
    {

        @Override
        public Long convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Long)
            {
                return (Long) value;
            }
            else if(value instanceof Number)
            {
                return ((Number) value).longValue();
            }
            else if(value instanceof String)
            {
                String interpolated = AbstractConfiguredObject.interpolate(object, (String) value);
                try
                {
                    return Long.valueOf(interpolated);
                }
                catch(NumberFormatException e)
                {
                    throw new IllegalArgumentException("Cannot convert string '" + interpolated + "' to a long integer",e);
                }
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a Long");
            }
        }
    };
    static final AttributeValueConverter<Integer> INT_CONVERTER = new AttributeValueConverter<Integer>()
    {

        @Override
        public Integer convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Integer)
            {
                return (Integer) value;
            }
            else if(value instanceof Number)
            {
                return ((Number) value).intValue();
            }
            else if(value instanceof String)
            {
                String interpolated = AbstractConfiguredObject.interpolate(object, (String) value);
                try
                {
                    return Integer.valueOf(interpolated);
                }
                catch(NumberFormatException e)
                {
                    throw new IllegalArgumentException("Cannot convert string '" + interpolated + "' to an integer",e);
                }
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to an Integer");
            }
        }
    };
    static final AttributeValueConverter<Short> SHORT_CONVERTER = new AttributeValueConverter<Short>()
    {

        @Override
        public Short convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Short)
            {
                return (Short) value;
            }
            else if(value instanceof Number)
            {
                return ((Number) value).shortValue();
            }
            else if(value instanceof String)
            {
                String interpolated = AbstractConfiguredObject.interpolate(object, (String) value);
                try
                {
                    return Short.valueOf(interpolated);
                }
                catch(NumberFormatException e)
                {
                    throw new IllegalArgumentException("Cannot convert string '" + interpolated + "' to a short integer",e);
                }
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a Short");
            }
        }
    };

    static final AttributeValueConverter<Double> DOUBLE_CONVERTER = new AttributeValueConverter<Double>()
    {

        @Override
        public Double convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Double)
            {
                return (Double) value;
            }
            else if(value instanceof Number)
            {
                return ((Number) value).doubleValue();
            }
            else if(value instanceof String)
            {
                String interpolated = AbstractConfiguredObject.interpolate(object, (String) value);
                try
                {
                    return Double.valueOf(interpolated);
                }
                catch(NumberFormatException e)
                {
                    throw new IllegalArgumentException("Cannot convert string '" + interpolated + "' to a Double",e);
                }
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a Double");
            }
        }
    };

    static final AttributeValueConverter<Boolean> BOOLEAN_CONVERTER = new AttributeValueConverter<Boolean>()
    {

        @Override
        public Boolean convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Boolean)
            {
                return (Boolean) value;
            }
            else if(value instanceof String)
            {
                return Boolean.valueOf(AbstractConfiguredObject.interpolate(object, (String) value));
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a Boolean");
            }
        }
    };
    static final AttributeValueConverter<List> LIST_CONVERTER = new AttributeValueConverter<List>()
    {
        @Override
        public List convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof List)
            {
                return Collections.unmodifiableList((List) value);
            }
            else if(value instanceof Object[])
            {
                return convert(Arrays.asList((Object[]) value),object);
            }
            else if(value instanceof String)
            {
                return Collections.unmodifiableList(convertFromJson((String) value, object, List.class));
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a List");
            }
        }
    };

    static final AttributeValueConverter<Set> SET_CONVERTER = new AttributeValueConverter<Set>()
    {
        @Override
        public Set convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Set)
            {
                return Collections.unmodifiableSet((Set) value);
            }

            else if(value instanceof Object[])
            {
                return convert(new HashSet(Arrays.asList((Object[])value)),object);
            }
            else if(value instanceof String)
            {
                return Collections.unmodifiableSet(convertFromJson((String) value, object, Set.class));
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a Set");
            }
        }
    };
    static final AttributeValueConverter<Collection>
            COLLECTION_CONVERTER = new AttributeValueConverter<Collection>()
    {
        @Override
        public Collection convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Collection)
            {
                return Collections.unmodifiableCollection((Collection) value);
            }
            else if(value instanceof Object[])
            {
                return convert(Arrays.asList((Object[]) value), object);
            }
            else if(value instanceof String)
            {
                return Collections.unmodifiableCollection(convertFromJson((String) value, object, Collection.class));
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a Collection");
            }
        }
    };

    static final AttributeValueConverter<Map> MAP_CONVERTER = new AttributeValueConverter<Map>()
    {
        @Override
        public Map convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Map)
            {
                Map<Object,Object> originalMap = (Map) value;
                Map resolvedMap = new LinkedHashMap(originalMap.size());
                for(Map.Entry<Object,Object> entry : originalMap.entrySet())
                {
                    Object key = entry.getKey();
                    Object val = entry.getValue();
                    resolvedMap.put(key instanceof String ? AbstractConfiguredObject.interpolate(object, (String) key) : key,
                                    val instanceof String ? AbstractConfiguredObject.interpolate(object, (String) val) : val);
                }
                return Collections.unmodifiableMap(resolvedMap);
            }
            else if(value == null)
            {
                return null;
            }
            else if(value instanceof String)
            {
                return Collections.unmodifiableMap(convertFromJson((String) value, object, Map.class));
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a Map");
            }
        }

    };

    static final AttributeValueConverter<Date> DATE_CONVERTER = new AttributeValueConverter<Date>()
    {

        @Override
        public Date convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Date)
            {
                return (Date) value;
            }
            else if(value instanceof Number)
            {
                return new Date(((Number) value).longValue());
            }
            else if(value instanceof String)
            {
                String interpolated = AbstractConfiguredObject.interpolate(object, (String) value);

                try
                {
                    return new Date(Long.valueOf(interpolated));
                }
                catch(NumberFormatException e)
                {
                    try
                    {
                        return ISO_DATE_TIME_FORMAT.parse(interpolated)
                                .query(this::convertToDate);
                    }
                    catch (DateTimeParseException e1)
                    {
                        throw new IllegalArgumentException("Cannot convert string '" + interpolated + "' to a Date."
                                                           + " It is neither a ISO-8601 date or date time nor a string"
                                                           + " containing a numeric value.");
                    }
                }
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a Date");
            }
        }

        private Date convertToDate(TemporalAccessor t)
        {
            if(!t.isSupported(ChronoField.INSTANT_SECONDS))
            {
                t = LocalDateTime.of(LocalDate.from(t), LocalTime.MIN).atOffset(ZoneOffset.UTC);
            }
            return new Date((t.getLong(ChronoField.INSTANT_SECONDS) * 1000L)
                             + t.getLong(ChronoField.MILLI_OF_SECOND));


        }
    };

    public static final AttributeValueConverter<Principal> PRINCIPAL_CONVERTER = new AttributeValueConverter<Principal>()
    {
        @Override
        public Principal convert(final Object value, final ConfiguredObject object)
        {
            if (value instanceof Principal)
            {
                return (Principal) value;
            }
            else if (value instanceof String)
            {
                String interpolated = AbstractConfiguredObject.interpolate(object, (String) value);
                return new GenericPrincipal(interpolated);
            }
            else if (value == null)
            {
                return null;
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a Principal");
            }
        }
    };

    private static <T> T convertFromJson(final String value, final ConfiguredObject object, final Class<T> valueType)
    {
        String interpolated = AbstractConfiguredObject.interpolate(object, value);
        ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            return objectMapper.readValue(interpolated, valueType);
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Cannot convert String '"
                  + value + "'"
                  + (value.equals(interpolated)
                               ? "" : (" (interpolated to '" + interpolated + "')"))
                                       + " to a " + valueType.getSimpleName());
        }
    }

    static <X> AttributeValueConverter<X> getConverter(final Class<X> type, final Type returnType)
    {
        if(type == String.class)
        {
            return (AttributeValueConverter<X>) STRING_CONVERTER;
        }
        else if(type == Integer.class)
        {
            return (AttributeValueConverter<X>) INT_CONVERTER;
        }
        else if(type == Short.class)
        {
            return (AttributeValueConverter<X>) SHORT_CONVERTER;
        }
        else if(type == Long.class)
        {
            return (AttributeValueConverter<X>) LONG_CONVERTER;
        }
        else if(type == Double.class)
        {
            return (AttributeValueConverter<X>) DOUBLE_CONVERTER;
        }
        else if(type == Boolean.class)
        {
            return (AttributeValueConverter<X>) BOOLEAN_CONVERTER;
        }
        else if(type == Date.class)
        {
            return (AttributeValueConverter<X>) DATE_CONVERTER;
        }
        else if(type == UUID.class)
        {
            return (AttributeValueConverter<X>) UUID_CONVERTER;
        }
        else if(type == URI.class)
        {
            return (AttributeValueConverter<X>) URI_CONVERTER;
        }
        else if(type == byte[].class)
        {
            return (AttributeValueConverter<X>) BINARY_CONVERTER;
        }
        else if(Certificate.class.isAssignableFrom(type))
        {
            return (AttributeValueConverter<X>) CERTIFICATE_CONVERTER;
        }
        else if(Enum.class.isAssignableFrom(type))
        {
            return (AttributeValueConverter<X>) new EnumConverter((Class<? extends Enum>)type);
        }
        else if(List.class.isAssignableFrom(type))
        {
            if (returnType instanceof ParameterizedType)
            {
                Type parameterizedType = ((ParameterizedType) returnType).getActualTypeArguments()[0];
                return (AttributeValueConverter<X>) new GenericListConverter(parameterizedType);
            }
            else
            {
                return (AttributeValueConverter<X>) LIST_CONVERTER;
            }
        }
        else if(Set.class.isAssignableFrom(type))
        {
            if (returnType instanceof ParameterizedType)
            {
                Type parameterizedType = ((ParameterizedType) returnType).getActualTypeArguments()[0];
                return (AttributeValueConverter<X>) new GenericSetConverter(parameterizedType);
            }
            else
            {
                return (AttributeValueConverter<X>) SET_CONVERTER;
            }
        }
        else if(Map.class.isAssignableFrom(type))
        {
            if(returnType instanceof ParameterizedType)
            {
                Type keyType = ((ParameterizedType) returnType).getActualTypeArguments()[0];
                Type valueType = ((ParameterizedType) returnType).getActualTypeArguments()[1];

                return (AttributeValueConverter<X>) new GenericMapConverter(keyType,valueType);
            }
            else
            {
                return (AttributeValueConverter<X>) MAP_CONVERTER;
            }
        }
        else if(Collection.class.isAssignableFrom(type))
        {
            if (returnType instanceof ParameterizedType)
            {
                Type parameterizedType = ((ParameterizedType) returnType).getActualTypeArguments()[0];
                return (AttributeValueConverter<X>) new GenericCollectionConverter(parameterizedType);
            }
            else
            {
                return (AttributeValueConverter<X>) COLLECTION_CONVERTER;
            }
        }
        else if(Principal.class.isAssignableFrom(type))
        {
            return (AttributeValueConverter<X>) PRINCIPAL_CONVERTER;
        }
        else if(ConfiguredObject.class.isAssignableFrom(type))
        {
            return (AttributeValueConverter<X>) new ConfiguredObjectConverter(type);
        }
        else if(ManagedAttributeValue.class.isAssignableFrom(type))
        {
            return (AttributeValueConverter<X>) new ManageableAttributeTypeConverter(type);
        }
        else if(Object.class == type)
        {
            return (AttributeValueConverter<X>) OBJECT_CONVERTER;
        }
        throw new IllegalArgumentException("Cannot create attribute converter of type " + type.getName());
    }

    static Class<?> getTypeFromMethod(final Method m)
    {
        return convertPrimitiveToBoxed(m.getReturnType());
    }

    static Class<?> convertPrimitiveToBoxed(Class<?> type)
    {
        if(type.isPrimitive())
        {
            if(type == Boolean.TYPE)
            {
                type = Boolean.class;
            }
            else if(type == Byte.TYPE)
            {
                type = Byte.class;
            }
            else if(type == Short.TYPE)
            {
                type = Short.class;
            }
            else if(type == Integer.TYPE)
            {
                type = Integer.class;
            }
            else if(type == Long.TYPE)
            {
                type = Long.class;
            }
            else if(type == Float.TYPE)
            {
                type = Float.class;
            }
            else if(type == Double.TYPE)
            {
                type = Double.class;
            }
            else if(type == Character.TYPE)
            {
                type = Character.class;
            }
        }
        return type;
    }

    static String getNameFromMethod(final Method m, final Class<?> type)
    {
        String methodName = m.getName();
        String baseName;

        if(type == Boolean.class )
        {
            if((methodName.startsWith("get") || methodName.startsWith("has")) && methodName.length() >= 4)
            {
                baseName = methodName.substring(3);
            }
            else if(methodName.startsWith("is") && methodName.length() >= 3)
            {
                baseName = methodName.substring(2);
            }
            else
            {
                throw new IllegalArgumentException("Method name " + methodName + " does not conform to the required pattern for ManagedAttributes");
            }
        }
        else
        {
            if(methodName.startsWith("get") && methodName.length() >= 4)
            {
                baseName = methodName.substring(3);
            }
            else
            {
                throw new IllegalArgumentException("Method name " + methodName + " does not conform to the required pattern for ManagedAttributes");
            }
        }

        String name = baseName.length() == 1 ? baseName.toLowerCase() : baseName.substring(0,1).toLowerCase() + baseName.substring(1);
        name = name.replace('_','.');
        return name;
    }

    abstract T convert(Object value, final ConfiguredObject object);

    public static class GenericListConverter extends AttributeValueConverter<List>
    {

        private final AttributeValueConverter<?> _memberConverter;

        public GenericListConverter(final Type genericType)
        {
            _memberConverter = getConverter(getRawType(genericType), genericType);
        }

        @Override
        public List convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Collection)
            {
                Collection original = (Collection)value;
                List converted = new ArrayList(original.size());
                for(Object member : original)
                {
                    converted.add(_memberConverter.convert(member, object));
                }
                return Collections.unmodifiableList(converted);
            }
            else if(value instanceof Object[])
            {
                return convert(Arrays.asList((Object[])value),object);
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                if(value instanceof String)
                {
                    String interpolated = AbstractConfiguredObject.interpolate(object, (String) value);
                    ObjectMapper objectMapper = new ObjectMapper();
                    try
                    {
                        return convert(objectMapper.readValue(interpolated, List.class), object);
                    }
                    catch (IOException e)
                    {
                        // fall through to the non-JSON single object case
                    }
                }
                return "".equals(value) ? Collections.emptyList() : Collections.unmodifiableList(Collections.singletonList(_memberConverter.convert(value, object)));
            }
        }
    }

    public static class GenericSetConverter extends AttributeValueConverter<Set>
    {

        private final AttributeValueConverter<?> _memberConverter;

        public GenericSetConverter(final Type genericType)
        {
            _memberConverter = getConverter(getRawType(genericType), genericType);
        }

        @Override
        public Set convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Collection)
            {
                Collection original = (Collection)value;
                Set converted = new HashSet(original.size());
                for(Object member : original)
                {
                    converted.add(_memberConverter.convert(member, object));
                }
                return Collections.unmodifiableSet(converted);
            }
            else if(value instanceof Object[])
            {
                return convert(new HashSet(Arrays.asList((Object[])value)),object);
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                if(value instanceof String)
                {
                    String interpolated = AbstractConfiguredObject.interpolate(object, (String) value);
                    ObjectMapper objectMapper = new ObjectMapper();
                    try
                    {
                        return convert(objectMapper.readValue(interpolated, Set.class), object);
                    }
                    catch (IOException e)
                    {
                        // fall through to the non-JSON single object case
                    }
                }
                return Collections.unmodifiableSet(Collections.singleton(_memberConverter.convert(value, object)));
            }
        }
    }

    public static class GenericCollectionConverter extends AttributeValueConverter<Collection>
    {

        private final AttributeValueConverter<?> _memberConverter;

        public GenericCollectionConverter(final Type genericType)
        {
            _memberConverter = getConverter(getRawType(genericType), genericType);
        }


        @Override
        public Collection convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Collection)
            {
                Collection original = (Collection)value;
                Collection converted = new ArrayList(original.size());
                for(Object member : original)
                {
                    converted.add(_memberConverter.convert(member, object));
                }
                return Collections.unmodifiableCollection(converted);
            }
            else if(value instanceof Object[])
            {
                return convert(Arrays.asList((Object[])value),object);
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                if(value instanceof String)
                {
                    String interpolated = AbstractConfiguredObject.interpolate(object, (String) value);
                    ObjectMapper objectMapper = new ObjectMapper();
                    try
                    {
                        return convert(objectMapper.readValue(interpolated, List.class), object);
                    }
                    catch (IOException e)
                    {
                        // fall through to the non-JSON single object case
                    }
                }
                return Collections.unmodifiableCollection(Collections.singletonList(_memberConverter.convert(value, object)));
            }
        }
    }

    public static class GenericMapConverter extends AttributeValueConverter<Map>
    {

        private final AttributeValueConverter<?> _keyConverter;
        private final AttributeValueConverter<?> _valueConverter;


        public GenericMapConverter(final Type keyType, final Type valueType)
        {
            _keyConverter = getConverter(getRawType(keyType), keyType);

            _valueConverter = getConverter(getRawType(valueType), valueType);
        }


        @Override
        public Map convert(final Object value, final ConfiguredObject object)
        {
            if(value instanceof Map)
            {
                Map<?,?> original = (Map<?,?>)value;
                Map converted = new LinkedHashMap(original.size());
                for(Map.Entry<?,?> entry : original.entrySet())
                {
                    converted.put(_keyConverter.convert(entry.getKey(),object),
                                  _valueConverter.convert(entry.getValue(), object));
                }
                return Collections.unmodifiableMap(converted);
            }
            else if(value == null)
            {
                return null;
            }
            else
            {
                if(value instanceof String)
                {
                    String interpolated = AbstractConfiguredObject.interpolate(object, (String) value);
                    ObjectMapper objectMapper = new ObjectMapper();
                    try
                    {
                        return convert(objectMapper.readValue(interpolated, Map.class), object);
                    }
                    catch (IOException e)
                    {
                        // fall through to the non-JSON single object case
                    }
                }

                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a Map");
            }

        }
    }


    static final class EnumConverter<X extends Enum<X>> extends AttributeValueConverter<X>
    {
        private final Class<X> _klazz;

        private EnumConverter(final Class<X> klazz)
        {
            _klazz = klazz;
        }

        @Override
        public X convert(final Object value, final ConfiguredObject object)
        {
            if(value == null)
            {
                return null;
            }
            else if(_klazz.isInstance(value))
            {
                return (X) value;
            }
            else if(value instanceof String)
            {
                return Enum.valueOf(_klazz, AbstractConfiguredObject.interpolate(object, (String) value));
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a " + _klazz.getName());
            }
        }
    }

    static final class ConfiguredObjectConverter<X extends ConfiguredObject<X>> extends AttributeValueConverter<X>
    {
        private final Class<X> _klazz;

        private ConfiguredObjectConverter(final Class<X> klazz)
        {
            _klazz = klazz;
        }

        @Override
        public X convert(final Object value, final ConfiguredObject object)
        {
            if(value == null)
            {
                return null;
            }
            else if(_klazz.isInstance(value))
            {
                return (X) value;
            }
            else if(value instanceof UUID)
            {
                Collection<X> reachable = object.getModel().getReachableObjects(object, _klazz);
                for(X candidate : reachable)
                {
                    if(candidate.getId().equals(value))
                    {
                        return candidate;
                    }
                }
                throw new UnknownConfiguredObjectException(_klazz, (UUID)value);
            }
            else if(value instanceof String)
            {
                String valueStr = AbstractConfiguredObject.interpolate(object, (String) value);
                Collection<X> reachable = object.getModel().getReachableObjects(object, _klazz);
                for(X candidate : reachable)
                {
                    if(candidate.getName().equals(valueStr))
                    {
                        return candidate;
                    }
                }
                try
                {
                    UUID id = UUID.fromString(valueStr);
                    return convert(id, object);
                }
                catch (IllegalArgumentException e)
                {
                    throw new UnknownConfiguredObjectException(_klazz, valueStr);
                }
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a " + _klazz.getName());
            }
        }
    }

    private static Class getRawType(Type t)
    {
        if(t instanceof Class)
        {
            return (Class)t;
        }
        else if(t instanceof ParameterizedType)
        {
            return (Class)((ParameterizedType)t).getRawType();
        }
        else if(t instanceof TypeVariable)
        {
            Type[] bounds = ((TypeVariable)t).getBounds();
            if(bounds.length == 1)
            {
                return getRawType(bounds[0]);
            }
        }
        throw new ServerScopedRuntimeException("Unable to process type when constructing configuration model: " + t);
    }

    private interface ValueMethod<X extends ManagedAttributeValue>
    {
        Object getValue(X object);
    }


    private static class ConstantValueMethod<X extends ManagedAttributeValue> implements ValueMethod<X>
    {

        private final String _value;

        public ConstantValueMethod(final String derivedMethodValue)
        {
            _value = derivedMethodValue;
        }

        @Override
        public Object getValue(final X object)
        {
            return _value;
        }
    }


    private static class ObjectMethodValueMethod<X extends ManagedAttributeValue> implements ValueMethod<X>
    {
        private final Method _sourceMethod;

        public ObjectMethodValueMethod(final Method sourceMethod)
        {
            _sourceMethod = sourceMethod;
        }

        @Override
        public Object getValue(final X object)
        {
            try
            {
                return _sourceMethod.invoke(object);
            }
            catch (IllegalAccessException | InvocationTargetException e)
            {
                throw new IllegalArgumentException(e);
            }
        }
    }


    private static class StaticMethodValueMethod<X extends ManagedAttributeValue> implements ValueMethod<X>
    {
        private final Method _sourceMethod;

        public StaticMethodValueMethod(final Method sourceMethod)
        {
            _sourceMethod = sourceMethod;
        }

        @Override
        public Object getValue(final X object)
        {
            try
            {
                return _sourceMethod.invoke(null, object);
            }
            catch (IllegalAccessException | InvocationTargetException e)
            {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static final class ManageableAttributeTypeConverter<X extends ManagedAttributeValue> extends AttributeValueConverter<X>
    {
        private static final Pattern STATIC_METHOD_PATTERN = Pattern.compile("([\\w][\\w\\d_]+\\.)+[\\w][\\w\\d_\\$]*#[\\w\\d_]+\\s*\\(\\s*\\)");
        private static final Pattern OBJECT_METHOD_PATTERN = Pattern.compile("#[\\w\\d_]+\\s*\\(\\s*\\)");

        private final Class<X> _klazz;
        private final Map<Method, AttributeValueConverter<?>> _propertyConverters = new HashMap<>();
        private final Map<Method, ValueMethod<X>> _derivedValueMethod = new HashMap<>();
        private Method _factoryMethod;

        private ManageableAttributeTypeConverter(final Class<X> klazz)
        {
            _klazz = klazz;
            for(Method method : klazz.getMethods())
            {
                final String methodName = method.getName();
                if(method.getParameterTypes().length == 0
                   && !Arrays.asList(Object.class.getMethods()).contains(method)
                   && (methodName.startsWith("get") || methodName.startsWith("is") || methodName.startsWith("has")))
                {
                    _propertyConverters.put(method, AttributeValueConverter.getConverter(getTypeFromMethod(method), method.getGenericReturnType()));
                }

                final ManagedAttributeValueTypeDerivedMethod derivedMethodAnnotation =
                        method.getAnnotation(ManagedAttributeValueTypeDerivedMethod.class);
                if (derivedMethodAnnotation != null)
                {
                    String derivedMethodValue = derivedMethodAnnotation.value();
                    if (STATIC_METHOD_PATTERN.matcher(derivedMethodValue).matches())
                    {
                        try
                        {
                            String className = derivedMethodValue.split("#")[0].trim();
                            String sourceMethodName = derivedMethodValue.split("#")[1].split("\\(")[0].trim();
                            Class<?> sourceMethodClass = Class.forName(className);
                            Method sourceMethod = sourceMethodClass.getMethod(sourceMethodName, klazz);
                            _derivedValueMethod.put(method, new StaticMethodValueMethod<X>(sourceMethod));

                        }
                        catch (ClassNotFoundException | NoSuchMethodException e)
                        {
                            throw new IllegalArgumentException(e);
                        }
                    }
                    else if (OBJECT_METHOD_PATTERN.matcher(derivedMethodValue).matches())
                    {
                        try
                        {
                            final Method sourceMethod =
                                    klazz.getMethod(derivedMethodValue.substring(1, derivedMethodValue.indexOf((int) '(')));
                            _derivedValueMethod.put(method, new ObjectMethodValueMethod<X>(sourceMethod));
                        }
                        catch (NoSuchMethodException e)
                        {
                            throw new IllegalArgumentException(e);
                        }
                    }
                    else
                    {
                        _derivedValueMethod.put(method, new ConstantValueMethod<X>(derivedMethodValue));
                    }
                }

                if(method.getName().equals("newInstance")
                   && Modifier.isStatic(method.getModifiers())
                   && Modifier.isPublic(method.getModifiers())
                   && method.getReturnType().equals(klazz)
                   && method.getParameterCount()==1
                   && method.getParameterTypes()[0].equals(klazz))
                {
                    _factoryMethod = method;
                }
            }

        }

        @Override
        X convert(final Object value, final ConfiguredObject object)
        {
            if(value == null)
            {
                return null;
            }
            else if(_klazz.isInstance(value))
            {
                return (X) value;
            }
            else if(value instanceof Map)
            {
                @SuppressWarnings("unchecked")
                X proxyObject =
                        (X) Proxy.newProxyInstance(_klazz.getClassLoader(), new Class[]{_klazz}, new InvocationHandler()
                        {
                            @Override
                            public Object invoke(final Object proxy, final Method method, final Object[] args)
                                    throws Throwable
                            {
                                AttributeValueConverter<?> converter = _propertyConverters.get(method);
                                Map map = (Map) value;
                                if (converter != null)
                                {
                                    return convertValue(map, converter, method, proxy);
                                }
                                else if ("toString".equals(method.getName()) && method.getParameterTypes().length == 0)
                                {
                                    return _klazz.getSimpleName() + " : " + map.toString();
                                }
                                else if ("hashCode".equals(method.getName()) && method.getParameterTypes().length == 0)
                                {
                                    return map.hashCode();
                                }
                                else if ("equals".equals(method.getName())
                                         && method.getParameterTypes().length == 1
                                         && method.getParameterTypes()[0] == Object.class)
                                {
                                    if (_klazz.isInstance(args[0]))
                                    {
                                        for (Map.Entry<Method, AttributeValueConverter<?>> entry : _propertyConverters.entrySet())
                                        {
                                            AttributeValueConverter<?> conv = entry.getValue();
                                            Method meth = entry.getKey();

                                            Object lvalue = convertValue(map, conv, meth, proxy);
                                            Object rvalue = meth.invoke(args[0]);
                                            if ((lvalue == null && rvalue != null) || (lvalue != null && !lvalue.equals(
                                                    rvalue)))
                                            {
                                                return false;
                                            }
                                        }
                                        return true;
                                    }
                                    else
                                    {
                                        return false;
                                    }
                                }
                                throw new UnsupportedOperationException(
                                        "The proxy class implements only attribute getters and toString(), hashCode() and equals()");
                            }

                            private Object convertValue(final Map map,
                                                        final AttributeValueConverter<?> converter,
                                                        final Method method, final Object proxy)
                            {
                                String attributeName = getNameFromMethod(method, getTypeFromMethod(method));
                                if (_derivedValueMethod.containsKey(method))
                                {
                                    return converter.convert(_derivedValueMethod.get(method).getValue((X) proxy),
                                                             object);
                                }
                                else if (map.containsKey(attributeName))
                                {
                                    return converter.convert(map.get(attributeName), object);
                                }
                                else
                                {
                                    return Defaults.defaultValue(method.getReturnType());
                                }
                            }
                        });
                if(_factoryMethod != null)
                {
                    try
                    {
                        @SuppressWarnings("unchecked")
                        X createdObject = (X) _factoryMethod.invoke(null, proxyObject);
                        return createdObject;
                    }
                    catch (IllegalAccessException | InvocationTargetException e)
                    {
                        throw new IllegalArgumentException("Cannot convert to " + _klazz.getName() + " due to error invoking factory method", e);
                    }
                }
                else
                {
                    return proxyObject;
                }

            }
            else if(value instanceof String)
            {
                String interpolated = AbstractConfiguredObject.interpolate(object, (String) value);
                if(interpolated.trim().isEmpty())
                {
                    return null;
                }
                ObjectMapper objectMapper = new ObjectMapper();
                try
                {
                    return convert(objectMapper.readValue(interpolated, Map.class), object);
                }
                catch (IOException e)
                {
                    throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a " + _klazz.getName(), e);
                }

            }
            throw new IllegalArgumentException("Cannot convert type " + value.getClass() + " to a " + _klazz.getName());
        }

    }

}
