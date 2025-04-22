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

package org.apache.qpid.server.message.mimecontentconverter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.plugin.QpidServiceLoader;

public class MimeContentConverterRegistry
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MimeContentConverterRegistry.class);
    private static final String SEQUENCED_MAP = "java.util.SequencedMap";
    private static final Map<String, MimeContentToObjectConverter> _mimeContentToObjectConverters;
    private static final Map<Class, List<ObjectToMimeContentConverter>> _classToMimeContentConverters;

    static
    {
        _classToMimeContentConverters = buildClassToMimeConverters();
        _mimeContentToObjectConverters = buildMimeContentToObjectMap();
    }

    private MimeContentConverterRegistry() { }

    private static Map<Class, List<ObjectToMimeContentConverter>> buildClassToMimeConverters()
    {
        Map<Class, List<ObjectToMimeContentConverter>> classToMineConverters = new HashMap<>();
        Iterable<ObjectToMimeContentConverter> objectToMimeContentConverters = new QpidServiceLoader().instancesOf(ObjectToMimeContentConverter.class);
        for(ObjectToMimeContentConverter converter : objectToMimeContentConverters)
        {
            Class objectClass = converter.getObjectClass();
            classToMineConverters.computeIfAbsent(objectClass, key -> new ArrayList<>()).add(converter);
            for(ObjectToMimeContentConverter existing : classToMineConverters.get(objectClass))
            {
                if (existing.getRank() == converter.getRank())
                {
                    LOGGER.warn("MIME converter for object class {} has two or more implementations"
                                + " with the same rank {}. It is undefined which one will be used."
                                + " Implementations are: {} {} ",
                                existing.getObjectClass().getName(),
                                existing.getRank(),
                                existing.getClass().getName(),
                                converter.getClass().getName());
                }
            }
        }
        classToMineConverters.put(Void.class, List.of(new IdentityConverter()));
        return Map.copyOf(classToMineConverters);
    }

    private static Map<String, MimeContentToObjectConverter> buildMimeContentToObjectMap()
    {
        final Map<String, MimeContentToObjectConverter> mimeContentToObjectConverters = new HashMap<>();
        for(MimeContentToObjectConverter converter : (new QpidServiceLoader()).instancesOf(MimeContentToObjectConverter.class))
        {
            final String mimeType = converter.getMimeType();
            final MimeContentToObjectConverter existing = mimeContentToObjectConverters.put(mimeType, converter);
            if (existing != null)
            {
                LOGGER.warn("MIME converter {} for mime type '{}' replaced by {}.",
                            existing.getClass().getName(),
                            existing.getMimeType(),
                            converter.getClass().getName());
            }

        }
        return Collections.unmodifiableMap(mimeContentToObjectConverters);
    }

    public static MimeContentToObjectConverter getMimeContentToObjectConverter(String mimeType)
    {
        return _mimeContentToObjectConverters.get(mimeType);
    }

    public static ObjectToMimeContentConverter getBestFitObjectToMimeContentConverter(Object object)
    {
        if (object == null)
        {
            return null;
        }

        final List<Class<?>> classes = Stream.of(object.getClass().getInterfaces())
                // Java 21 compatibility fix
                .flatMap(anInterface -> SEQUENCED_MAP.equals(anInterface.getName()) ? Stream.of(anInterface.getInterfaces()) : Stream.of(anInterface))
                .collect(Collectors.toList());
        classes.add(object.getClass());

        return findBestConverter(object, classes.stream().flatMap(type ->
                _classToMimeContentConverters.getOrDefault(type, List.of()).stream()));
    }

    public static ObjectToMimeContentConverter getBestFitObjectToMimeContentConverter(Object object, Class<?> typeHint)
    {
        if (typeHint == null)
        {
            return null;
        }
        return findBestConverter(object, _classToMimeContentConverters.getOrDefault(typeHint, List.of()).stream());
    }

    private static ObjectToMimeContentConverter findBestConverter(final Object object,
                                                                  final Stream<ObjectToMimeContentConverter> converters)
    {
        return converters
                .filter(candidate -> candidate.isAcceptable(object))
                .max(Comparator.comparing(ObjectToMimeContentConverter::getRank))
                .orElse(null);
    }
}
