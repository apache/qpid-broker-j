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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.qpid.server.plugin.QpidServiceLoader;

public class MimeContentConverterRegistry
{
    private static final Map<String, MimeContentToObjectConverter> _mimeContentToObjectConverters;
    private static final Map<Class, SortedMap<Integer, ObjectToMimeContentConverter>> _classToRankedMimeContentConverter;

    static
    {
        _classToRankedMimeContentConverter = buildClassToRankedMap();
        _mimeContentToObjectConverters = buildMimeContentToObjectMap();
    }

    private static Map<Class, SortedMap<Integer, ObjectToMimeContentConverter>> buildClassToRankedMap()
    {
        Map<Class, SortedMap<Integer, ObjectToMimeContentConverter>> classToRankedMimeContentConverter = new HashMap<>();
        for(ObjectToMimeContentConverter converter : (new QpidServiceLoader()).instancesOf(ObjectToMimeContentConverter.class))
        {
            SortedMap<Integer, ObjectToMimeContentConverter> rankMap = classToRankedMimeContentConverter.get(converter.getObjectClass());
            if (rankMap == null)
            {
                rankMap = new TreeMap<>();
                classToRankedMimeContentConverter.put(converter.getObjectClass(), rankMap);
            }
            rankMap.put(converter.getRank(), converter);
        }
        return Collections.unmodifiableMap(classToRankedMimeContentConverter);
    }

    private static Map<String, MimeContentToObjectConverter> buildMimeContentToObjectMap()
    {
        final Map<String, MimeContentToObjectConverter> mimeContentToObjectConverters = new HashMap<>();
        for(MimeContentToObjectConverter converter : (new QpidServiceLoader()).instancesOf(MimeContentToObjectConverter.class))
        {
            final String mineType = converter.getMimeType();
            mimeContentToObjectConverters.put(mineType, converter);
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
        SortedMap<Integer, ObjectToMimeContentConverter> potentialConverters = new TreeMap<>();
        final List<Class<?>> classes = new ArrayList<>(Arrays.asList(object.getClass().getInterfaces()));
        classes.add(object.getClass());
        for (Class<?> i : classes)
        {
            if (_classToRankedMimeContentConverter.get(i) != null)
            {
                SortedMap<Integer, ObjectToMimeContentConverter> ranked = new TreeMap<>(_classToRankedMimeContentConverter.get(i));
                Iterator<Map.Entry<Integer, ObjectToMimeContentConverter>> itr = ranked.entrySet().iterator();
                while (itr.hasNext())
                {
                    final Map.Entry<Integer, ObjectToMimeContentConverter> entry = itr.next();
                    if (!entry.getValue().isAcceptable(object))
                    {
                        itr.remove();
                    }
                }
                potentialConverters.putAll(ranked);
            }
        }

        return potentialConverters.isEmpty() ? null : potentialConverters.get(potentialConverters.lastKey());
    }
}
