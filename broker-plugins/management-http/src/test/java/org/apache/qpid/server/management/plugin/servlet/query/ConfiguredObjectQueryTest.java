/*
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

package org.apache.qpid.server.management.plugin.servlet.query;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.test.utils.QpidTestCase;

public class ConfiguredObjectQueryTest extends QpidTestCase
{
    private static final String NUMBER_ATTR = "numberAttr";
    private static final String DATE_ATTR = "dateAttr";

    private ConfiguredObjectQuery _query;

    public void testSingleResultNoClauses() throws Exception
    {
        final List<ConfiguredObject<?>> objects = new ArrayList<>();

        final UUID objectUuid = UUID.randomUUID();
        final String objectName = "obj1";

        ConfiguredObject obj1 = createCO(new HashMap<String, Object>()
        {{
            put(ConfiguredObject.ID, objectUuid);
            put(ConfiguredObject.NAME, objectName);
        }});

        objects.add(obj1);

        _query = new ConfiguredObjectQuery(objects, null, null);

        final List<String> headers = _query.getHeaders();
        assertEquals("Unexpected headers", Lists.newArrayList(ConfiguredObject.ID, ConfiguredObject.NAME), headers);

        List<List<Object>> results = _query.getResults();
        assertEquals("Unexpected number of results", 1, results.size());

        List<Object> row = results.iterator().next();
        assertEquals("Unexpected row", Lists.newArrayList(objectUuid, objectName), row);
    }

    public void testTwoResultNoClauses() throws Exception
    {
        final List<ConfiguredObject<?>> objects = new ArrayList<>();

        final UUID object1Uuid = UUID.randomUUID();
        final String object1Name = "obj1";

        final UUID object2Uuid = UUID.randomUUID();
        final String object2Name = "obj2";

        ConfiguredObject obj1 = createCO(new HashMap<String, Object>()
        {{
            put(ConfiguredObject.ID, object1Uuid);
            put(ConfiguredObject.NAME, object1Name);
        }});

        ConfiguredObject obj2 = createCO(new HashMap<String, Object>()
        {{
            put(ConfiguredObject.ID, object2Uuid);
            put(ConfiguredObject.NAME, object2Name);
        }});

        objects.add(obj1);
        objects.add(obj2);

        _query = new ConfiguredObjectQuery(objects, null, null);

        List<List<Object>> results = _query.getResults();
        assertEquals("Unexpected number of results", 2, results.size());

        final Iterator<List<Object>> iterator = results.iterator();
        List<Object> row1 = iterator.next();
        assertEquals("Unexpected row", Lists.newArrayList(object1Uuid, object1Name), row1);

        List<Object> row2 = iterator.next();
        assertEquals("Unexpected row", Lists.newArrayList(object2Uuid, object2Name), row2);
    }

    public void testQuery_StringEquality() throws Exception
    {
        final List<ConfiguredObject<?>> objects = new ArrayList<>();

        final UUID objectUuid = UUID.randomUUID();
        final String objectName = "obj2";

        ConfiguredObject nonMatch = createCO(new HashMap<String, Object>()
        {{
            put(ConfiguredObject.ID, UUID.randomUUID());
            put(ConfiguredObject.NAME, "obj1");
        }});

        ConfiguredObject match = createCO(new HashMap<String, Object>()
        {{
            put(ConfiguredObject.ID, objectUuid);
            put(ConfiguredObject.NAME, objectName);
        }});

        objects.add(nonMatch);
        objects.add(match);

        _query = new ConfiguredObjectQuery(objects, null, String.format("name = '%s'", objectName));

        final List<String> headers = _query.getHeaders();
        assertEquals("Unexpected headers", Lists.newArrayList(ConfiguredObject.ID, ConfiguredObject.NAME), headers);

        List<List<Object>> results = _query.getResults();
        assertEquals("Unexpected number of results", 1, results.size());

        final Iterator<List<Object>> iterator = results.iterator();
        List<Object> row = iterator.next();
        assertEquals("Unexpected row", objectUuid, row.get(0));
    }

    public void testQuery_DateInequality() throws Exception
    {
        final List<ConfiguredObject<?>> objects = new ArrayList<>();

        final long now = System.currentTimeMillis();
        final UUID objectUuid = UUID.randomUUID();
        final long oneDayInMillis = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
        final Date yesterday = new Date(now - oneDayInMillis);
        final Date tomorrow = new Date(now + oneDayInMillis);

        ConfiguredObject nonMatch = createCO(new HashMap<String, Object>()
        {{
            put(ConfiguredObject.ID, UUID.randomUUID());
            put(DATE_ATTR, yesterday);
        }});

        ConfiguredObject match = createCO(new HashMap<String, Object>()
        {{
            put(ConfiguredObject.ID, objectUuid);
            put(DATE_ATTR, tomorrow);
        }});

        objects.add(nonMatch);
        objects.add(match);

        _query = new ConfiguredObjectQuery(objects,
                                           String.format("%s,%s", ConfiguredObject.ID, DATE_ATTR),
                                           String.format("%s > NOW()", DATE_ATTR));

        List<List<Object>> results = _query.getResults();
        assertEquals("Unexpected number of results", 1, results.size());

        final Iterator<List<Object>> iterator = results.iterator();
        List<Object> row = iterator.next();
        assertEquals("Unexpected row", objectUuid, row.get(0));
    }

    public void testQuery_DateEquality() throws Exception
    {
        final List<ConfiguredObject<?>> objects = new ArrayList<>();

        final long now = System.currentTimeMillis();
        final Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(now);

        final UUID objectUuid = UUID.randomUUID();

        ConfiguredObject nonMatch = createCO(new HashMap<String, Object>()
        {{
            put(ConfiguredObject.ID, UUID.randomUUID());
            put(DATE_ATTR, new Date(0));
        }});

        ConfiguredObject match = createCO(new HashMap<String, Object>()
        {{
            put(ConfiguredObject.ID, objectUuid);
            put(DATE_ATTR, new Date(now));
        }});

        objects.add(nonMatch);
        objects.add(match);

        _query = new ConfiguredObjectQuery(objects,
                                           String.format("%s,%s", ConfiguredObject.ID, DATE_ATTR),
                                           String.format("%s = TO_DATE('%s')", DATE_ATTR, DatatypeConverter.printDateTime(calendar)));

        List<List<Object>> results = _query.getResults();
        assertEquals("Unexpected number of results", 1, results.size());

        final Iterator<List<Object>> iterator = results.iterator();
        List<Object> row = iterator.next();
        assertEquals("Unexpected row", objectUuid, row.get(0));
    }

    private ConfiguredObject createCO(final HashMap<String, Object> map)
    {
        ConfiguredObject object = mock(ConfiguredObject.class);

        Map<String, Object> orderedMap = Maps.newTreeMap();
        orderedMap.putAll(map);

        when(object.getAttributeNames()).thenReturn(orderedMap.keySet());
        for(String attributeName : orderedMap.keySet())
        {
            when(object.getAttribute(attributeName)).thenReturn(orderedMap.get(attributeName));
        }
        return object;
    }
}