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
 */
package org.apache.qpid.server.security.access.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;

class ObjectPropertiesTest extends UnitTestBase
{
    @Test
    void name()
    {
        ObjectProperties properties = new ObjectProperties("name");
        assertEquals("name", properties.get(Property.NAME));

        properties = new ObjectProperties();
        properties.setName("name");
        assertEquals("name", properties.get(Property.NAME));
    }

    @Test
    void name_NullInput()
    {
        ObjectProperties properties = new ObjectProperties((String) null);
        assertEquals("", properties.get(Property.NAME));

        properties = new ObjectProperties();
        properties.setName(null);
        assertEquals("", properties.get(Property.NAME));
    }

    @Test
    void createdBy_Null()
    {
        final ObjectProperties properties = new ObjectProperties();
        properties.setCreatedBy(null);
        assertNull(properties.get(Property.CREATED_BY));
    }

    @Test
    void createdBy()
    {
        final ObjectProperties properties = new ObjectProperties();
        properties.setCreatedBy("username");
        assertEquals("username", properties.get(Property.CREATED_BY));
    }

    @Test
    void owner_Null()
    {
        final ObjectProperties properties = new ObjectProperties();
        properties.setOwner(null);
        assertNull(properties.get(Property.OWNER));
    }

    @Test
    void owner()
    {
        final ObjectProperties properties = new ObjectProperties();
        properties.setOwner("username");
        assertEquals("username", properties.get(Property.OWNER));
    }

    @Test
    void put_NullInput()
    {
        final ObjectProperties properties = new ObjectProperties();

        assertNull(properties.get(Property.ROUTING_KEY));
        properties.put(Property.ROUTING_KEY, null);
        assertEquals("", properties.get(Property.ROUTING_KEY));
    }

    @Test
    void put_EmptyString()
    {
        final ObjectProperties properties = new ObjectProperties();

        assertNull(properties.get(Property.ROUTING_KEY));
        properties.put(Property.ROUTING_KEY, null);
        assertEquals("", properties.get(Property.ROUTING_KEY));

        assertNull(properties.get(Property.QUEUE_NAME));
        properties.put(Property.QUEUE_NAME, " ");
        assertEquals("", properties.get(Property.QUEUE_NAME));
    }

    @Test
    void put()
    {
        final ObjectProperties properties = new ObjectProperties();

        assertNull(properties.get(Property.TYPE));
        properties.put(Property.TYPE, "EX ");
        assertEquals("EX", properties.get(Property.TYPE));

        assertNull(properties.get(Property.DURABLE));
        properties.put(Property.DURABLE, true);
        assertEquals(Boolean.TRUE, properties.get(Property.DURABLE));
    }

    @Test
    void description()
    {
        final ObjectProperties properties = new ObjectProperties();
        assertTrue(properties.withDescription("DES").toString().contains("DES"));
    }

    @Test
    void attributeNames_NullInput()
    {
        ObjectProperties properties = new ObjectProperties();
        assertTrue(properties.getAttributeNames().isEmpty());

        properties.addAttributeNames((String[]) null);
        assertTrue(properties.getAttributeNames().isEmpty());

        properties = new ObjectProperties();
        assertTrue(properties.getAttributeNames().isEmpty());

        properties.addAttributeNames((Collection<String>) null);
        assertTrue(properties.getAttributeNames().isEmpty());
    }

    @Test
    void attributeNames_EmptyInput()
    {
        ObjectProperties properties = new ObjectProperties();
        assertTrue(properties.getAttributeNames().isEmpty());

        properties.addAttributeNames();
        assertTrue(properties.getAttributeNames().isEmpty());

        properties = new ObjectProperties();
        assertTrue(properties.getAttributeNames().isEmpty());

        properties.addAttributeNames(Collections.emptyList());
        assertTrue(properties.getAttributeNames().isEmpty());
    }

    @Test
    void attributeNames()
    {
        ObjectProperties properties = new ObjectProperties();
        assertTrue(properties.getAttributeNames().isEmpty());

        properties.addAttributeNames("name", "host");
        assertEquals(2, properties.getAttributeNames().size());
        assertTrue(properties.getAttributeNames().contains("name"));
        assertTrue(properties.getAttributeNames().contains("host"));

        properties = new ObjectProperties();
        assertTrue(properties.getAttributeNames().isEmpty());

        properties.addAttributeNames(Collections.singleton("name"));
        assertEquals(1, properties.getAttributeNames().size());
        assertTrue(properties.getAttributeNames().contains("name"));
    }

    @Test
    void equals()
    {
        final ObjectProperties first = new ObjectProperties();
        first.addAttributeNames("name");
        first.put(Property.ROUTING_KEY, "broadcast");
        first.withDescription("desc1");

        final ObjectProperties second = new ObjectProperties();
        second.addAttributeNames("name");
        second.put(Property.ROUTING_KEY, "broadcast");
        second.withDescription("desc2");

        assertEquals(first, first);
        assertEquals(first, second);
        assertEquals(second, first);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void notEquals()
    {
        final ObjectProperties first = new ObjectProperties();
        first.addAttributeNames("name");
        first.put(Property.ROUTING_KEY, "broadcast");
        first.withDescription("desc");

        ObjectProperties another = new ObjectProperties();
        another.addAttributeNames("name");
        another.put(Property.ROUTING_KEY, "public");
        another.withDescription("desc");
        assertNotEquals(first, another);
        assertNotEquals(another, first);

        another = new ObjectProperties();
        another.addAttributeNames("host");
        another.put(Property.ROUTING_KEY, "broadcast");
        another.withDescription("desc");
        assertNotEquals(first, another);
        assertNotEquals(another, first);

        assertNotEquals("first", first);
        assertNotEquals(null, first);
    }

    @Test
    void toString_withProperties()
    {
        final ObjectProperties properties = new ObjectProperties("vName");
        final String str = properties.toString();
        assertNotNull(str);
        assertTrue(str.contains(Property.NAME.name()));
        assertTrue(str.contains("vName"));
    }

    @Test
    void toString_withAttributes()
    {
        final ObjectProperties properties = new ObjectProperties();
        properties.addAttributeNames("queue");
        final String str = properties.toString();
        assertNotNull(str);
        assertTrue(str.contains(Property.ATTRIBUTES.name()));
        assertTrue(str.contains("queue"));
    }

    @Test
    void toString_description()
    {
        final ObjectProperties properties = new ObjectProperties().withDescription("desc");
        final String str = properties.toString();
        assertNotNull(str);
        assertTrue(str.contains("desc"));
    }

    @Test
    void toString_mixing()
    {
        final ObjectProperties properties = new ObjectProperties("vName").withDescription("desc");
        properties.addAttributeNames("queue");
        final String str = properties.toString();
        assertNotNull(str);
        assertTrue(str.contains(Property.NAME.name()));
        assertTrue(str.contains("vName"));
        assertTrue(str.contains(Property.ATTRIBUTES.name()));
        assertTrue(str.contains("queue"));
        assertTrue(str.contains("desc"));
    }
}