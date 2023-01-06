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
package org.apache.qpid.disttest.client.property;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class PropertyValueFactoryTest extends UnitTestBase
{
    private PropertyValueFactory _factory;

    @BeforeEach
    public void setUp() throws Exception
    {
        _factory = new PropertyValueFactory();
    }

    @Test
    public void testCreateListPropertyValue()
    {
        PropertyValue propertyValue = _factory.createPropertyValue("list");
        assertNotNull(propertyValue, "List generator is not created");
        final boolean condition = propertyValue instanceof ListPropertyValue;
        assertTrue(condition, "Unexpected type of list generator");
    }

    @Test
    public void testCreateRangePropertyValue()
    {
        PropertyValue propertyValue = _factory.createPropertyValue("range");
        assertNotNull(propertyValue, "Range generator is not created");
        final boolean condition = propertyValue instanceof RangePropertyValue;
        assertTrue(condition, "Unexpected type of range generator");
    }

    @Test
    public void testCreateRandomPropertyValue()
    {
        PropertyValue propertyValue = _factory.createPropertyValue("random");
        assertNotNull(propertyValue, "Random generator is not created");
        final boolean condition = propertyValue instanceof RandomPropertyValue;
        assertTrue(condition, "Unexpected type of range generator");
    }

    @Test
    public void testCreateSimplePropertyValue()
    {
        PropertyValue propertyValue = _factory.createPropertyValue("simple");
        assertNotNull(propertyValue, "Simple property value is not created");
        final boolean condition = propertyValue instanceof SimplePropertyValue;
        assertTrue(condition, "Unexpected type of property value");
    }

    @Test
    public void testCreateNonExistingPropertyValue()
    {
        try
        {
            _factory.createPropertyValue("nonExisting");
            fail("Non existing property value should not be created");
        }
        catch (Exception e)
        {
            // pass
        }
    }
}
