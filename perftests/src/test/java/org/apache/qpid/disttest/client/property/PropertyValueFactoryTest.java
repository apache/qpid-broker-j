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

import org.junit.Assert;

import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PropertyValueFactoryTest extends UnitTestBase
{
    private PropertyValueFactory _factory;

    @Before
    public void setUp() throws Exception
    {
        _factory = new PropertyValueFactory();
    }

    @Test
    public void testCreateListPropertyValue()
    {
        PropertyValue propertyValue = _factory.createPropertyValue("list");
        assertNotNull("List generator is not created", propertyValue);
        final boolean condition = propertyValue instanceof ListPropertyValue;
        assertTrue("Unexpected type of list generator", condition);
    }

    @Test
    public void testCreateRangePropertyValue()
    {
        PropertyValue propertyValue = _factory.createPropertyValue("range");
        assertNotNull("Range generator is not created", propertyValue);
        final boolean condition = propertyValue instanceof RangePropertyValue;
        assertTrue("Unexpected type of range generator", condition);
    }

    @Test
    public void testCreateRandomPropertyValue()
    {
        PropertyValue propertyValue = _factory.createPropertyValue("random");
        assertNotNull("Random generator is not created", propertyValue);
        final boolean condition = propertyValue instanceof RandomPropertyValue;
        assertTrue("Unexpected type of range generator", condition);
    }

    @Test
    public void testCreateSimplePropertyValue()
    {
        PropertyValue propertyValue = _factory.createPropertyValue("simple");
        assertNotNull("Simple property value is not created", propertyValue);
        final boolean condition = propertyValue instanceof SimplePropertyValue;
        assertTrue("Unexpected type of property value", condition);
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
