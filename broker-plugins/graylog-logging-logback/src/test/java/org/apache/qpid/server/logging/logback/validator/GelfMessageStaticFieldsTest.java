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

package org.apache.qpid.server.logging.logback.validator;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class GelfMessageStaticFieldsTest extends UnitTestBase
{
    @Test
    public void testValidator()
    {
        assertNotNull("Factory method has to produce a instance", GelfMessageStaticFields.validator());
    }

    @Test
    public void testValidate_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        try
        {
            GelfMessageStaticFields.validateStaticFields(null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be 'null'", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidateKey_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        try
        {
            Map<String, Object> map = new HashMap<>();
            map.put("B", "B");
            map.put(null, "A");
            GelfMessageStaticFields.validateStaticFields(map, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Key of 'attr attribute instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be 'null'. Key pattern is: [\\w\\.\\-]+", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidateValue_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        try
        {
            Map<String, Object> map = new HashMap<>();
            map.put("B", "B");
            map.put("A", null);
            GelfMessageStaticFields.validateStaticFields(map, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Value of 'attr attribute instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be 'null', as it has to be a string or number", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidateKey_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        try
        {
            Map<String, Object> map = new HashMap<>();
            map.put("B", "AB");
            map.put("A", 234);
            GelfMessageStaticFields.validateStaticFields(map, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidateKey_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();

        try
        {
            GelfMessageStaticFields.validateStaticFields(Collections.singletonMap("{abc}", "true"), object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Key of 'attr attribute instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be '{abc}'. Key pattern is: [\\w\\.\\-]+", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidateValue_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();

        try
        {
            GelfMessageStaticFields.validateStaticFields(Collections.singletonMap("A", true), object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Value of 'attr attribute instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be 'true', as it has to be a string or number", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }
}