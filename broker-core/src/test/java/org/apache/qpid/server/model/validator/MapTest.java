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

package org.apache.qpid.server.model.validator;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MapTest extends UnitTestBase
{
    @Test
    public void testValidator()
    {
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        assertNotNull("Factory method has to produce a instance", Map.validator(keyValidator, valueValidator));
    }

    @Test
    public void testIsValid_ValidInput()
    {
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        assertTrue(validator.isValid(Collections.emptyMap()));

        java.util.Map<String, Integer> map = new HashMap<>();
        map.put("a", 10);
        map.put("b", 20);
        assertTrue(validator.isValid(map));
    }

    @Test
    public void testIsValid_InvalidInput()
    {
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        assertFalse(validator.isValid(17));

        java.util.Map<String, Integer> map = new HashMap<>();
        map.put("a", 12);
        map.put("b", 5);
        assertFalse(validator.isValid(map));

        map = new HashMap<>();
        map.put("a", 12);
        map.put("{}", 17);
        map.put("x", 17);
        assertFalse(validator.isValid(map));

        map = new HashMap<>();
        map.put("a", 12);
        map.put("{}", 230456);
        map.put("x", 17);

        map = new HashMap<>();
        map.put("a", 12);
        map.put("{}", 230456);
        map.put("x", 0);
        map.put("%_", 15);
        assertFalse(validator.isValid(map));
    }

    @Test
    public void testIsValid_NullAsInput()
    {
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        assertFalse(validator.isValid((java.util.Map<String, Integer>) null));
    }

    @Test
    public void testIsValid_ValidInput_MapOfSuppliers()
    {
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        assertTrue(validator.isValid(Collections.emptyMap()));

        java.util.Map<String, Supplier<Integer>> map = new HashMap<>();
        map.put("a", () -> 10);
        map.put("b", () -> 20);
        assertTrue(validator.isValid(map));
    }

    @Test
    public void testIsValid_InvalidInput_MapOfSuppliers()
    {
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        assertFalse(validator.isValid(17));

        java.util.Map<String, Supplier<Integer>> map = new HashMap<>();
        map.put("a", () -> 12);
        map.put("b", () -> 5);
        assertFalse(validator.isValid(map));
    }

    @Test
    public void testValidate_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        java.util.Map<String, Integer> map = new HashMap<>();
        map.put("a", 10);
        map.put("b", 20);
        try
        {
            validator.validate(map, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            validator.validate(Collections.emptyMap(), object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_InValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        try
        {
            validator.validate("a", object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("A map 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' is expected", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        java.util.Map<String, Integer> map = new HashMap<>();
        map.put("a", 10);
        map.put("b{}", 20);
        map.put("c", 30);
        try
        {
            validator.validate(map, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr.key' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'b{}'. Valid value pattern is: \\w+", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        map = new HashMap<>();
        map.put("a", 10);
        map.put("b", 2000);
        map.put("c", 30);
        try
        {
            validator.validate(map, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr.value' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '2000' as it is not in range [10, 100)", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidate_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        try
        {
            validator.validate((java.util.Map<String, Object>) null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("A map 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' is expected", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidate_ValidInput_MapOfSuppliers()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        java.util.Map<String, Supplier<Integer>> map = new HashMap<>();
        map.put("a", () -> 10);
        map.put("b", () -> 20);
        try
        {
            validator.validate(map, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_InValidInput_MapOfSuppliers()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        java.util.Map<String, Supplier<Integer>> map = new HashMap<>();
        map.put("a", () -> 10);
        map.put("b", () -> 2000);
        map.put("c", () -> 30);
        try
        {
            validator.validate(map, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr.value' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '2000' as it is not in range [10, 100)", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testErrorMessage()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        try
        {
            String message = validator.errorMessage("a", object, "attr");
            assertEquals("A map 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' is expected", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            String message = validator.errorMessage(Collections.singletonMap("{}", 17), object, "attr");
            assertEquals("Attribute 'attr.key' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '{}'. Valid value pattern is: \\w+", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            String message = validator.errorMessage(Collections.singletonMap("a", 1778), object, "attr");
            assertEquals("Attribute 'attr.value' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '1778' as it is not in range [10, 100)", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            validator.errorMessage(Collections.singletonMap("a", 15), object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testErrorMessage_MapOfSuppliers()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator keyValidator = Regex.validator("\\w+");
        ValueValidator valueValidator = InRange.validator(10L, 100L);
        ValueValidator validator = Map.validator(keyValidator, valueValidator);
        assertNotNull(validator);

        try
        {
            Supplier<Integer> supplier1778 = () -> 1778;
            String message = validator.errorMessage(Collections.singletonMap("a", supplier1778), object, "attr");
            assertEquals("Attribute 'attr.value' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '1778' as it is not in range [10, 100)", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }
}