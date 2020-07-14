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

package org.apache.qpid.server.model.validator;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RegexTest extends UnitTestBase
{

    @Test
    public void testValidator()
    {
        assertNotNull("Factory method has to produce a instance", Regex.validator("abc"));
    }

    @Test
    public void testIsValid_NullAsInput()
    {
        ValueValidator validator = Regex.validator("abc");
        assertNotNull(validator);
        assertFalse(validator.isValid((String) null));
    }

    @Test
    public void testIsValid_NullAsInput_withSupplier()
    {
        ValueValidator validator = Regex.validator("abc");
        assertNotNull(validator);

        Supplier<String> nullSupplier = () -> null;
        ValidationResult<String> result = validator.isValid(nullSupplier);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertNull(result.get());
        assertFalse(validator.isValid((Object) nullSupplier));
    }

    @Test
    public void testIsValid_ValidInput()
    {
        ValueValidator validator = Regex.validator("\\d+");
        assertNotNull(validator);
        assertTrue(validator.isValid("123"));
    }

    @Test
    public void testIsValid_ValidInput_withSupplier()
    {
        ValueValidator validator = Regex.validator("\\d+");
        assertNotNull(validator);

        Supplier<String> stringSupplier = () -> "123";
        ValidationResult<String> result = validator.isValid(stringSupplier);
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals("123", result.get());
        assertTrue(validator.isValid((Object) stringSupplier));
    }

    @Test
    public void testIsValid_InvalidInput()
    {
        ValueValidator validator = Regex.validator("\\w+");
        assertNotNull(validator);
        assertFalse(validator.isValid("{abc}"));
    }

    @Test
    public void testIsValid_InvalidInput_withSupplier()
    {
        ValueValidator validator = Regex.validator("\\w+");
        assertNotNull(validator);

        Supplier<String> stringSupplier = () -> "{abc}";
        ValidationResult<String> result = validator.isValid(stringSupplier);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals("{abc}", result.get());
        assertFalse(validator.isValid((Object) stringSupplier));
    }

    @Test
    public void testValidate_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Regex.validator("\\d+");
        assertNotNull(validator);

        try
        {
            validator.validate((String) null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null'. Valid value pattern is: \\d+", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidate_NullAsInput_withSupplier()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Regex.validator("\\d+");
        assertNotNull(validator);

        Supplier<Object> nullSupplier = () -> null;
        try
        {
            validator.validate(nullSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null'. Valid value pattern is: \\d+", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate((Object) nullSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null'. Valid value pattern is: \\d+", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidate_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Regex.validator("\\d+");
        assertNotNull(validator);

        try
        {
            validator.validate("123", object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_ValidInput_withSupplier()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Regex.validator("\\d+");
        assertNotNull(validator);

        try
        {
            Supplier<String> stringSupplier = () -> "123";
            Supplier<String> supplier = validator.validate(stringSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals("123", supplier.get());

            validator.validate((Object) stringSupplier, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Regex.validator("\\w+");
        assertNotNull(validator);

        try
        {
            validator.validate("{abc}", object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '{abc}'. Valid value pattern is: \\w+", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidate_InvalidInput_withSupplier()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Regex.validator("\\w+");
        assertNotNull(validator);

        Supplier<String> stringSupplier = () -> "{abc}";
        try
        {
            validator.validate(stringSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '{abc}'. Valid value pattern is: \\w+", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate((Object) stringSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '{abc}'. Valid value pattern is: \\w+", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidate_NumberAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Regex.validator("\\d+");
        assertNotNull(validator);

        try
        {
            validator.validate(123, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_NumberAsInput_withSupplier()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Regex.validator("\\d+");
        assertNotNull(validator);

        try
        {
            Supplier<Integer> integerSupplier = () -> 123;
            Supplier<Integer> supplier = validator.validate(integerSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals(Integer.valueOf(123), supplier.get());

            validator.validate((Object) integerSupplier, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testErrorMessage()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Regex.validator("\\w+");
        assertNotNull(validator);
        assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '{}'. Valid value pattern is: \\w+", validator.errorMessage("{}", object, "attr"));
    }

    @Test
    public void testAndThen_IsValid_InvalidInput()
    {
        ValueValidator secondaryValidator = value -> false;
        ValueValidator validator = Regex.validator("\\w+").andThen(secondaryValidator);
        assertNotNull(validator);

        assertFalse(validator.isValid("abcd"));
        assertFalse(validator.isValid("{}"));
    }

    @Test
    public void testAndThen_Validate_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator secondaryValidator = value -> false;
        ValueValidator validator = Regex.validator("\\w+").andThen(secondaryValidator);
        assertNotNull(validator);

        try
        {
            validator.validate("abcd", object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'abcd'", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate("{}", object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '{}'. Valid value pattern is: \\w+", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testAndThen_IsValid_ValidInput()
    {
        ValueValidator secondaryValidator = value -> true;
        ValueValidator validator = Regex.validator("\\w+").andThen(secondaryValidator);
        assertNotNull(validator);

        assertTrue(validator.isValid("abcd"));
    }

    @Test
    public void testAndThen_Validate_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator secondaryValidator = value -> true;
        ValueValidator validator = Regex.validator("\\w+").andThen(secondaryValidator);
        assertNotNull(validator);

        try
        {
            validator.validate("abcd", object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testAndThen_errorMessage()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator secondaryValidator = value -> false;
        ValueValidator validator = Regex.validator("\\w+").andThen(secondaryValidator);
        assertNotNull(validator);

        String message = validator.errorMessage("abcd", object, "attr");
        assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'abcd'", message);

        message = validator.errorMessage("{}", object, "attr");
        assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '{}'. Valid value pattern is: \\w+", message);
    }
}