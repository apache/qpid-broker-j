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

import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OptionalTest extends UnitTestBase
{
    @Test
    public void testValidator()
    {
        assertNotNull("Factory method has to produce a instance", Optional.validator());
    }

    @Test
    public void testIsValid_NullAsInput()
    {
        ValueValidator validator = Optional.validator();
        assertNotNull(validator);
        assertTrue("Null is valid input", validator.isValid((String) null));
    }

    @Test
    public void testIsValid_NullAsInput_withSupplier()
    {
        ValueValidator validator = Optional.validator();
        assertNotNull(validator);

        Supplier<Object> nullSupplier = () -> null;
        ValidationResult<Object> result = validator.isValid(nullSupplier);
        assertNotNull(result);
        assertTrue("Null is valid input", result.isValid());
        assertNull(result.get());
        assertTrue("Null is valid input", validator.isValid((Object) nullSupplier));
    }

    @Test
    public void testIsValid_ValidInput()
    {
        ValueValidator validator = Optional.validator();
        assertNotNull(validator);
        assertTrue("Any string is valid input", validator.isValid(getTestName()));
    }

    @Test
    public void testIsValid_ValidInput_withSupplier()
    {
        ValueValidator validator = Optional.validator();
        assertNotNull(validator);

        Supplier<String> testName = this::getTestName;
        ValidationResult<String> result = validator.isValid(testName);
        assertNotNull(result);
        assertTrue("Any string is valid input", result.isValid());
        assertEquals(getTestName(), result.get());
        assertTrue("Any string is valid input", validator.isValid((Object) testName));
    }

    @Test
    public void testValidate_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Optional.validator();
        assertNotNull(validator);
        try
        {
            validator.validate((String) null, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_NullAsInput_withSupplier()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Optional.validator();
        assertNotNull(validator);
        try
        {
            Supplier<Object> nullSupplier = () -> null;
            Supplier<Object> supplier = validator.validate(nullSupplier, object, "attr");
            assertNotNull(supplier);
            assertNull(supplier.get());

            validator.validate((Object) nullSupplier, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testValidate_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Optional.validator();
        assertNotNull(validator);
        try
        {
            validator.validate(getTestName(), object, "attr");
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
        ValueValidator validator = Optional.validator();
        assertNotNull(validator);
        try
        {
            Supplier<String> testName = this::getTestName;
            Supplier<String> supplier = validator.validate(testName, object, "attr");
            assertNotNull(supplier);
            assertEquals(getTestName(), supplier.get());

            validator.validate((Object) testName, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testErrorMessage_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Optional.validator();
        assertNotNull(validator);
        try
        {
            validator.errorMessage(null, object, "attr");
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
        ValueValidator validator = Optional.validator();
        assertNotNull(validator);
        try
        {
            validator.errorMessage(getTestName(), object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testAndThen_NullAsInput()
    {
        ValueValidator validator = Optional.validator();
        assertNotNull(validator);
        assertEquals(validator, validator.andThen(null));
    }

    @Test
    public void testAndThen_IsValid()
    {
        ValueValidator another = "abcd"::equals;
        ValueValidator validator = Optional.validator().andThen(another);
        assertNotNull(validator);

        assertTrue(validator.isValid((String) null));
        assertTrue(validator.isValid("abcd"));
        assertFalse(validator.isValid("x"));
    }

    @Test
    public void testAndThen_IsValid_withSupplier()
    {
        ValueValidator another = "abcd"::equals;
        ValueValidator validator = Optional.validator().andThen(another);
        assertNotNull(validator);

        Supplier<Object> nullSupplier = () -> null;
        ValidationResult<Object> supplier = validator.isValid(nullSupplier);
        assertNotNull(supplier);
        assertTrue(supplier.isValid());
        assertNull(supplier.get());
        assertTrue(validator.isValid((Object) nullSupplier));

        Supplier<String> abcdSupplier = () -> "abcd";
        ValidationResult<String> supplier2 = validator.isValid(abcdSupplier);
        assertNotNull(supplier);
        assertTrue(supplier2.isValid());
        assertEquals("abcd", supplier2.get());
        assertTrue(validator.isValid((Object) abcdSupplier));

        Supplier<String> stringSupplier = () -> "x";
        ValidationResult<String> supplier3 = validator.isValid(stringSupplier);
        assertNotNull(supplier3);
        assertFalse(supplier3.isValid());
        assertEquals("x", supplier3.get());
        assertFalse(validator.isValid((Object) stringSupplier));
    }

    @Test
    public void testAndThen_validate()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator another = "abcd"::equals;
        ValueValidator validator = Optional.validator().andThen(another);
        assertNotNull(validator);

        try
        {
            validator.validate((String) null, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            validator.validate("abcd", object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            validator.validate("x", object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'x'", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testAndThen_validate_withSupplier()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator another = "abcd"::equals;
        ValueValidator validator = Optional.validator().andThen(another);
        assertNotNull(validator);

        try
        {
            Supplier<Object> nullSupplier = () -> null;
            Supplier<Object> supplier = validator.validate(nullSupplier, object, "attr");
            assertNotNull(supplier);
            assertNull(supplier.get());

            validator.validate((Object) nullSupplier, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            Supplier<String> stringSupplier = () -> "abcd";
            Supplier<String> supplier = validator.validate(stringSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals("abcd", supplier.get());

            validator.validate((Object) stringSupplier, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        Supplier<String> stringSupplier = () -> "x";
        try
        {
            validator.validate(stringSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'x'", e.getMessage());
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
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'x'", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testAndThen_errorMessage()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator another = "abcd"::equals;
        ValueValidator validator = Optional.validator().andThen(another);
        assertNotNull(validator);

        try
        {
            String message = validator.errorMessage("x", object, "attr");
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'x'", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            validator.errorMessage(null, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }
}