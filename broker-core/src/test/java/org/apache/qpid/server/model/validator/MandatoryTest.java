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

public class MandatoryTest extends UnitTestBase
{
    @Test
    public void testValidator()
    {
        assertNotNull("Factory method has to produce a instance", Mandatory.validator());
    }

    @Test
    public void testIsValid_NullAsInput()
    {
        ValueValidator validator = Mandatory.validator();
        assertNotNull(validator);
        assertFalse("Null is not valid input", validator.isValid((String) null));
    }

    @Test
    public void testIsValid_NullAsInput_withSupplier()
    {
        ValueValidator validator = Mandatory.validator();
        assertNotNull(validator);

        Supplier<Object> nullSupplier = () -> null;
        ValidationResult<Object> result = validator.isValid(nullSupplier);
        assertNotNull(result);
        assertFalse("Null is not valid input", result.isValid());
        assertNull(result.get());

        assertFalse("Null is not valid input", validator.isValid((Object) nullSupplier));
    }

    @Test
    public void testIsValid()
    {
        ValueValidator validator = Mandatory.validator();
        assertNotNull(validator);
        assertTrue("Any string is valid input", validator.isValid(getTestName()));
    }

    @Test
    public void testIsValid_withSupplier()
    {
        ValueValidator validator = Mandatory.validator();
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
        ValueValidator validator = Mandatory.validator();
        assertNotNull(validator);
        try
        {
            validator.validate((String) null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be null, as it is mandatory", e.getMessage());
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
        ValueValidator validator = Mandatory.validator();
        assertNotNull(validator);
        Supplier<Object> nullSupplier = () -> null;
        try
        {
            validator.validate(nullSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be null, as it is mandatory", e.getMessage());
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
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be null, as it is mandatory", e.getMessage());
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
        ValueValidator validator = Mandatory.validator();
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
        ValueValidator validator = Mandatory.validator();
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
        ValueValidator validator = Mandatory.validator();
        assertNotNull(validator);
        try
        {
            String errorMessage = validator.errorMessage(null, object, "attr");
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be null, as it is mandatory", errorMessage);
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
        ValueValidator validator = Mandatory.validator();
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
        ValueValidator validator = Mandatory.validator();
        assertNotNull(validator);
        assertEquals(validator, validator.andThen(null));
    }

    @Test
    public void testAndThen_IsValid()
    {
        ValueValidator another = "abcd"::equals;
        ValueValidator validator = Mandatory.validator().andThen(another);
        assertNotNull(validator);

        assertFalse(validator.isValid((String) null));
        assertTrue(validator.isValid("abcd"));
        assertFalse(validator.isValid("x"));
    }

    @Test
    public void testAndThen_IsValid_withSupplier()
    {
        ValueValidator another = "abcd"::equals;
        ValueValidator validator = Mandatory.validator().andThen(another);
        assertNotNull(validator);

        Supplier<Object> nullSupplier = () -> null;
        ValidationResult<Object> supplier = validator.isValid(nullSupplier);
        assertNotNull(supplier);
        assertFalse(supplier.isValid());
        assertNull(supplier.get());
        assertFalse(validator.isValid((Object) nullSupplier));

        Supplier<String> abcdSupplier = () -> "abcd";
        ValidationResult<String> supplier2 = validator.isValid(abcdSupplier);
        assertNotNull(supplier2);
        assertTrue(supplier2.isValid());
        assertEquals("abcd", supplier2.get());
        assertTrue(validator.isValid((Object) abcdSupplier));

        Supplier<String> xSupplier = () -> "x";
        ValidationResult<String> supplier3 = validator.isValid(xSupplier);
        assertNotNull(supplier3);
        assertFalse(supplier3.isValid());
        assertEquals("x", supplier3.get());
        assertFalse(validator.isValid((Object) xSupplier));
    }

    @Test
    public void testAndThen_validate()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator another = "abcd"::equals;
        ValueValidator validator = Mandatory.validator().andThen(another);
        assertNotNull(validator);

        try
        {
            validator.validate((String) null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be null, as it is mandatory", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
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
        ValueValidator validator = Mandatory.validator().andThen(another);
        assertNotNull(validator);

        Supplier<Object> nullSupplier = () -> null;
        try
        {
            validator.validate(nullSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be null, as it is mandatory", e.getMessage());
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
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be null, as it is mandatory", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            Supplier<String> abcdSupplier = () -> "abcd";
            Supplier<String> supplier = validator.validate(abcdSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals("abcd", supplier.get());

            validator.validate((Object) abcdSupplier, object, "attr");
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
        ValueValidator validator = Mandatory.validator().andThen(another);
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
            String message = validator.errorMessage(null, object, "attr");
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot be null, as it is mandatory", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }
}