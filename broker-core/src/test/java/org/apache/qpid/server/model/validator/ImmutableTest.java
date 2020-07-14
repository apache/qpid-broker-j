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

public class ImmutableTest extends UnitTestBase
{

    @Test
    public void testValidator()
    {
        assertNotNull("Factory method has to produce a instance", Immutable.validator("abc"));
        assertNotNull("Factory method has to produce a instance", Immutable.validator(() -> "abc"));
    }

    @Test
    public void testIsValid_NullAsInput()
    {
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);
        assertFalse("Null is not valid input", validator.isValid((String) null));
    }

    @Test
    public void testIsValid_NullAsInput_withSupplier()
    {
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);

        Supplier<String> stringSupplier = () -> null;
        ValidationResult<String> result = validator.isValid(stringSupplier);
        assertNotNull(result);
        assertFalse("Null is not valid input", result.isValid());
        assertNull(result.get());

        assertFalse("Null is not valid input", validator.isValid((Object) stringSupplier));
    }

    @Test
    public void testIsValid_ValidInput()
    {
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);
        assertTrue(validator.isValid("abc"));
    }

    @Test
    public void testIsValid_ValidInput_withSupplier()
    {
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);

        Supplier<String> stringSupplier = () -> "abc";
        ValidationResult<String> result = validator.isValid(stringSupplier);
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals("abc", result.get());

        assertTrue(validator.isValid((Object) stringSupplier));
    }

    @Test
    public void testIsValid_InvalidInput()
    {
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);
        assertFalse(validator.isValid("ac"));
    }

    @Test
    public void testIsValid_InvalidInput_withSupplier()
    {
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);

        Supplier<String> stringSupplier = () -> "ac";
        ValidationResult<String> result = validator.isValid(stringSupplier);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals("ac", result.get());

        assertFalse(validator.isValid((Object) stringSupplier));
    }

    @Test
    public void testValidate_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);
        try
        {
            validator.validate((String) null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
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
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);

        Supplier<String> nullSupplier = () -> null;
        try
        {
            validator.validate(nullSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
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
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
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
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);
        try
        {
            validator.validate("abc", object, "attr");
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
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);

        try
        {
            Supplier<String> stringSupplier = () -> "abc";
            Supplier<String> supplier = validator.validate(stringSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals("abc", supplier.get());

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
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);
        try
        {
            validator.validate("ac", object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
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
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);

        Supplier<String> bcSupplier = () -> "bc";
        try
        {
            validator.validate(bcSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate((Object) bcSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testErrorMessage_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);
        try
        {
            String errorMessage = validator.errorMessage(null, object, "attr");
            assertEquals("Attribute 'attr' cannot be changed", errorMessage);
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
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);
        try
        {
            String errorMessage = validator.errorMessage("bc", object, "attr");
            assertEquals("Attribute 'attr' cannot be changed", errorMessage);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testAndThen_NullAsInput()
    {
        ValueValidator validator = Immutable.validator("abc");
        assertNotNull(validator);
        assertEquals(validator, validator.andThen(null));
    }

    @Test
    public void testAndThen_IsValid()
    {
        ValueValidator another = v -> String.class.isInstance(v) && "abcd".contains((String) v);
        ValueValidator validator = Immutable.validator("abc").andThen(another);
        assertNotNull(validator);

        assertFalse(validator.isValid((String) null));
        assertFalse(validator.isValid("ab"));
        assertFalse(validator.isValid("abcd"));
        assertTrue(validator.isValid("abc"));
    }

    @Test
    public void testAndThen_IsValid_withSupplier()
    {
        ValueValidator another = v -> String.class.isInstance(v) && "abcd".contains((String) v);
        ValueValidator validator = Immutable.validator("abc").andThen(another);
        assertNotNull(validator);

        Supplier<String> nullSupplier = () -> null;
        ValidationResult<String> supplier = validator.isValid(nullSupplier);
        assertNotNull(supplier);
        assertFalse(supplier.isValid());
        assertNull(supplier.get());
        assertFalse(validator.isValid((Object) nullSupplier));

        Supplier<String> abcSupplier = () -> "abc";
        supplier = validator.isValid(abcSupplier);
        assertNotNull(supplier);
        assertTrue(supplier.isValid());
        assertEquals("abc", supplier.get());
        assertTrue(validator.isValid((Object) abcSupplier));

        Supplier<String> abcdSupplier = () -> "abcd";
        supplier = validator.isValid(abcdSupplier);
        assertNotNull(supplier);
        assertFalse(supplier.isValid());
        assertEquals("abcd", supplier.get());
        assertFalse(validator.isValid((Object) abcdSupplier));

        Supplier<String> abSupplier = () -> "ab";
        supplier = validator.isValid(abSupplier);
        assertNotNull(supplier);
        assertFalse(supplier.isValid());
        assertEquals("ab", supplier.get());
        assertFalse(validator.isValid((Object) abSupplier));
    }

    @Test
    public void testAndThen_validate()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator another = v -> String.class.isInstance(v) && "abcd".contains((String) v);
        ValueValidator validator = Immutable.validator("abc").andThen(another);
        assertNotNull(validator);

        try
        {
            validator.validate((String) null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate("abc", object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            validator.validate("abcd", object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
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
        ValueValidator another = v -> String.class.isInstance(v) && "abcd".contains((String) v);
        ValueValidator validator = Immutable.validator("abc").andThen(another);
        assertNotNull(validator);

        Supplier<String> nullSupplier = () -> null;
        try
        {
            validator.validate(nullSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
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
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            Supplier<String> stringSupplier = () -> "abc";
            Supplier<String> supplier = validator.validate(stringSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals("abc", supplier.get());

            validator.validate((Object) stringSupplier, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        Supplier<String> abSupplier = () -> "ab";
        try
        {
            validator.validate(abSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate((Object) abSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' cannot be changed", e.getMessage());
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
        ValueValidator another = v -> String.class.isInstance(v) && "abcd".contains((String) v);
        ValueValidator validator = Immutable.validator("abc").andThen(another);
        assertNotNull(validator);

        try
        {
            String message = validator.errorMessage("abcd", object, "attr");
            assertEquals("Attribute 'attr' cannot be changed", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        another = "abcd"::equals;
        validator = Immutable.validator("abc").andThen(another);
        assertNotNull(validator);

        try
        {
            String message = validator.errorMessage("abc", object, "attr");
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'abc'", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }
}