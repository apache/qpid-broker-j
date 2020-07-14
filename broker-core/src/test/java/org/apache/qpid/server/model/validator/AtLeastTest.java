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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AtLeastTest extends UnitTestBase
{
    @Test
    public void testValidator()
    {
        assertNotNull("Factory method has to produce a instance", AtLeast.validator(42L));
        assertNotNull("Factory method has to produce a instance", BigInteger.TEN);
    }

    @Test
    public void testIsValid_NullAsInput()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);
        assertFalse("Null is not valid input", validator.isValid((Number) null));
    }

    @Test
    public void testIsValid_NullAsInput_withSupplier()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        Supplier<Long> longSupplier = () -> null;
        ValidationResult<Long> result = validator.isValid(longSupplier);
        assertNotNull(result);
        assertFalse("Null is not valid input", result.isValid());
        assertNull(result.get());
        assertFalse("Null is not valid input", validator.isValid((Object) longSupplier));
    }

    @Test
    public void testIsValid_StringAsInput()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        assertFalse("A string is not valid input", validator.isValid(getTestName()));
    }

    @Test
    public void testIsValid_StringAsInput_withSupplier()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        Supplier<String> testName = this::getTestName;
        ValidationResult<String> result = validator.isValid(testName);
        assertNotNull(result);
        assertFalse("A string is not valid input", result.isValid());
        assertEquals(getTestName(), result.get());
        assertFalse("A string is not valid input", validator.isValid((Object) testName));
    }

    @Test
    public void testIsValid_BigIntegerAsInput()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        assertFalse(validator.isValid(BigInteger.valueOf(41L)));
        assertTrue(validator.isValid(BigInteger.valueOf(42L)));
        assertTrue(validator.isValid(BigInteger.valueOf(43L)));
    }

    @Test
    public void testIsValid_IntegerAsInput()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        assertFalse(validator.isValid(41));
        assertTrue(validator.isValid(42));
        assertTrue(validator.isValid(43));
    }

    @Test
    public void testIsValid_LongAsInput()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        assertFalse(validator.isValid(41L));
        assertTrue(validator.isValid(42L));
        assertTrue(validator.isValid(43L));
    }

    @Test
    public void testIsValid_ByteAsInput()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        assertFalse(validator.isValid((byte) 41));
        assertTrue(validator.isValid((byte) 42));
        assertTrue(validator.isValid((byte) 43));
    }

    @Test
    public void testIsValid_ShortAsInput()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        assertFalse(validator.isValid((short) 41));
        assertTrue(validator.isValid((short) 42));
        assertTrue(validator.isValid((short) 43));
    }

    @Test
    public void testIsValid_BigDecimalAsInput()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        assertFalse(validator.isValid(new BigDecimal("41.9")));
        assertTrue(validator.isValid(new BigDecimal("42.0")));
        assertTrue(validator.isValid(new BigDecimal("42.1")));
    }

    @Test
    public void testIsValid_DoubleAsInput()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        assertFalse(validator.isValid(41.9d));
        assertTrue(validator.isValid(42.0d));
        assertTrue(validator.isValid(42.1d));
    }

    @Test
    public void testIsValid_FloatAsInput()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        assertFalse(validator.isValid(41.9f));
        assertTrue(validator.isValid(42.0f));
        assertTrue(validator.isValid(42.1f));
    }

    @Test
    public void testIsValid_DoubleAccumulator()
    {
        DoubleAccumulator accumulator = new DoubleAccumulator(Double::sum, 0.0);

        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        accumulator.accumulate(41.9);
        assertFalse(validator.isValid(accumulator));
        accumulator.accumulate(0.1);
        assertTrue(validator.isValid(accumulator));
        accumulator.accumulate(0.1);
        assertTrue(validator.isValid(accumulator));
    }

    @Test
    public void testIsValid_DoubleAdder()
    {
        DoubleAdder accumulator = new DoubleAdder();

        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        accumulator.add(41.9);
        assertFalse(validator.isValid(accumulator));
        accumulator.add(0.1);
        assertTrue(validator.isValid(accumulator));
        accumulator.add(0.1);
        assertTrue(validator.isValid(accumulator));
    }

    @Test
    public void testIsValid_Overflow()
    {
        ValueValidator validator = AtLeast.validator(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TEN));
        assertNotNull(validator);

        assertTrue(validator.isValid(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.TEN)));

        assertFalse(validator.isValid(Integer.MAX_VALUE));

        assertFalse(validator.isValid(Long.MAX_VALUE));

        assertFalse(validator.isValid(Byte.MAX_VALUE));

        assertFalse(validator.isValid(Short.MAX_VALUE));

        assertTrue(validator.isValid(new BigDecimal("4e20")));

        assertTrue(validator.isValid(4.0e20d));

        assertTrue(validator.isValid(4e20f));
    }

    @Test
    public void testIsValid_withSupplier()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        Supplier<String> testName = this::getTestName;
        ValidationResult<String> result = validator.isValid(testName);
        assertNotNull(result);
        assertFalse("A string is not valid input", result.isValid());
        assertEquals(getTestName(), result.get());
        assertFalse("A string is not valid input", validator.isValid((Object) testName));

        Supplier<Integer> integerSupplier = () -> 20;
        ValidationResult<Integer> result2 = validator.isValid(integerSupplier);
        assertNotNull(result2);
        assertFalse(result2.isValid());
        assertEquals(Integer.valueOf(20), result2.get());
        assertFalse(validator.isValid((Object) integerSupplier));

        integerSupplier = () -> 42;
        ValidationResult<Integer> result3 = validator.isValid(integerSupplier);
        assertNotNull(result3);
        assertTrue(result3.isValid());
        assertEquals(Integer.valueOf(42), result3.get());
        assertTrue(validator.isValid((Object) integerSupplier));
    }

    @Test
    public void testIsValid_ValidInput_withSupplier()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        Supplier<Integer> integerSupplier = () -> 42;
        ValidationResult<Integer> result = validator.isValid(integerSupplier);
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Integer.valueOf(42), result.get());
        assertTrue(validator.isValid((Object) integerSupplier));
    }

    @Test
    public void testIsValid_InvalidInput_withSupplier()
    {
        ValueValidator validator = AtLeast.validator(42L);
        assertNotNull(validator);

        Supplier<Integer> integerSupplier = () -> 20;
        ValidationResult<Integer> result = validator.isValid(integerSupplier);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Integer.valueOf(20), result.get());
        assertFalse(validator.isValid((Object) integerSupplier));
    }

    @Test
    public void testValidate_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = AtLeast.validator(42);
        assertNotNull(validator);
        try
        {
            validator.validate((String) null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null' as it has to be at least 42", e.getMessage());
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
        ValueValidator validator = AtLeast.validator(42);
        assertNotNull(validator);

        Supplier<Long> nullSupplier = () -> null;
        try
        {
            validator.validate(nullSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null' as it has to be at least 42", e.getMessage());
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
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null' as it has to be at least 42", e.getMessage());
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
        ValueValidator validator = AtLeast.validator(42);
        assertNotNull(validator);

        try
        {
            validator.validate(42, object, "attr");
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
        ValueValidator validator = AtLeast.validator(42);
        assertNotNull(validator);

        try
        {
            Supplier<Integer> integerSupplier = () -> 42;
            Supplier<Integer> supplier = validator.validate(integerSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals(Integer.valueOf(42), supplier.get());

            validator.validate((Object) integerSupplier, object, "attr");
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
        ValueValidator validator = AtLeast.validator(42);
        assertNotNull(validator);

        try
        {
            validator.validate(41.99f, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '41.99' as it has to be at least 42", e.getMessage());
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
        ValueValidator validator = AtLeast.validator(42);
        assertNotNull(validator);

        Supplier<Float> floatSupplier = () -> 41.99f;
        try
        {
            validator.validate(floatSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '41.99' as it has to be at least 42", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate((Object) floatSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '41.99' as it has to be at least 42", e.getMessage());
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
        ValueValidator validator = AtLeast.validator(42);
        assertNotNull(validator);

        try
        {
            String errorMessage = validator.errorMessage(null, object, "attr");
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null' as it has to be at least 42", errorMessage);
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
        ValueValidator validator = AtLeast.validator(42);
        assertNotNull(validator);

        try
        {
            String errorMessage = validator.errorMessage(41, object, "attr");
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '41' as it has to be at least 42", errorMessage);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testAndThen_NullAsInput()
    {
        ValueValidator validator = AtLeast.validator(42);
        assertNotNull(validator);
        assertEquals(validator, validator.andThen(null));
    }

    @Test
    public void testAndThen_IsValid()
    {
        ValueValidator another = Integer.valueOf(44)::equals;
        ValueValidator validator = AtLeast.validator(42).andThen(another);
        assertNotNull(validator);

        assertFalse(validator.isValid((String) null));
        assertTrue(validator.isValid(44));
        assertFalse(validator.isValid(41));
        assertFalse(validator.isValid(42));
    }

    @Test
    public void testAndThen_IsValid_withSupplier()
    {
        ValueValidator another = Integer.valueOf(44)::equals;
        ValueValidator validator = AtLeast.validator(42).andThen(another);
        assertNotNull(validator);

        Supplier<Integer> nullSupplier = () -> null;
        ValidationResult<Integer> supplier = validator.isValid(nullSupplier);
        assertNotNull(supplier);
        assertFalse(supplier.isValid());
        assertFalse(validator.isValid((Object) nullSupplier));

        Supplier<Integer> supplier44 = () -> 44;
        supplier = validator.isValid(supplier44);
        assertNotNull(supplier);
        assertTrue(supplier.isValid());
        assertTrue(validator.isValid((Object) supplier44));

        Supplier<Integer> supplier42 = () -> 42;
        supplier = validator.isValid(supplier42);
        assertNotNull(supplier);
        assertFalse(supplier.isValid());
        assertFalse(validator.isValid((Object) supplier42));

        Supplier<Integer> supplier41 = () -> 41;
        supplier = validator.isValid(supplier41);
        assertNotNull(supplier);
        assertFalse(supplier.isValid());
        assertFalse(validator.isValid((Object) supplier41));
    }

    @Test
    public void testAndThen_validate()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator another = Integer.valueOf(44)::equals;
        ValueValidator validator = AtLeast.validator(42).andThen(another);
        assertNotNull(validator);

        try
        {
            validator.validate((Number) null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null' as it has to be at least 42", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate(44, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            validator.validate(42, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '42'", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate(41, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '41' as it has to be at least 42", e.getMessage());
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
        ValueValidator another = Integer.valueOf(44)::equals;
        ValueValidator validator = AtLeast.validator(42).andThen(another);
        assertNotNull(validator);

        Supplier<Object> nullSupplier = () -> null;
        try
        {
            validator.validate(nullSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null' as it has to be at least 42", e.getMessage());
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
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null' as it has to be at least 42", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            Supplier<Integer> integerSupplier = () -> 44;
            Supplier<?> supplier = validator.validate(integerSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals(44, supplier.get());

            validator.validate((Object) integerSupplier, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        Supplier<Integer> supplier42 = () -> 42;
        try
        {
            validator.validate(supplier42, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '42'", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate((Object) supplier42, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '42'", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        Supplier<Integer> supplier41 = () -> 41;
        try
        {
            validator.validate(supplier41, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '41' as it has to be at least 42", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate((Object) supplier41, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '41' as it has to be at least 42", e.getMessage());
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
        ValueValidator another = Integer.valueOf(44)::equals;
        ValueValidator validator = AtLeast.validator(42).andThen(another);
        assertNotNull(validator);

        try
        {
            String message = validator.errorMessage(42, object, "attr");
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '42'", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            String message = validator.errorMessage(41, object, "attr");
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '41' as it has to be at least 42", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }
}