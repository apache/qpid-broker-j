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

import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EverTest extends UnitTestBase
{

    @Test
    public void testInstance()
    {
        assertNotNull("Factory method has to produce a instance", Ever.instance());
    }

    @Test
    public void testIsValid_NullAsInput()
    {
        ValueValidator validator = Ever.instance();
        assertNotNull(validator);

        assertTrue("Null is valid input", validator.isValid((String) null));
    }

    @Test
    public void testIsValid_NullAsInput_withSupplier()
    {
        ValueValidator validator = Ever.instance();
        assertNotNull(validator);

        Supplier<String> stringSupplier = () -> null;
        ValidationResult<String> result = validator.isValid(stringSupplier);
        assertNotNull(result);
        assertTrue("Null is valid input", result.isValid());
        assertNull(result.get());
        assertTrue("Null is valid input", validator.isValid((Object) stringSupplier));
    }

    @Test
    public void testIsValid_StringAsInput()
    {
        ValueValidator validator = Ever.instance();
        assertNotNull(validator);

        assertTrue("Any string is valid", validator.isValid(getTestName()));
    }

    @Test
    public void testIsValid_StringAsInput_withSupplier()
    {
        ValueValidator validator = Ever.instance();
        assertNotNull(validator);

        Supplier<String> testName = this::getTestName;
        ValidationResult<String> result = validator.isValid(testName);
        assertNotNull(result);
        assertTrue("Null is valid input", result.isValid());
        assertEquals(getTestName(), result.get());
        assertTrue("Null is valid input", validator.isValid((Object) testName));
    }

    @Test
    public void testIsValid_NumberAsInput()
    {
        ValueValidator validator = Ever.instance();
        assertNotNull(validator);

        assertTrue("Number is valid input", validator.isValid(1234L));
    }

    @Test
    public void testIsValid_NumberAsInput_withSupplier()
    {
        ValueValidator validator = Ever.instance();
        assertNotNull(validator);

        Supplier<Long> longSupplier = () -> 1234L;
        ValidationResult<Long> result = validator.isValid(longSupplier);
        assertNotNull(result);
        assertTrue("Number is valid input", result.isValid());
        assertEquals(Long.valueOf(1234L), result.get());
        assertTrue("Number is valid input", validator.isValid((Object) longSupplier));
    }

    @Test
    public void testValidate_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Ever.instance();
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
        ValueValidator validator = Ever.instance();
        assertNotNull(validator);

        Supplier<String> nullSupplier = () -> null;
        try
        {
            Supplier<String> supplier = validator.validate(nullSupplier, object, "attr");
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
    public void testValidate_StringAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Ever.instance();
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
    public void testValidate_StringAsInput_withSupplier()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Ever.instance();
        assertNotNull(validator);

        Supplier<String> testName = this::getTestName;
        try
        {
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
    public void testValidate_NumberAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Ever.instance();
        assertNotNull(validator);
        try
        {
            validator.validate(1234L, object, "attr");
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
        ValueValidator validator = Ever.instance();
        assertNotNull(validator);
        try
        {
            Supplier<Long> longSupplier = () -> 1234L;
            Supplier<Long> supplier = validator.validate(longSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals(Long.valueOf(1234L), supplier.get());

            validator.validate((Object) longSupplier, object, "attr");
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
        ValueValidator validator = Ever.instance();
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
    public void testErrorMessage_StringAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Ever.instance();
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
    public void testErrorMessage_NumberAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Ever.instance();
        try
        {
            validator.errorMessage(1234L, object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testAndThen_NullAsInput()
    {
        ValueValidator validator = Ever.instance();
        assertNotNull(validator);
        assertEquals(validator, validator.andThen(null));
    }

    @Test
    public void testAndThen()
    {
        ValueValidator validator = Ever.instance();
        ValueValidator another = "J"::equals;
        assertEquals(another, validator.andThen(another));
    }
}