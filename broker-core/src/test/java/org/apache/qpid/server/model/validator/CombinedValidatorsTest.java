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

public class CombinedValidatorsTest extends UnitTestBase
{
    @Test
    public void testIsValid_NullAsInput()
    {
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);
        assertNotNull(validator);

        assertFalse(validator.isValid((Number) null));
    }

    @Test
    public void testIsValid_ValidInput()
    {
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);
        assertNotNull(validator);

        assertTrue(validator.isValid(234567L));
    }

    @Test
    public void testIsValid_InvalidInput()
    {
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);
        assertNotNull(validator);

        assertFalse(validator.isValid(2L));
        assertFalse(validator.isValid(345L));
    }

    @Test
    public void testIsValid_NullAsInput_withSupplier()
    {
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);
        assertNotNull(validator);

        Supplier<Long> longSupplier = () -> null;
        ValidationResult<Long> result = validator.isValid(longSupplier);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertNull(result.get());
        assertFalse(validator.isValid((Object) longSupplier));
    }

    @Test
    public void testIsValid_ValidInput_withSupplier()
    {
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);
        assertNotNull(validator);

        Supplier<Long> longSupplier = () -> 234567L;
        ValidationResult<Long> result = validator.isValid(longSupplier);
        assertNotNull(result);
        assertTrue(result.isValid());
        assertEquals(Long.valueOf(234567L), result.get());
        assertTrue(validator.isValid((Object) longSupplier));
    }

    @Test
    public void testIsValid_InvalidInput_withSupplier()
    {
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);
        assertNotNull(validator);

        Supplier<Long> longSupplier = () -> 2L;
        ValidationResult<Long> result = validator.isValid(longSupplier);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(2L), result.get());
        assertFalse(validator.isValid((Object) longSupplier));

        longSupplier = () -> 345L;
        result = validator.isValid(longSupplier);
        assertNotNull(result);
        assertFalse(result.isValid());
        assertEquals(Long.valueOf(345L), result.get());
        assertFalse(validator.isValid((Object) longSupplier));
    }

    @Test
    public void testValidate_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);

        try
        {
            validator.validate((Number) null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null' as it has to be at least 245", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidate_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);

        try
        {
            validator.validate(2L, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '2' as it has to be at least 245", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate(278L, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '278' as it has to be at least 2345", e.getMessage());
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
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);

        try
        {
            validator.validate(256734L, object, "attr");
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
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);

        Supplier<Object> objectSupplier = () -> null;
        try
        {
            validator.validate(objectSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null' as it has to be at least 245", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate((Object) objectSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null' as it has to be at least 245", e.getMessage());
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
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);

        Supplier<Long> longSupplier = () -> 2L;
        try
        {
            validator.validate(longSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '2' as it has to be at least 245", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate((Object) longSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '2' as it has to be at least 245", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        longSupplier = () -> 278L;
        try
        {
            validator.validate(longSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '278' as it has to be at least 2345", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate((Object) longSupplier, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '278' as it has to be at least 2345", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidate_ValidInput_withSupplier()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);

        Supplier<Long> longSupplier = () -> 256734L;
        try
        {
            Supplier<Long> supplier = validator.validate(longSupplier, object, "attr");
            assertNotNull(supplier);
            assertEquals(Long.valueOf(256734L), supplier.get());

            validator.validate((Object) longSupplier, object, "attr");
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
        ValueValidator primary = AtLeast.validator(245L);
        ValueValidator secondary = AtLeast.validator(2345L);
        ValueValidator validator = CombinedValidators.validator(primary, secondary);

        String errorMessage = validator.errorMessage(3L, object, "attr");
        assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '3' as it has to be at least 245", errorMessage);

        errorMessage = validator.errorMessage(345L, object, "attr");
        assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '345' as it has to be at least 2345", errorMessage);
    }
}
