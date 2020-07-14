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

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CollectionTest extends UnitTestBase
{
    @Test
    public void testValidator()
    {
        assertNotNull("Factory method has to produce a instance", Collection.validator("abc"::equals));
    }

    @Test
    public void testIsValid_ValidInput()
    {
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);

        assertTrue(validator.isValid(Arrays.asList("abc", "abc", "abc")));
        assertTrue(validator.isValid(Collections.emptyList()));
    }

    @Test
    public void testIsValid_InvalidInput()
    {
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);

        assertFalse(validator.isValid(Arrays.asList("abc", "ac", "abc")));
        assertFalse(validator.isValid("abc"));
    }

    @Test
    public void testIsValid_NullAsInput()
    {
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);

        assertFalse(validator.isValid((java.util.Collection<?>) null));
    }

    @Test
    public void testIsValid_ValidInput_CollectionOfSuppliers()
    {
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);

        Supplier<String> abcSupplier = () -> "abc";
        assertTrue(validator.isValid(Arrays.asList(abcSupplier, abcSupplier)));
    }

    @Test
    public void testIsValid_InvalidInput_CollectionOfSuppliers()
    {
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);

        Supplier<String> abcSupplier = () -> "abc";
        Supplier<String> bcSupplier = () -> "bc";
        assertFalse(validator.isValid(Arrays.asList(abcSupplier, bcSupplier)));
    }

    @Test
    public void testValidate_ValidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);
        try
        {
            validator.validate(Arrays.asList("abc", "abc", "abc"), object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("An exception is not expected");
        }

        try
        {
            validator.validate(Collections.emptyList(), object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("An exception is not expected");
        }
    }

    @Test
    public void testValidate_InvalidInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);
        try
        {
            validator.validate(Arrays.asList("abc", "abc", "ac"), object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'ac'", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate("abc", object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("A collection 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' is expected", e.getMessage());
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
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);
        try
        {
            validator.validate((String) null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("A collection 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' is expected", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }

    @Test
    public void testValidate_ValidInput_CollectionOfSuppliers()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);
        try
        {
            Supplier<String> abcSupplier = () -> "abc";
            validator.validate(Arrays.asList(abcSupplier, abcSupplier), object, "attr");
        }
        catch (RuntimeException e)
        {
            fail("An exception is not expected");
        }
    }

    @Test
    public void testValidate_InvalidInput_CollectionOfSuppliers()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);
        try
        {
            Supplier<String> abcSupplier = () -> "abc";
            Supplier<String> acSupplier = () -> "ac";
            validator.validate(Arrays.asList(abcSupplier, acSupplier), object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'ac'", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }

        try
        {
            validator.validate("abc", object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("A collection 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' is expected", e.getMessage());
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
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);

        String message = validator.errorMessage(null, object, "attr");
        assertEquals("A collection 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' is expected", message);
    }

    @Test
    public void testErrorMessage()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);

        try
        {
            String message = validator.errorMessage("a", object, "attr");
            assertEquals("A collection 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' is expected", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }

        try
        {
            String message = validator.errorMessage(Arrays.asList("abc", "a", "abc"), object, "attr");
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'a'", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }

    @Test
    public void testErrorMessage_CollectionOfSuppliers()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Collection.validator("abc"::equals);
        assertNotNull(validator);

        try
        {
            Supplier<String> abcSupplier = () -> "abc";
            Supplier<String> aSupplier = () -> "a";
            String message = validator.errorMessage(Arrays.asList(abcSupplier, aSupplier), object, "attr");
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'a'", message);
        }
        catch (RuntimeException e)
        {
            fail("Any exception is not expected");
        }
    }
}