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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AtLeastOneTest extends UnitTestBase
{
    @Test
    public void validator()
    {
        assertNotNull("Factory method has to produce a instance", AtLeastOne.validator());
    }

    @Test
    public void testValidate_NullAsInput()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        try
        {
            AtLeastOne.validateValue(null, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value 'null' as it has to be at least 1", e.getMessage());
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
        try
        {
            AtLeastOne.validateValue(2, object, "attr");
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
        try
        {
            AtLeastOne.validateValue(0, object, "attr");
            fail("An exception is expected");
        }
        catch (IllegalConfigurationException e)
        {
            assertEquals("Attribute 'attr' instance of org.apache.qpid.server.logging.logback.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '0' as it has to be at least 1", e.getMessage());
        }
        catch (RuntimeException e)
        {
            fail("A generic exception is not expected");
        }
    }
}