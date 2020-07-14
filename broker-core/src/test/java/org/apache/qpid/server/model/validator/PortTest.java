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

import org.apache.qpid.test.utils.UnitTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PortTest extends UnitTestBase
{

    @Test
    public void testValidator()
    {
        assertNotNull("Factory method has to produce a instance", Port.validator());
    }

    @Test
    public void testIsValid()
    {
        ValueValidator validator = Port.validator();
        assertNotNull(validator);

        assertFalse(validator.isValid(-1));
        assertFalse(validator.isValid(0));
        assertTrue(validator.isValid(1));
        assertTrue(validator.isValid(23000));
        assertTrue(validator.isValid(65535));
        assertFalse(validator.isValid(65536));
    }

    @Test
    public void errorMessage()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = Port.validator();
        assertNotNull(validator);

        String message = validator.errorMessage(2000000, object, "attr");
        assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '2000000' as it is not valid port number", message);
    }
}