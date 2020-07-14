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

public class InRangeTest extends UnitTestBase
{

    @Test
    public void TestValidate()
    {
        assertNotNull("Factory method has to produce a instance", InRange.validator(10L, 20L));
    }

    @Test
    public void testIsValid()
    {
        ValueValidator validator = InRange.validator(10L, 20L);
        assertFalse(validator.isValid(9));
        assertTrue(validator.isValid(10));
        assertTrue(validator.isValid(19));
        assertFalse(validator.isValid(20));
        assertFalse(validator.isValid(29));
    }

    @Test
    public void errorMessage()
    {
        TestConfiguredObject object = new TestConfiguredObject();
        ValueValidator validator = InRange.validator(10L, 20L);

        String message = validator.errorMessage(9, object, "attr");
        assertEquals("Attribute 'attr' instance of org.apache.qpid.server.model.validator.TestConfiguredObject named 'TestConfiguredObject' cannot have value '9' as it is not in range [10, 20)", message);
    }
}