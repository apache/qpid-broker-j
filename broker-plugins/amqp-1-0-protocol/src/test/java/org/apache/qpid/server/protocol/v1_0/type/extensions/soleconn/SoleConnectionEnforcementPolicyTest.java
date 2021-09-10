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
 */
package org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn;

import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;
import org.apache.qpid.test.utils.UnitTestBase;

import org.junit.Test;

import static org.junit.Assert.*;

public class SoleConnectionEnforcementPolicyTest extends UnitTestBase
{
    @Test
    public void testValue()
    {
        assertEquals(new UnsignedInteger(0), SoleConnectionEnforcementPolicy.REFUSE_CONNECTION.getValue());
        assertEquals(new UnsignedInteger(1), SoleConnectionEnforcementPolicy.CLOSE_EXISTING.getValue());
    }

    @Test
    public void testValueOf()
    {
        assertEquals(SoleConnectionEnforcementPolicy.REFUSE_CONNECTION, SoleConnectionEnforcementPolicy.valueOf(new UnsignedInteger(0)));
        assertEquals(SoleConnectionEnforcementPolicy.CLOSE_EXISTING, SoleConnectionEnforcementPolicy.valueOf(new UnsignedInteger(1)));

        try
        {
            SoleConnectionEnforcementPolicy.valueOf(new UnsignedInteger(2));
            fail("An exception is expected");
        }
        catch (RuntimeException e)
        {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testToString()
    {
        assertEquals("refuse-connection", SoleConnectionEnforcementPolicy.REFUSE_CONNECTION.toString());
        assertEquals("close-existing", SoleConnectionEnforcementPolicy.CLOSE_EXISTING.toString());
    }
}
