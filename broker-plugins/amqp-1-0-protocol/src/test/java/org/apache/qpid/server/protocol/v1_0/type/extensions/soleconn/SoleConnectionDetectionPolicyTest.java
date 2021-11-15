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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SoleConnectionDetectionPolicyTest extends UnitTestBase
{
    @Test
    public void testValue()
    {
        assertEquals(new UnsignedInteger(0), SoleConnectionDetectionPolicy.STRONG.getValue());
        assertEquals(new UnsignedInteger(1), SoleConnectionDetectionPolicy.WEAK.getValue());
    }

    @Test
    public void testValueOf()
    {
        assertEquals(SoleConnectionDetectionPolicy.STRONG, SoleConnectionDetectionPolicy.valueOf(new UnsignedInteger(0)));
        assertEquals(SoleConnectionDetectionPolicy.WEAK, SoleConnectionDetectionPolicy.valueOf(new UnsignedInteger(1)));

        try
        {
            SoleConnectionDetectionPolicy.valueOf(new UnsignedInteger(2));
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
        assertEquals("strong", SoleConnectionDetectionPolicy.STRONG.toString());
        assertEquals("weak", SoleConnectionDetectionPolicy.WEAK.toString());
    }
}
