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
package org.apache.qpid.server.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class JMSSelectorFilterTest extends UnitTestBase
{
    @Test
    public void testEqualsAndHashCodeUsingSelectorString() throws Exception
    {
        final String selectorString = "1 = 1";
        final JMSSelectorFilter filter1 = new JMSSelectorFilter(selectorString);
        final JMSSelectorFilter filter2 = new JMSSelectorFilter(selectorString);

        assertEquals(filter1, filter1, filter1 + " should equal itself");
        assertNotNull(filter1, filter1 + " should not equal null");
        assertEqualsAndHashCodeMatch(filter1, filter2);

        final JMSSelectorFilter differentFilter = new JMSSelectorFilter("2 = 2");
        assertNotEqual(filter1, differentFilter);
    }

    private void assertEqualsAndHashCodeMatch(final JMSSelectorFilter filter1, final JMSSelectorFilter filter2)
    {
        final String message = filter1 + " and " + filter2 + " should be equal";

        assertEquals(filter1, filter2, message);
        assertEquals(filter2, filter1, message);

        assertEquals(filter1.hashCode(), (long) filter2.hashCode(), "HashCodes of " + filter1 + " and " +
                filter2 + " should be equal");
    }

    private void assertNotEqual(final JMSSelectorFilter filter, final JMSSelectorFilter differentFilter)
    {
        assertNotEquals(filter, differentFilter);
        assertNotEquals(differentFilter, filter);
    }
}
