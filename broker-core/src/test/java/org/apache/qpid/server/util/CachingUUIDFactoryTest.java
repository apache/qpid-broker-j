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

package org.apache.qpid.server.util;

import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class CachingUUIDFactoryTest extends UnitTestBase
{
    private final CachingUUIDFactory _factory = new CachingUUIDFactory();

    @Test
    public void testUuidFromBits()
    {
        final UUID first = _factory.createUuidFromBits(0L,0L);
        final UUID second = _factory.createUuidFromBits(0L,0L);
        assertSame(first, second, "UUIDFactory should return the same object");
    }

    @Test
    public void testUuidFromString()
    {
        final String uuidStr = randomUUID().toString();
        final UUID first = _factory.createUuidFromString(uuidStr);
        final UUID second = _factory.createUuidFromString(uuidStr);
        final UUID third = _factory.createUuidFromBits(second.getMostSignificantBits(), second.getLeastSignificantBits());
        assertSame(first, second, "UUIDFactory should return the same object");
        assertSame(first, third, "UUIDFactory should return the same object");
    }
}