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
package org.apache.qpid.server.security.access.config;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Test;

import org.apache.qpid.test.utils.UnitTestBase;

public class ActionTest extends UnitTestBase
{
    private ObjectProperties _properties1 = mock(ObjectProperties.class);
    private ObjectProperties _properties2 = mock(ObjectProperties.class);

    @Test
    public void testMatchesReturnsTrueForMatchingActions()
    {
        when(_properties1.propertiesMatch(_properties2)).thenReturn(true);

        assertMatches(
                new Action(LegacyOperation.CONSUME, ObjectType.QUEUE, _properties1),
                new Action(LegacyOperation.CONSUME, ObjectType.QUEUE, _properties2));
    }

    @Test
    public void testMatchesReturnsFalseWhenOperationsDiffer()
    {
        assertDoesntMatch(
                new Action(LegacyOperation.CONSUME, ObjectType.QUEUE, _properties1),
                new Action(LegacyOperation.CREATE, ObjectType.QUEUE, _properties1));
    }

    @Test
    public void testMatchesReturnsFalseWhenOperationTypesDiffer()
    {
        assertDoesntMatch(
                new Action(LegacyOperation.CREATE, ObjectType.QUEUE, _properties1),
                new Action(LegacyOperation.CREATE, ObjectType.EXCHANGE, _properties1));
    }

    @Test
    public void testMatchesReturnsFalseWhenOperationPropertiesDiffer()
    {
        assertDoesntMatch(
                new Action(LegacyOperation.CREATE, ObjectType.QUEUE, _properties1),
                new Action(LegacyOperation.CREATE, ObjectType.QUEUE, _properties2));
    }

    @Test
    public void testMatchesReturnsFalseWhenMyOperationPropertiesIsNull()
    {
        assertDoesntMatch(
                new Action(LegacyOperation.CREATE, ObjectType.QUEUE, (ObjectProperties)null),
                new Action(LegacyOperation.CREATE, ObjectType.QUEUE, _properties1));
    }

    @Test
    public void testMatchesReturnsFalseWhenOtherOperationPropertiesIsNull()
    {
        assertDoesntMatch(
                new Action(LegacyOperation.CREATE, ObjectType.QUEUE, _properties1),
                new Action(LegacyOperation.CREATE, ObjectType.QUEUE, (ObjectProperties)null));
    }

    @Test
    public void testMatchesReturnsTrueWhenBothOperationPropertiesAreNull()
    {
        assertMatches(
                new Action(LegacyOperation.CREATE, ObjectType.QUEUE, (ObjectProperties)null),
                new Action(LegacyOperation.CREATE, ObjectType.QUEUE, (ObjectProperties)null));
    }

    @Test
    public void testAttributesIgnoredForCreate()
    {
        final ObjectProperties objectProperties1 = new ObjectProperties();
        objectProperties1.setAttributeNames(Collections.singleton("test1"));
        final ObjectProperties objectProperties2 = new ObjectProperties();
        objectProperties2.setAttributeNames(Collections.singleton("test2"));
        assertMatches(new Action(LegacyOperation.CREATE, ObjectType.QUEUE, objectProperties1),
                      new Action(LegacyOperation.CREATE, ObjectType.QUEUE, objectProperties2));
    }

    @Test
    public void testAttributesDifferForUpdate()
    {
        final ObjectProperties objectProperties1 = new ObjectProperties();
        objectProperties1.setAttributeNames(Collections.singleton("test1"));
        final ObjectProperties objectProperties2 = new ObjectProperties();
        objectProperties2.setAttributeNames(Collections.singleton("test2"));
        assertDoesntMatch(new Action(LegacyOperation.UPDATE, ObjectType.QUEUE, objectProperties1),
                          new Action(LegacyOperation.UPDATE, ObjectType.QUEUE, objectProperties2));
    }

    @Test
    public void testAttributesMatchForUpdate()
    {
        final ObjectProperties objectProperties1 = new ObjectProperties();
        objectProperties1.setAttributeNames(Collections.singleton("test1"));
        final ObjectProperties objectProperties2 = new ObjectProperties();
        objectProperties2.setAttributeNames(Collections.singleton("test1"));
        assertMatches(new Action(LegacyOperation.UPDATE, ObjectType.QUEUE, objectProperties1),
                      new Action(LegacyOperation.UPDATE, ObjectType.QUEUE, objectProperties2));
    }

    private void assertMatches(Action action1, Action action2)
    {
        assertTrue(action1 + " should match " + action2, action1.matches(action2));
    }

    private void assertDoesntMatch(Action action1, Action action2)
    {
        assertFalse(action1 + " should not match " + action2, action1.matches(action2));
    }

}
