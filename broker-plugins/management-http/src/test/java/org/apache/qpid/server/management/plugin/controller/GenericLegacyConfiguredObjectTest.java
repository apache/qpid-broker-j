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
package org.apache.qpid.server.management.plugin.controller;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ResponseType;
import org.apache.qpid.test.utils.UnitTestBase;

public class GenericLegacyConfiguredObjectTest extends UnitTestBase
{

    private GenericLegacyConfiguredObject _object;
    private LegacyConfiguredObject _nextVersionLegacyConfiguredObject;
    private static final String CATEGORY = "testCategory";
    private LegacyManagementController _managementController;

    @Before
    public void setUp()
    {
        _managementController = mock(LegacyManagementController.class);
        _nextVersionLegacyConfiguredObject = mock(LegacyConfiguredObject.class);

        _object = new GenericLegacyConfiguredObject(_managementController,
                                                    _nextVersionLegacyConfiguredObject,
                                                    CATEGORY);
    }

    @Test
    public void getAttributeNames()
    {
        final Collection<String> attributeNames = Arrays.asList("foo", "bar", "test");
        when(_nextVersionLegacyConfiguredObject.getAttributeNames()).thenReturn(attributeNames);
        Collection<String> names = _object.getAttributeNames();

        assertThat(names, is(equalTo(attributeNames)));
        verify(_nextVersionLegacyConfiguredObject).getAttributeNames();
    }

    @Test
    public void getAttribute()
    {
        final String attributeName = "name";
        final String attributeValue = "test";

        when(_nextVersionLegacyConfiguredObject.getAttribute(attributeName)).thenReturn(attributeValue);
        final Object value = _object.getAttribute(attributeName);

        assertThat(value, is(equalTo(attributeValue)));
        verify(_nextVersionLegacyConfiguredObject).getAttribute(attributeName);
    }

    @Test
    public void getActualAttribute()
    {
        final String attributeName = "name";
        final String attributeValue = "test";

        when(_nextVersionLegacyConfiguredObject.getActualAttribute(attributeName)).thenReturn(attributeValue);
        final Object value = _object.getActualAttribute(attributeName);

        assertThat(value, is(equalTo(attributeValue)));
        verify(_nextVersionLegacyConfiguredObject).getActualAttribute(attributeName);
    }

    @Test
    public void getChildren()
    {
        final String childrenCategory = "testChildrenCategory";

        final LegacyConfiguredObject child = mock(LegacyConfiguredObject.class);
        final Collection<LegacyConfiguredObject> children = Collections.singleton(child);
        when(_nextVersionLegacyConfiguredObject.getChildren(childrenCategory)).thenReturn(children);
        final LegacyConfiguredObject converted = mock(LegacyConfiguredObject.class);
        when(_managementController.convertFromNextVersion(child)).thenReturn(converted);
        final Collection<LegacyConfiguredObject> value = _object.getChildren(childrenCategory);

        assertThat(value.size(), is(equalTo(1)));
        final LegacyConfiguredObject convertedChild = value.iterator().next();
        assertThat(convertedChild, is(equalTo(converted)));
        verify(_nextVersionLegacyConfiguredObject).getChildren(childrenCategory);
        verify(_managementController).convertFromNextVersion(child);
    }

    @Test
    public void getCategory()
    {
        assertThat(_object.getCategory(), is(equalTo(CATEGORY)));
    }

    @Test
    public void invoke()
    {
        final String operationName = "testOperation";
        final Map<String, Object> operationArguments = Collections.singletonMap("arg", "argValue");
        final String operationResult = "testOperationResult";
        final ControllerManagementResponse managementResponse = new ControllerManagementResponse(
                ResponseType.DATA, operationResult);
        when(_nextVersionLegacyConfiguredObject.invoke(operationName,
                                                       operationArguments,
                                                       true)).thenReturn(managementResponse);

        final ManagementResponse result =
                _object.invoke(operationName, operationArguments, true);

        assertThat(result, is(notNullValue()));
        assertThat(result.getResponseCode(), is(equalTo(200)));
        assertThat(result.getBody(), is(equalTo(operationResult)));
    }

    @Test
    public void getNextVersionConfiguredObject()
    {
        assertThat(_object.getNextVersionConfiguredObject(), is(equalTo(_nextVersionLegacyConfiguredObject)));
    }

    @Test
    public void getParent()
    {
        final String parentCategory = "testParentCategory";
        final LegacyConfiguredObject nextVersionParent = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject nextVersionParentConverted = mock(LegacyConfiguredObject.class);
        when(_nextVersionLegacyConfiguredObject.getParent(parentCategory)).thenReturn(nextVersionParent);
        when(_managementController.convertFromNextVersion(nextVersionParent)).thenReturn(
                nextVersionParentConverted);
        final LegacyConfiguredObject parent = _object.getParent(parentCategory);
        assertThat(parent, is(equalTo(nextVersionParentConverted)));
        verify(_nextVersionLegacyConfiguredObject).getParent(parentCategory);
    }

    @Test
    public void isSecureAttribute()
    {
        final String attributeName = "testAttribute";
        when(_nextVersionLegacyConfiguredObject.isSecureAttribute(attributeName)).thenReturn(true);
        assertThat(_object.isSecureAttribute(attributeName), is(equalTo(true)));
        verify(_nextVersionLegacyConfiguredObject).isSecureAttribute(attributeName);
    }

    @Test
    public void isOversizedAttribute()
    {
        final String attributeName = "testAttribute";
        when(_nextVersionLegacyConfiguredObject.isOversizedAttribute(attributeName)).thenReturn(true);
        assertThat(_object.isOversizedAttribute(attributeName), is(equalTo(true)));
        verify(_nextVersionLegacyConfiguredObject).isOversizedAttribute(attributeName);
    }

    @Test
    public void getContextValue()
    {
        final String contextName = "testContext";
        final String contextValue = "testValue";
        when(_nextVersionLegacyConfiguredObject.getContextValue(contextName)).thenReturn(contextValue);
        assertThat(_object.getContextValue(contextName), is(equalTo(contextValue)));
        verify(_nextVersionLegacyConfiguredObject).getContextValue(contextName);
    }

    @Test
    public void getStatistics()
    {
        Map<String, Object> stats = Collections.singletonMap("testStat", "statValue");
        when(_nextVersionLegacyConfiguredObject.getStatistics()).thenReturn(stats);
        assertThat(_object.getStatistics(), is(equalTo(stats)));
        verify(_nextVersionLegacyConfiguredObject).getStatistics();
    }

    @Test
    public void getManagementController()
    {
        assertThat(_object.getManagementController(), is(equalTo(_managementController)));
    }

    @Test
    public void getNextVersionLegacyConfiguredObject()
    {
        assertThat(_object.getNextVersionConfiguredObject(), is(equalTo(_nextVersionLegacyConfiguredObject)));
    }
}
