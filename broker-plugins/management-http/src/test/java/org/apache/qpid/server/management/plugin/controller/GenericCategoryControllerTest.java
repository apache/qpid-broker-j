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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ResponseType;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.test.utils.UnitTestBase;

public class GenericCategoryControllerTest extends UnitTestBase
{
    private static final String TEST_CATEGORY = "testCategory";
    private static final String DEFAULT_TYPE = "defaultType";
    private LegacyManagementController _managementController;
    private LegacyManagementController _nextVersionManagementController;
    private GenericCategoryController _controller;
    private ConfiguredObject _root;
    private TypeController _typeController;
    private LegacyConfiguredObject _converted;

    @Before
    public void setUp()
    {
        _managementController = mock(LegacyManagementController.class);
        _nextVersionManagementController = mock(LegacyManagementController.class);
        _typeController = mock(TypeController.class);
        when(_typeController.getTypeName()).thenReturn(DEFAULT_TYPE);
        when(_typeController.getNextVersionTypeName()).thenReturn(DEFAULT_TYPE);
        _converted = mock(LegacyConfiguredObject.class);
        _controller = new GenericCategoryController(_managementController,
                                                    _nextVersionManagementController,
                                                    TEST_CATEGORY,
                                                    DEFAULT_TYPE,
                                                    Collections.singleton(_typeController))
        {
            @Override
            protected LegacyConfiguredObject convertNextVersionLegacyConfiguredObject(final LegacyConfiguredObject object)
            {
                return _converted;
            }

            @Override
            public String[] getParentCategories()
            {
                return new String[0];
            }
        };
        _root = mock(ConfiguredObject.class);
    }

    @Test
    public void getCategory()
    {
        assertThat(_controller.getCategory(), is(equalTo(TEST_CATEGORY)));
    }

    @Test
    public void getDefaultType()
    {
        assertThat(_controller.getDefaultType(), is(equalTo(DEFAULT_TYPE)));
    }

    @Test
    public void getManagementController()
    {
        assertThat(_controller.getManagementController(), is(equalTo(_managementController)));
    }

    @Test
    public void get()
    {
        final List<String> path = Arrays.asList("test1", "test2");
        final Map<String, List<String>> parameters =
                Collections.singletonMap("testParam", Collections.singletonList("testValue"));
        final LegacyConfiguredObject result = mock(LegacyConfiguredObject.class);
        when(result.getAttribute(LegacyConfiguredObject.TYPE)).thenReturn(DEFAULT_TYPE);
        final LegacyConfiguredObject convertedResult = mock(LegacyConfiguredObject.class);
        when(_nextVersionManagementController.get(_root, TEST_CATEGORY, path, parameters)).thenReturn(result);
        when(_typeController.convertFromNextVersion(result)).thenReturn(convertedResult);
        final Object readResult = _controller.get(_root, path, parameters);
        assertThat(readResult, is(equalTo(convertedResult)));
        verify(_nextVersionManagementController).get(_root, TEST_CATEGORY, path, parameters);
    }

    @Test
    public void createOrUpdate()
    {
        final List<String> path = Arrays.asList("test1", "test2");
        final Map<String, Object> attributes = Collections.singletonMap("test", "testValue");
        final LegacyConfiguredObject result = mock(LegacyConfiguredObject.class);
        final LegacyConfiguredObject convertedResult = mock(LegacyConfiguredObject.class);
        when(result.getAttribute(LegacyConfiguredObject.TYPE)).thenReturn(DEFAULT_TYPE);

        when(_typeController.convertAttributesToNextVersion(_root, path, attributes)).thenReturn(attributes);
        when(_typeController.convertFromNextVersion(result)).thenReturn(convertedResult);
        when(_nextVersionManagementController.createOrUpdate(_root, TEST_CATEGORY, path, attributes, false))
                .thenReturn(result);
        when(_typeController.convertFromNextVersion(result)).thenReturn(convertedResult);
        final Object createResult = _controller.createOrUpdate(_root, path, attributes, false);
        assertThat(createResult, is(equalTo(convertedResult)));
        verify(_nextVersionManagementController).createOrUpdate(_root, TEST_CATEGORY, path, attributes, false);
    }

    @Test
    public void delete()
    {
        final List<String> path = Arrays.asList("test1", "test2");
        final Map<String, List<String>> parameters =
                Collections.singletonMap("testParam", Collections.singletonList("testValue"));

        final int result = 1;
        when(_nextVersionManagementController.delete(_root, TEST_CATEGORY, path, parameters)).thenReturn(result);
        final Object deleteResult = _controller.delete(_root, path, parameters);
        assertThat(deleteResult, is(equalTo(result)));
        verify(_nextVersionManagementController).delete(_root, TEST_CATEGORY, path, parameters);
    }

    @Test
    public void invoke()
    {
        final List<String> path = Arrays.asList("test1", "test2");
        final String operationName = "testOperation";
        final Map<String, Object> operationParameters = Collections.singletonMap("testParam", "testValue");

        final LegacyConfiguredObject result = mock(LegacyConfiguredObject.class);
        when(result.getAttribute(LegacyConfiguredObject.TYPE)).thenReturn(DEFAULT_TYPE);
        final LegacyConfiguredObject convertedResult = mock(LegacyConfiguredObject.class);
        when(_nextVersionManagementController.get(eq(_root),
                                                  eq(TEST_CATEGORY),
                                                  eq(path),
                                                  eq(Collections.emptyMap()))).thenReturn(result);
        when(_typeController.convertFromNextVersion(result)).thenReturn(convertedResult);


        final Object operationValue = "testValue";
        final ManagementResponse operationResult = new ControllerManagementResponse(ResponseType.DATA, operationValue);
        when(convertedResult.invoke(operationName, operationParameters, true)).thenReturn(operationResult);
        final ManagementResponse response =
                _controller.invoke(_root, path, operationName, operationParameters, true, true);
        assertThat(response, is(notNullValue()));
        assertThat(response.getResponseCode(), is(equalTo(200)));
        assertThat(response.getBody(), is(equalTo(operationValue)));
        verify(_nextVersionManagementController).get(_root, TEST_CATEGORY, path, Collections.emptyMap());
        verify(convertedResult).invoke(operationName, operationParameters, true);
    }

    @Test
    public void getPreferences()
    {
        final List<String> path = Arrays.asList("test1", "test2");
        final Map<String, List<String>> parameters =
                Collections.singletonMap("testParam", Collections.singletonList("testValue"));

        final Object result = mock(Object.class);
        when(_nextVersionManagementController.getPreferences(_root,
                                                             TEST_CATEGORY,
                                                             path,
                                                             parameters)).thenReturn(result);

        final Object preferences = _controller.getPreferences(_root, path, parameters);
        assertThat(preferences, is(equalTo(result)));
        verify(_nextVersionManagementController).getPreferences(_root, TEST_CATEGORY, path, parameters);
    }

    @Test
    public void setPreferences()
    {
        final List<String> path = Arrays.asList("test1", "test2");
        final Map<String, List<String>> parameters =
                Collections.singletonMap("testParam", Collections.singletonList("testValue"));

        final Object preferences = mock(Object.class);
        _controller.setPreferences(_root, path, preferences, parameters, true);

        verify(_nextVersionManagementController).setPreferences(_root,
                                                                TEST_CATEGORY,
                                                                path,
                                                                preferences,
                                                                parameters,
                                                                true);
    }

    @Test
    public void deletePreferences()
    {
        final List<String> path = Arrays.asList("test1", "test2");
        final Map<String, List<String>> parameters =
                Collections.singletonMap("testParam", Collections.singletonList("testValue"));

        _controller.deletePreferences(_root, path, parameters);

        verify(_nextVersionManagementController).deletePreferences(_root, TEST_CATEGORY, path, parameters);
    }

    @Test
    public void getNextVersionManagementController()
    {
        assertThat(_controller.getNextVersionManagementController(), is(equalTo(_nextVersionManagementController)));
    }

    @Test
    public void convertAttributesToNextVersion()
    {
        final List<String> path = Arrays.asList("test1", "test2");
        final Map<String, Object> attributes = Collections.singletonMap("testParam", "testValue");
        final Map<String, Object> convertedAttributes = Collections.singletonMap("testParam", "testValue2");

        when(_typeController.convertAttributesToNextVersion(_root, path, attributes)).thenReturn(convertedAttributes);
        final Map<String, Object> converted = _controller.convertAttributesToNextVersion(_root, path, attributes);
        assertThat(converted, is(equalTo(convertedAttributes)));
    }
}
