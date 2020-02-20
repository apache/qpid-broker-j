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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementRequest;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.RequestType;
import org.apache.qpid.server.management.plugin.ResponseType;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.test.utils.UnitTestBase;

public class AbstractLegacyConfiguredObjectControllerTest extends UnitTestBase
{

    private static final String TEST_VERSION = "testVersion";
    private static final String TEST_CATEGORY = "testCategory";
    private static final String TEST_CATEGORY_2 = "testCategory2";
    private static final String TEST_TYPE = "testType";
    private ManagementController _nextVersionManagementController;
    private AbstractLegacyConfiguredObjectController _controller;
    private CategoryController _categoryController2;
    private ConfiguredObject<?> _root;
    private TypeController _typeController;

    @Before
    public void setUp()
    {
        _nextVersionManagementController = mock(ManagementController.class);
        _controller = new AbstractLegacyConfiguredObjectController(TEST_VERSION, _nextVersionManagementController)
        {
            @Override
            public Object formatConfiguredObject(final Object content,
                                                 final Map<String, List<String>> parameters,
                                                 final boolean isSecureOrAllowedOnInsecureChannel)
            {
                return null;
            }

            @Override
            protected Map<String, List<String>> convertQueryParameters(final Map<String, List<String>> parameters)
            {
                return parameters;
            }
        };
        _root = mock(ConfiguredObject.class);
        final CategoryControllerFactory categoryFactory = mock(CategoryControllerFactory.class);
        final TypeControllerFactory typeFactory = mock(TypeControllerFactory.class);
        final CategoryController categoryController = mock(CategoryController.class);
        when(categoryController.getParentCategories()).thenReturn(new String[0]);
        when(categoryController.getCategory()).thenReturn(TEST_CATEGORY);
        _categoryController2 = mock(CategoryController.class);
        when(_categoryController2.getCategory()).thenReturn(TEST_CATEGORY_2);
        when(_categoryController2.getParentCategories()).thenReturn(new String[]{TEST_CATEGORY});

        when(categoryFactory.getModelVersion()).thenReturn(TEST_VERSION);
        when(categoryFactory.getSupportedCategories()).thenReturn(new HashSet<>(Arrays.asList(TEST_CATEGORY,
                                                                                              TEST_CATEGORY_2)));
        when(categoryFactory.createController(TEST_CATEGORY, _controller)).thenReturn(categoryController);
        when(categoryFactory.createController(TEST_CATEGORY_2, _controller)).thenReturn(_categoryController2);
        when(typeFactory.getCategory()).thenReturn(TEST_CATEGORY);
        when(typeFactory.getModelVersion()).thenReturn(TEST_VERSION);
        _typeController = mock(TypeController.class);
        when(_typeController.getTypeName()).thenReturn(TEST_TYPE);
        when(typeFactory.createController(_controller)).thenReturn(_typeController);
        _controller.initialize(Collections.singleton(categoryFactory), Collections.singleton(typeFactory));
    }

    @Test
    public void getVersion()
    {
        assertThat(_controller.getVersion(), is(equalTo(TEST_VERSION)));
    }

    @Test
    public void getCategories()
    {
        Collection<String> categories = _controller.getCategories();
        Set<String> expected = new HashSet<>(Arrays.asList(TEST_CATEGORY, TEST_CATEGORY_2));
        assertThat(new HashSet<>(categories), is(equalTo(expected)));
    }

    @Test
    public void getCategoryMapping()
    {
        assertThat(_controller.getCategoryMapping(TEST_CATEGORY), is(equalTo(
                String.format("/api/v%s/%s/", TEST_VERSION, TEST_CATEGORY.toLowerCase()))));
    }

    @Test
    public void getCategory()
    {
        final ConfiguredObject<?> managementObject = mock(ConfiguredObject.class);
        when(_nextVersionManagementController.getCategory(managementObject)).thenReturn(TEST_CATEGORY);
        String category = _controller.getCategory(managementObject);
        assertThat(category, is(equalTo(TEST_CATEGORY)));
    }

    @Test
    public void getCategoryHierarchy()
    {
        when(_nextVersionManagementController.getCategory(_root)).thenReturn(TEST_CATEGORY);
        final List<String> hierarchy = _controller.getCategoryHierarchy(_root, TEST_CATEGORY_2);

        final Set<String> expected = Collections.singleton(TEST_CATEGORY_2);
        assertThat(new HashSet<>(hierarchy), is(equalTo(expected)));
    }

    @Test
    public void getNextVersionManagementController()
    {
        assertThat(_controller.getNextVersionManagementController(), is(equalTo(_nextVersionManagementController)));
    }

    @Test
    public void createOrUpdate()
    {
        final List<String> path = Collections.singletonList("test");
        final Map<String, Object> attributes = Collections.singletonMap("name", "test");
        final LegacyConfiguredObject object = mock(LegacyConfiguredObject.class);
        when(_categoryController2.createOrUpdate(_root, path, attributes, false)).thenReturn(object);
        final LegacyConfiguredObject result =
                _controller.createOrUpdate(_root, TEST_CATEGORY_2, path, attributes, false);
        assertThat(result, is(equalTo(object)));
    }

    @Test
    public void get()
    {
        final List<String> path = Collections.singletonList("test");
        final LegacyConfiguredObject object = mock(LegacyConfiguredObject.class);
        final Map<String, List<String>> parameters =
                Collections.singletonMap("name", Collections.singletonList("test"));
        when(_categoryController2.get(_root, path, parameters)).thenReturn(object);
        final Object result = _controller.get(_root, TEST_CATEGORY_2, path, parameters);
        assertThat(result, is(equalTo(object)));
    }

    @Test
    public void delete()
    {
        final List<String> path = Collections.singletonList("test");
        final Map<String, List<String>> parameters =
                Collections.singletonMap("name", Collections.singletonList("test"));
        _controller.delete(_root, TEST_CATEGORY_2, path, parameters);
        verify(_categoryController2).delete(_root, path, parameters);
    }

    @Test
    public void invoke()
    {
        final List<String> path = Collections.singletonList("test");
        final Map<String, Object> parameters = Collections.singletonMap("name", "test");
        final Object operationResult = mock(Object.class);

        final String operationName = "testOperation";

        final ManagementResponse managementResponse =
                new ControllerManagementResponse(ResponseType.DATA, operationResult);
        when(_categoryController2.invoke(_root, path, operationName, parameters, true, true)).thenReturn(
                managementResponse);
        final ManagementResponse result =
                _controller.invoke(_root, TEST_CATEGORY_2, path, operationName, parameters, true, true);

        assertThat(result, is(notNullValue()));
        assertThat(result.getResponseCode(), is(equalTo(200)));
        assertThat(result.getBody(), is(equalTo(operationResult)));
        verify(_categoryController2).invoke(_root, path, operationName, parameters, true, true);
    }

    @Test
    public void getPreferences()
    {
        final List<String> path = Arrays.asList("test", "preferences");
        final Map<String, List<String>> parameters =
                Collections.singletonMap("name", Collections.singletonList("test"));
        final Object prefs = mock(Object.class);
        when(_categoryController2.getPreferences(_root, path, parameters)).thenReturn(prefs);
        final Object preferences = _controller.getPreferences(_root, TEST_CATEGORY_2, path, parameters);
        assertThat(preferences, is(equalTo(prefs)));
        verify(_categoryController2).getPreferences(_root, path, parameters);
    }

    @Test
    public void setPreferences()
    {
        final List<String> path = Arrays.asList("test", "preferences");
        final Map<String, List<String>> parameters =
                Collections.singletonMap("name", Collections.singletonList("test"));
        final Object prefs = mock(Object.class);
        _controller.setPreferences(_root, TEST_CATEGORY_2, path, prefs, parameters, true);
        verify(_categoryController2).setPreferences(_root, path, prefs, parameters, true);
    }

    @Test
    public void deletePreferences()
    {
        final List<String> path = Arrays.asList("test", "preferences");
        final Map<String, List<String>> parameters =
                Collections.singletonMap("name", Collections.singletonList("test"));
        _controller.deletePreferences(_root, TEST_CATEGORY_2, path, parameters);
        verify(_categoryController2).deletePreferences(_root, path, parameters);
    }

    @Test
    public void getCategoryController()
    {
        assertThat(_controller.getCategoryController(TEST_CATEGORY_2), is(equalTo(_categoryController2)));
    }

    @Test
    public void getTypeControllersByCategory()
    {
        final Set<TypeController> typeControllers = _controller.getTypeControllersByCategory(TEST_CATEGORY);
        assertThat(typeControllers, is(equalTo(Collections.singleton(_typeController))));
    }

    @Test
    public void getChildrenCategories()
    {
        final Collection<String> childrenCategories = _controller.getChildrenCategories(TEST_CATEGORY);
        assertThat(new HashSet<>(childrenCategories), is(equalTo(Collections.singleton(TEST_CATEGORY_2))));
    }

    @Test
    public void getParentTypes()
    {
        final Collection<String> childrenCategories = _controller.getParentTypes(TEST_CATEGORY_2);
        assertThat(new HashSet<>(childrenCategories), is(equalTo(Collections.singleton(TEST_CATEGORY))));
    }

    @Test
    public void getNextVersionLegacyConfiguredObjectConverter()
    {
        assertThat(_controller.getNextVersionManagementController(), is(equalTo(_nextVersionManagementController)));
    }

    @Test
    public void getRequestType()
    {
        final Map<String, List<String>> parameters =
                Collections.singletonMap("name", Collections.singletonList("test"));
        final ManagementRequest managementRequest = mock(ManagementRequest.class);
        doReturn(_root).when(managementRequest).getRoot();
        when(managementRequest.getMethod()).thenReturn("GET");
        when(managementRequest.getParameters()).thenReturn(parameters);
        when(managementRequest.getPath()).thenReturn(Collections.singletonList("test"));
        when(managementRequest.getCategory()).thenReturn(TEST_CATEGORY_2);
        when(_controller.getCategory(_root)).thenReturn(TEST_CATEGORY);

        final RequestType requestType = _controller.getRequestType(managementRequest);
        assertThat(requestType, is(equalTo(RequestType.MODEL_OBJECT)));
    }
}
