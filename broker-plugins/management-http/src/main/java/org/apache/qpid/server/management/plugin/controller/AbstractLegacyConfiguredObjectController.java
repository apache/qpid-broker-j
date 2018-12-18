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

import static org.apache.qpid.server.management.plugin.ManagementException.createBadRequestManagementException;
import static org.apache.qpid.server.management.plugin.ManagementException.createInternalServerErrorManagementException;
import static org.apache.qpid.server.management.plugin.ManagementException.createNotFoundManagementException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.ManagementRequest;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.RequestType;
import org.apache.qpid.server.model.ConfiguredObject;

public abstract class AbstractLegacyConfiguredObjectController extends AbstractManagementController
        implements LegacyManagementController
{
    private final ManagementController _nextVersionManagementController;
    private final String _modelVersion;

    private final Map<String, String> _categoryNames = new HashMap<>();
    private final Map<String, List<String>> _parents = new HashMap<>();
    private final Map<String, List<String>> _children = new HashMap<>();

    private final Map<String, CategoryController> _categoryConverters = new HashMap<>();
    private final Map<String, Set<TypeController>> _typeControllers = new HashMap<>();
    private volatile LegacyConfiguredObjectToMapConverter _legacyConfiguredObjectToMapConverter;

    public AbstractLegacyConfiguredObjectController(final String modelVersion,
                                                    final ManagementController nextVersionManagementController)
    {
        _modelVersion = modelVersion;
        _nextVersionManagementController = nextVersionManagementController;
    }

    @Override
    public String getVersion()
    {
        return _modelVersion;
    }

    @Override
    public Collection<String> getCategories()
    {
        return Collections.unmodifiableCollection(_categoryNames.values());
    }

    @Override
    public String getCategoryMapping(final String category)
    {
        return String.format("/api/v%s/%s/", getVersion(), category.toLowerCase());
    }

    @Override
    public String getCategory(final ConfiguredObject<?> managedObject)
    {
        return _nextVersionManagementController.getCategory(managedObject);
    }

    @Override
    public List<String> getCategoryHierarchy(final ConfiguredObject<?> root, final String categoryName)
    {
        return getCategoryHierarchy(getCategory(root), categoryName);
    }

    @Override
    public ManagementController getNextVersionManagementController()
    {
        return _nextVersionManagementController;
    }


    @Override
    public LegacyConfiguredObject createOrUpdate(final ConfiguredObject<?> root,
                                                 final String category,
                                                 final List<String> path,
                                                 final Map<String, Object> attributes,
                                                 final boolean isPost) throws ManagementException
    {
        return getCategoryController(category).createOrUpdate(root, path, attributes, isPost);
    }


    @Override
    public Object get(final ConfiguredObject<?> root,
                      final String category,
                      final List<String> path,
                      final Map<String, List<String>> parameters) throws ManagementException
    {
        return getCategoryController(category).get(root, path, convertQueryParameters(parameters));
    }

    @Override
    public int delete(final ConfiguredObject<?> root,
                      final String category,
                      final List<String> path,
                      final Map<String, List<String>> parameters) throws ManagementException
    {
        return getCategoryController(category).delete(root, path, convertQueryParameters(parameters));
    }

    @Override
    public ManagementResponse invoke(final ConfiguredObject<?> root,
                                     final String category,
                                     final List<String> path,
                                     final String operation,
                                     final Map<String, Object> parameters,
                                     final boolean isPost,
                                     final boolean isSecureOrAllowedOnInsecureChannel) throws ManagementException
    {
        return getCategoryController(category).invoke(root, path, operation, parameters, isPost,
                                                      isSecureOrAllowedOnInsecureChannel);
    }

    @Override
    public Object getPreferences(final ConfiguredObject<?> root,
                                 final String category,
                                 final List<String> path,
                                 final Map<String, List<String>> parameters) throws ManagementException
    {
        return getCategoryController(category).getPreferences(root, path, convertQueryParameters(parameters));
    }

    @Override
    public void setPreferences(final ConfiguredObject<?> root,
                               final String category,
                               final List<String> path,
                               final Object preferences,
                               final Map<String, List<String>> parameters,
                               final boolean isPost) throws ManagementException
    {
        getCategoryController(category).setPreferences(root, path, preferences, parameters, isPost);
    }

    @Override
    public int deletePreferences(final ConfiguredObject<?> root,
                                 final String category,
                                 final List<String> path,
                                 final Map<String, List<String>> parameters) throws ManagementException
    {

        return getCategoryController(category).deletePreferences(root, path, convertQueryParameters(parameters));
    }

    @Override
    public CategoryController getCategoryController(final String category)
    {
        CategoryController converter = _categoryConverters.get(category.toLowerCase());
        if (converter == null)
        {
            throw createInternalServerErrorManagementException(String.format("Converter for type '%s' cannot be found ",
                                                                             category));
        }
        return converter;
    }

    @Override
    public Set<TypeController> getTypeControllersByCategory(final String name)
    {
        Set<TypeController> typeControllers = _typeControllers.get(name.toLowerCase());
        if (typeControllers == null)
        {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(typeControllers);
    }

    @Override
    public List<String> getCategoryHierarchy(final String rootCategory,
                                             final String categoryName)
    {
        if (!_categoryNames.containsKey(rootCategory.toLowerCase()))
        {
            throw createInternalServerErrorManagementException(String.format("Unsupported root category '%s'",
                                                                             rootCategory));
        }
        if (!_categoryNames.containsKey(categoryName.toLowerCase()))
        {
            throw createInternalServerErrorManagementException(String.format("Unsupported category '%s'",
                                                                             categoryName));
        }
        final List<String> hierarchyList = new ArrayList<>();

        String category = _categoryNames.get(categoryName.toLowerCase());
        if (!category.equals(rootCategory))
        {
            Collection<String> parentCategories;

            hierarchyList.add(category);

            while (!(parentCategories = _parents.get(category)).contains(rootCategory))
            {
                hierarchyList.addAll(parentCategories);
                category = parentCategories.iterator().next();
            }

            Collections.reverse(hierarchyList);
        }
        return Collections.unmodifiableList(hierarchyList);
    }

    @Override
    public Collection<String> getChildrenCategories(String category)
    {
        List<String> children = _children.get(_categoryNames.get(category.toLowerCase()));
        if (children == null)
        {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(children);
    }

    @Override
    public Collection<String> getParentTypes(final String category)
    {
        return _parents.get(category);
    }

    public void initialize()
    {
        initialize(CategoryControllerFactory.findFactories(getVersion()),
                   TypeControllerFactory.findFactories(getVersion()));
    }

    protected void initialize(final Set<CategoryControllerFactory> categoryFactories,
                           final Set<TypeControllerFactory> typeFactories)
    {
        createTypeControllers(typeFactories);
        createCategoryControllers(categoryFactories);
        _legacyConfiguredObjectToMapConverter = new LegacyConfiguredObjectToMapConverter(this);
    }


    @Override
    protected RequestType getRequestType(final ManagementRequest managementRequest) throws ManagementException
    {
        final List<String> path = managementRequest.getPath();
        final String category = managementRequest.getCategory();
        if (category == null)
        {
            throw createNotFoundManagementException(String.format("Category is not found for path '%s%s'",
                                                                  getCategoryMapping(category),
                                                                  buildPath(path)));
        }

        final List<String> hierarchy = getCategoryHierarchy(managementRequest.getRoot(), category);
        return getManagementRequestType(managementRequest.getMethod(), category, path, hierarchy);
    }

    @Override
    public LegacyConfiguredObject convertFromNextVersion(final LegacyConfiguredObject nextVersionObject)
    {
        return getCategoryController(nextVersionObject.getCategory()).convertFromNextVersion(nextVersionObject);
    }

    protected abstract Map<String, List<String>> convertQueryParameters(final Map<String, List<String>> parameters);

    private void addRelationship(String parentCategory, String childCategory)
    {
        Collection<String> parents = _parents.computeIfAbsent(childCategory, k -> new ArrayList<>());
        parents.add(parentCategory);

        Collection<String> children = _children.computeIfAbsent(parentCategory, k -> new ArrayList<>());
        children.add(childCategory);

        _categoryNames.put(childCategory.toLowerCase(), childCategory);
    }


    private void createCategoryControllers(final Set<CategoryControllerFactory> factories)
    {
        factories.stream()
                 .map(this::create)
                 .flatMap(Collection::stream)
                 .peek(this::register)
                 .forEach(c -> {
                     if (_categoryConverters.put(c.getCategory().toLowerCase(), c) != null)
                     {
                         throw new IllegalStateException(String.format(
                                 "Category converter for category '%s' is already registered",
                                 c.getCategory()));
                     }
                 });
    }

    private Set<CategoryController> create(final CategoryControllerFactory factory)
    {
        return factory.getSupportedCategories()
                      .stream()
                      .map(type -> factory.createController(type, this))
                      .collect(Collectors.toSet());
    }

    private void register(final CategoryController converter)
    {
        String type = converter.getCategory();
        for (String category : converter.getParentCategories())
        {
            addRelationship(category, type);
        }
        _categoryNames.put(type.toLowerCase(), type);
    }

    private void createTypeControllers(Set<TypeControllerFactory> typeFactories)
    {
        for (TypeControllerFactory factory : typeFactories)
        {
            TypeController controller = factory.createController(this);
            Set<TypeController> categoryTypeConverters =
                    _typeControllers.computeIfAbsent(factory.getCategory().toLowerCase(), k -> new HashSet<>());

            categoryTypeConverters.add(controller);
        }
    }

    protected LegacyConfiguredObjectToMapConverter getLegacyConfiguredObjectToMapConverter()
    {
        return _legacyConfiguredObjectToMapConverter;
    }


    private RequestType getManagementRequestType(final String method,
                                                 final String category,
                                                 final List<String> parts,
                                                 final List<String> hierarchy)
    {
        if ("POST".equals(method))
        {
            return getPostRequestType(category, parts, hierarchy);
        }
        else if ("PUT".equals(method))
        {
            return getPutRequestType(category, parts, hierarchy);
        }
        else if ("GET".equals(method))
        {
            return getGetRequestType(category, parts, hierarchy);
        }
        else if ("DELETE".equals(method))
        {
            return getDeleteRequestType(category, parts, hierarchy);
        }
        else
        {
            throw createBadRequestManagementException(String.format("Unexpected method type '%s' for path '%s%s'",
                                                                    method,
                                                                    getCategoryMapping(category),
                                                                    buildPath(parts)));
        }
    }

    private RequestType getDeleteRequestType(final String category,
                                             final List<String> parts,
                                             final List<String> hierarchy)
    {
        if (parts.size() <= hierarchy.size())
        {
            return RequestType.MODEL_OBJECT;
        }
        else
        {
            if (USER_PREFERENCES.equals(parts.get(hierarchy.size())))
            {
                return RequestType.USER_PREFERENCES;
            }
        }
        final String categoryMapping = getCategoryMapping(category);
        final String expectedPath = buildExpectedPath(categoryMapping, hierarchy);
        throw createBadRequestManagementException(String.format(
                "Invalid DELETE path '%s%s'. Expected: '%s' or '%s/userpreferences[/<preference type>[/<preference name>]]'",
                categoryMapping,
                buildPath(parts),
                expectedPath,
                expectedPath));
    }

    private RequestType getGetRequestType(final String category,
                                          final List<String> parts,
                                          final List<String> hierarchy)
    {
        if (parts.size() <= hierarchy.size())
        {
            return RequestType.MODEL_OBJECT;
        }
        else
        {
            if (USER_PREFERENCES.equals(parts.get(hierarchy.size())))
            {
                return RequestType.USER_PREFERENCES;
            }
            else if (VISIBLE_USER_PREFERENCES.equals(parts.get(hierarchy.size())))
            {
                return RequestType.VISIBLE_PREFERENCES;
            }
            else if (parts.size() == hierarchy.size() + 1)
            {
                return RequestType.OPERATION;
            }
        }

        final String categoryMapping = getCategoryMapping(category);
        throw createBadRequestManagementException(String.format(
                "Invalid GET path '%s%s'. Expected: '%s[/<operation name>]'",
                categoryMapping,
                buildPath(parts),
                buildExpectedPath(categoryMapping, hierarchy)));
    }

    private RequestType getPutRequestType(final String category,
                                          final List<String> parts,
                                          final List<String> hierarchy)
    {
        if (parts.size() == hierarchy.size() || parts.size() == hierarchy.size() - 1)
        {
            return RequestType.MODEL_OBJECT;
        }
        else if (parts.size() > hierarchy.size() && USER_PREFERENCES.equals(parts.get(hierarchy.size())))
        {
            return RequestType.USER_PREFERENCES;
        }
        else
        {
            final String categoryMapping = getCategoryMapping(category);
            throw createBadRequestManagementException(String.format("Invalid PUT path '%s%s'. Expected: '%s'",
                                                                    categoryMapping, buildPath(parts),
                                                                    buildExpectedPath(categoryMapping, hierarchy)));
        }
    }

    private RequestType getPostRequestType(final String category,
                                           final List<String> parts,
                                           final List<String> hierarchy)
    {
        if (parts.size() == hierarchy.size() || parts.size() == hierarchy.size() - 1)
        {
            return RequestType.MODEL_OBJECT;
        }
        else if (parts.size() > hierarchy.size())
        {
            if (USER_PREFERENCES.equals(parts.get(hierarchy.size())))
            {
                return RequestType.USER_PREFERENCES;
            }
            else if (parts.size() == hierarchy.size() + 1
                     && !VISIBLE_USER_PREFERENCES.equals(parts.get(hierarchy.size())))
            {
                return RequestType.OPERATION;
            }
        }

        final String categoryMapping = getCategoryMapping(category);
        final String expectedFullPath = buildExpectedPath(categoryMapping, hierarchy);
        final String expectedParentPath =
                buildExpectedPath(categoryMapping, hierarchy.subList(0, hierarchy.size() - 1));

        throw createBadRequestManagementException(String.format(
                "Invalid POST path '%s%s'. Expected: '%s/<operation name>'"
                + " or '%s'"
                + " or '%s/userpreferences[/<preference type>]'",
                categoryMapping,
                buildPath(parts),
                expectedFullPath,
                expectedParentPath,
                expectedFullPath));
    }

    private String buildExpectedPath(final String servletPath, final List<String> hierarchy)
    {
        return hierarchy.stream()
                        .map(h -> String.format("/<%s name>", h))
                        .collect((Collectors.joining("", servletPath, "")));
    }

    private String buildPath(final List<String> path)
    {
        return path.isEmpty() ? "" : "/" + String.join("/", path);
    }

    protected Object formatConfiguredObject(final Object content,
                                            final boolean isSecureOrAllowedOnInsecureChannel,
                                            final int depth,
                                            final int oversizeThreshold,
                                            final boolean actuals,
                                            final boolean excludeInheritedContext, final boolean responseAsList)
    {
        if (content instanceof LegacyConfiguredObject)
        {
            Object object = convertObject(
                    (LegacyConfiguredObject) content,
                    depth,
                    actuals,
                    oversizeThreshold,
                    isSecureOrAllowedOnInsecureChannel,
                    excludeInheritedContext);
            return responseAsList ? Collections.singletonList(object) : object;
        }
        else if (content instanceof Collection)
        {
            Collection<Map<String, Object>> results = ((Collection<?>) content).stream()
                                                                               .filter(o -> o instanceof LegacyConfiguredObject)
                                                                               .map(LegacyConfiguredObject.class::cast)
                                                                               .map(o -> convertObject(
                                                                                       o,
                                                                                       depth,
                                                                                       actuals,
                                                                                       oversizeThreshold,
                                                                                       isSecureOrAllowedOnInsecureChannel,
                                                                                       excludeInheritedContext))
                                                                               .collect(Collectors.toSet());
            if (!results.isEmpty())
            {
                return results;
            }
        }
        return content;
    }

    protected Map<String, Object> convertObject(final LegacyConfiguredObject legacyConfiguredObjectObject,
                                                final int depth,
                                                final boolean actuals,
                                                final int oversizeThreshold,
                                                final boolean isSecureOrConfidentialOperationAllowedOnInsecureChannel,
                                                final boolean excludeInheritedContext)
    {
        return getLegacyConfiguredObjectToMapConverter().convertManageableToMap(legacyConfiguredObjectObject,
                                                                                depth,
                                                                                actuals,
                                                                                oversizeThreshold,
                                                                                excludeInheritedContext);
    }
}
