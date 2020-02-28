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
package org.apache.qpid.server.management.plugin.controller.latest;

import static org.apache.qpid.server.management.plugin.HttpManagementConfiguration.DEFAULT_PREFERENCE_OPERATION_TIMEOUT;
import static org.apache.qpid.server.management.plugin.HttpManagementConfiguration.PREFERENCE_OPERTAION_TIMEOUT_CONTEXT_NAME;
import static org.apache.qpid.server.management.plugin.ManagementException.createBadRequestManagementException;
import static org.apache.qpid.server.management.plugin.ManagementException.createForbiddenManagementException;
import static org.apache.qpid.server.management.plugin.ManagementException.createNotAllowedManagementException;
import static org.apache.qpid.server.management.plugin.ManagementException.createNotFoundManagementException;
import static org.apache.qpid.server.management.plugin.controller.ConverterHelper.getParameter;
import static org.apache.qpid.server.management.plugin.servlet.rest.AbstractServlet.CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM;
import static org.apache.qpid.server.model.ConfiguredObjectTypeRegistry.returnsCollectionOfConfiguredObjects;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.ManagementRequest;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.RequestType;
import org.apache.qpid.server.management.plugin.ResponseType;
import org.apache.qpid.server.management.plugin.controller.AbstractManagementController;
import org.apache.qpid.server.management.plugin.controller.ControllerManagementResponse;
import org.apache.qpid.server.management.plugin.controller.ConverterHelper;
import org.apache.qpid.server.management.plugin.servlet.rest.ConfiguredObjectToMapConverter;
import org.apache.qpid.server.management.plugin.servlet.rest.NotFoundException;
import org.apache.qpid.server.management.plugin.servlet.rest.RequestInfo;
import org.apache.qpid.server.management.plugin.servlet.rest.RestUserPreferenceHandler;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFinder;
import org.apache.qpid.server.model.ConfiguredObjectOperation;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.OperationParameter;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.preferences.UserPreferences;

public class LatestManagementController extends AbstractManagementController
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LatestManagementController.class);

    private static final String DEPTH_PARAM = "depth";
    private static final String OVERSIZE_PARAM = "oversize";
    private static final String ACTUALS_PARAM = "actuals";
    private static final String SORT_PARAM = "sort";
    private static final String EXTRACT_INITIAL_CONFIG_PARAM = "extractInitialConfig";
    private static final String EXCLUDE_INHERITED_CONTEXT_PARAM = "excludeInheritedContext";
    private static final String SINGLETON_MODEL_OBJECT_RESPONSE_AS_LIST = "singletonModelObjectResponseAsList";
    private static final Set<String> RESERVED_PARAMS =
            new HashSet<>(Arrays.asList(DEPTH_PARAM,
                                        SORT_PARAM,
                                        OVERSIZE_PARAM,
                                        ACTUALS_PARAM,
                                        EXTRACT_INITIAL_CONFIG_PARAM,
                                        CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM,
                                        EXCLUDE_INHERITED_CONTEXT_PARAM,
                                        SINGLETON_MODEL_OBJECT_RESPONSE_AS_LIST));

    private static final int DEFAULT_DEPTH = 0;
    private static final int DEFAULT_OVERSIZE = 120;
    private static final Class<? extends ConfiguredObject>[] EMPTY_HIERARCHY = new Class[0];


    private final ConcurrentMap<ConfiguredObject<?>, ConfiguredObjectFinder> _configuredObjectFinders =
            new ConcurrentHashMap<>();
    private final Set<String> _supportedCategories;

    private final ConfiguredObjectToMapConverter _objectConverter = new ConfiguredObjectToMapConverter();
    private final RestUserPreferenceHandler _userPreferenceHandler;


    LatestManagementController(final HttpManagementConfiguration<?> httpManagement)
    {
        final Long preferenceOperationTimeout =
                httpManagement.getContextValue(Long.class, PREFERENCE_OPERTAION_TIMEOUT_CONTEXT_NAME);
        _userPreferenceHandler = new RestUserPreferenceHandler(preferenceOperationTimeout == null
                                                                       ? DEFAULT_PREFERENCE_OPERATION_TIMEOUT
                                                                       : preferenceOperationTimeout);
        _supportedCategories = Collections.unmodifiableSet(httpManagement.getModel()
                                                                         .getSupportedCategories()
                                                                         .stream()
                                                                         .map(Class::getSimpleName)
                                                                         .collect(Collectors.toSet()));
    }

    @Override
    public String getVersion()
    {
        return BrokerModel.MODEL_VERSION;
    }

    @Override
    public Set<String> getCategories()
    {
        return _supportedCategories;
    }

    @Override
    public String getCategoryMapping(final String category)
    {
        return String.format("/api/v%s/%s/", getVersion(), category.toLowerCase());
    }

    @Override
    public String getCategory(final ConfiguredObject<?> managedObject)
    {
        return managedObject.getCategoryClass().getSimpleName();
    }

    @Override
    public List<String> getCategoryHierarchy(final ConfiguredObject<?> root, final String category)
    {
        ConfiguredObjectFinder finder = getConfiguredObjectFinder(root);
        Class<? extends ConfiguredObject>[] hierarchy = finder.getHierarchy(category.toLowerCase());
        if (hierarchy == null)
        {
            return Collections.emptyList();
        }
        return Arrays.stream(hierarchy).map(Class::getSimpleName).collect(Collectors.toList());
    }

    @Override
    public ManagementController getNextVersionManagementController()
    {
        return null;
    }

    @Override
    protected RequestType getRequestType(final ManagementRequest request) throws ManagementException
    {
        final ConfiguredObject<?> root = request.getRoot();
        if (root == null)
        {
            final String message =
                    String.format("No HTTP Management alias mapping found for '%s'", request.getRequestURL());
            LOGGER.info(message);
            throw createNotFoundManagementException(message);
        }
        final ConfiguredObjectFinder finder = getConfiguredObjectFinder(root);
        final String category = request.getCategory();
        final Class<? extends ConfiguredObject> configuredClass = getRequestCategoryClass(category, root.getModel());
        final Class<? extends ConfiguredObject>[] hierarchy = getHierarchy(finder, configuredClass);
        return getManagementRequestType(request.getMethod(), category, request.getPath(), hierarchy);
    }

    @Override
    public Object get(final ConfiguredObject<?> root,
                      final String category,
                      final List<String> path,
                      final Map<String, List<String>> parameters) throws ManagementException
    {
        try
        {
            final Predicate<ConfiguredObject<?>> filterPredicate = buildFilterPredicates(parameters);
            final boolean singleObjectRequest = isFullPath(root, path, category) && !hasFilter(parameters);
            final Collection<ConfiguredObject<?>> allObjects = getTargetObjects(root, category, path, filterPredicate);
            if (singleObjectRequest)
            {
                if (allObjects.isEmpty())
                {
                    throw createNotFoundManagementException("Not Found");
                }
                else if (allObjects.size() != 1)
                {
                    throw createBadRequestManagementException(String.format(
                            "Unexpected number of objects found [%d] for singleton request URI '%s'",
                            allObjects.size(), ManagementException.getRequestURI(path, getCategoryMapping(category))));
                }
                else
                {
                    return allObjects.iterator().next();
                }
            }
            else
            {
                return allObjects;
            }
        }
        catch (RuntimeException e)
        {
            throw ManagementException.toManagementException(e, getCategoryMapping(category), path);
        }
        catch (Error e)
        {
            throw ManagementException.handleError(e);
        }
     }

    @Override
    public ConfiguredObject<?> createOrUpdate(final ConfiguredObject<?> root,
                                              final String category,
                                              final List<String> path,
                                              final Map<String, Object> providedObject,
                                              final boolean isPost) throws ManagementException
    {
        try
        {
            final List<String> hierarchy = getCategoryHierarchy(root, category);
            if (path.isEmpty() && hierarchy.size() == 0)
            {
                root.setAttributes(providedObject);
                return null;
            }
            final Class<? extends ConfiguredObject> categoryClass = getRequestCategoryClass(category, root.getModel());
            ConfiguredObject theParent = root;
            if (hierarchy.size() > 1)
            {
                final ConfiguredObjectFinder finder = getConfiguredObjectFinder(root);
                theParent = finder.findObjectParentsFromPath(path, getHierarchy(finder, categoryClass), categoryClass);
            }

            final boolean isFullObjectURL = path.size() == hierarchy.size();
            if (isFullObjectURL)
            {
                final String name = path.get(path.size() - 1);
                final ConfiguredObject<?> configuredObject = theParent.getChildByName(categoryClass, name);
                if (configuredObject != null)
                {
                    configuredObject.setAttributes(providedObject);
                    return null;
                }
                else if (isPost)
                {
                    throw createNotFoundManagementException(String.format("%s '%s' not found",
                                                                          categoryClass.getSimpleName(),
                                                                          name));
                }
                else
                {
                    providedObject.put(ConfiguredObject.NAME, name);
                }
            }

            return theParent.createChild(categoryClass, providedObject);
        }
        catch (RuntimeException e)
        {
            throw ManagementException.toManagementException(e, getCategoryMapping(category), path);
        }
        catch (Error e)
        {
            throw ManagementException.handleError(e);
        }
    }

    @Override
    public int delete(final ConfiguredObject<?> root,
                      final String category,
                      final List<String> names,
                      final Map<String, List<String>> parameters) throws ManagementException
    {
        int counter = 0;
        try
        {
            final Predicate<ConfiguredObject<?>> filterPredicate = buildFilterPredicates(parameters);
            final Collection<ConfiguredObject<?>> allObjects = getTargetObjects(root, category, names, filterPredicate);
            if (allObjects.isEmpty())
            {
                throw createNotFoundManagementException("Not Found");
            }

            for (ConfiguredObject o : allObjects)
            {
                o.delete();
                counter++;
            }
        }
        catch (RuntimeException e)
        {
            throw ManagementException.toManagementException(e, getCategoryMapping(category), names);
        }
        catch (Error e)
        {
            throw ManagementException.handleError(e);
        }
        return counter;
    }


    @Override
    @SuppressWarnings("unchecked")
    public ManagementResponse invoke(final ConfiguredObject<?> root,
                                     final String category,
                                     final List<String> names,
                                     final String operationName,
                                     final Map<String, Object> operationArguments,
                                     final boolean isPost,
                                     final boolean isSecureOrAllowedOnInsecureChannel) throws ManagementException
    {
        ResponseType responseType = ResponseType.DATA;
        Object returnValue;
        try
        {
            final ConfiguredObject<?> target = getTarget(root, category, names);
            final Map<String, ConfiguredObjectOperation<?>> availableOperations =
                    root.getModel().getTypeRegistry().getOperations(target.getClass());
            final ConfiguredObjectOperation operation = availableOperations.get(operationName);
            if (operation == null)
            {
                throw createNotFoundManagementException(String.format("No such operation '%s' in '%s'",
                                                                      operationName,
                                                                      category));
            }

            final Map<String, Object> arguments;
            if (isPost)
            {
                arguments = operationArguments;
            }
            else
            {
                final Set<String> supported = ((List<OperationParameter>) operation.getParameters()).stream()
                                                                                                    .map(OperationParameter::getName)
                                                                                                    .collect(Collectors.toSet());
                arguments = operationArguments.entrySet()
                                              .stream()
                                              .filter(e -> !RESERVED_PARAMS.contains(e.getKey())
                                                           || supported.contains(e.getKey()))
                                              .collect(Collectors.toMap(Map.Entry::getKey,
                                                                        Map.Entry::getValue));
            }

            if (operation.isSecure(target, arguments) && !isSecureOrAllowedOnInsecureChannel)
            {
                throw createForbiddenManagementException(String.format(
                        "Operation '%s' can only be performed over a secure (HTTPS) connection",
                        operationName));
            }

            if (!isPost && !operation.isNonModifying())
            {
                throw createNotAllowedManagementException(String.format(
                        "Operation '%s' modifies the object so you must use POST.",
                        operationName), Collections.singletonMap("Allow", "POST"));
            }

            returnValue = operation.perform(target, arguments);

            if (ConfiguredObject.class.isAssignableFrom(operation.getReturnType())
                || returnsCollectionOfConfiguredObjects(operation))
            {
                responseType = ResponseType.MODEL_OBJECT;
            }

        }
        catch (RuntimeException e)
        {
            throw ManagementException.toManagementException(e, getCategoryMapping(category), names);
        }
        catch (Error e)
        {
            throw ManagementException.handleError(e);
        }
        return new ControllerManagementResponse(responseType,returnValue);
    }

    @Override
    public Object getPreferences(final ConfiguredObject<?> root,
                                 final String category,
                                 final List<String> path,
                                 final Map<String, List<String>> parameters) throws ManagementException
    {
        Object responseObject;
        try
        {
            final List<String> hierarchy = getCategoryHierarchy(root, category);
            final Collection<ConfiguredObject<?>> allObjects = getTargetObjects(root, category, path, null);
            if (allObjects.isEmpty() && isFullPath(root, path, category))
            {
                throw createNotFoundManagementException("Not Found");
            }

            final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(path.subList(0, hierarchy.size()),
                                                                                     path.subList(hierarchy.size() + 1,
                                                                                                  path.size()),
                                                                                     parameters);

            if (path.contains("*"))
            {
                List<Object> preferencesList = new ArrayList<>(allObjects.size());
                responseObject = preferencesList;
                for (ConfiguredObject<?> target : allObjects)
                {
                    try
                    {
                        final UserPreferences userPreferences = target.getUserPreferences();
                        final Object preferences = _userPreferenceHandler.handleGET(userPreferences, requestInfo);
                        if (preferences == null
                            || (preferences instanceof Collection && ((Collection) preferences).isEmpty())
                            || (preferences instanceof Map && ((Map) preferences).isEmpty()))
                        {
                            continue;
                        }
                        preferencesList.add(preferences);
                    }
                    catch (NotFoundException e)
                    {
                        // The case where the preference's type and name is provided, but this particular object does not
                        // have a matching preference.
                    }
                }
            }
            else
            {
                final ConfiguredObject<?> target = allObjects.iterator().next();
                final UserPreferences userPreferences = target.getUserPreferences();
                responseObject = _userPreferenceHandler.handleGET(userPreferences, requestInfo);
            }
        }
        catch (RuntimeException e)
        {
            throw ManagementException.toManagementException(e, getCategoryMapping(category), path);
        }
        catch (Error e)
        {
            throw ManagementException.handleError(e);
        }
        return responseObject;
    }

    @Override
    public void setPreferences(final ConfiguredObject<?> root,
                               final String category,
                               final List<String> path,
                               final Object providedObject,
                               final Map<String, List<String>> parameters,
                               final boolean isPost) throws ManagementException

    {
        try
        {
            final List<String> hierarchy = getCategoryHierarchy(root, category);
            final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(path.subList(0, hierarchy.size()),
                                                                                     path.subList(hierarchy.size() + 1,
                                                                                                  path.size()),
                                                                                     parameters);
            final ConfiguredObject<?> target = getTarget(root, category, requestInfo.getModelParts());
            if (isPost)
            {
                _userPreferenceHandler.handlePOST(target, requestInfo, providedObject);
            }
            else
            {
                _userPreferenceHandler.handlePUT(target, requestInfo, providedObject);
            }
        }
        catch (RuntimeException e)
        {
            throw ManagementException.toManagementException(e, getCategoryMapping(category), path);
        }
        catch (Error e)
        {
            throw ManagementException.handleError(e);
        }
    }

    @Override
    public int deletePreferences(final ConfiguredObject<?> root,
                                 final String category,
                                 final List<String> names,
                                 final Map<String, List<String>> parameters) throws ManagementException
    {
        int counter = 0;
        try
        {
            final List<String> hierarchy = getCategoryHierarchy(root, category);
            final RequestInfo requestInfo = RequestInfo.createPreferencesRequestInfo(names.subList(0, hierarchy.size()),
                                                                                     names.subList(hierarchy.size() + 1,
                                                                                                   names.size()),
                                                                                     parameters);
            final Collection<ConfiguredObject<?>> objects = getTargetObjects(root,
                                                                             category,
                                                                             requestInfo.getModelParts(),
                                                                             buildFilterPredicates(parameters));
            if (objects == null)
            {
                throw createNotFoundManagementException("Not Found");
            }

            //TODO: define format how to report the results for bulk delete, i.e. how to report individual errors and success statuses
            if (objects.size() > 1)
            {
                throw createBadRequestManagementException("Deletion of user preferences using wildcards is unsupported");
            }

            for (ConfiguredObject o : objects)
            {
                _userPreferenceHandler.handleDELETE(o.getUserPreferences(), requestInfo);
                counter++;
            }
        }
        catch (RuntimeException e)
        {
            throw ManagementException.toManagementException(e, getCategoryMapping(category), names);
        }
        catch (Error e)
        {
            throw ManagementException.handleError(e);
        }
        return counter;
    }

    @Override
    public Object formatConfiguredObject(final Object content,
                                         final Map<String, List<String>> parameters,
                                         final boolean isSecureOrAllowedOnInsecureChannel)
    {

        final int depth = ConverterHelper.getIntParameterFromRequest(parameters, DEPTH_PARAM, DEFAULT_DEPTH);
        final int oversizeThreshold = ConverterHelper.getIntParameterFromRequest(parameters, OVERSIZE_PARAM, DEFAULT_OVERSIZE);
        final boolean actuals = Boolean.parseBoolean(getParameter(ACTUALS_PARAM, parameters));
        final String excludeInheritedContextParameter = getParameter(EXCLUDE_INHERITED_CONTEXT_PARAM, parameters);
        final boolean excludeInheritedContext = excludeInheritedContextParameter == null
                                                || Boolean.parseBoolean(excludeInheritedContextParameter);
        final boolean responseAsList =
                Boolean.parseBoolean(getParameter(SINGLETON_MODEL_OBJECT_RESPONSE_AS_LIST, parameters));

        if (content instanceof ConfiguredObject)
        {
            Object object = convertObject(
                    (ConfiguredObject) content,
                    depth,
                    actuals,
                    oversizeThreshold,
                    isSecureOrAllowedOnInsecureChannel,
                    excludeInheritedContext);
            return responseAsList ? Collections.singletonList(object) : object;
        }
        else if (content instanceof Collection)
        {
            Collection<Map<String,Object>> results = ((Collection<?>) content).stream()
                                                                              .filter(o -> o instanceof ConfiguredObject)
                                                                              .map(ConfiguredObject.class::cast)
                                                                              .map(o -> convertObject(
                                                                                      o,
                                                                                      depth,
                                                                                      actuals,
                                                                                      oversizeThreshold,
                                                                                      isSecureOrAllowedOnInsecureChannel,
                                                                                      excludeInheritedContext)).collect(Collectors.toSet());
            if (!results.isEmpty())
            {
                return results;
            }
        }
        return content;
    }

    private Map<String,Object> convertObject(final ConfiguredObject<?> configuredObject, final int depth,
                                 final boolean actuals,
                                 final int oversizeThreshold,
                                 final boolean isSecureOrConfidentialOperationAllowedOnInsecureChannel,
                                 final boolean excludeInheritedContext)
    {
        return _objectConverter.convertObjectToMap(configuredObject, configuredObject.getCategoryClass(),
                                                   new ConfiguredObjectToMapConverter.ConverterOptions(
                                                           depth,
                                                           actuals,
                                                           oversizeThreshold,
                                                           isSecureOrConfidentialOperationAllowedOnInsecureChannel,
                                                           excludeInheritedContext));
    }

    private boolean isFullPath(final ConfiguredObject root, final List<String> parts, final String category)
    {
        List<String> hierarchy = getCategoryHierarchy(root, category);
        return parts.size() == hierarchy.size() && !parts.contains("*");
    }

    private ConfiguredObjectFinder getConfiguredObjectFinder(final ConfiguredObject<?> root)
    {
        ConfiguredObjectFinder finder = _configuredObjectFinders.get(root);
        if (finder == null)
        {
            finder = new ConfiguredObjectFinder(root);
            final ConfiguredObjectFinder existingValue = _configuredObjectFinders.putIfAbsent(root, finder);
            if (existingValue != null)
            {
                finder = existingValue;
            }
            else
            {
                final AbstractConfigurationChangeListener deletionListener =
                        new AbstractConfigurationChangeListener()
                        {
                            @Override
                            public void stateChanged(final ConfiguredObject<?> object,
                                                     final State oldState,
                                                     final State newState)
                            {
                                if (newState == State.DELETED)
                                {
                                    _configuredObjectFinders.remove(root);
                                }
                            }
                        };
                root.addChangeListener(deletionListener);
                if (root.getState() == State.DELETED)
                {
                    _configuredObjectFinders.remove(root);
                    root.removeChangeListener(deletionListener);
                }
            }
        }
        return finder;
    }

    private Collection<ConfiguredObject<?>> getTargetObjects(final ConfiguredObject<?> root,
                                                             final String category,
                                                             final List<String> path,
                                                             final Predicate<ConfiguredObject<?>> filterPredicate)
    {
        final ConfiguredObjectFinder finder = getConfiguredObjectFinder(root);
        final Class<? extends ConfiguredObject> configuredClass = getRequestCategoryClass(category, root.getModel());
        Collection<ConfiguredObject<?>> targetObjects =
                finder.findObjectsFromPath(path, getHierarchy(finder, configuredClass), true);

        if (targetObjects == null)
        {
            targetObjects = Collections.emptySet();
        }
        else if (filterPredicate != null)
        {
            targetObjects = targetObjects.stream().filter(filterPredicate).collect(Collectors.toList());
        }
        return targetObjects;
    }

    private Class<? extends ConfiguredObject>[] getHierarchy(final ConfiguredObjectFinder finder,
                                                             final Class<? extends ConfiguredObject> configuredClass)
    {
        final Class<? extends ConfiguredObject>[] hierarchy = finder.getHierarchy(configuredClass);
        if (hierarchy == null)
        {
            return EMPTY_HIERARCHY;
        }
        return hierarchy;
    }

    private RequestType getManagementRequestType(final String method,
                                                 final String categoryName,
                                                 final List<String> parts,
                                                 final Class<? extends ConfiguredObject>[] hierarchy)
    {
        String servletPath = getCategoryMapping(categoryName);
        if ("POST".equals(method))
        {
            return getPostRequestType(parts, hierarchy, servletPath);
        }
        else if ("PUT".equals(method))
        {
            return getPutRequestType(parts, hierarchy, servletPath);
        }
        else if ("GET".equals(method))
        {
            return getGetRequestType(parts, hierarchy, servletPath);
        }
        else if ("DELETE".equals(method))
        {
            return getDeleteRequestType(parts, hierarchy, servletPath);
        }
        else
        {
            throw createBadRequestManagementException(String.format("Unexpected method type '%s' for path '%s/%s'",
                                                                    method,
                                                                    servletPath,
                                                                    String.join("/", parts)));
        }
    }

    private RequestType getDeleteRequestType(final List<String> parts,
                                             final Class<? extends ConfiguredObject>[] hierarchy,
                                             final String servletPath)
    {
        if (parts.size() <= hierarchy.length)
        {
            return RequestType.MODEL_OBJECT;
        }
        else
        {
            if (USER_PREFERENCES.equals(parts.get(hierarchy.length)))
            {
                return RequestType.USER_PREFERENCES;
            }
        }
        final String expectedPath = buildExpectedPath(servletPath, Arrays.asList(hierarchy));
        throw createBadRequestManagementException(String.format(
                "Invalid DELETE path '%s/%s'. Expected: '%s' or '%s/userpreferences[/<preference type>[/<preference name>]]'",
                servletPath,
                String.join("/", parts),
                expectedPath,
                expectedPath));
    }

    private RequestType getGetRequestType(final List<String> parts,
                                          final Class<? extends ConfiguredObject>[] hierarchy,
                                          final String servletPath)
    {
        if (parts.size() <= hierarchy.length)
        {
            return RequestType.MODEL_OBJECT;
        }
        else
        {
            if (USER_PREFERENCES.equals(parts.get(hierarchy.length)))
            {
                return RequestType.USER_PREFERENCES;
            }
            else if (VISIBLE_USER_PREFERENCES.equals(parts.get(hierarchy.length)))
            {
                return RequestType.VISIBLE_PREFERENCES;
            }
            else if (parts.size() == hierarchy.length + 1)
            {
                return RequestType.OPERATION;
            }
        }
        final String expectedPath = buildExpectedPath(servletPath, Arrays.asList(hierarchy));
        throw new IllegalArgumentException(String.format("Invalid GET path '%s/%s'. Expected: '%s[/<operation name>]'",
                                                         servletPath,
                                                         String.join("/", parts),
                                                         expectedPath));
    }

    private RequestType getPutRequestType(final List<String> parts,
                                          final Class<? extends ConfiguredObject>[] hierarchy,
                                          final String servletPath)
    {
        if (parts.size() == hierarchy.length || parts.size() == hierarchy.length - 1)
        {
            return RequestType.MODEL_OBJECT;
        }
        else if (parts.size() > hierarchy.length && USER_PREFERENCES.equals(parts.get(hierarchy.length)))
        {
            return RequestType.USER_PREFERENCES;
        }
        else
        {
            final String expectedPath = buildExpectedPath(servletPath, Arrays.asList(hierarchy));
            throw createBadRequestManagementException(String.format("Invalid PUT path '%s/%s'. Expected: '%s'",
                                                                    servletPath,
                                                                    String.join("/", parts),
                                                                    expectedPath));
        }
    }

    private RequestType getPostRequestType(final List<String> parts,
                                           final Class<? extends ConfiguredObject>[] hierarchy,
                                           final String servletPath)
    {
        if (parts.size() == hierarchy.length || parts.size() == hierarchy.length - 1)
        {
            return RequestType.MODEL_OBJECT;
        }
        else if (parts.size() > hierarchy.length)
        {
            if (USER_PREFERENCES.equals(parts.get(hierarchy.length)))
            {
                return RequestType.USER_PREFERENCES;
            }
            else if (parts.size() == hierarchy.length + 1
                     && !VISIBLE_USER_PREFERENCES.equals(parts.get(hierarchy.length)))
            {
                return RequestType.OPERATION;
            }
        }
        final List<Class<? extends ConfiguredObject>> hierarchyList = Arrays.asList(hierarchy);
        final String expectedFullPath = buildExpectedPath(servletPath, hierarchyList);
        final String expectedParentPath = buildExpectedPath(servletPath, hierarchyList.subList(0, hierarchy.length - 1));

        throw createBadRequestManagementException(String.format(
                "Invalid POST path '%s/%s'. Expected: '%s/<operation name>'"
                + " or '%s'"
                + " or '%s/userpreferences[/<preference type>]'",
                servletPath,
                String.join("/", parts),
                expectedFullPath,
                expectedParentPath,
                expectedFullPath));
    }

    private Class<? extends ConfiguredObject> getRequestCategoryClass(final String categoryName,
                                                                      final Model model)
    {
        for (Class<? extends ConfiguredObject> category : model.getSupportedCategories())
        {
            if (category.getSimpleName().toLowerCase().equals(categoryName.toLowerCase()))
            {
                return category;
            }
        }
        throw createNotFoundManagementException(String.format("Category is not found for '%s'", categoryName));
    }

    private String buildExpectedPath(final String servletPath, final List<Class<? extends ConfiguredObject>> hierarchy)
    {
        final StringBuilder expectedPath = new StringBuilder(servletPath);
        for (Class<? extends ConfiguredObject> part : hierarchy)
        {
            expectedPath.append("/<");
            expectedPath.append(part.getSimpleName().toLowerCase());
            expectedPath.append(" name>");
        }
        return expectedPath.toString();
    }

    private Predicate<ConfiguredObject<?>> buildFilterPredicates(final Map<String, List<String>> parameters)
    {
        return parameters.entrySet().stream()
                  .filter(entry -> !RESERVED_PARAMS.contains(entry.getKey()))
                  .map(entry -> {
                      final String paramName = entry.getKey();
                      final List<String> allowedValues = entry.getValue();
                      return (Predicate<ConfiguredObject<?>>) object -> {
                          Object value = object.getAttribute(paramName);
                          return allowedValues.contains(String.valueOf(value));
                      };
                  }).reduce(Predicate::and).orElse(t -> true);
    }

    private ConfiguredObject<?> getTarget(final ConfiguredObject<?> root,
                                          final String category,
                                          final List<String> names)
    {
        final Class<? extends ConfiguredObject> configuredClass = getRequestCategoryClass(category, root.getModel());
        final ConfiguredObject<?> target;
        final ConfiguredObjectFinder finder = getConfiguredObjectFinder(root);
        final Class<? extends ConfiguredObject>[] hierarchy = getHierarchy(finder, configuredClass);
        if (names.isEmpty() && hierarchy.length == 0)
        {
            target = root;
        }
        else
        {
            ConfiguredObject theParent = root;
            if (hierarchy.length > 1)
            {
                theParent = finder.findObjectParentsFromPath(names, hierarchy, configuredClass);
            }
            final String name = names.get(names.size() - 1);
            target = theParent.getChildByName(configuredClass, name);
            if (target == null)
            {

                final String errorMessage = String.format("%s '%s' not found",
                                                          configuredClass.getSimpleName(),
                                                          String.join("/", names));
                throw createNotFoundManagementException(errorMessage);
            }
        }
        return target;
    }

    private boolean hasFilter(Map<String, List<String>> parameters)
    {
        return parameters.keySet().stream().anyMatch(parameter -> !RESERVED_PARAMS.contains(parameter));
    }

}
