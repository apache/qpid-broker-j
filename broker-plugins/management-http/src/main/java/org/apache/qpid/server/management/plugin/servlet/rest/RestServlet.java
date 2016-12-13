/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.server.management.plugin.servlet.rest;

import static org.apache.qpid.server.management.plugin.HttpManagementConfiguration.DEFAULT_PREFERENCE_OPERTAION_TIMEOUT;
import static org.apache.qpid.server.management.plugin.HttpManagementConfiguration.PREFERENCE_OPERTAION_TIMEOUT_CONTEXT_NAME;
import static org.apache.qpid.server.management.plugin.HttpManagementUtil.ensureFilenameIsRfc2183;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.management.plugin.HttpManagement;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFinder;
import org.apache.qpid.server.model.ConfiguredObjectOperation;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.IllegalStateTransitionException;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.OperationTimeoutException;
import org.apache.qpid.server.model.preferences.UserPreferences;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;
import org.apache.qpid.util.DataUrlUtils;

public class RestServlet extends AbstractServlet
{
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RestServlet.class);

    public static final String DEPTH_PARAM = "depth";
    public static final String OVERSIZE_PARAM = "oversize";
    public static final String ACTUALS_PARAM = "actuals";
    public static final String SORT_PARAM = "sort";
    public static final String INCLUDE_SYS_CONTEXT_PARAM = "includeSysContext";
    public static final String INHERITED_ACTUALS_PARAM = "inheritedActuals";
    public static final String EXTRACT_INITIAL_CONFIG_PARAM = "extractInitialConfig";
    public static final String EXCLUDE_INHERITED_CONTEXT_PARAM = "excludeInheritedContext";

    /**
     * Signifies that the agent wishes the servlet to set the Content-Disposition on the
     * response with the value attachment.  This filename will be derived from the parameter value.
     */
    public static final String CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM = "contentDispositionAttachmentFilename";
    public static final Set<String> RESERVED_PARAMS =
            new HashSet<>(Arrays.asList(DEPTH_PARAM,
                                        SORT_PARAM,
                                        OVERSIZE_PARAM,
                                        ACTUALS_PARAM,
                                        INCLUDE_SYS_CONTEXT_PARAM,
                                        EXTRACT_INITIAL_CONFIG_PARAM,
                                        INHERITED_ACTUALS_PARAM,
                                        CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM,
                                        EXCLUDE_INHERITED_CONTEXT_PARAM));
    public static final int DEFAULT_DEPTH = 1;
    public static final int DEFAULT_OVERSIZE = 120;

    private transient final ConfiguredObjectToMapConverter _objectConverter = new ConfiguredObjectToMapConverter();
    private transient RestUserPreferenceHandler _userPreferenceHandler;

    @SuppressWarnings("unused")
    public RestServlet()
    {
        super();
    }

    @Override
    public void init() throws ServletException
    {
        super.init();

        Handler.register();
        Long preferenceOperationTimeout = getManagementConfiguration().getContextValue(Long.class, PREFERENCE_OPERTAION_TIMEOUT_CONTEXT_NAME);
        _userPreferenceHandler = new RestUserPreferenceHandler(preferenceOperationTimeout == null
                                                                       ? DEFAULT_PREFERENCE_OPERTAION_TIMEOUT
                                                                       : preferenceOperationTimeout);
    }


    private Collection<ConfiguredObject<?>> getTargetObjects(final Class<? extends ConfiguredObject> configuredClass,
                                                             final ConfiguredObjectFinder finder,
                                                             RequestInfo requestInfo,
                                                             List<Predicate<ConfiguredObject<?>>> filterPredicateList)
    {
        List<String> names = requestInfo.getModelParts();

        Collection<ConfiguredObject<?>> parents = finder.findObjectsFromPath(names, finder.getHierarchy(configuredClass), true);

        if (!(parents == null || filterPredicateList.isEmpty()))
        {
            Iterator<ConfiguredObject<?>> iter = parents.iterator();
            while (iter.hasNext())
            {
                ConfiguredObject obj = iter.next();
                for (Predicate<ConfiguredObject<?>> predicate : filterPredicateList)
                {
                    if (!predicate.apply(obj))
                    {
                        iter.remove();
                        break;
                    }
                }
            }

        }
        return parents;
    }

    private List<Predicate<ConfiguredObject<?>>> buildFilterPredicates(final HttpServletRequest request)
    {
        List<Predicate<ConfiguredObject<?>>> predicates = new ArrayList<>();

        for (final String paramName : Collections.list(request.getParameterNames()))
        {
            if (!RESERVED_PARAMS.contains(paramName))
            {
                final List<String> allowedValues = Arrays.asList(request.getParameterValues(paramName));

                predicates.add(new Predicate<ConfiguredObject<?>>()
                {
                    @Override
                    public boolean apply(final ConfiguredObject<?> obj)
                    {
                        Object value = obj.getAttribute(paramName);
                        return allowedValues.contains(String.valueOf(value));
                    }
                });
            }
        }
        return Collections.unmodifiableList(predicates);
    }

    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response,
                         final ConfiguredObject<?> managedObject)
            throws ServletException, IOException
    {
        ConfiguredObjectFinder finder = getConfiguredObjectFinder(managedObject);
        Class<? extends ConfiguredObject> configuredClass = getConfiguredClass(request, managedObject);
        if(configuredClass == null)
        {
            sendError(response, HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        final Class<? extends ConfiguredObject>[] hierarchy = finder.getHierarchy(configuredClass);
        if(hierarchy == null)
        {
            sendError(response, HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        RequestInfoParser requestInfoParser = new RequestInfoParser(hierarchy);

        RequestInfo requestInfo = requestInfoParser.parse(request);
        switch (requestInfo.getType())
        {
            case OPERATION:
            {
                doOperation(requestInfo, managedObject, configuredClass, finder, request, response);
                break;
            }
            case MODEL_OBJECT:
            {
                // TODO - sort special params, everything else should act as a filter
                String attachmentFilename = request.getParameter(CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM);

                if (attachmentFilename != null)
                {
                    setContentDispositionHeaderIfNecessary(response, attachmentFilename);
                }

                Collection<ConfiguredObject<?>> allObjects =
                        getTargetObjects(configuredClass, finder, requestInfo, buildFilterPredicates(request));

                if (allObjects == null || (allObjects.isEmpty() && isSingleObjectRequest(requestInfo, finder.getHierarchy(configuredClass))))
                {
                    sendJsonErrorResponse(request, response, HttpServletResponse.SC_NOT_FOUND, "Not Found");
                    return;
                }

                int depth;
                boolean actuals;
                int oversizeThreshold;
                boolean excludeInheritedContext;

                depth = getIntParameterFromRequest(request, DEPTH_PARAM, DEFAULT_DEPTH);
                oversizeThreshold = getIntParameterFromRequest(request, OVERSIZE_PARAM, DEFAULT_OVERSIZE);
                actuals = getBooleanParameterFromRequest(request, ACTUALS_PARAM);
                String includeSystemContextParameter = request.getParameter(INCLUDE_SYS_CONTEXT_PARAM);
                String inheritedActualsParameter = request.getParameter(INHERITED_ACTUALS_PARAM);
                String excludeInheritedContextParameter = request.getParameter(EXCLUDE_INHERITED_CONTEXT_PARAM);

                if (excludeInheritedContextParameter == null)
                {
                    /* backward (pre v6.1) compatible behaviour */
                    if (inheritedActualsParameter == null && includeSystemContextParameter == null)
                    {
                        excludeInheritedContext = actuals;
                    }
                    else if (inheritedActualsParameter != null && includeSystemContextParameter != null)
                    {
                        if (actuals)
                        {
                            excludeInheritedContext = !Boolean.parseBoolean(inheritedActualsParameter);
                        }
                        else
                        {
                            excludeInheritedContext = !Boolean.parseBoolean(includeSystemContextParameter);
                        }
                    }
                    else if (inheritedActualsParameter != null)
                    {
                        if (actuals)
                        {
                            excludeInheritedContext = !Boolean.parseBoolean(inheritedActualsParameter);
                        }
                        else
                        {
                            excludeInheritedContext = false;
                        }
                    }
                    else
                    {
                        if (actuals)
                        {
                            excludeInheritedContext = true;
                        }
                        else
                        {
                            excludeInheritedContext = !Boolean.parseBoolean(includeSystemContextParameter);
                        }
                    }
                }
                else
                {
                    if (inheritedActualsParameter != null || includeSystemContextParameter != null)
                    {
                        sendJsonErrorResponse(request,
                                              response,
                                              SC_UNPROCESSABLE_ENTITY,
                                              String.format(
                                                      "Parameter '%s' cannot be specified together with '%s' or '%s'",
                                                      EXCLUDE_INHERITED_CONTEXT_PARAM,
                                                      INHERITED_ACTUALS_PARAM,
                                                      INCLUDE_SYS_CONTEXT_PARAM));
                        return;
                    }
                    excludeInheritedContext = Boolean.parseBoolean(excludeInheritedContextParameter);
                }


                List<Map<String, Object>> output = new ArrayList<>();
                for (ConfiguredObject configuredObject : allObjects)
                {

                    output.add(_objectConverter.convertObjectToMap(configuredObject, configuredClass,
                                                                   new ConfiguredObjectToMapConverter.ConverterOptions(
                                                                           depth,
                                                                           actuals,
                                                                           oversizeThreshold,
                                                                           request.isSecure(),
                                                                           excludeInheritedContext)));
                }

                boolean sendCachingHeaders = attachmentFilename == null;
                sendJsonResponse(output,
                                 request,
                                 response,
                                 HttpServletResponse.SC_OK,
                                 sendCachingHeaders);
                break;
            }
            case VISIBLE_PREFERENCES:
            case USER_PREFERENCES:
            {
                doGetUserPreferences(managedObject, configuredClass, finder, requestInfo, request, response);
                break;
            }

            default:
            {
                throw new IllegalStateException(String.format("Unexpected request type '%s' for path '%s'",
                                                              requestInfo.getType(),
                                                              request.getPathInfo()));
            }
        }
    }


    private boolean isSingleObjectRequest(final RequestInfo requestInfo,
                                          final Class<? extends ConfiguredObject>[] hierarchy)
    {
        if (hierarchy.length > 0)
        {
            List<String> pathInfoElements = requestInfo.getModelParts();
            return pathInfoElements.size() == hierarchy.length;
        }

        return false;
    }

    private void setContentDispositionHeaderIfNecessary(final HttpServletResponse response,
                                                        final String attachmentFilename)
    {
        if (attachmentFilename != null)
        {
            String filenameRfc2183 = ensureFilenameIsRfc2183(attachmentFilename);
            if (filenameRfc2183.length() > 0)
            {
                response.setHeader(CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", filenameRfc2183));
            }
            else
            {
                response.setHeader(CONTENT_DISPOSITION, "attachment");  // Agent will allow user to choose a name
            }
        }
    }

    private Class<? extends ConfiguredObject> getConfiguredClass(HttpServletRequest request, ConfiguredObject<?> managedObject)
    {
        final String[] servletPathElements = request.getServletPath().split("/");
        String categoryName = servletPathElements[servletPathElements.length-1];
        Model model = managedObject.getModel();
        for(Class<? extends ConfiguredObject> category : model.getSupportedCategories())
        {
            if(category.getSimpleName().toLowerCase().equals(categoryName))
            {
                return category;
            }
        }
        return null;
    }

    @Override
    protected void doPut(HttpServletRequest request,
                         HttpServletResponse response,
                         final ConfiguredObject<?> managedObject)
            throws ServletException, IOException
    {
        performCreateOrUpdate(request, response, managedObject);
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException
    {
        try
        {
            super.service(request, response);
        }
        catch (IllegalArgumentException | IllegalConfigurationException | IllegalStateException | SecurityException
                | IntegrityViolationException | IllegalStateTransitionException | NoClassDefFoundError
                | OperationTimeoutException e)
        {
            setResponseStatus(request, response, e);
        }
    }

    private void performCreateOrUpdate(HttpServletRequest request,
                                       HttpServletResponse response,
                                       final ConfiguredObject<?> managedObject)
            throws IOException, ServletException
    {

        ConfiguredObjectFinder finder = getConfiguredObjectFinder(managedObject);
        final Class<? extends ConfiguredObject> configuredClass = getConfiguredClass(request, managedObject);
        final Class<? extends ConfiguredObject>[] hierarchy = finder.getHierarchy(configuredClass);
        RequestInfoParser requestInfoParser = new RequestInfoParser(hierarchy);

        response.setContentType("application/json");

        RequestInfo requestInfo = requestInfoParser.parse(request);
        switch (requestInfo.getType())
        {
            case MODEL_OBJECT:
            {
                List<String> names = requestInfo.getModelParts();
                boolean isFullObjectURL = names.size() == hierarchy.length;
                Map<String, Object> providedObject = getRequestProvidedObject(request, requestInfo);
                if (names.isEmpty() && hierarchy.length == 0)
                {
                    managedObject.setAttributes(providedObject);
                    response.setStatus(HttpServletResponse.SC_OK);
                    return;
                }

                ConfiguredObject theParent = managedObject;
                ConfiguredObject[] otherParents = null;
                Class<? extends ConfiguredObject> objClass = configuredClass;
                if (hierarchy.length > 1)
                {

                    List<ConfiguredObject> parents =
                            finder.findObjectParentsFromPath(names, hierarchy, configuredClass);
                    theParent = parents.remove(0);
                    otherParents = parents.toArray(new ConfiguredObject[parents.size()]);
                }

                if (isFullObjectURL)
                {
                    providedObject.put("name", names.get(names.size() - 1));
                    ConfiguredObject<?> configuredObject =
                            findObjectToUpdateInParent(objClass, providedObject, theParent, otherParents);

                    if (configuredObject != null)
                    {
                        configuredObject.setAttributes(providedObject);
                        response.setStatus(HttpServletResponse.SC_OK);
                        return;
                    }
                    else if ("POST".equalsIgnoreCase(request.getMethod()))
                    {
                        sendJsonErrorResponse(request, response, HttpServletResponse.SC_NOT_FOUND, "Object with "
                                                                                                   + (providedObject.containsKey(
                                "id") ? " id '" + providedObject.get("id") : " name '" + providedObject.get("name"))
                                                                                                   + "' does not exist!");
                        return;
                    }
                }

                ConfiguredObject<?> configuredObject = theParent.createChild(objClass, providedObject, otherParents);
                StringBuffer requestURL = request.getRequestURL();
                if (!isFullObjectURL)
                {
                    requestURL.append("/").append(configuredObject.getName());
                }
                response.setHeader("Location", requestURL.toString());
                response.setStatus(HttpServletResponse.SC_CREATED);
                break;
            }
            case OPERATION:
            {
                doOperation(requestInfo, managedObject, configuredClass, finder, request, response);
                break;
            }
            case USER_PREFERENCES:
            {
                doPostOrPutUserPreference(requestInfo, managedObject, configuredClass, finder, request, response);
                break;
            }
            default:
            {
                throw new IllegalStateException(String.format("Unexpected request type '%s' for path '%s'",
                                                              requestInfo.getType(),
                                                              request.getPathInfo()));
            }
        }
    }

    private void doGetUserPreferences(final ConfiguredObject<?> managedObject,
                                      final Class<? extends ConfiguredObject> configuredClass,
                                      final ConfiguredObjectFinder finder, final RequestInfo requestInfo,
                                      final HttpServletRequest request,
                                      final HttpServletResponse response) throws IOException, ServletException
    {
        Collection<ConfiguredObject<?>> allObjects = getTargetObjects(
                configuredClass,
                                                                      finder,
                                                                      requestInfo,
                                                                      Collections.<Predicate<ConfiguredObject<?>>>emptyList());

        if (allObjects == null || (allObjects.isEmpty() && isSingleObjectRequest(requestInfo, finder.getHierarchy(configuredClass))))
        {
            sendJsonErrorResponse(request, response, HttpServletResponse.SC_NOT_FOUND, "Not Found");
            return;
        }

        final Object responseObject;
        if (requestInfo.hasWildcard())
        {
            responseObject = new ArrayList<>(allObjects.size());
            for (ConfiguredObject<?> target : allObjects)
            {
                final UserPreferences userPreferences = target.getUserPreferences();
                try
                {
                    final Object preferences = _userPreferenceHandler.handleGET(userPreferences, requestInfo);
                    if (preferences == null || (preferences instanceof Collection
                                                && ((Collection) preferences).isEmpty()) || (preferences instanceof Map
                                                                                             && ((Map) preferences).isEmpty()))
                    {
                        continue;
                    }
                    ((List<Object>) responseObject).add(preferences);
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
            ConfiguredObject<?> target = allObjects.iterator().next();
            final UserPreferences userPreferences = target.getUserPreferences();

            responseObject = _userPreferenceHandler.handleGET(userPreferences, requestInfo);
        }
        sendJsonResponse(responseObject, request, response);
    }

    private void doPostOrPutUserPreference(final RequestInfo requestInfo,
                                           final ConfiguredObject<?> managedObject,
                                           final Class<? extends ConfiguredObject> configuredClass,
                                           final ConfiguredObjectFinder finder,
                                           final HttpServletRequest request,
                                           final HttpServletResponse response) throws IOException, ServletException
    {
        ConfiguredObject<?> target = getTarget(requestInfo, managedObject, configuredClass, finder);

        final Object providedObject = getRequestProvidedObject(request, requestInfo, Object.class);
        if ("POST".equals(request.getMethod()))
        {
            _userPreferenceHandler.handlePOST(target, requestInfo, providedObject);
        }
        else if ("PUT".equals(request.getMethod()))
        {
            _userPreferenceHandler.handlePUT(target, requestInfo, providedObject);
        }
        else
        {
            sendJsonErrorResponse(request,
                                  response,
                                  HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                                  "unexpected http method");
        }
    }

    private void doOperation(final RequestInfo requestInfo,
                             final ConfiguredObject<?> managedObject,
                             final Class<? extends ConfiguredObject> configuredClass,
                             final ConfiguredObjectFinder finder,
                             final HttpServletRequest request,
                             final HttpServletResponse response) throws IOException, ServletException
    {
        ConfiguredObject<?> target = getTarget(requestInfo, managedObject, configuredClass, finder);
        if (target == null)
        {
            return;
        }
        String operationName = requestInfo.getOperationName();
        final Map<String, ConfiguredObjectOperation<?>> availableOperations =
                managedObject.getModel().getTypeRegistry().getOperations(target.getClass());
        ConfiguredObjectOperation operation = availableOperations.get(operationName);
        Map<String, Object> operationArguments;


        String requestMethod = request.getMethod();
        if (operation == null)
        {
            sendJsonErrorResponse(request,
                                  response,
                                  HttpServletResponse.SC_NOT_FOUND,
                                  "No such operation: " + operationName);
            return;
        }
        else
        {
            switch (requestMethod)
            {
                case "GET":
                    if (operation.isNonModifying())
                    {
                        operationArguments = getOperationArgumentsAsMap(request);
                        operationArguments.keySet().removeAll(Arrays.asList(CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM));
                    }
                    else
                    {
                        response.addHeader("Allow", "POST");
                        sendJsonErrorResponse(request,
                                              response,
                                              HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                                              "Operation "
                                              + operationName
                                              + " modifies the object so you must use POST.");
                        return;
                    }

                    break;
                case "POST":
                    operationArguments = getRequestProvidedObject(request, requestInfo);
                    break;
                default:
                    response.addHeader("Allow", (operation.isNonModifying() ? "POST, GET" : "POST"));
                    sendJsonErrorResponse(request,
                                          response,
                                          HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                                          "Operation "
                                          + operationName
                                          + " does not support the "
                                          + requestMethod
                                          + " requestMethod.");
                    return;
            }
        }


        if(operation.isSecure(target, operationArguments) && !(request.isSecure() || HttpManagement.getPort(request).isAllowConfidentialOperationsOnInsecureChannels()))
        {
            sendJsonErrorResponse(request,
                                  response,
                                  HttpServletResponse.SC_FORBIDDEN,
                                  "Operation '" + operationName + "' can only be performed over a secure (HTTPS) connection");
            return;
        }

        Object returnVal = operation.perform(target, operationArguments);
        if(returnVal instanceof Content)
        {
            Content content = (Content)returnVal;
            try
            {

                writeTypedContent(content, request, response);
            }
            finally
            {
                content.release();
            }
        }
        else
        {
            final ConfiguredObjectToMapConverter.ConverterOptions converterOptions =
                    new ConfiguredObjectToMapConverter.ConverterOptions(DEFAULT_DEPTH,
                                                                        false,
                                                                        DEFAULT_OVERSIZE,
                                                                        request.isSecure(),
                                                                        true);
            if (ConfiguredObject.class.isAssignableFrom(operation.getReturnType()))
            {
                returnVal = _objectConverter.convertObjectToMap((ConfiguredObject<?>) returnVal,
                                                                operation.getReturnType(),
                                                                converterOptions);
            }
            else if (returnsCollectionOfConfiguredObjects(operation))
            {
                List<Map<String, Object>> output = new ArrayList<>();
                for (Object configuredObject : (Collection)returnVal)
                {
                    output.add(_objectConverter.convertObjectToMap((ConfiguredObject<?>) configuredObject,
                                                                   getCollectionMemberType((ParameterizedType) operation.getGenericReturnType()),
                                                                   converterOptions));
                }
                returnVal = output;
            }

            String attachmentFilename = request.getParameter(CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM);

            if (attachmentFilename != null)
            {
                setContentDispositionHeaderIfNecessary(response, attachmentFilename);
            }

            sendJsonResponse(returnVal, request, response);
        }
    }

    private ConfiguredObject<?> getTarget(final RequestInfo requestInfo,
                                          final ConfiguredObject<?> managedObject,
                                          final Class<? extends ConfiguredObject> configuredClass,
                                          final ConfiguredObjectFinder finder) throws IOException
    {
        final ConfiguredObject<?> target;
        final List<String> names = requestInfo.getModelParts();
        final Class<? extends ConfiguredObject>[] hierarchy = finder.getHierarchy(configuredClass);
        if (names.isEmpty() && hierarchy.length == 0)
        {
            target = managedObject;
        }
        else
        {
            ConfiguredObject theParent = managedObject;
            ConfiguredObject[] otherParents = null;
            if (hierarchy.length > 1)
            {

                List<ConfiguredObject> parents =
                        finder.findObjectParentsFromPath(names, hierarchy, configuredClass);
                theParent = parents.remove(0);
                otherParents = parents.toArray(new ConfiguredObject[parents.size()]);
            }
            Class<? extends ConfiguredObject> objClass = configuredClass;
            Map<String, Object> objectName =
                    Collections.<String, Object>singletonMap("name", names.get(names.size() - 1));
            target = findObjectToUpdateInParent(objClass, objectName, theParent, otherParents);
            if (target == null)
            {

                final String errorMessage = String.format("%s '%s' not found",
                                                          configuredClass.getSimpleName(),
                                                          Joiner.on("/").join(names));
                throw new NotFoundException(errorMessage);
            }
        }
        return target;
    }

    private boolean returnsCollectionOfConfiguredObjects(ConfiguredObjectOperation operation)
    {
        return Collection.class.isAssignableFrom(operation.getReturnType())
                 && operation.getGenericReturnType() instanceof ParameterizedType
                 && ConfiguredObject.class.isAssignableFrom(getCollectionMemberType((ParameterizedType) operation.getGenericReturnType()));
    }

    private Class getCollectionMemberType(ParameterizedType collectionType)
    {
        return getRawType((collectionType).getActualTypeArguments()[0]);
    }

    private static Class getRawType(Type t)
    {
        if(t instanceof Class)
        {
            return (Class)t;
        }
        else if(t instanceof ParameterizedType)
        {
            return (Class)((ParameterizedType)t).getRawType();
        }
        else if(t instanceof TypeVariable)
        {
            Type[] bounds = ((TypeVariable)t).getBounds();
            if(bounds.length == 1)
            {
                return getRawType(bounds[0]);
            }
        }
        else if(t instanceof WildcardType)
        {
            Type[] upperBounds = ((WildcardType)t).getUpperBounds();
            if(upperBounds.length == 1)
            {
                return getRawType(upperBounds[0]);
            }
        }
        throw new ServerScopedRuntimeException("Unable to process type when constructing configuration model: " + t);
    }

    private Map<String, Object> getOperationArgumentsAsMap(HttpServletRequest request)
    {
        Map<String, Object> providedObject;
        providedObject = new HashMap<>();
        for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet())
        {
            String[] value = entry.getValue();
            if (value != null)
            {
                if(value.length > 1)
                {
                    providedObject.put(entry.getKey(), Arrays.asList(value));
                }
                else
                {
                    providedObject.put(entry.getKey(), value[0]);
                }
            }
        }
        return providedObject;
    }

    private Map<String, Object> getRequestProvidedObject(HttpServletRequest request, final RequestInfo requestInfo)
            throws IOException, ServletException
    {
        return getRequestProvidedObject(request, requestInfo, LinkedHashMap.class);
    }

    private <T> T getRequestProvidedObject(HttpServletRequest request,
                                           final RequestInfo requestInfo,
                                           Class<T> expectedClass)
            throws IOException, ServletException
    {
        T providedObject;

        ArrayList<String> headers = Collections.list(request.getHeaderNames());
        ObjectMapper mapper = new ObjectMapper();

        if (headers.contains("Content-Type") && request.getHeader("Content-Type").startsWith("multipart/form-data"))
        {
            providedObject = (T) new LinkedHashMap<>();
            Map<String, String> fileUploads = new HashMap<>();
            Collection<Part> parts = request.getParts();
            for (Part part : parts)
            {
                if ("data".equals(part.getName()) && "application/json".equals(part.getContentType()))
                {
                    try
                    {
                        providedObject = (T) mapper.readValue(part.getInputStream(), LinkedHashMap.class);
                    }
                    catch (JsonProcessingException e)
                    {
                        throw new IllegalArgumentException("Cannot parse the operation body as json",e);
                    }

                }
                else
                {
                    byte[] data = new byte[(int) part.getSize()];
                    part.getInputStream().read(data);
                    String inlineURL = DataUrlUtils.getDataUrlForBytes(data);
                    fileUploads.put(part.getName(), inlineURL);
                }
            }
            ((Map<String, Object>) providedObject).putAll(fileUploads);
        }
        else
        {
            try
            {
                providedObject = mapper.readValue(request.getInputStream(), expectedClass);
            }
            catch (JsonProcessingException e)
            {
                throw new IllegalArgumentException("Cannot parse the operation body as json",e);
            }
        }
        return providedObject;
    }

    private ConfiguredObject<?> findObjectToUpdateInParent(Class<? extends ConfiguredObject> objClass, Map<String, Object> providedObject, ConfiguredObject theParent, ConfiguredObject[] otherParents)
    {
        Collection<? extends ConfiguredObject> existingChildren = theParent.getChildren(objClass);

        for (ConfiguredObject obj : existingChildren)
        {
            if ((providedObject.containsKey("id") && String.valueOf(providedObject.get("id")).equals(obj.getId().toString()))
                    || (obj.getName().equals(providedObject.get("name")) && sameOtherParents(obj, otherParents, objClass)))
            {
                return obj;
            }
        }
        return null;
    }

    private boolean sameOtherParents(ConfiguredObject obj, ConfiguredObject[] otherParents, Class<? extends ConfiguredObject> objClass)
    {
        Collection<Class<? extends ConfiguredObject>> parentClasses = obj.getModel().getParentTypes(objClass);

        if(otherParents == null || otherParents.length == 0)
        {
            return parentClasses.size() == 1;
        }


        for (ConfiguredObject parent : otherParents)
        {
            boolean found = false;
            for (Class<? extends ConfiguredObject> parentClass : parentClasses)
            {
                if (parent == obj.getParent(parentClass))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                return false;
            }
        }

        return true;
    }

    private void setResponseStatus(HttpServletRequest request, HttpServletResponse response, Throwable e)
            throws IOException
    {
        if (e instanceof SecurityException)
        {
            LOGGER.debug("{}, sending {}", e.getClass().getName(), HttpServletResponse.SC_FORBIDDEN, e);
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        }
        else
        {
            int responseCode = HttpServletResponse.SC_BAD_REQUEST;
            String message = e.getMessage();
            if (e instanceof AbstractConfiguredObject.DuplicateIdException
                || e instanceof AbstractConfiguredObject.DuplicateNameException
                || e instanceof IntegrityViolationException
                || e instanceof IllegalStateTransitionException)
            {
                responseCode = HttpServletResponse.SC_CONFLICT;
            }
            else if (e instanceof NotFoundException)
            {
                if (LOGGER.isTraceEnabled())
                {
                    LOGGER.trace(e.getClass().getSimpleName() + " processing request", e);
                }
                responseCode = HttpServletResponse.SC_NOT_FOUND;
            }
            else if (e instanceof IllegalConfigurationException || e instanceof IllegalArgumentException)
            {
                LOGGER.warn("{} processing request {} from user '{}': {}",
                            e.getClass().getSimpleName(),
                            HttpManagementUtil.getRequestURL(request),
                            HttpManagementUtil.getRequestPrincipals(request),
                            message);
                Throwable t = e;
                int maxDepth = 10;
                while ((t = t.getCause()) != null && maxDepth-- != 0)
                {
                    LOGGER.warn("... caused by " + t.getClass().getSimpleName() + "  : " + t.getMessage());
                }
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug(e.getClass().getSimpleName() + " processing request", e);
                }
                responseCode = SC_UNPROCESSABLE_ENTITY;
            }
            else if (e instanceof OperationTimeoutException)
            {
                message = "Timeout occurred";
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Timeout during processing of request {} from user '{}'",
                                 HttpManagementUtil.getRequestURL(request),
                                 HttpManagementUtil.getRequestPrincipals(request),
                                 e);
                }
                else
                {
                    LOGGER.info("Timeout during processing of request {} from user '{}'",
                            HttpManagementUtil.getRequestURL(request),
                            HttpManagementUtil.getRequestPrincipals(request));
                }

                responseCode = HttpServletResponse.SC_BAD_GATEWAY;
            }
            else if (e instanceof NoClassDefFoundError)
            {
                message = "Not found: " + message;
                LOGGER.warn("Unexpected exception processing request ", e);
            }
            else
            {
                // This should not happen
                if (e instanceof RuntimeException)
                {
                    throw (RuntimeException) e;
                }
                else if (e instanceof Error)
                {
                    throw (Error) e;
                }
                else
                {
                    throw new RuntimeException("Unexpected Exception", e);
                }
            }

            sendJsonErrorResponse(request, response, responseCode, message);
        }
    }

    @Override
    protected void doDelete(HttpServletRequest request,
                            HttpServletResponse response,
                            final ConfiguredObject<?> managedObject) throws ServletException, IOException
    {
        ConfiguredObjectFinder finder = getConfiguredObjectFinder(managedObject);
        Class<? extends ConfiguredObject> configuredClass = getConfiguredClass(request, managedObject);
        final Class<? extends ConfiguredObject>[] hierarchy = finder.getHierarchy(configuredClass);
        RequestInfoParser requestInfoParser = new RequestInfoParser(hierarchy);

        RequestInfo requestInfo = requestInfoParser.parse(request);

        Collection<ConfiguredObject<?>> allObjects = getTargetObjects(configuredClass, finder, requestInfo, buildFilterPredicates(request));
        if (allObjects == null)
        {
            throw new NotFoundException("Not Found");
        }

        switch (requestInfo.getType())
        {
            case MODEL_OBJECT:
            {
                for (ConfiguredObject o : allObjects)
                {
                    o.delete();
                }

                sendCachingHeadersOnResponse(response);
                response.setStatus(HttpServletResponse.SC_OK);
                break;
            }
            case USER_PREFERENCES:
            {
                //TODO: define format how to report the results for bulk delete, i.e. how to report individual errors and success statuses
                if (allObjects.size() > 1)
                {
                    sendJsonErrorResponse(request,
                                          response,
                                          HttpServletResponse.SC_BAD_REQUEST,
                                          "Deletion of user preferences using wildcards is unsupported");
                    return;
                }
                for (ConfiguredObject o : allObjects)
                {
                    _userPreferenceHandler.handleDELETE(o.getUserPreferences(), requestInfo);
                }
                break;
            }

            default:
            {
                sendJsonErrorResponse(request, response, HttpServletResponse.SC_BAD_REQUEST, "Unsupported delete call");
            }
        }
    }

    @Override
    protected void doPost(HttpServletRequest request,
                          HttpServletResponse response,
                          final ConfiguredObject<?> managedObject) throws ServletException, IOException
    {
        performCreateOrUpdate(request, response, managedObject);
    }


    private int getIntParameterFromRequest(final HttpServletRequest request,
                                           final String paramName,
                                           final int defaultValue)
    {
        int intValue = defaultValue;
        final String stringValue = request.getParameter(paramName);
        if(stringValue!=null)
        {
            try
            {
                intValue = Integer.parseInt(stringValue);
            }
            catch (NumberFormatException e)
            {
                LOGGER.warn("Could not parse " + stringValue + " as integer for parameter " + paramName);
            }
        }
        return intValue;
    }

    private boolean getBooleanParameterFromRequest(HttpServletRequest request, final String paramName)
    {
        return getBooleanParameterFromRequest(request, paramName, false);
    }

    private boolean getBooleanParameterFromRequest(HttpServletRequest request, final String paramName, final boolean defaultValue)
    {
        String value = request.getParameter(paramName);
        if (value == null)
        {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }
}
