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

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.security.AccessControlException;
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

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectOperation;
import org.apache.qpid.server.model.IllegalStateTransitionException;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.virtualhost.ExchangeExistsException;
import org.apache.qpid.server.virtualhost.QueueExistsException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.util.urlstreamhandler.data.Handler;
import org.apache.qpid.util.DataUrlUtils;

public class RestServlet extends AbstractServlet
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestServlet.class);
    /**
     * An initialization parameter to specify hierarchy
     */
    private static final String HIERARCHY_INIT_PARAMETER = "hierarchy";

    public static final String DEPTH_PARAM = "depth";
    public static final String OVERSIZE_PARAM = "oversize";
    public static final String ACTUALS_PARAM = "actuals";
    public static final String SORT_PARAM = "sort";
    public static final String INCLUDE_SYS_CONTEXT_PARAM = "includeSysContext";
    public static final String INHERITED_ACTUALS_PARAM = "inheritedActuals";
    public static final String EXTRACT_INITIAL_CONFIG_PARAM = "extractInitialConfig";
    public static final int SC_UNPROCESSABLE_ENTITY = 422;

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
                                        CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM));
    public static final int DEFAULT_DEPTH = 1;
    public static final int DEFAULT_OVERSIZE = 120;

    private Class<? extends ConfiguredObject>[] _hierarchy;

    private final ConfiguredObjectToMapConverter _objectConverter = new ConfiguredObjectToMapConverter();
    private final boolean _hierarchyInitializationRequired;

    public RestServlet()
    {
        super();
        _hierarchyInitializationRequired = true;
    }

    public RestServlet(Class<? extends ConfiguredObject>... hierarchy)
    {
        super();
        _hierarchy = hierarchy;
        _hierarchyInitializationRequired = false;
    }

    @Override
    public void init() throws ServletException
    {
        super.init();
        if (_hierarchyInitializationRequired)
        {
            doInitialization();
        }
        Handler.register();
    }

    @SuppressWarnings("unchecked")
    private void doInitialization() throws ServletException
    {
        ServletConfig config = getServletConfig();
        String hierarchy = config.getInitParameter(HIERARCHY_INIT_PARAMETER);
        if (hierarchy != null && !"".equals(hierarchy))
        {
            List<Class<? extends ConfiguredObject>> classes = new ArrayList<Class<? extends ConfiguredObject>>();
            String[] hierarchyItems = hierarchy.split(",");
            for (String item : hierarchyItems)
            {
                Class<?> itemClass;
                try
                {
                    itemClass = Class.forName(item);
                }
                catch (ClassNotFoundException e)
                {
                    try
                    {
                        itemClass = Class.forName("org.apache.qpid.server.model." + item);
                    }
                    catch (ClassNotFoundException e1)
                    {
                        throw new ServletException("Unknown configured object class '" + item
                                + "' is specified in hierarchy for " + config.getServletName());
                    }
                }
                Class<? extends ConfiguredObject> clazz = (Class<? extends ConfiguredObject>)itemClass;
                classes.add(clazz);
            }
            Class<? extends ConfiguredObject>[] hierarchyClasses = (Class<? extends ConfiguredObject>[])new Class[classes.size()];
            _hierarchy = classes.toArray(hierarchyClasses);
        }
        else
        {
            _hierarchy = (Class<? extends ConfiguredObject>[])new Class[0];
        }
    }

    protected Collection<ConfiguredObject<?>> getObjects(HttpServletRequest request)
    {
        String[] pathInfoElements = getPathInfoElements(request);
        List<String> names = new ArrayList<>();
        if(pathInfoElements != null)
        {
            if(pathInfoElements.length > _hierarchy.length)
            {
                throw new IllegalArgumentException("Too many entries in path for REST servlet "
                        + getServletName() + ". Expected hierarchy length: " + _hierarchy.length
                        + "; Request hierarchy length: " + pathInfoElements.length
                        + "; Path Elements: " + Arrays.toString(pathInfoElements));
            }
            names.addAll(Arrays.asList(pathInfoElements));
        }

        Collection<ConfiguredObject<?>> parents = new ArrayList<>();
        parents.add(getBroker());
        Collection<ConfiguredObject<?>> children = new ArrayList<>();

        Map<Class<? extends ConfiguredObject>, String> filters =
                new HashMap<>();

        final Model model = getBroker().getModel();
        boolean wildcard = false;
        for(int i = 0; i < _hierarchy.length; i++)
        {
            if(i == 0 || model.getChildTypes(_hierarchy[i - 1]).contains(_hierarchy[i]))
            {

                for(ConfiguredObject<?> parent : parents)
                {
                    if(names.size() > i
                            && names.get(i) != null
                            && !names.get(i).equals("*")
                            && names.get(i).trim().length() != 0)
                    {
                        for(ConfiguredObject<?> child : parent.getChildren(_hierarchy[i]))
                        {
                            if(child.getName().equals(names.get(i)))
                            {
                                children.add(child);
                            }
                        }
                        if(children.isEmpty())
                        {
                            return null;
                        }
                    }
                    else
                    {
                        wildcard = true;
                        children.addAll((Collection<? extends ConfiguredObject<?>>) parent.getChildren(_hierarchy[i]));
                    }
                }
            }
            else
            {
                children = parents;
                if(names.size() > i
                        && names.get(i) != null
                        && !names.get(i).equals("*")
                        && names.get(i).trim().length() != 0)
                {
                    filters.put(_hierarchy[i], names.get(i));
                }
                else
                {
                    wildcard = true;
                }
            }

            parents = children;
            children = new ArrayList<>();
        }

        if(!filters.isEmpty() && !parents.isEmpty())
        {
            Collection<ConfiguredObject<?>> potentials = parents;
            parents = new ArrayList<>();

            for(ConfiguredObject o : potentials)
            {

                boolean match = true;

                for(Map.Entry<Class<? extends ConfiguredObject>, String> entry : filters.entrySet())
                {
                    Collection<? extends ConfiguredObject> ancestors =
                            getAncestors(getConfiguredClass(),entry.getKey(), o);
                    match = false;
                    for(ConfiguredObject ancestor : ancestors)
                    {
                        if(ancestor.getName().equals(entry.getValue()))
                        {
                            match = true;
                            break;
                        }
                    }
                    if(!match)
                    {
                        break;
                    }
                }
                if(match)
                {
                    parents.add(o);
                }

            }
        }

        return parents.isEmpty() && !wildcard ? null : filter(parents, request);
    }

    private Collection<ConfiguredObject<?>> filter(Collection<ConfiguredObject<?>> objects, HttpServletRequest request)
    {


        Map<String, Collection<String>> filters = new HashMap<>();

        for(String param : Collections.list(request.getParameterNames()))
        {
            if(!RESERVED_PARAMS.contains(param))
            {
                filters.put(param, Arrays.asList(request.getParameterValues(param)));
            }
        }

        if(filters.isEmpty())
        {
            return objects;
        }

        Collection<ConfiguredObject<?>> filteredObj = new ArrayList<>(objects);

        Iterator<ConfiguredObject<?>> iter = filteredObj.iterator();

        while(iter.hasNext())
        {
            ConfiguredObject obj = iter.next();
            for(Map.Entry<String, Collection<String>> entry : filters.entrySet())
            {
                Object value = obj.getAttribute(entry.getKey());
                if(!entry.getValue().contains(String.valueOf(value)))
                {
                    iter.remove();
                }
            }

        }

        return filteredObj;
    }

    private Collection<? extends ConfiguredObject> getAncestors(Class<? extends ConfiguredObject> childType,
                                                                Class<? extends ConfiguredObject> ancestorType,
                                                                ConfiguredObject child)
    {
        Collection<ConfiguredObject> ancestors = new HashSet<>();
        Collection<Class<? extends ConfiguredObject>> parentTypes = child.getModel().getParentTypes(childType);

        for(Class<? extends ConfiguredObject> parentClazz : parentTypes)
        {
            if(parentClazz == ancestorType)
            {
                ConfiguredObject parent = child.getParent(parentClazz);
                if(parent != null)
                {
                    ancestors.add(parent);
                }
            }
            else
            {
                ConfiguredObject parent = child.getParent(parentClazz);
                if(parent != null)
                {
                    ancestors.addAll(getAncestors(parentClazz, ancestorType, parent));
                }
            }
        }

        return ancestors;
    }

    @Override
    protected void doGetWithSubjectAndActor(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        try
        {
            String[] pathInfoElements = getPathInfoElements(request);
            if (pathInfoElements != null && pathInfoElements.length == _hierarchy.length + 1)
            {
                doOperation(request, response);
            }
            else
            {
                // TODO - sort special params, everything else should act as a filter
                String attachmentFilename = request.getParameter(CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM);
                boolean extractInitialConfig = getBooleanParameterFromRequest(request, EXTRACT_INITIAL_CONFIG_PARAM);

                if (attachmentFilename != null)
                {
                    setContentDispositionHeaderIfNecessary(response, attachmentFilename);
                }

                Collection<ConfiguredObject<?>> allObjects = getObjects(request);

                if (allObjects == null || allObjects.isEmpty() && isSingleObjectRequest(request))
                {
                    sendJsonErrorResponse(request, response, HttpServletResponse.SC_NOT_FOUND, "Not Found");
                    return;
                }

                int depth;
                boolean actuals;
                boolean includeSystemContext;
                boolean inheritedActuals;
                int oversizeThreshold;

                if (extractInitialConfig)
                {
                    depth = Integer.MAX_VALUE;
                    oversizeThreshold = Integer.MAX_VALUE;
                    actuals = true;
                    includeSystemContext = false;
                    inheritedActuals = false;
                }
                else
                {
                    depth = getIntParameterFromRequest(request, DEPTH_PARAM, DEFAULT_DEPTH);
                    oversizeThreshold = getIntParameterFromRequest(request, OVERSIZE_PARAM, DEFAULT_OVERSIZE);
                    actuals = getBooleanParameterFromRequest(request, ACTUALS_PARAM);
                    includeSystemContext = getBooleanParameterFromRequest(request, INCLUDE_SYS_CONTEXT_PARAM);
                    inheritedActuals = getBooleanParameterFromRequest(request, INHERITED_ACTUALS_PARAM);
                }

                List<Map<String, Object>> output = new ArrayList<>();
                for (ConfiguredObject configuredObject : allObjects)
                {

                    output.add(_objectConverter.convertObjectToMap(configuredObject, getConfiguredClass(),
                            depth, actuals, inheritedActuals, includeSystemContext, extractInitialConfig, oversizeThreshold, request.isSecure()));
                }


                boolean sendCachingHeaders = attachmentFilename == null;
                sendJsonResponse(extractInitialConfig && output.size() == 1 ? output.get(0) : output,
                        request,
                        response,
                        HttpServletResponse.SC_OK,
                        sendCachingHeaders);
            }
        }
        catch (RuntimeException e)
        {
            setResponseStatus(request, response, e);
        }
    }

    private boolean isSingleObjectRequest(HttpServletRequest request)
    {
        if (_hierarchy.length > 0)
        {
            String[] pathInfoElements = getPathInfoElements(request);
            return pathInfoElements != null && pathInfoElements.length == _hierarchy.length;
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

    private Class<? extends ConfiguredObject> getConfiguredClass()
    {
        return _hierarchy.length == 0 ? Broker.class : _hierarchy[_hierarchy.length-1];
    }

    @Override
    protected void doPutWithSubjectAndActor(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        performCreateOrUpdate(request, response);
    }

    private void performCreateOrUpdate(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
    {
        response.setContentType("application/json");

        List<String> names = getParentNamesFromServletPath(request);
        boolean isFullObjectURL = names.size() == _hierarchy.length;
        boolean isPostToFullURL = isFullObjectURL && "POST".equalsIgnoreCase(request.getMethod());
        final String[] pathInfoElements = getPathInfoElements(request);
        boolean isOperation = pathInfoElements != null && pathInfoElements.length == _hierarchy.length + 1 && isPostToFullURL;
        try
        {
            if(!isOperation)
            {
                Map<String, Object> providedObject = getRequestProvidedObject(request);
                if (names.isEmpty() && _hierarchy.length == 0)
                {
                    getBroker().setAttributes(providedObject);
                    response.setStatus(HttpServletResponse.SC_OK);
                    return;
                }


                ConfiguredObject theParent = getBroker();
                ConfiguredObject[] otherParents = null;
                Class<? extends ConfiguredObject> objClass = getConfiguredClass();
                if (_hierarchy.length > 1)
                {
                    List<ConfiguredObject> parents = findAllObjectParents(names);
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
                    else if (isPostToFullURL)
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
            }
            else
            {
                doOperation(request, response);
            }
        }
        catch (RuntimeException e)
        {
            setResponseStatus(request, response, e);
        }

    }

    private void doOperation(final HttpServletRequest request,
                             final HttpServletResponse response) throws IOException, ServletException
    {
        ConfiguredObject<?> subject;
        final List<String> names = getParentNamesFromServletPath(request);
        final String[] pathInfoElements = getPathInfoElements(request);

        if (names.isEmpty() && _hierarchy.length == 0)
        {
            subject = getBroker();
        }
        else
        {
            ConfiguredObject theParent = getBroker();
            ConfiguredObject[] otherParents = null;
            Class<? extends ConfiguredObject> objClass = getConfiguredClass();
            if (_hierarchy.length > 1)
            {
                List<ConfiguredObject> parents = findAllObjectParents(names);
                theParent = parents.remove(0);
                otherParents = parents.toArray(new ConfiguredObject[parents.size()]);
            }
            Map<String, Object> objectName =
                    Collections.<String, Object>singletonMap("name", names.get(names.size() - 1));
            subject = findObjectToUpdateInParent(objClass, objectName, theParent, otherParents);
            if(subject == null)
            {
                sendJsonErrorResponse(request,
                                      response,
                                      HttpServletResponse.SC_NOT_FOUND,
                                      getConfiguredClass().getSimpleName()
                                      + " '"
                                      + pathInfoElements[pathInfoElements.length - 2]
                                      + "' not found.");
                return;
            }
        }
        String operationName = pathInfoElements[pathInfoElements.length - 1];
        final Map<String, ConfiguredObjectOperation<?>> availableOperations =
                getBroker().getModel().getTypeRegistry().getOperations(subject.getClass());
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
                    operationArguments = getRequestProvidedObject(request);
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
        Object returnVal = operation.perform(subject, operationArguments);
        if(returnVal instanceof Content)
        {
            Content content = (Content)returnVal;
            writeTypedContent(content, request, response);
        }
        else
        {
            if (ConfiguredObject.class.isAssignableFrom(operation.getReturnType()))
            {
                returnVal = _objectConverter.convertObjectToMap((ConfiguredObject<?>) returnVal, operation.getReturnType(), DEFAULT_DEPTH,
                                                                false, false, false, false, DEFAULT_OVERSIZE, request.isSecure());
            }
            else if (returnsCollectionOfConfiguredObjects(operation))
            {
                List<Map<String, Object>> output = new ArrayList<>();
                for (Object configuredObject : (Collection)returnVal)
                {
                    output.add(_objectConverter.convertObjectToMap((ConfiguredObject<?>) configuredObject,
                               getCollectionMemberType((ParameterizedType) operation.getGenericReturnType()),
                               DEFAULT_DEPTH, false, false, false, false, DEFAULT_OVERSIZE, request.isSecure()));
                }
                returnVal = output;
            }
            sendJsonResponse(returnVal, request, response);
        }

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

    private List<ConfiguredObject> findAllObjectParents(List<String> names)
    {
        Collection<ConfiguredObject>[] objects = new Collection[_hierarchy.length];
        for (int i = 0; i < _hierarchy.length - 1; i++)
        {
            objects[i] = new HashSet<>();
            if (i == 0)
            {
                for (ConfiguredObject object : getBroker().getChildren(_hierarchy[0]))
                {
                    if (object.getName().equals(names.get(0)))
                    {
                        objects[0].add(object);
                        break;
                    }
                }
            }
            else
            {
                for (int j = i - 1; j >= 0; j--)
                {
                    if (getBroker().getModel().getChildTypes(_hierarchy[j]).contains(_hierarchy[i]))
                    {
                        for (ConfiguredObject<?> parent : objects[j])
                        {
                            for (ConfiguredObject<?> object : parent.getChildren(_hierarchy[i]))
                            {
                                if (object.getName().equals(names.get(i)))
                                {
                                    objects[i].add(object);
                                }
                            }
                        }
                        break;
                    }
                }
            }

        }
        List<ConfiguredObject> parents = new ArrayList<>();
        Class<? extends ConfiguredObject> objClass = getConfiguredClass();
        Collection<Class<? extends ConfiguredObject>> parentClasses =
                getBroker().getModel().getParentTypes(objClass);
        for (int i = _hierarchy.length - 2; i >= 0; i--)
        {
            if (parentClasses.contains(_hierarchy[i]))
            {
                if (objects[i].size() == 1)
                {
                    parents.add(objects[i].iterator().next());
                }
                else
                {
                    throw new IllegalArgumentException("Cannot deduce parent of class "
                                                       + _hierarchy[i].getSimpleName());
                }
            }

        }
        return parents;
    }

    private List<String> getParentNamesFromServletPath(HttpServletRequest request)
    {
        List<String> names = new ArrayList<>();
        String[] pathInfoElements = getPathInfoElements(request);
        if (pathInfoElements != null)
        {
            if (!(pathInfoElements.length == _hierarchy.length ||
                    (_hierarchy.length > 0 && pathInfoElements.length == _hierarchy.length - 1)))
            {
                if(pathInfoElements.length == _hierarchy.length + 1)
                {
                    names.addAll(Arrays.asList(pathInfoElements).subList(0,pathInfoElements.length-1));
                }
                else
                {
                    throw new IllegalArgumentException(
                            "Either parent path or full object path must be specified on object creation."
                            + " Full object path must be specified on object update. "
                            + "Found "
                            + names
                            + " of size "
                            + names.size()
                            + " expecting "
                            + _hierarchy.length);
                }
            }
            else
            {
                names.addAll(Arrays.asList(pathInfoElements));
            }
        }
        return names;
    }

    private Map<String, Object> getRequestProvidedObject(HttpServletRequest request) throws IOException, ServletException
    {
        Map<String, Object> providedObject;

        ArrayList<String> headers = Collections.list(request.getHeaderNames());
        ObjectMapper mapper = new ObjectMapper();

        if(headers.contains("Content-Type") && request.getHeader("Content-Type").startsWith("multipart/form-data"))
        {
            providedObject = new HashMap<>();
            Map<String,String> fileUploads = new HashMap<>();
            Collection<Part> parts = request.getParts();
            for(Part part : parts)
            {
                if("data".equals(part.getName()) && "application/json".equals(part.getContentType()))
                {
                    providedObject = mapper.readValue(part.getInputStream(), LinkedHashMap.class);
                }
                else
                {
                    byte[] data = new byte[(int) part.getSize()];
                    part.getInputStream().read(data);
                    String inlineURL = DataUrlUtils.getDataUrlForBytes(data);
                    fileUploads.put(part.getName(), inlineURL);
                }
            }
            providedObject.putAll(fileUploads);
        }
        else
        {

            providedObject = mapper.readValue(request.getInputStream(), LinkedHashMap.class);
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

    private void setResponseStatus(HttpServletRequest request, HttpServletResponse response, RuntimeException e) throws IOException
    {
        if (e instanceof AccessControlException)
        {
            LOGGER.debug("AccessControlException, sending {}", HttpServletResponse.SC_FORBIDDEN, e);
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        }
        else
        {
            int responseCode = HttpServletResponse.SC_BAD_REQUEST;
            String message = e.getMessage();
            if (e instanceof ExchangeExistsException || e instanceof QueueExistsException
                    || e instanceof AbstractConfiguredObject.DuplicateIdException
                    || e instanceof AbstractConfiguredObject.DuplicateNameException
                    || e instanceof IntegrityViolationException
                    || e instanceof IllegalStateTransitionException)
            {
                responseCode = HttpServletResponse.SC_CONFLICT;
            }
            else if (e instanceof IllegalConfigurationException || e instanceof IllegalArgumentException)
            {
                LOGGER.warn(e.getClass().getSimpleName() + " processing request : " + message);
                Throwable t = e;
                int maxDepth = 10;
                while((t = t.getCause())!=null && maxDepth-- != 0)
                {
                    LOGGER.warn("... caused by " + t.getClass().getSimpleName() + "  : " + t.getMessage());
                }
                if (LOGGER.isTraceEnabled())
                {
                    LOGGER.trace(e.getClass().getSimpleName() + " processing request", e);
                }
                responseCode = SC_UNPROCESSABLE_ENTITY;
            }
            else
            {
                LOGGER.warn("Unexpected exception processing request ", e);
            }


            sendJsonErrorResponse(request, response, responseCode, message);

        }
    }

    @Override
    protected void doDeleteWithSubjectAndActor(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        try
        {
            Collection<ConfiguredObject<?>> allObjects = getObjects(request);
            if(allObjects != null)
            {
                for (ConfiguredObject o : allObjects)
                {
                    o.delete();
                }

                sendCachingHeadersOnResponse(response);
                response.setStatus(HttpServletResponse.SC_OK);
            }
            else
            {
                sendJsonErrorResponse(request, response, HttpServletResponse.SC_NOT_FOUND, "Not Found");
            }
        }
        catch(RuntimeException e)
        {
            setResponseStatus(request, response, e);
        }
    }

    @Override
    protected void doPostWithSubjectAndActor(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
    {
        performCreateOrUpdate(request, response);
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
        return Boolean.parseBoolean(request.getParameter(paramName));
    }

    private String ensureFilenameIsRfc2183(final String requestedFilename)
    {
        return requestedFilename.replaceAll("[\\P{InBasic_Latin}\\\\:/]", "");
    }

}
