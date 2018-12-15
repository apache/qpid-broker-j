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

import static org.apache.qpid.server.management.plugin.controller.ConverterHelper.encode;
import static org.apache.qpid.server.management.plugin.ManagementException.createBadRequestManagementException;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.RequestType;
import org.apache.qpid.server.management.plugin.ManagementRequest;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ResponseType;

public abstract class AbstractManagementController implements ManagementController
{
    protected static final String USER_PREFERENCES = "userpreferences";
    protected static final String VISIBLE_USER_PREFERENCES = "visiblepreferences";

    public ManagementResponse handleGet(final ManagementRequest request) throws ManagementException
    {
        final RequestType type = getRequestType(request);
        switch (type)
        {
            case OPERATION:
            {
                final Collection<String> hierarchy = getCategoryHierarchy(request.getRoot(), request.getCategory());
                final List<String> operationPath = request.getPath().subList(0, hierarchy.size());
                final String operationName = request.getPath().get(hierarchy.size());
                return invoke(request.getRoot(),
                                  request.getCategory(),
                                  operationPath,
                                  operationName,
                                  request.getParametersAsFlatMap(),
                                  false,
                                  request.isSecure() || request.isConfidentialOperationAllowedOnInsecureChannel());
            }
            case MODEL_OBJECT:
            {
                final Object response = get(request.getRoot(),
                                            request.getCategory(),
                                            request.getPath(),
                                            request.getParameters());

                return new ControllerManagementResponse(ResponseType.MODEL_OBJECT, response);
            }
            case VISIBLE_PREFERENCES:
            case USER_PREFERENCES:
            {
                final Object response = getPreferences(request.getRoot(),
                                          request.getCategory(),
                                          request.getPath(),
                                          request.getParameters());
                return new ControllerManagementResponse(ResponseType.DATA, response);
            }
            default:
            {
                throw createBadRequestManagementException(String.format("Unexpected request type '%s' for path '%s'",
                                                                        type,
                                                                        getCategoryMapping(request.getCategory())));
            }
        }


    }

    public ManagementResponse handlePut(final ManagementRequest request) throws ManagementException
    {
        return handlePostOrPut(request);
    }

    public ManagementResponse handlePost(final ManagementRequest request) throws ManagementException
    {
        return handlePostOrPut(request);
    }

    public ManagementResponse handleDelete(final ManagementRequest request) throws ManagementException
    {
        final RequestType type = getRequestType(request);
        switch (type)
        {
            case MODEL_OBJECT:
            {
                delete(request.getRoot(),
                       request.getCategory(),
                       request.getPath(),
                       request.getParameters());
                break;
            }
            case VISIBLE_PREFERENCES:
            case USER_PREFERENCES:
            {
                deletePreferences(request.getRoot(),
                                  request.getCategory(),
                                  request.getPath(),
                                  request.getParameters());
                break;
            }
            default:
            {
                throw createBadRequestManagementException(String.format("Unexpected request type '%s' for path '%s'",
                                                                        type,
                                                                        getCategoryMapping(request.getCategory())));
            }
        }
        return new ControllerManagementResponse(ResponseType.EMPTY, null);
    }

    private ManagementResponse handlePostOrPut(final ManagementRequest request) throws ManagementException
    {
        final RequestType type = getRequestType(request);
        final Collection<String> hierarchy = getCategoryHierarchy(request.getRoot(), request.getCategory());
        switch (type)
        {
            case OPERATION:
            {
                final List<String> operationPath = request.getPath().subList(0, hierarchy.size());
                final String operationName = request.getPath().get(hierarchy.size());
                @SuppressWarnings("unchecked")
                final Map<String, Object> arguments = request.getBody(LinkedHashMap.class);
                return invoke(request.getRoot(),
                                  request.getCategory(),
                                  operationPath,
                                  operationName,
                                  arguments,
                                  true,
                                  request.isSecure()
                                  || request.isConfidentialOperationAllowedOnInsecureChannel());
            }
            case MODEL_OBJECT:
            {
                @SuppressWarnings("unchecked")
                final Map<String, Object> attributes = request.getBody(LinkedHashMap.class);
                final Object response = createOrUpdate(request.getRoot(),
                                          request.getCategory(),
                                          request.getPath(),
                                          attributes,
                                          "POST".equalsIgnoreCase(request.getMethod()));
                int responseCode = HttpServletResponse.SC_OK;
                ResponseType responseType = ResponseType.EMPTY;
                Map<String, String> headers = Collections.emptyMap();
                if (response != null)
                {
                    responseCode = HttpServletResponse.SC_CREATED;
                    final StringBuilder requestURL = new StringBuilder(request.getRequestURL());
                    if (hierarchy.size() != request.getPath().size())
                    {
                        Object name = attributes.get("name");
                        requestURL.append("/").append(encode(String.valueOf(name)));
                    }
                    headers = Collections.singletonMap("Location", requestURL.toString());
                    responseType = ResponseType.MODEL_OBJECT;
                }
                return new ControllerManagementResponse(responseType, response, responseCode, headers);
            }
            case VISIBLE_PREFERENCES:
            case USER_PREFERENCES:
            {
                setPreferences(request.getRoot(),
                               request.getCategory(),
                               request.getPath(),
                               request.getBody(Object.class),
                               request.getParameters(),
                               "POST".equalsIgnoreCase(request.getMethod()));
                return new ControllerManagementResponse(ResponseType.EMPTY, null);
            }
            default:
            {
                throw createBadRequestManagementException(String.format("Unexpected request type '%s' for path '%s'",
                                                                        type,
                                                                        getCategoryMapping(request.getCategory())));
            }
        }

    }

    protected abstract RequestType getRequestType(ManagementRequest managementRequest) throws ManagementException;
}
