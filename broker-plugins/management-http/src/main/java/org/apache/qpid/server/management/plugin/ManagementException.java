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
package org.apache.qpid.server.management.plugin;

import java.security.AccessController;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.management.plugin.controller.ConverterHelper;
import org.apache.qpid.server.management.plugin.servlet.rest.NotFoundException;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.IllegalStateTransitionException;
import org.apache.qpid.server.model.IntegrityViolationException;
import org.apache.qpid.server.model.OperationTimeoutException;
import org.apache.qpid.server.util.ExternalServiceException;
import org.apache.qpid.server.util.ExternalServiceTimeoutException;

public class ManagementException extends RuntimeException
{
    private static final int SC_UNPROCESSABLE_ENTITY = 422;

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagementException.class);

    private final int _statusCode;
    private final Map<String, String> _headers;

    private ManagementException(final int statusCode,
                                  final String message,
                                  final Map<String, String> headers)
    {
        super(message);
        _statusCode = statusCode;
        _headers = headers;
    }

    private ManagementException(final int statusCode,
                                  final String message,
                                  final Throwable cause,
                                  final Map<String, String> headers)
    {
        super(message, cause);
        _statusCode = statusCode;
        _headers = headers;
    }

    public int getStatusCode()
    {
        return _statusCode;
    }

    public Map<String, String> getHeaders()
    {
        return _headers;
    }

    public static ManagementException createNotFoundManagementException(final Exception e)
    {
        return new ManagementException(HttpServletResponse.SC_NOT_FOUND, e.getMessage(), e, null);
    }

    public static ManagementException createNotFoundManagementException(final String message)
    {
        return new ManagementException(HttpServletResponse.SC_NOT_FOUND, message, null);
    }

    public static ManagementException createGoneManagementException(final String message)
    {
        return new ManagementException(HttpServletResponse.SC_GONE, message, null);
    }

    public static ManagementException createUnprocessableManagementException(final String message)
    {
        return new ManagementException(SC_UNPROCESSABLE_ENTITY, message, null);
    }

    public static ManagementException createUnprocessableManagementException(final Exception e)
    {
        return new ManagementException(SC_UNPROCESSABLE_ENTITY, e.getMessage(), e, null);
    }

    private static ManagementException createConflictManagementException(final Exception e)
    {
        return new ManagementException(HttpServletResponse.SC_CONFLICT, e.getMessage(), e, null);
    }


    public static ManagementException createNotAllowedManagementException(final String message,
                                                                          final Map<String, String> headers)
    {
        return new ManagementException(HttpServletResponse.SC_METHOD_NOT_ALLOWED, message, headers);
    }

    public static ManagementException createForbiddenManagementException(final String message)
    {
        return new ManagementException(HttpServletResponse.SC_FORBIDDEN, message, null);
    }


    public static ManagementException createForbiddenManagementException(final Exception e)
    {
        return new ManagementException(HttpServletResponse.SC_FORBIDDEN, e.getMessage(), e, null);
    }


    public static ManagementException createInternalServerErrorManagementException(final String message)
    {
        return new ManagementException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, message, null);
    }

    public static ManagementException createInternalServerErrorManagementException(final String message,
                                                                                   final Exception e)
    {
        return new ManagementException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, message, e, null);
    }


    private static ManagementException createBadGatewayManagementException(final String message,
                                                                           final RuntimeException e)
    {
        return new ManagementException(HttpServletResponse.SC_BAD_GATEWAY, message, e, null);
    }

    private static ManagementException createGatewayTimeoutManagementException(final RuntimeException e)
    {
        return new ManagementException(HttpServletResponse.SC_GATEWAY_TIMEOUT, e.getMessage(), e, null);
    }


    public static ManagementException createBadRequestManagementException(final String message)
    {
        return new ManagementException(HttpServletResponse.SC_BAD_REQUEST, message, null);
    }

    public static ManagementException createBadRequestManagementException(final String message, final Throwable e)
    {
        return new ManagementException(HttpServletResponse.SC_BAD_REQUEST, message, e, null);
    }


    public static ManagementException toManagementException(final RuntimeException e,
                                                            final String categoryMapping,
                                                            final List<String> path)
    {
        if (e instanceof SecurityException)
        {
            LOGGER.debug("{}, sending {}", e.getClass().getName(), HttpServletResponse.SC_FORBIDDEN, e);
            return createForbiddenManagementException(e);
        }
        else if (e instanceof AbstractConfiguredObject.DuplicateIdException
                 || e instanceof AbstractConfiguredObject.DuplicateNameException
                 || e instanceof IntegrityViolationException
                 || e instanceof IllegalStateTransitionException)
        {
            return createConflictManagementException(e);
        }
        else if (e instanceof NotFoundException)
        {
            if (LOGGER.isTraceEnabled())
            {
                LOGGER.trace(e.getClass().getSimpleName() + " processing request", e);
            }
            return createNotFoundManagementException(e);
        }
        else if (e instanceof IllegalConfigurationException || e instanceof IllegalArgumentException)
        {
            LOGGER.warn("{} processing request {} from user '{}': {}",
                        e.getClass().getSimpleName(),
                        getRequestURI(path, categoryMapping),
                        getRequestPrincipals(),
                        e.getMessage());
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
            return createUnprocessableManagementException(e);
        }
        else if (e instanceof OperationTimeoutException)
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Timeout during processing of request {} from user '{}'",
                             getRequestURI(path, categoryMapping),
                             getRequestPrincipals(),
                             e);
            }
            else
            {
                LOGGER.info("Timeout during processing of request {} from user '{}'",
                            getRequestURI(path, categoryMapping), getRequestPrincipals());
            }
            return createBadGatewayManagementException("Timeout occurred", e);
        }

        else if (e instanceof ExternalServiceTimeoutException)
        {
            LOGGER.warn("External request timeout ", e);
            return createGatewayTimeoutManagementException(e);
        }
        else if (e instanceof ExternalServiceException)
        {
            LOGGER.warn("External request failed ", e);
            return createBadGatewayManagementException(e.getMessage(), e);
        }
        else if (e instanceof ManagementException)
        {
            return (ManagementException)e;
        }
        else
        {
            LOGGER.warn("Unexpected Exception", e);
            return createInternalServerErrorManagementException("Unexpected Exception", e);
        }
    }

    public static ManagementException handleError(final Error e)
    {
        if (e instanceof NoClassDefFoundError)
        {
            LOGGER.warn("Unexpected exception processing request ", e);
            return createBadRequestManagementException("Not found: " + e.getMessage(), e);
        }
        else
        {
            throw e;
        }
    }

    public static String getRequestURI(final List<String> path, final String categoryMapping)
    {
        return categoryMapping + (categoryMapping.endsWith("/") ? "" : "/")
               + path.stream().map(ConverterHelper::encode).collect(Collectors.joining("/"));
    }

    private static String getRequestPrincipals()
    {
        final Subject subject = Subject.getSubject(AccessController.getContext());
        if (subject == null)
        {
            return null;
        }
        final Set<Principal> principalSet = subject.getPrincipals();
        return String.join("/",
                           principalSet.stream()
                                       .map(Principal::getName)
                                       .collect(Collectors.toCollection(TreeSet::new)));
    }
}
