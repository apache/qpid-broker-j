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

package org.apache.qpid.server.management.plugin.servlet.rest;

import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.model.ConfiguredObject;

public class RequestInfoParser
{
    private static final String USER_PREFERENCES = "userpreferences";

    private final List<Class<? extends ConfiguredObject>> _hierarchy;

    public RequestInfoParser(final Class<? extends ConfiguredObject>... hierarchy)
    {
        _hierarchy = Arrays.asList(hierarchy);
    }

    public RequestInfo parse(final HttpServletRequest request)
    {
        final String servletPath = request.getServletPath();
        final String pathInfo = (request.getPathInfo() != null ? request.getPathInfo() : "");
        List<String> parts = HttpManagementUtil.getPathInfoElements(servletPath, pathInfo);

        final String method = request.getMethod();
        if ("POST".equals(method))
        {
            return parsePost(servletPath, pathInfo, parts);
        }
        else if ("PUT".equals(method))
        {
            return parsePut(servletPath, pathInfo, parts);
        }
        else if ("GET".equals(method))
        {
            return parseGet(servletPath, pathInfo, parts);
        }
        else if ("DELETE".equals(method))
        {
            return parseDelete(servletPath, pathInfo, parts);
        }
        else
        {
            throw new IllegalArgumentException(String.format("Unexpected method type '%s' for path '%s%s'",
                                                             method, servletPath, pathInfo));
        }
    }

    private RequestInfo parseDelete(final String servletPath, final String pathInfo, final List<String> parts)
    {
        if (parts.size() <= _hierarchy.size())
        {
            return RequestInfo.createModelRequestInfo(parts);
        }
        else if (parts.size() > _hierarchy.size())
        {
            if (USER_PREFERENCES.equals(parts.get(_hierarchy.size())))
            {
                return RequestInfo.createPreferencesRequestInfo(parts.subList(0, _hierarchy.size()),
                                                                parts.subList(_hierarchy.size() + 1, parts.size()));
            }
        }
        String expectedPath = buildExpectedPath(servletPath, _hierarchy);
        throw new IllegalArgumentException(String.format(
                "Invalid DELETE path '%s%s'. Expected: '%s' or '%s/userpreferences[/<preference type>[/<preference name>]]'",
                servletPath,
                pathInfo,
                expectedPath,
                expectedPath));
    }

    private RequestInfo parseGet(final String servletPath, final String pathInfo, final List<String> parts)
    {
        if (parts.size() <= _hierarchy.size())
        {
            return RequestInfo.createModelRequestInfo(parts);
        }
        else if (parts.size() > _hierarchy.size())
        {
            if (USER_PREFERENCES.equals(parts.get(_hierarchy.size())))
            {
                return RequestInfo.createPreferencesRequestInfo(parts.subList(0, _hierarchy.size()),
                                                                parts.subList(_hierarchy.size() + 1, parts.size()));
            }
            else if (parts.size() == _hierarchy.size() + 1)
            {
                return RequestInfo.createOperationRequestInfo(parts.subList(0, _hierarchy.size()),
                                                              parts.get(parts.size() - 1));
            }
        }
        String expectedPath = buildExpectedPath(servletPath, _hierarchy);
        throw new IllegalArgumentException(String.format("Invalid GET path '%s%s'. Expected: '%s[/<operation name>]'",
                                                         servletPath, pathInfo,
                                                         expectedPath));
    }

    private RequestInfo parsePut(final String servletPath, final String pathInfo, final List<String> parts)
    {
        if (parts.size() == _hierarchy.size() || parts.size() == _hierarchy.size() - 1)
        {
            return RequestInfo.createModelRequestInfo(parts);
        }
        else if (parts.size() > _hierarchy.size() && USER_PREFERENCES.equals(parts.get(_hierarchy.size())))
        {
            return RequestInfo.createPreferencesRequestInfo(parts.subList(0, _hierarchy.size()),
                                                            parts.subList(_hierarchy.size() + 1, parts.size()));
        }
        else
        {
            String expectedPath = buildExpectedPath(servletPath, _hierarchy);
            throw new IllegalArgumentException(String.format("Invalid PUT path '%s%s'. Expected: '%s'",
                                                             servletPath, pathInfo,
                                                             expectedPath));
        }
    }

    private RequestInfo parsePost(final String servletPath, final String pathInfo, final List<String> parts)
    {
        if (parts.size() == _hierarchy.size() || parts.size() == _hierarchy.size() - 1)
        {
            return RequestInfo.createModelRequestInfo(parts);
        }
        else if (parts.size() > _hierarchy.size())
        {
            if (USER_PREFERENCES.equals(parts.get(_hierarchy.size())))
            {
                return RequestInfo.createPreferencesRequestInfo(parts.subList(0, _hierarchy.size()),
                                                                parts.subList(_hierarchy.size() + 1, parts.size()));
            }
            else if (parts.size() == _hierarchy.size() + 1)
            {
                return RequestInfo.createOperationRequestInfo(parts.subList(0, _hierarchy.size()),
                                                              parts.get(parts.size() - 1));
            }
        }
        String expectedFullPath = buildExpectedPath(servletPath, _hierarchy);
        String expectedParentPath = buildExpectedPath(servletPath, _hierarchy.subList(0, _hierarchy.size() - 1));

        throw new IllegalArgumentException(String.format("Invalid POST path '%s%s'. Expected: '%s/<operation name>'"
                                                         + " or '%s'"
                                                         + " or '%s/userpreferences[/<preference type>]'",
                                                         servletPath, pathInfo,
                                                         expectedFullPath, expectedParentPath, expectedFullPath));
    }

    private String buildExpectedPath(final String servletPath, final List<Class<? extends ConfiguredObject>> hierarchy)
    {
        StringBuilder expectedPath = new StringBuilder(servletPath);
        for (Class<? extends ConfiguredObject> part : hierarchy)
        {
            expectedPath.append("/<");
            expectedPath.append(part.getSimpleName().toLowerCase());
            expectedPath.append(" name>");
        }
        return expectedPath.toString();
    }
}
