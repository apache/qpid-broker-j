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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.model.ConfiguredObject;

public class RequestInfoParser
{
    private static final String USER_PREFERENCES = "userpreferences";
    private static final String VISIBLE_USER_PREFERENCES = "visiblepreferences";
    private static final String UTF8 = StandardCharsets.UTF_8.name();

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
        Map<String, List<String>> queryParameters =  parseQueryString(request.getQueryString());

        final String method = request.getMethod();
        if ("POST".equals(method))
        {
            return parsePost(servletPath, pathInfo, parts, queryParameters);
        }
        else if ("PUT".equals(method))
        {
            return parsePut(servletPath, pathInfo, parts, queryParameters);
        }
        else if ("GET".equals(method))
        {
            return parseGet(servletPath, pathInfo, parts, queryParameters);
        }
        else if ("DELETE".equals(method))
        {
            return parseDelete(servletPath, pathInfo, parts, queryParameters);
        }
        else
        {
            throw new IllegalArgumentException(String.format("Unexpected method type '%s' for path '%s%s'",
                                                             method, servletPath, pathInfo));
        }
    }

    private RequestInfo parseDelete(final String servletPath,
                                    final String pathInfo,
                                    final List<String> parts,
                                    final Map<String, List<String>> queryParameters)
    {
        if (parts.size() <= _hierarchy.size())
        {
            return RequestInfo.createModelRequestInfo(parts,
                                                      queryParameters, parts.size() == _hierarchy.size());
        }
        else if (parts.size() > _hierarchy.size())
        {
            if (USER_PREFERENCES.equals(parts.get(_hierarchy.size())))
            {
                return RequestInfo.createPreferencesRequestInfo(parts.subList(0, _hierarchy.size()),
                                                                parts.subList(_hierarchy.size() + 1, parts.size()),
                                                                queryParameters);
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

    private RequestInfo parseGet(final String servletPath,
                                 final String pathInfo,
                                 final List<String> parts,
                                 final Map<String, List<String>> queryParameters)
    {
        if (parts.size() <= _hierarchy.size())
        {
            return RequestInfo.createModelRequestInfo(parts,
                                                      queryParameters,
                                                      parts.size() == _hierarchy.size());
        }
        else if (parts.size() > _hierarchy.size())
        {
            if (USER_PREFERENCES.equals(parts.get(_hierarchy.size())))
            {
                return RequestInfo.createPreferencesRequestInfo(parts.subList(0, _hierarchy.size()),
                                                                parts.subList(_hierarchy.size() + 1, parts.size()),
                                                                queryParameters);
            }
            else if (VISIBLE_USER_PREFERENCES.equals(parts.get(_hierarchy.size())))
            {
                return RequestInfo.createVisiblePreferencesRequestInfo(parts.subList(0, _hierarchy.size()),
                                                                       parts.subList(_hierarchy.size() + 1, parts.size()),
                                                                       queryParameters);
            }
            else if (parts.size() == _hierarchy.size() + 1)
            {
                return RequestInfo.createOperationRequestInfo(parts.subList(0, _hierarchy.size()),
                                                              parts.get(parts.size() - 1),
                                                              queryParameters);
            }
        }
        String expectedPath = buildExpectedPath(servletPath, _hierarchy);
        throw new IllegalArgumentException(String.format("Invalid GET path '%s%s'. Expected: '%s[/<operation name>]'",
                                                         servletPath, pathInfo,
                                                         expectedPath));
    }

    private RequestInfo parsePut(final String servletPath,
                                 final String pathInfo,
                                 final List<String> parts,
                                 final Map<String, List<String>> queryParameters)
    {
        if (parts.size() == _hierarchy.size() || parts.size() == _hierarchy.size() - 1)
        {
            return RequestInfo.createModelRequestInfo(parts, queryParameters, parts.size() == _hierarchy.size());
        }
        else if (parts.size() > _hierarchy.size() && USER_PREFERENCES.equals(parts.get(_hierarchy.size())))
        {
            return RequestInfo.createPreferencesRequestInfo(parts.subList(0, _hierarchy.size()),
                                                            parts.subList(_hierarchy.size() + 1, parts.size()),
                                                            queryParameters);
        }
        else
        {
            String expectedPath = buildExpectedPath(servletPath, _hierarchy);
            throw new IllegalArgumentException(String.format("Invalid PUT path '%s%s'. Expected: '%s'",
                                                             servletPath, pathInfo,
                                                             expectedPath));
        }
    }

    private RequestInfo parsePost(final String servletPath,
                                  final String pathInfo,
                                  final List<String> parts,
                                  final Map<String, List<String>> queryParameters)
    {
        if (parts.size() == _hierarchy.size() || parts.size() == _hierarchy.size() - 1)
        {
            return RequestInfo.createModelRequestInfo(parts, queryParameters, parts.size() == _hierarchy.size());
        }
        else if (parts.size() > _hierarchy.size())
        {
            if (USER_PREFERENCES.equals(parts.get(_hierarchy.size())))
            {

                return RequestInfo.createPreferencesRequestInfo(parts.subList(0, _hierarchy.size()),
                                                                parts.subList(_hierarchy.size() + 1, parts.size()),
                                                                queryParameters);
            }
            else if (parts.size() == _hierarchy.size() + 1 && !VISIBLE_USER_PREFERENCES.equals(parts.get(_hierarchy.size())))
            {
                return RequestInfo.createOperationRequestInfo(parts.subList(0, _hierarchy.size()),
                                                              parts.get(parts.size() - 1),
                                                              queryParameters);
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

    private Map<String, List<String>> parseQueryString(String queryString)
    {

        Map<String, List<String>> query = new LinkedHashMap<>();
        if (queryString == null || queryString.isEmpty())
        {
            return query;
        }

        final List<String> pairs = Arrays.asList(queryString.split("&"));
        for (String pairString : pairs)
        {
            List<String> pair = new ArrayList<>(Arrays.asList(pairString.split("=")));
            if (pair.size() == 1)
            {
                pair.add(null);
            }
            else if (pair.size() != 2)
            {
                throw new IllegalArgumentException(String.format("could not parse query string '%s'", queryString));
            }

            String key;
            String value;
            try
            {
                key = URLDecoder.decode(pair.get(0), UTF8);
                value = pair.get(1) == null ? null : URLDecoder.decode(pair.get(1), UTF8);
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException(e);
            }
            if (!query.containsKey(key))
            {
                query.put(key, new ArrayList<String>());
            }
            query.get(key).add(value);
        }
        return query;
    }
}
