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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.qpid.server.management.plugin.RequestType;

public class RequestInfo
{
    private final RequestType _type;
    private final List<String> _modelParts;
    private final boolean _hierarchySatisfied;
    private final String _operationName;
    private final List<String> _preferencesParts;
    private final Map<String, List<String>> _queryParameters;
    private final boolean _hasWildcard;

    public static RequestInfo createModelRequestInfo(final List<String> modelParts,
                                                     Map<String, List<String>> queryParameters,
                                                     final boolean hierarchySatisfied)
    {
        return new RequestInfo(RequestType.MODEL_OBJECT, modelParts, null, Collections.<String>emptyList(), queryParameters, hierarchySatisfied );
    }

    public static RequestInfo createOperationRequestInfo(final List<String> modelParts, final String operationName, Map<String, List<String>> queryParameters)
    {
        return new RequestInfo(RequestType.OPERATION, modelParts, operationName, Collections.<String>emptyList(), queryParameters,
                               true);
    }

    public static RequestInfo createPreferencesRequestInfo(final List<String> modelParts, final List<String> preferencesParts)
    {
        return new RequestInfo(RequestType.USER_PREFERENCES, modelParts, null, preferencesParts, Collections.<String,
                List<String>>emptyMap(),
                               true);
    }

    public static RequestInfo createPreferencesRequestInfo(final List<String> modelParts, final List<String> preferencesParts, Map<String, List<String>> queryParameters)
    {
        return new RequestInfo(RequestType.USER_PREFERENCES, modelParts, null, preferencesParts, queryParameters,
                               true);
    }

    public static RequestInfo createVisiblePreferencesRequestInfo(final List<String> modelParts,
                                                                  final List<String> preferencesParts,
                                                                  final Map<String, List<String>> queryParameters)
    {
        return new RequestInfo(RequestType.VISIBLE_PREFERENCES, modelParts, null, preferencesParts, queryParameters,
                               true);
    }

    private RequestInfo(final RequestType type,
                        final List<String> modelParts,
                        final String operationName,
                        final List<String> preferencesParts,
                        final Map<String, List<String>> queryParameters, final boolean hierarchySatisfied)
    {
        _type = type;
        _operationName = operationName;
        _modelParts = ImmutableList.copyOf(modelParts);
        _hierarchySatisfied = hierarchySatisfied;
        _hasWildcard = _modelParts.contains("*");
        _preferencesParts = ImmutableList.copyOf(preferencesParts);
        _queryParameters = ImmutableMap.copyOf(queryParameters);
    }

    public RequestType getType()
    {
        return _type;
    }

    public List<String> getModelParts()
    {
        return _modelParts;
    }

    public String getOperationName()
    {
        if (_type != RequestType.OPERATION)
        {
            throw new IllegalStateException("Must not call getOperationName on non-Operation RequestInfo");
        }
        return _operationName;
    }

    public List<String> getPreferencesParts()
    {
        return _preferencesParts;
    }

    public Map<String, List<String>> getQueryParameters()
    {
        return _queryParameters;
    }

    public boolean hasWildcard()
    {
        return _hasWildcard;
    }

    public boolean isSingletonRequest()
    {
        return _hierarchySatisfied && !_hasWildcard;
    }


}
