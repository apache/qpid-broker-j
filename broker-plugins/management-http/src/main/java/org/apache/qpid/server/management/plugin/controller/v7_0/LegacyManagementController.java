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
package org.apache.qpid.server.management.plugin.controller.v7_0;

import static org.apache.qpid.server.management.plugin.controller.ConverterHelper.getIntParameterFromRequest;
import static org.apache.qpid.server.management.plugin.controller.ConverterHelper.getParameter;

import java.util.List;
import java.util.Map;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.controller.AbstractLegacyConfiguredObjectController;

public class LegacyManagementController extends AbstractLegacyConfiguredObjectController
{
    private static final String DEPTH_PARAM = "depth";
    private static final String OVERSIZE_PARAM = "oversize";
    private static final String ACTUALS_PARAM = "actuals";
    private static final String EXCLUDE_INHERITED_CONTEXT_PARAM = "excludeInheritedContext";
    private static final String SINGLETON_MODEL_OBJECT_RESPONSE_AS_LIST = "singletonModelObjectResponseAsList";

    private static final int DEFAULT_DEPTH = 0;
    private static final int DEFAULT_OVERSIZE = 120;

    public LegacyManagementController(final ManagementController nextVersionManagementController,
                                      final String modelVersion)
    {
        super(modelVersion, nextVersionManagementController);
    }

    @Override
    protected Map<String, List<String>> convertQueryParameters(final Map<String, List<String>> parameters)
    {
        return parameters;
    }

    @Override
    public Object formatConfiguredObject(final Object content,
                                         final Map<String, List<String>> parameters,
                                         final boolean isSecureOrAllowedOnInsecureChannel)
    {
        final int depth = getIntParameterFromRequest(parameters, DEPTH_PARAM, DEFAULT_DEPTH);
        final int oversizeThreshold = getIntParameterFromRequest(parameters, OVERSIZE_PARAM, DEFAULT_OVERSIZE);
        final boolean actuals = Boolean.parseBoolean(getParameter(ACTUALS_PARAM, parameters));
        final String excludeInheritedContextParameter = getParameter(EXCLUDE_INHERITED_CONTEXT_PARAM, parameters);
        final boolean excludeInheritedContext = excludeInheritedContextParameter == null
                                                || Boolean.parseBoolean(excludeInheritedContextParameter);
        final boolean responseAsList =
                Boolean.parseBoolean(getParameter(SINGLETON_MODEL_OBJECT_RESPONSE_AS_LIST, parameters));

        return formatConfiguredObject(content,
                                      isSecureOrAllowedOnInsecureChannel,
                                      depth,
                                      oversizeThreshold,
                                      actuals,
                                      excludeInheritedContext,
                                      responseAsList);
    }
}
