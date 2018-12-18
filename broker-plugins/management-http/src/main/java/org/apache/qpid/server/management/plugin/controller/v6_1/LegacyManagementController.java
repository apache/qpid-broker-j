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
package org.apache.qpid.server.management.plugin.controller.v6_1;

import static org.apache.qpid.server.management.plugin.ManagementException.createUnprocessableManagementException;
import static org.apache.qpid.server.management.plugin.controller.ConverterHelper.getIntParameterFromRequest;
import static org.apache.qpid.server.management.plugin.controller.ConverterHelper.getParameter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.controller.AbstractLegacyConfiguredObjectController;
import org.apache.qpid.server.management.plugin.controller.ConverterHelper;
import org.apache.qpid.server.management.plugin.controller.LegacyConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;

public class LegacyManagementController extends AbstractLegacyConfiguredObjectController
{
    private static final String DEPTH_PARAM = "depth";
    private static final String OVERSIZE_PARAM = "oversize";
    private static final String ACTUALS_PARAM = "actuals";
    private static final String INCLUDE_SYS_CONTEXT_PARAM = "includeSysContext";
    private static final String INHERITED_ACTUALS_PARAM = "inheritedActuals";
    private static final String EXCLUDE_INHERITED_CONTEXT_PARAM = "excludeInheritedContext";


    private static final int DEFAULT_DEPTH = 1;
    private static final int DEFAULT_OVERSIZE = 120;

    LegacyManagementController(final ManagementController nextVersionManagementController)
    {
        super(LegacyManagementControllerFactory.MODEL_VERSION, nextVersionManagementController);
    }

    @Override
    public Object get(final ConfiguredObject<?> root,
                      final String category,
                      final List<String> path,
                      final Map<String, List<String>> parameters) throws ManagementException
    {
        Object result = super.get(root, category, path, convertQueryParameters(parameters));
        if (result instanceof LegacyConfiguredObject)
        {
            return Collections.singletonList(result);
        }
        return result;
    }


    @Override
    public Map<String, List<String>> convertQueryParameters(final Map<String, List<String>> requestParameters)
    {
        Map<String, List<String>> params = requestParameters
                .entrySet()
                .stream()
                .filter(e -> !INCLUDE_SYS_CONTEXT_PARAM.equals(e.getKey())
                             && !INHERITED_ACTUALS_PARAM.equals(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, List<String>> parameters = new HashMap<>(params);
        boolean excludeInheritedContext = isInheritedContextExcluded(params);
        parameters.put(EXCLUDE_INHERITED_CONTEXT_PARAM,
                       Collections.singletonList(String.valueOf(excludeInheritedContext)));
        if (!parameters.containsKey(DEPTH_PARAM))
        {
            parameters.put(DEPTH_PARAM, Collections.singletonList("1"));
        }
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
        final boolean excludeInheritedContext = isInheritedContextExcluded(parameters);

        if (content instanceof LegacyConfiguredObject)
        {
            LegacyConfiguredObject legacyConfiguredObjectObject = (LegacyConfiguredObject) content;
            return convertManageableToMap(
                    legacyConfiguredObjectObject,
                    depth,
                    actuals,
                    oversizeThreshold,
                    excludeInheritedContext);
        }
        else if (content instanceof Collection)
        {
            return ((Collection<?>) content).stream()
                                            .filter(o -> o instanceof LegacyConfiguredObject)
                                            .map(LegacyConfiguredObject.class::cast)
                                            .map(o -> convertManageableToMap(
                                                    o,
                                                    depth,
                                                    actuals,
                                                    oversizeThreshold,
                                                    excludeInheritedContext))
                                            .collect(Collectors.toSet());
        }
        return content;
    }


    private Map<String, Object> convertManageableToMap(final LegacyConfiguredObject legacyConfiguredObjectObject,
                                                       final int depth,
                                                       final boolean actuals,
                                                       final int oversizeThreshold,
                                                       final boolean excludeInheritedContext)
    {
        return convertObject(legacyConfiguredObjectObject,
                             depth,
                             actuals,
                             oversizeThreshold,
                             true,
                             excludeInheritedContext);
    }


    private boolean isInheritedContextExcluded(final Map<String, List<String>> params)
    {
        boolean excludeInheritedContext;
        boolean actuals = Boolean.parseBoolean(ConverterHelper.getParameter(ACTUALS_PARAM, params));
        String includeSystemContextParameter = ConverterHelper.getParameter(INCLUDE_SYS_CONTEXT_PARAM, params);
        String inheritedActualsParameter = ConverterHelper.getParameter(INHERITED_ACTUALS_PARAM, params);
        String excludeInheritedContextParameter =
                ConverterHelper.getParameter(EXCLUDE_INHERITED_CONTEXT_PARAM, params);
        if (excludeInheritedContextParameter == null)
        {
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
                excludeInheritedContext = actuals && !Boolean.parseBoolean(inheritedActualsParameter);
            }
            else
            {
                excludeInheritedContext = actuals || !Boolean.parseBoolean(includeSystemContextParameter);
            }
        }
        else
        {
            if (inheritedActualsParameter != null || includeSystemContextParameter != null)
            {
                throw createUnprocessableManagementException(String.format(
                        "Parameter '%s' cannot be specified together with '%s' or '%s'",
                        EXCLUDE_INHERITED_CONTEXT_PARAM,
                        INHERITED_ACTUALS_PARAM,
                        INCLUDE_SYS_CONTEXT_PARAM));
            }
            excludeInheritedContext = Boolean.parseBoolean(excludeInheritedContextParameter);
        }
        return excludeInheritedContext;
    }
}
