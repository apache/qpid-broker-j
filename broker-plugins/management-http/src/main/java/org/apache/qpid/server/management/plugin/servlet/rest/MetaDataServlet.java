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
package org.apache.qpid.server.management.plugin.servlet.rest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectAttribute;
import org.apache.qpid.server.model.ConfiguredObjectFinder;
import org.apache.qpid.server.model.ConfiguredObjectOperation;
import org.apache.qpid.server.model.ConfiguredObjectStatistic;
import org.apache.qpid.server.model.ConfiguredObjectTypeRegistry;
import org.apache.qpid.server.model.ConfiguredSettableAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.OperationParameter;

public class MetaDataServlet extends AbstractServlet
{
    private static final long serialVersionUID = 1L;


    public MetaDataServlet()
    {
    }

    @Override
    public void init() throws ServletException
    {
        super.init();
    }

    @Override
    protected void doGet(final HttpServletRequest request,
                         final HttpServletResponse response,
                         final ConfiguredObject<?> managedObject)
            throws ServletException, IOException
    {
        response.setContentType("application/json");
        sendCachingHeadersOnResponse(response);
        response.setStatus(HttpServletResponse.SC_OK);
        ConfiguredObjectFinder finder = getConfiguredObjectFinder(managedObject);


        Map<String, Map> classToDataMap = new TreeMap<>();

        for (Class<? extends ConfiguredObject> clazz : finder.getManagedCategories())
        {
            classToDataMap.put(clazz.getSimpleName(), processCategory(clazz, managedObject.getModel()));
        }

        final OutputStream stream = getOutputStream(request, response);
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.writeValue(stream, classToDataMap);

        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

    }

    private Map<String, Map> processCategory(final Class<? extends ConfiguredObject> clazz, final Model model)
    {
        Map<String, Map> typeToDataMap = new TreeMap<>();
        ConfiguredObjectTypeRegistry typeRegistry = model.getTypeRegistry();
        for (Class<? extends ConfiguredObject> type : typeRegistry.getTypeSpecialisations(clazz))
        {
            typeToDataMap.put(ConfiguredObjectTypeRegistry.getType(type), processType(type, model));
        }
        return typeToDataMap;
    }

    private Map<String, Object> processType(final Class<? extends ConfiguredObject> type, final Model model)
    {
        Map<String, Object> typeDetails = new LinkedHashMap<>();
        typeDetails.put("attributes", processAttributes(type, model));
        typeDetails.put("statistics", processStatistics(type, model));

        typeDetails.put("operations", processOperations(type, model));
        typeDetails.put("managedInterfaces", getManagedInterfaces(type, model));
        typeDetails.put("validChildTypes", getValidChildTypes(type, model));
        typeDetails.put("contextDependencies", getContextDependencies(type, model));
        ManagedObject annotation = type.getAnnotation(ManagedObject.class);
        if (annotation != null)
        {
            if (annotation.deprecated())
            {
                typeDetails.put("deprecated", true);
            }
            if (!"".equals(annotation.description()))
            {
                typeDetails.put("description", annotation.description());
            }
        }
        return typeDetails;
    }

    private Map<String, String> getContextDependencies(final Class<? extends ConfiguredObject> type, final Model model)
    {
        final Collection<ManagedContextDefault> contextDependencies =
                model.getTypeRegistry().getContextDependencies(type);
        Map<String,String> result = new TreeMap<>();

        if(contextDependencies != null)
        {
            for(ManagedContextDefault contextDefault : contextDependencies)
            {
                result.put(contextDefault.name(), contextDefault.description());
            }
        }
        return result;
    }

    private Map<String, Collection<String>> getValidChildTypes(final Class<? extends ConfiguredObject> type,
                                                               final Model model)
    {
        Map<String, Collection<String>> validChildTypes = new HashMap<>();
        for (Class<? extends ConfiguredObject> childType : model.getChildTypes(ConfiguredObjectTypeRegistry.getCategory(
                type)))
        {
            Collection<String> validValues = model.getTypeRegistry().getValidChildTypes(type, childType);
            if (validValues != null)
            {
                validChildTypes.put(childType.getSimpleName(), validValues);
            }
        }
        return validChildTypes;
    }

    private Set<String> getManagedInterfaces(Class<? extends ConfiguredObject> type, final Model model)
    {
        Set<String> interfaces = new HashSet<>();
        for (Class<?> classObject : model.getTypeRegistry().getManagedInterfaces(type))
        {
            interfaces.add(classObject.getSimpleName());
        }
        return interfaces;
    }

    private Map<String, Map> processAttributes(final Class<? extends ConfiguredObject> type, final Model model)
    {
        Collection<ConfiguredObjectAttribute<?, ?>> attributes =
                model.getTypeRegistry().getAttributeTypes(type).values();

        Map<String, Map> attributeDetails = new LinkedHashMap<>();
        for (ConfiguredObjectAttribute<?, ?> attribute : attributes)
        {
            Map<String, Object> attrDetails = new LinkedHashMap<>();
            attrDetails.put("type", attribute.getType().getSimpleName());
            if (!"".equals(attribute.getDescription()))
            {
                attrDetails.put("description", attribute.getDescription());
            }
            if (attribute.isDerived())
            {
                attrDetails.put("derived", attribute.isDerived());
            }
            if (!attribute.isDerived())
            {
                ConfiguredSettableAttribute automatedAttribute = (ConfiguredSettableAttribute) attribute;
                if (!"".equals(automatedAttribute.defaultValue()))
                {
                    attrDetails.put("defaultValue", automatedAttribute.defaultValue());
                }
                if (automatedAttribute.isMandatory())
                {
                    attrDetails.put("mandatory", automatedAttribute.isMandatory());
                }
                if (automatedAttribute.isImmutable())
                {
                    attrDetails.put("immutable", automatedAttribute.isImmutable());
                }
                if (!(automatedAttribute.validValues()).isEmpty())
                {
                    Collection<String> validValues = ((ConfiguredSettableAttribute<?, ?>) attribute).validValues();

                    Collection<Object> convertedValues = new ArrayList<>(validValues.size());
                    for (String value : validValues)
                    {
                        convertedValues.add(((ConfiguredSettableAttribute<?, ?>) attribute).convert(value, null));
                    }
                    attrDetails.put("validValues", convertedValues);
                }
                else if(!"".equals(automatedAttribute.validValuePattern()))
                {
                    attrDetails.put("validValuesPattern", automatedAttribute.validValuePattern());
                }

            }
            if (attribute.isSecure())
            {
                attrDetails.put("secure", attribute.isSecure());
            }
            if (attribute.isOversized())
            {
                attrDetails.put("oversize", attribute.isOversized());
            }
            attributeDetails.put(attribute.getName(), attrDetails);
        }
        return attributeDetails;
    }

    private Map<String, Map> processOperations(final Class<? extends ConfiguredObject> type, final Model model)
    {
        Collection<ConfiguredObjectOperation<?>> operations =
                model.getTypeRegistry().getOperations(type).values();

        Map<String, Map> attributeDetails = new LinkedHashMap<>();
        for (ConfiguredObjectOperation<?> operation : operations)
        {
            Map<String, Object> attrDetails = new LinkedHashMap<>();
            attrDetails.put("name", operation.getName());
            attrDetails.put("returnType", operation.getReturnType().getSimpleName());
            if (!"".equals(operation.getDescription()))
            {
                attrDetails.put("description", operation.getDescription());
            }

            List<OperationParameter> parameters = operation.getParameters();
            if (!parameters.isEmpty())
            {
                Map<String, Map> paramDetails = new LinkedHashMap<>();
                for (OperationParameter param : parameters)
                {
                    Map<String, Object> paramAttrs = new LinkedHashMap<>();

                    paramAttrs.put("type", param.getType().getSimpleName());
                    paramAttrs.put("mandatory", param.isMandatory());
                    if (!"".equals(param.getDefaultValue()))
                    {
                        paramAttrs.put("defaultValue", param.getDefaultValue());
                    }

                    paramDetails.put(param.getName(), paramAttrs);
                }
                attrDetails.put("parameters", paramDetails);
            }

            attributeDetails.put(operation.getName(), attrDetails);
        }
        return attributeDetails;
    }


    private Map<String, Map> processStatistics(final Class<? extends ConfiguredObject> type, final Model model)
    {
        Collection<ConfiguredObjectStatistic<?, ?>> statistics = model.getTypeRegistry().getStatistics(type);

        Map<String, Map> allStatisticsDetails = new LinkedHashMap<>();
        for (ConfiguredObjectStatistic<?, ?> statistic : statistics)
        {
            Map<String, Object> stat = new LinkedHashMap<>();
            stat.put("name", statistic.getName());
            stat.put("type", statistic.getType().getSimpleName());
            if (!"".equals(statistic.getDescription()))
            {
                stat.put("description", statistic.getDescription());
            }
            if (!"".equals(statistic.getLabel()))
            {
                stat.put("label", statistic.getLabel());
            }

            stat.put("units", statistic.getUnits());
            stat.put("statisticType", statistic.getStatisticType().toString());
            allStatisticsDetails.put(statistic.getName(), stat);
        }
        return allStatisticsDetails;
    }
}
