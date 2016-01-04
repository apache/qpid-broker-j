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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.filter.BooleanExpression;
import org.apache.qpid.filter.Expression;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.servlet.query.ConfiguredObjectExpressionFactory;
import org.apache.qpid.server.management.plugin.servlet.query.ConfiguredObjectFilterParser;
import org.apache.qpid.server.management.plugin.servlet.query.ParseException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;

public abstract class QueryServlet<X extends ConfiguredObject<?>> extends AbstractServlet
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryServlet.class);
    private static final String[] STANDARD_FIELDS = new String[]{ConfiguredObject.ID, ConfiguredObject.NAME};

    @Override
    protected void doGetWithSubjectAndActor(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException
    {
        performQuery(request, response);
    }


    @Override
    protected void doPostWithSubjectAndActor(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException
    {
        performQuery(request, response);
    }

    private void performQuery(final HttpServletRequest request, final HttpServletResponse response)
            throws IOException, ServletException
    {
        String categoryName;
        X parent = getParent(request);
        if( parent != null && ((categoryName = getRequestedCategory(request)) != null))
        {
            Model model = parent.getModel();
            ConfiguredObjectExpressionFactory expressionFactory = new ConfiguredObjectExpressionFactory();

            Class<? extends ConfiguredObject> category = getSupportedCategory(categoryName, model);

            if (category != null)
            {
                List<ConfiguredObject<?>> objects = getAllObjects(parent, category, request);
                Map<String, List<?>> resultsObject = new LinkedHashMap<>();
                List<String> headers = new ArrayList<>();
                List<Expression<ConfiguredObject<?>>> valueEvaluators = new ArrayList<>();

                if (request.getParameterMap().containsKey("select"))
                {
                    ConfiguredObjectFilterParser parser = new ConfiguredObjectFilterParser();
                    parser.setConfiguredObjectExpressionFactory(expressionFactory);
                    String selectClause = request.getParameter("select");

                    try
                    {
                        final List<Map<String, Expression>> expressions =
                                parser.parseSelect(selectClause);
                        for (Map<String, Expression> expression : expressions)
                        {
                            final Map.Entry<String, Expression> entry = expression.entrySet().iterator().next();
                            headers.add(entry.getKey());
                            valueEvaluators.add(entry.getValue());
                        }
                    }
                    catch (ParseException e)
                    {
                        sendJsonErrorResponse                               (request,
                                                                             response,
                                                                             HttpServletResponse.SC_NOT_FOUND,
                                                                             "Unable to parse where clause");
                        return;
                    }
                }
                else
                {
                    for (String field : STANDARD_FIELDS)
                    {
                        headers.add(field);
                        valueEvaluators.add(expressionFactory.createConfiguredObjectExpression(field));
                    }
                }

                if (request.getParameterMap().containsKey("where"))
                {
                    String whereClause = request.getParameter("where");

                    ConfiguredObjectFilterParser parser = new ConfiguredObjectFilterParser();
                    parser.setConfiguredObjectExpressionFactory(expressionFactory);
                    parser.allowNonPropertyInExpressions(true);
                    try
                    {
                        final BooleanExpression<ConfiguredObject> expression =
                                parser.parseWhere(whereClause);

                        List<ConfiguredObject<?>> filteredObjects = new ArrayList<>();

                        for (ConfiguredObject<?> object : objects)
                        {
                            try
                            {
                                if (expression.matches(object))
                                {
                                    filteredObjects.add(object);
                                }
                            }
                            catch (RuntimeException e)
                            {
                                LOGGER.debug("Error while evaluating object against where clause", e);
                            }
                        }

                        objects = filteredObjects;
                    }
                    catch (ParseException e)
                    {
                        sendJsonErrorResponse                               (request,
                                                                             response,
                                                                             HttpServletResponse.SC_NOT_FOUND,
                                                                             "Unable to parse where clause");
                        return;
                    }

                }

                resultsObject.put("headers", headers);
                List<List<Object>> values = new ArrayList<>();
                for (ConfiguredObject<?> object : objects)
                {
                    List<Object> objectVals = new ArrayList<>();
                    for (Expression<ConfiguredObject<?>> evaluator : valueEvaluators)
                    {
                        Object value;
                        try
                        {
                            value = evaluator.evaluate(object);
                        }
                        catch (RuntimeException e)
                        {
                            LOGGER.debug("Error while evaluating select clause", e);
                            value = null;
                        }
                        objectVals.add(value);
                    }
                    values.add(objectVals);
                }
                resultsObject.put("results", values);
                sendJsonResponse(resultsObject, request, response);
            }

            else
            {
                sendJsonErrorResponse(request,
                                      response,
                                      HttpServletResponse.SC_NOT_FOUND,
                                      "Unknown object type " + categoryName);
            }

        }
        else
        {
            sendJsonErrorResponse(request, response, HttpServletResponse.SC_NOT_FOUND, "Invalid path");
        }
    }

    abstract protected X getParent(final HttpServletRequest request);

    abstract protected Class<? extends ConfiguredObject> getSupportedCategory(final String categoryName,
                                                                   final Model brokerModel);

    abstract protected String getRequestedCategory(final HttpServletRequest request);

    abstract protected List<ConfiguredObject<?>> getAllObjects(final X parent,
                                                               final Class<? extends ConfiguredObject> category,
                                                               final HttpServletRequest request);
}
