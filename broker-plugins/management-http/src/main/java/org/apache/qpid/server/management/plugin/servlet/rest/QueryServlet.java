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
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.filter.SelectorParsingException;
import org.apache.qpid.server.management.plugin.csv.CSVFormat;
import org.apache.qpid.server.management.plugin.servlet.query.ConfiguredObjectQuery;
import org.apache.qpid.server.management.plugin.servlet.query.EvaluationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;

public abstract class QueryServlet<X extends ConfiguredObject<?>> extends AbstractServlet
{
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryServlet.class);

    private static final CSVFormat CSV_FORMAT = new CSVFormat();

    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response,
                         final ConfiguredObject<?> managedObject)
            throws IOException, ServletException
    {
        performQuery(request, response, managedObject);
    }


    @Override
    protected void doPost(HttpServletRequest request,
                          HttpServletResponse response,
                          final ConfiguredObject<?> managedObject)
            throws IOException, ServletException
    {
        performQuery(request, response, managedObject);
    }

    private void performQuery(final HttpServletRequest request,
                              final HttpServletResponse response,
                              final ConfiguredObject<?> managedObject)
            throws IOException, ServletException
    {
        String categoryName;
        X parent = getParent(request, managedObject);
        if (parent != null && ((categoryName = getRequestedCategory(request, managedObject)) != null))
        {
            Model model = parent.getModel();

            Class<? extends ConfiguredObject> category = getSupportedCategory(categoryName, model);

            if (category != null)
            {
                List<ConfiguredObject<?>> objects = getAllObjects(parent, category, request);

                try
                {
                    ConfiguredObjectQuery query = new ConfiguredObjectQuery(objects,
                                                                            request.getParameter("select"),
                                                                            request.getParameter("where"),
                                                                            request.getParameter("orderBy"),
                                                                            request.getParameter("limit"),
                                                                            request.getParameter("offset"));


                    String attachmentFilename = request.getParameter(CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM);
                    if (attachmentFilename != null)
                    {
                        setContentDispositionHeaderIfNecessary(response, attachmentFilename);
                    }

                    if ("csv".equalsIgnoreCase(request.getParameter("format")))
                    {
                        sendCsvResponse(query, response);
                    }
                    else
                    {
                        Map<String, Object> resultsObject = new LinkedHashMap<>();
                        resultsObject.put("headers", query.getHeaders());
                        resultsObject.put("results", query.getResults());
                        resultsObject.put("total", query.getTotalNumberOfRows());

                        sendJsonResponse(resultsObject, request, response);
                    }
                }
                catch (SelectorParsingException e)
                {
                    sendJsonErrorResponse(request,
                                          response,
                                          HttpServletResponse.SC_BAD_REQUEST,
                                          e.getMessage());
                }
                catch (EvaluationException e)
                {
                    sendJsonErrorResponse(request,
                                          response,
                                          SC_UNPROCESSABLE_ENTITY,
                                          e.getMessage());
                }
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

    private void sendCsvResponse(final ConfiguredObjectQuery query,
                                 final HttpServletResponse response)
            throws IOException
    {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/csv;charset=utf-8;");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        sendCachingHeadersOnResponse(response);
        try (PrintWriter writer = response.getWriter())
        {
            CSV_FORMAT.printRecord(writer, query.getHeaders());
            CSV_FORMAT.printRecords(writer, query.getResults());
        }
    }

    abstract protected X getParent(final HttpServletRequest request, final ConfiguredObject<?> managedObject);

    abstract protected Class<? extends ConfiguredObject> getSupportedCategory(final String categoryName,
                                                                   final Model brokerModel);

    abstract protected String getRequestedCategory(final HttpServletRequest request,
                                                   final ConfiguredObject<?> managedObject);

    abstract protected List<ConfiguredObject<?>> getAllObjects(final X parent,
                                                               final Class<? extends ConfiguredObject> category,
                                                               final HttpServletRequest request);

}
