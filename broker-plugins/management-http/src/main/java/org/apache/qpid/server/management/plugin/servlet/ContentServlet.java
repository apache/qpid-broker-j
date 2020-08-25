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
 *
 */
package org.apache.qpid.server.management.plugin.servlet;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.management.plugin.servlet.rest.AbstractServlet;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFinder;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.plugin.ContentFactory;

public class ContentServlet extends AbstractServlet
{
    private static final long serialVersionUID = 1L;
    private final ContentFactory _contentFactory;

    public ContentServlet(final ContentFactory contentFactory)
    {
        super();
        _contentFactory = contentFactory;
    }

    @Override
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response,
                      final ConfiguredObject<?> managedObject) throws IOException
    {

        ConfiguredObject root = managedObject;
        String pathInfo = request.getPathInfo();
        if (managedObject instanceof Broker && null != pathInfo && !pathInfo.isEmpty())
        {
            final ConfiguredObjectFinder finder = getConfiguredObjectFinder(managedObject);
            final ConfiguredObject virtualHost = finder.findObjectFromPath(pathInfo.substring(1), VirtualHost.class);
            if (null == virtualHost)
            {
                sendError(response, HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            else
            {
                root = virtualHost;
            }
        }
        else if (managedObject instanceof VirtualHost && null != pathInfo && !pathInfo.isEmpty())
        {
            sendError(response, HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        final Map<String, String[]> parameters = request.getParameterMap();
        Content content = _contentFactory.createContent(root, parameters);
        try
        {
            writeContent(content, request, response);
        }
        finally
        {
            content.release();
        }
    }

    @Override
    protected void doPost(final HttpServletRequest req,
                          final HttpServletResponse resp,
                          final ConfiguredObject<?> managedObject)
            throws IOException
    {
        doGet(req, resp, managedObject);
    }

}
