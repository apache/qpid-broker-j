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

package org.apache.qpid.tests.http.query;


import javax.servlet.http.HttpServletResponse;

import org.junit.Test;

import org.apache.qpid.server.management.plugin.servlet.rest.AbstractServlet;
import org.apache.qpid.tests.http.HttpTestBase;

public class QueryBrokerTest extends HttpTestBase
{
    @Test
    public void testInvalidOrderBy() throws Exception
    {
        getHelper().submitRequest("querybroker/port?select=id&orderBy=0", "GET", AbstractServlet.SC_UNPROCESSABLE_ENTITY);
    }

    @Test
    public void testInvalidSelectSyntax() throws Exception
    {
        getHelper().submitRequest("querybroker/port?select=,,(", "GET", HttpServletResponse.SC_BAD_REQUEST);
    }

    @Test
    public void testInvalidWhereSyntax() throws Exception
    {
        getHelper().submitRequest("querybroker/port?where=id in(", "GET", HttpServletResponse.SC_BAD_REQUEST);
    }

    @Test
    public void testInvalidWhere() throws Exception
    {
        getHelper().submitRequest("querybroker/port?where=transports>1", "GET", AbstractServlet.SC_UNPROCESSABLE_ENTITY);
    }
}
