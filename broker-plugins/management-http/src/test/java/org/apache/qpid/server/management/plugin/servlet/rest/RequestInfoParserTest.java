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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import javax.servlet.http.HttpServletRequest;

import org.junit.Test;

import org.apache.qpid.server.management.plugin.RequestType;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.test.utils.UnitTestBase;

public class RequestInfoParserTest extends UnitTestBase
{
    private HttpServletRequest _request = mock(HttpServletRequest.class);

    @Test
    public void testGetNoHierarchy()
    {
        RequestInfoParser parser = new RequestInfoParser();

        configureRequest("GET", "servletPath", null);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Collections.emptyList(), info.getModelParts());
    }

    @Test
    public void testGetWithHierarchy()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        final String pathInfo = "/" + vhnName + "/" + vhName;

        configureRequest("GET", "servletPath", pathInfo);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
        assertTrue("Expected exact object request", info.isSingletonRequest());
    }

    @Test
    public void testGetParent()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);
        final String vhnName = "testVHNName";
        configureRequest("GET", "servletPath", "/" + vhnName);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName), info.getModelParts());
        assertFalse("Expected exact object request", info.isSingletonRequest());
    }

    @Test
    public void testGetTooManyParts()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class);

        try
        {
            configureRequest("GET", "servletPath", "/testVHNName/testOp/invalidAdditionalPart");

            parser.parse(_request);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testGetOperation()
    {
        doOperationTest("GET");
    }

    @Test
    public void testPostToParent()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        final String vhnName = "testVHNName";
        final String pathInfo = "/" + vhnName;

        configureRequest("POST", "servletPath", pathInfo);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName), info.getModelParts());
    }


    @Test
    public void testPostToObject()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        final String pathInfo = "/" + vhnName + "/" + vhName;

        configureRequest("POST", "servletPath", pathInfo);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
    }

    @Test
    public void testPostTooFewParts()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class, Queue.class);

        try
        {
            configureRequest("POST", "servletPath", "/testVHNName");

            parser.parse(_request);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testPostTooManyParts()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class, Queue.class);

        try
        {
            configureRequest("POST", "servletPath", "/testVHNName/testVNName/testQueue/testOp/testUnknown");

            parser.parse(_request);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testPostOperation()
    {
        doOperationTest("POST");
    }

    @Test
    public void testPutToObject()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        final String pathInfo = "/" + vhnName + "/" + vhName;

        configureRequest("PUT", "servletPath", pathInfo);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
    }

    @Test
    public void testPutToParent()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class, Queue.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        configureRequest("PUT", "servletPath", "/" + vhnName + "/" + vhName);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
    }

    @Test
    public void testPutTooFewParts()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class, Queue.class);

        try
        {
            configureRequest("PUT", "servletPath", "/testVHNName");

            parser.parse(_request);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testPutTooManyParts()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class, Queue.class);

        try
        {
            configureRequest("PUT", "servletPath", "/testVHNName/testVNName/testQueue/testUnknown");

            parser.parse(_request);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testDeleteObject()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        final String pathInfo = "/" + vhnName + "/" + vhName;

        configureRequest("DELETE", "servletPath", pathInfo);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
    }


    @Test
    public void testDeleteParent()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class, Queue.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        configureRequest("DELETE", "servletPath", "/" + vhnName + "/" + vhName);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
    }

    @Test
    public void testDeleteTooManyParts()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class, Queue.class);

        try
        {
            configureRequest("DELETE", "servletPath", "/testVHNName/testVNName/testQueue/testUnknown");

            parser.parse(_request);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testParseWithURLEncodedName() throws UnsupportedEncodingException
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class);
        final String vhnName = "vhnName/With/slashes?and&other/stuff";
        final String encodedVHNName = URLEncoder.encode(vhnName, StandardCharsets.UTF_8.name());

        configureRequest("GET", "servletPath", "/" + encodedVHNName);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName), info.getModelParts());
    }

    @Test
    public void testWildHierarchy()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        configureRequest("GET", "servletPath", "/*/*");
        assertTrue("Fully wildcarded path should be wild", parser.parse(_request).hasWildcard());

        configureRequest("GET", "servletPath", "/myvhn/*");
        assertTrue("Partially wildcarded path should be wild too", parser.parse(_request).hasWildcard());

        configureRequest("GET", "servletPath", "/myvhn/myvh");
        assertFalse("Path with no wildcards should not be wild", parser.parse(_request).hasWildcard());
    }

    private void configureRequest(final String method,
                                  final String servletPath,
                                  final String pathInfo)
    {
        when(_request.getServletPath()).thenReturn(servletPath);
        when(_request.getPathInfo()).thenReturn(pathInfo);
        when(_request.getMethod()).thenReturn(method);
    }

    private void doOperationTest(final String method)
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class);
        final String vhnName = "testVHNName";
        final String operationName = "testOperation";
        final String pathInfo = "/" + vhnName + "/" + operationName;

        configureRequest(method, "servletPath", pathInfo);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestType.OPERATION, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName), info.getModelParts());
        assertEquals("Unexpected operation name", operationName, info.getOperationName());
        assertTrue("Expected exact object request", info.isSingletonRequest());
    }
}
