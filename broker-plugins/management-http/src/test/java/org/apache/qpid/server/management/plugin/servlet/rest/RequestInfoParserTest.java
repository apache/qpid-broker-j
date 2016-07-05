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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import javax.servlet.http.HttpServletRequest;

import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.test.utils.QpidTestCase;

public class RequestInfoParserTest extends QpidTestCase
{
    private HttpServletRequest _request = mock(HttpServletRequest.class);

    public void testGetNoHierarchy()
    {
        RequestInfoParser parser = new RequestInfoParser();

        configureRequest("GET", "servletPath", null);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestInfo.RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Collections.emptyList(), info.getModelParts());
    }

    public void testGetWithHierarchy()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        final String pathInfo = "/" + vhnName + "/" + vhName;

        configureRequest("GET", "servletPath", pathInfo);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestInfo.RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
    }

    public void testGetParent()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);
        final String vhnName = "testVHNName";
        configureRequest("GET", "servletPath", "/" + vhnName);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestInfo.RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName), info.getModelParts());
    }

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

    public void testGetOperation()
    {
        doOperationTest("GET");
    }

    public void testPostToParent()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        final String vhnName = "testVHNName";
        final String pathInfo = "/" + vhnName;

        configureRequest("POST", "servletPath", pathInfo);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestInfo.RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName), info.getModelParts());
    }


    public void testPostToObject()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        final String pathInfo = "/" + vhnName + "/" + vhName;

        configureRequest("POST", "servletPath", pathInfo);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestInfo.RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
    }

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

    public void testPostOperation()
    {
        doOperationTest("POST");
    }

    public void testPutToObject()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        final String pathInfo = "/" + vhnName + "/" + vhName;

        configureRequest("PUT", "servletPath", pathInfo);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestInfo.RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
    }

    public void testPutToParent()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class, Queue.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        configureRequest("PUT", "servletPath", "/" + vhnName + "/" + vhName);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestInfo.RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
    }

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

    public void testDeleteObject()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        final String pathInfo = "/" + vhnName + "/" + vhName;

        configureRequest("DELETE", "servletPath", pathInfo);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestInfo.RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
    }


    public void testDeleteParent()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class, Queue.class);

        final String vhnName = "testVHNName";
        final String vhName = "testVHName";
        configureRequest("DELETE", "servletPath", "/" + vhnName + "/" + vhName);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestInfo.RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName, vhName), info.getModelParts());
    }

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

    public void testParseWithURLEncodedName() throws UnsupportedEncodingException
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class);
        final String vhnName = "vhnName/With/slashes?and&other/stuff";
        final String encodedVHNName = URLEncoder.encode(vhnName, StandardCharsets.UTF_8.name());

        configureRequest("GET", "servletPath", "/" + encodedVHNName);

        RequestInfo info = parser.parse(_request);

        assertEquals("Unexpected request type", RequestInfo.RequestType.MODEL_OBJECT, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName), info.getModelParts());
    }

    public void testWildHierarchy()
    {
        RequestInfoParser parser = new RequestInfoParser(VirtualHostNode.class, VirtualHost.class);

        configureRequest("GET", "servletPath", "/*/*");
        assertTrue("Fully wildcard path should be wild", parser.parse(_request).isWild());

        configureRequest("GET", "servletPath", "/myvhn/*");
        assertTrue("Partially wildcarded path should be wild too", parser.parse(_request).isWild());

        configureRequest("GET", "servletPath", "/myvhn/myvh");
        assertFalse("Path with no wildcards should not be wild", parser.parse(_request).isWild());
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

        assertEquals("Unexpected request type", RequestInfo.RequestType.OPERATION, info.getType());
        assertEquals("Unexpected model parts", Arrays.asList(vhnName), info.getModelParts());
        assertEquals("Unexpected operation name", operationName, info.getOperationName());
    }
}
