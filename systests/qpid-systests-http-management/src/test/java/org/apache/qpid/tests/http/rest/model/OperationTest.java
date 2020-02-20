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
package org.apache.qpid.tests.http.rest.model;

import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.qpid.server.management.plugin.servlet.rest.AbstractServlet.SC_UNPROCESSABLE_ENTITY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Test;

import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;
import org.apache.qpid.tests.http.HttpTestHelper;

@HttpRequestConfig
public class OperationTest extends HttpTestBase
{
    // TODO multipart posts

    private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE_REF = new TypeReference<LinkedHashMap<String, Object>>()
    {
    };

    @Test
    public void invokeNoParameters() throws Exception
    {
        Map<String, Object> response = getHelper().postJson("virtualhost/getStatistics",
                                                            Collections.emptyMap(),
                                                            MAP_TYPE_REF, SC_OK);
        assertThat(response.size(), is(greaterThan(1)));
    }

    @Test
    public void invokeWithParameters() throws Exception
    {
        Map<Object, Object> params = Collections.singletonMap("statistics",
                                                              Collections.singletonList("connectionCount"));

        Map<String, Object> response = getHelper().postJson("virtualhost/getStatistics",
                                                            params,
                                                            MAP_TYPE_REF, SC_OK);
        assertThat(response.size(), is(equalTo(1)));
    }

    @Test
    public void invokeGetWithParameters() throws Exception
    {
        Map<String, Object> response = getHelper().getJson("virtualhost/getStatistics?statistics=bytesIn&statistics=bytesOut",
                                                            MAP_TYPE_REF, SC_OK);
        assertThat(response.size(), is(equalTo(2)));
    }

    @Test
    public void invalidParameter() throws Exception
    {
        Map<String, Object> params = Collections.singletonMap("unknown", Collections.emptyMap());

        getHelper().submitRequest("virtualhost/getStatistics", "POST", params, SC_UNPROCESSABLE_ENTITY);
    }

    @Test
    public void operationNotFound() throws Exception
    {
        getHelper().submitRequest("virtualhost/notfound", "POST", Collections.emptyMap(), SC_NOT_FOUND);
    }

    @Test
    public void invokeOperationReturningVoid() throws Exception
    {
        final HttpTestHelper brokerHelper = new HttpTestHelper(getBrokerAdmin());
        final Void response = brokerHelper.postJson("broker/performGC",
                                                    Collections.emptyMap(),
                                                    new TypeReference<Void>()
                                                    {
                                                    },
                                                    SC_OK);
        assertThat(response, is(nullValue()));
    }

    @Test
    public void invokeOperationForUnknownCategory() throws Exception
    {

        try
        {
            getHelper().postJson("broker/performGC",
                                 Collections.emptyMap(),
                                 new TypeReference<Void>()
                                 {
                                 },
                                 SC_NOT_FOUND);
            fail("The request is executed against root object VirtualHost. Thus, any broker request should fail.");
        }
        catch (FileNotFoundException e)
        {
            //pass
        }

    }

    @Test
    public void invokeOperationWithReservedParameter() throws Exception
    {
        final HttpTestHelper brokerHelper = new HttpTestHelper(getBrokerAdmin());
        final byte[] response = brokerHelper.getBytes(
                "broker/getThreadStackTraces?contentDispositionAttachmentFilename=stack-traces.txt&appendToLog=false");
        assertThat(response, is(notNullValue()));
        assertThat(new String(response, UTF_8).contains("Full thread dump captured"), is(equalTo(true)));

    }
}
