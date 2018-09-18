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
package org.apache.qpid.server.security.auth.manager.oauth2;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

class OAuth2MockEndpoint
{
    private HttpServletResponse _servletResponse;
    private Map<String, String> _expectedParameters = new HashMap<>();
    private String _expectedMethod;
    private String _responseString;
    private int _responseCode = 200;
    private String _redirectUrlString;
    private boolean _needsAuth;

    public void handleRequest(HttpServletRequest request, HttpServletResponse response) throws IOException
    {
        _servletResponse = response;
        response.setContentType("application/json");
        if (_needsAuth)
        {
            String expected = "Basic " + Base64.getEncoder().encodeToString((OAuth2AuthenticationProviderImplTest.TEST_CLIENT_ID + ":" + OAuth2AuthenticationProviderImplTest.TEST_CLIENT_SECRET).getBytes(
                    OAuth2AuthenticationProviderImplTest.UTF8));
            doAssertEquals("Authorization required",
                           expected,
                           request.getHeader("Authorization"));
        }
        if (_expectedMethod != null)
        {
            doAssertEquals("Request uses unexpected HTTP method", _expectedMethod, request.getMethod());
        }
        if (_expectedParameters != null)
        {
            Map<String, String[]> parameters = request.getParameterMap();
            for (String expectedParameter : _expectedParameters.keySet())
            {
                doAssertTrue(String.format("Request is missing parameter '%s'", expectedParameter),
                             parameters.containsKey(expectedParameter));
                String[] parameterValues = parameters.get(expectedParameter);
                doAssertEquals(String.format("Request has parameter '%s' specified more than once", expectedParameter),
                               1, parameterValues.length);
                doAssertEquals(String.format("Request parameter '%s' has unexpected value", expectedParameter),
                               _expectedParameters.get(expectedParameter), parameterValues[0]);
            }
        }
        if (_redirectUrlString != null)
        {
            response.sendRedirect(_redirectUrlString);
        }
        else
        {
            if (_responseCode != 0)
            {
                response.setStatus(_responseCode);
            }
            response.getOutputStream().write(_responseString.getBytes(OAuth2AuthenticationProviderImplTest.UTF8));
        }
    }

    public void putExpectedParameter(String key, String value)
    {
        _expectedParameters.put(key, value);
    }

    public void setExpectedMethod(final String expectedMethod)
    {
        _expectedMethod = expectedMethod;
    }

    public void setResponseString(final String responseString)
    {
        _responseString = responseString;
    }

    public void setResponseCode(final int responseCode)
    {
        _responseCode = responseCode;
    }

    public void setResponse(int code, String message)
    {
        setResponseCode(code);
        setResponseString(message);
    }

    public void setRedirectUrlString(final String redirectUrlString)
    {
        _redirectUrlString = redirectUrlString;
    }

    public void setNeedsAuth(final boolean needsAuth)
    {
        this._needsAuth = needsAuth;
    }

    private void doAssertEquals(String msg, Object expected, Object actual) throws IOException
    {
        if ((expected == null && actual != null) || (expected != null && !expected.equals(actual)))
        {
            sendError(String.format("%s; Expected: '%s'; Actual: '%s'", msg, expected, actual));
        }
    }

    private void doAssertTrue(String msg, boolean condition) throws IOException
    {
        if (!condition)
        {
            sendError(msg);
        }
    }

    private void sendError(String errorDescription) throws IOException
    {
        _servletResponse.setStatus(500);
        String responseString = String.format("{\"error\":\"test_failure\","
                                              + "\"error_description\":\"%s\"}", errorDescription);
        _servletResponse.getOutputStream().write(responseString.getBytes());
        throw new AssertionError(responseString);
    }
}
