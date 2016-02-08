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

import javax.security.sasl.SaslException;

import org.apache.qpid.test.utils.QpidTestCase;

public class OAuth2SaslServerTest extends QpidTestCase
{

    private OAuth2SaslServer _server = new OAuth2SaslServer();

    public void testEvaluateResponse_ResponseHasAuthOnly() throws Exception
    {
        assertFalse(_server.isComplete());
        _server.evaluateResponse("auth=Bearer token\1\1".getBytes());
        assertTrue(_server.isComplete());
        assertEquals("token", _server.getNegotiatedProperty(OAuth2SaslServer.ACCESS_TOKEN_PROPERTY));
    }

    public void testEvaluateResponse_ResponseAuthAndOthers() throws Exception
    {
        _server.evaluateResponse("user=xxx\1auth=Bearer token\1host=localhost\1\1".getBytes());
        assertEquals("token", _server.getNegotiatedProperty(OAuth2SaslServer.ACCESS_TOKEN_PROPERTY));
    }

    public void testEvaluateResponse_ResponseAuthAbsent() throws Exception
    {
        try
        {
            _server.evaluateResponse("host=localhost\1\1".getBytes());
            fail("Exception not thrown");
        }
        catch (SaslException se)
        {
            // PASS
        }
        assertFalse(_server.isComplete());
    }

    public void testEvaluateResponse_ResponseAuthMalformed() throws Exception
    {
        try
        {
            _server.evaluateResponse("auth=wibble\1\1".getBytes());
            fail("Exception not thrown");
        }
        catch (SaslException se)
        {
            // PASS
        }
        assertFalse(_server.isComplete());
    }

    public void testEvaluateResponse_PrematureGetNegotiatedProperty() throws Exception
    {
        try
        {
            _server.getNegotiatedProperty(OAuth2SaslServer.ACCESS_TOKEN_PROPERTY);
        }
        catch (IllegalStateException ise)
        {
            // PASS
        }

        _server.evaluateResponse("auth=Bearer token\1\1".getBytes());
        assertEquals("token", _server.getNegotiatedProperty(OAuth2SaslServer.ACCESS_TOKEN_PROPERTY));
    }

}
