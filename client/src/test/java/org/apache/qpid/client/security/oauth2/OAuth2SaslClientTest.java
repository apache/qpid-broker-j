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

package org.apache.qpid.client.security.oauth2;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.security.sasl.SaslException;

import org.junit.Assert;

import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.test.utils.QpidTestCase;

public class OAuth2SaslClientTest extends QpidTestCase
{
    private static final String URL_WITH_TOKEN = "amqp://:token@client/test?brokerlist='tcp://localhost:1'";
    private static final String URL_WITHOUT_TOKEN = "amqp://:@client/test?brokerlist='tcp://localhost:1'";
    private OAuth2SaslClient _client;

    public void testEvaluateChallenge_WithToken() throws Exception
    {
        OAuth2AccessTokenCallbackHandler handler = new OAuth2AccessTokenCallbackHandler();
        handler.initialise(new AMQConnectionURL(URL_WITH_TOKEN));

        _client = new OAuth2SaslClient(handler);
         byte[] actualChallenge = _client.evaluateChallenge(new byte[] {});

        byte[] expectedChallenge = "auth=Bearer token\1\1".getBytes();
        Assert.assertArrayEquals(expectedChallenge, actualChallenge);
    }

    public void testEvaluateChallenge_NoToken() throws Exception
    {
        OAuth2AccessTokenCallbackHandler handler = new OAuth2AccessTokenCallbackHandler();
        handler.initialise(new AMQConnectionURL(URL_WITHOUT_TOKEN));

        _client = new OAuth2SaslClient(handler);
        try
        {
            _client.evaluateChallenge(new byte[]{});
            fail("Exception not thrown");
        }
        catch (SaslException se)
        {
            // PASS
        }
    }

    public void testHasInitialResponse() throws Exception
    {
        OAuth2AccessTokenCallbackHandler handler = new OAuth2AccessTokenCallbackHandler();
        handler.initialise(new AMQConnectionURL(URL_WITH_TOKEN));

        _client = new OAuth2SaslClient(handler);
        assertTrue(_client.hasInitialResponse());
    }


}
