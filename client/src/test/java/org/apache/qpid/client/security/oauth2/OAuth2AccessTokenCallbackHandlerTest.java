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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.PasswordCallback;

import org.junit.Assert;

import org.apache.qpid.client.AMQConnectionURL;
import org.apache.qpid.client.security.AMQCallbackHandler;
import org.apache.qpid.test.utils.QpidTestCase;

public class OAuth2AccessTokenCallbackHandlerTest extends QpidTestCase
{
    private static final String ACCESS_TOKEN = "accessToken";
    private AMQCallbackHandler _callbackHandler = new OAuth2AccessTokenCallbackHandler(); // Class under test

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        String url = String.format("amqp://:%s@client/test?brokerlist='tcp://localhost:1'", ACCESS_TOKEN);
        _callbackHandler.initialise(new AMQConnectionURL(url));
    }

    public void testAccessTokenCallback() throws Exception
    {
        PasswordCallback callback = new PasswordCallback("prompt", false);

        assertNull("Unexpected access token before test", callback.getPassword());
        _callbackHandler.handle(new Callback[] {callback});
        Assert.assertArrayEquals("Unexpected access token", ACCESS_TOKEN.toCharArray(), callback.getPassword());
    }
}
