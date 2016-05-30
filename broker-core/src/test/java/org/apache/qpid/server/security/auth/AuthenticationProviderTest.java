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
package org.apache.qpid.server.security.auth;


import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.security.auth.manager.*;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProvider;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2AuthenticationProviderImplFactory;
import org.apache.qpid.server.security.auth.manager.oauth2.OAuth2IdentityResolverService;
import org.apache.qpid.server.util.BrokerTestHelper;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestFileUtils;

public class AuthenticationProviderTest extends QpidTestCase
{
    private Broker _broker;
    private File _testFile;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        _broker = BrokerTestHelper.createBrokerMock();
        _testFile = TestFileUtils.createTempFile(this);
    }

    @Override
    public void tearDown() throws Exception
    {
        try
        {
            if (_testFile != null)
            {
                _testFile.delete();
            }
        }
        finally
        {
            super.tearDown();
        }
    }

    public void testAuthenticateFinalChallenge() throws SaslException
    {
        Map<String, Object> attributes = Collections.<String, Object>singletonMap("name", "test");
        PlainAuthenticationProvider plain = new PlainAuthenticationProviderFactory()
                .create(_broker.getObjectFactory(), attributes, _broker);
        MD5AuthenticationProvider md5 = new MD5AuthenticationProviderFactory()
                .create(_broker.getObjectFactory(), attributes, _broker);
        ScramSHA256AuthenticationManager scramSha256 = new ScramSHA256AuthenticationManagerFactory()
                .create(_broker.getObjectFactory(), attributes, _broker);
        ScramSHA1AuthenticationManager scramSha1 = new ScramSHA1AuthenticationManagerFactory()
                .create(_broker.getObjectFactory(), attributes, _broker);
        SimpleAuthenticationManager simple = new SimpleAuthenticationManager(attributes, _broker);

        KerberosAuthenticationManager kerberos = new KerberosAuthenticationManagerFactory()
                .create(_broker.getObjectFactory(), attributes, _broker);

        final Map<String, Object> fileBasedProviderAttributes = new HashMap<>(attributes);
        fileBasedProviderAttributes.put("path", _testFile.getAbsolutePath());
        PlainPasswordDatabaseAuthenticationManager plainPasswordFile =
                new PlainPasswordDatabaseAuthenticationManagerFactory()
                        .create(_broker.getObjectFactory(), fileBasedProviderAttributes, _broker);
        Base64MD5PasswordDatabaseAuthenticationManager bas64Md5 =
                new Base64MD5PasswordDatabaseAuthenticationManagerFactory()
                        .create(_broker.getObjectFactory(), fileBasedProviderAttributes, _broker);

        // Oauth2 and Ldap auth providers need special services to be pre-configured

        List<? extends AuthenticationProvider<?>> testAuthenticationProviders =
                Arrays.asList(plain, md5, scramSha256, scramSha1, simple, kerberos, plainPasswordFile, bas64Md5);
        for (AuthenticationProvider<?> provider : testAuthenticationProviders)
        {
            performTestAuthenticateFinalChallenge(provider);
        }
    }

    private void performTestAuthenticateFinalChallenge(AuthenticationProvider authenticationProvider)
            throws SaslException
    {
        TestSaslServer saslServer = new TestSaslServer();

        AuthenticationResult result = authenticationProvider.authenticate(saslServer, new byte[1]);
        assertEquals("Unexpected authentication status " + authenticationProvider,
                     AuthenticationResult.AuthenticationStatus.CONTINUE,
                     result.getStatus());
        assertTrue("Unexpected challenge " + authenticationProvider, Arrays.equals(new byte[1], result.getChallenge()));

        result = authenticationProvider.authenticate(saslServer, new byte[1]);
        assertEquals("Unexpected authentication status for " + authenticationProvider,
                     AuthenticationResult.AuthenticationStatus.SUCCESS,
                     result.getStatus());
        assertNull("Unexpected challenge " + authenticationProvider, result.getChallenge());
    }


    private class TestSaslServer implements SaslServer
    {

        private boolean _complete;

        @Override
        public String getMechanismName()
        {
            return null;
        }

        @Override
        public byte[] evaluateResponse(final byte[] response) throws SaslException
        {
            if (_complete)
            {
                throw new IllegalStateException();
            }
            _complete = true;
            return  new byte[1];
        }

        @Override
        public boolean isComplete()
        {
            return _complete;
        }

        @Override
        public String getAuthorizationID()
        {
            return _complete ? "testPrincipal" : null;
        }

        @Override
        public byte[] unwrap(final byte[] incoming, final int offset, final int len) throws SaslException
        {
            return new byte[0];
        }

        @Override
        public byte[] wrap(final byte[] outgoing, final int offset, final int len) throws SaslException
        {
            return new byte[0];
        }

        @Override
        public Object getNegotiatedProperty(final String propName)
        {
            return null;
        }

        @Override
        public void dispose() throws SaslException
        {

        }
    }

}
