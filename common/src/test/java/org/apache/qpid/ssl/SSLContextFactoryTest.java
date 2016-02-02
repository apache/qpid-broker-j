/* Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.qpid.ssl;

import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestSSLConstants;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;

public class SSLContextFactoryTest extends QpidTestCase
{
    private static final String STORE_TYPE = "JKS";
    private static final String DEFAULT_KEY_MANAGER_ALGORITHM = KeyManagerFactory.getDefaultAlgorithm();
    private static final String DEFAULT_TRUST_MANAGER_ALGORITHM = TrustManagerFactory.getDefaultAlgorithm();

    public void testTrustStoreDoesNotExist() throws Exception
    {
        try
        {

            final TrustManager[] trustManagers;
            final KeyManager[] keyManagers;

            trustManagers =
                    SSLContextFactory.getTrustManagers("/path/to/nothing",
                                                       TestSSLConstants.TRUSTSTORE_PASSWORD,
                                                       STORE_TYPE,
                                                       DEFAULT_TRUST_MANAGER_ALGORITHM);

            keyManagers =
                    SSLContextFactory.getKeyManagers(TestSSLConstants.KEYSTORE,
                                                     TestSSLConstants.KEYSTORE_PASSWORD,
                                                     STORE_TYPE,
                                                     DEFAULT_KEY_MANAGER_ALGORITHM,
                                                     null);

            SSLContextFactory.buildClientContext(trustManagers, keyManagers);


            fail("Exception was not thrown due to incorrect path");
        }
        catch (IOException e)
        {
            //expected
        }
    }

    public void testBuildClientContextForSSLEncryptionOnly() throws Exception
    {

        final TrustManager[] trustManagers;
        final KeyManager[] keyManagers;

        trustManagers =
                SSLContextFactory.getTrustManagers(TestSSLConstants.TRUSTSTORE,
                                                   TestSSLConstants.TRUSTSTORE_PASSWORD,
                                                   STORE_TYPE,
                                                   DEFAULT_TRUST_MANAGER_ALGORITHM);

        keyManagers =
                SSLContextFactory.getKeyManagers(null, null, null, null, null);


        SSLContext context = SSLContextFactory.buildClientContext(trustManagers, keyManagers);
        assertNotNull("SSLContext should not be null", context);
    }

    public void testBuildClientContextWithForClientAuth() throws Exception
    {

        final TrustManager[] trustManagers;
        final KeyManager[] keyManagers;

        trustManagers =
                SSLContextFactory.getTrustManagers(TestSSLConstants.TRUSTSTORE,
                                                   TestSSLConstants.TRUSTSTORE_PASSWORD,
                                                   STORE_TYPE,
                                                   DEFAULT_TRUST_MANAGER_ALGORITHM);

        keyManagers =
                SSLContextFactory.getKeyManagers(TestSSLConstants.KEYSTORE,
                                                 TestSSLConstants.KEYSTORE_PASSWORD,
                                                 STORE_TYPE,
                                                 DEFAULT_KEY_MANAGER_ALGORITHM,
                                                 null);


        SSLContext context = SSLContextFactory.buildClientContext(trustManagers, keyManagers);
        assertNotNull("SSLContext should not be null", context);
    }

    public void testBuildClientContextWithForClientAuthWithCertAlias() throws Exception
    {

        final TrustManager[] trustManagers;
        final KeyManager[] keyManagers;

        trustManagers =
                SSLContextFactory.getTrustManagers(TestSSLConstants.TRUSTSTORE,
                                                   TestSSLConstants.TRUSTSTORE_PASSWORD,
                                                   STORE_TYPE,
                                                   DEFAULT_TRUST_MANAGER_ALGORITHM);

        keyManagers =
                SSLContextFactory.getKeyManagers(TestSSLConstants.KEYSTORE,
                                                 TestSSLConstants.KEYSTORE_PASSWORD,
                                                 STORE_TYPE,
                                                 DEFAULT_KEY_MANAGER_ALGORITHM,
                                                 TestSSLConstants.CERT_ALIAS_APP1);


        SSLContext context = SSLContextFactory.buildClientContext(trustManagers, keyManagers);
        assertNotNull("SSLContext should not be null", context);
    }
}
