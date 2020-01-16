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
package org.apache.qpid.test.utils;

public final class TestSSLConstants
{
    public static final String JAVA_KEYSTORE_TYPE = "pkcs12";
    public static final String PASSWORD = "password";
    private static final String TEST_RESOURCES;
    static
    {
        if (System.getProperty("user.dir").contains("systests"))
        {
            TEST_RESOURCES = System.getProperty("user.dir") + "/../../test-profiles/test_resources/";
        }
        else if (System.getProperty("user.dir").contains(".."))
        {
            TEST_RESOURCES = System.getProperty("user.dir") + "/test-profiles/test_resources/";
        }
        else
        {
            TEST_RESOURCES = System.getProperty("user.dir") + "/../test-profiles/test_resources/";
        }
    }
    public static final String CLIENT_KEYSTORE = TEST_RESOURCES + "ssl/certificates/client_keystore.jks";
    public static final String CLIENT_TRUSTSTORE = TEST_RESOURCES + "ssl/certificates/client_truststore.jks";
    public static final String CLIENT_EXPIRED_KEYSTORE = TEST_RESOURCES + "ssl/certificates/client_expired_keystore.jks";
    public static final String CLIENT_EXPIRED_CRT = TEST_RESOURCES + "ssl/certificates/client_expired.crt";
    public static final String CLIENT_UNTRUSTED_KEYSTORE = TEST_RESOURCES + "ssl/certificates/client_untrusted_keystore.jks";

    public static final String CERT_ALIAS_ROOT_CA = "rootca";
    public static final String CERT_ALIAS_APP1 = "app1";
    public static final String CERT_ALIAS_APP2 = "app2";
    public static final String CERT_ALIAS_ALLOWED = "allowed_by_ca";
    public static final String CERT_ALIAS_REVOKED = "revoked_by_ca";
    public static final String CERT_ALIAS_REVOKED_EMPTY_CRL = "revoked_by_ca_empty_crl";
    public static final String CERT_ALIAS_REVOKED_INVALID_CRL_PATH = "revoked_by_ca_invalid_crl_path";
    public static final String CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE = "allowed_by_ca_with_intermediate";
    public static final String CERT_ALIAS_UNTRUSTED_CLIENT = "untrusted_client";

    public static final String BROKER_KEYSTORE = TEST_RESOURCES + "ssl/certificates/broker_keystore.jks";
    public static final String BROKER_CRT = TEST_RESOURCES + "ssl/certificates/broker.crt";
    public static final String BROKER_CSR = TEST_RESOURCES + "ssl/certificates/broker.csr";
    public static final String BROKER_TRUSTSTORE = TEST_RESOURCES + "ssl/certificates/broker_truststore.jks";
    public static final String BROKER_PEERSTORE = TEST_RESOURCES + "ssl/certificates/broker_peerstore.jks";
    public static final String BROKER_EXPIRED_TRUSTSTORE = TEST_RESOURCES + "ssl/certificates/broker_expired_truststore.jks";
    public static final String BROKER_KEYSTORE_ALIAS = "broker";

    public static final String TEST_EMPTY_KEYSTORE = TEST_RESOURCES + "ssl/certificates/test_empty_keystore.jks";
    public static final String TEST_KEYSTORE = TEST_RESOURCES + "ssl/certificates/test_keystore.jks";
    public static final String TEST_CERT_ONLY_KEYSTORE = TEST_RESOURCES + "ssl/certificates/test_cert_only_keystore.jks";
    public static final String TEST_PK_ONLY_KEYSTORE = TEST_RESOURCES + "ssl/certificates/test_pk_only_keystore.jks";
    public static final String TEST_SYMMETRIC_KEY_KEYSTORE = TEST_RESOURCES + "ssl/certificates/test_symmetric_key_keystore.jks";

    public static final String CA_CRL_EMPTY = TEST_RESOURCES + "ssl/certificates/MyRootCA.empty.crl";
    public static final String CA_CRL = TEST_RESOURCES + "ssl/certificates/MyRootCA.crl";
}
