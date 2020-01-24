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

import java.nio.file.Paths;

public final class TestSSLConstants
{
    public static final String JAVA_KEYSTORE_TYPE = "pkcs12";
    public static final String PASSWORD = "password";
    private static final String TEST_CERTIFICATES_DIRECTORY;
    static
    {
        final String testCertificatesDirectoryPrefix;
        if (System.getProperty("user.dir").contains("systests"))
        {
            testCertificatesDirectoryPrefix = Paths.get(System.getProperty("user.dir"), "..", "..").toString();
        }
        else if (System.getProperty("user.dir").contains(".."))
        {
            testCertificatesDirectoryPrefix = System.getProperty("user.dir");
        }
        else
        {
            testCertificatesDirectoryPrefix = Paths.get(System.getProperty("user.dir"), "..").toString();
        }
        TEST_CERTIFICATES_DIRECTORY =
                Paths.get(testCertificatesDirectoryPrefix,
                        "qpid-test-utils", "src", "main", "resources", "ssl", "certificates").toString();
    }
    public static final String CLIENT_KEYSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "client_keystore.jks").toString();
    public static final String CLIENT_TRUSTSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "client_truststore.jks").toString();
    public static final String CLIENT_EXPIRED_KEYSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "client_expired_keystore.jks").toString();
    public static final String CLIENT_EXPIRED_CRT =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "client_expired.crt").toString();
    public static final String CLIENT_UNTRUSTED_KEYSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "client_untrusted_keystore.jks").toString();

    public static final String CERT_ALIAS_ROOT_CA = "rootca";
    public static final String CERT_ALIAS_APP1 = "app1";
    public static final String CERT_ALIAS_APP2 = "app2";
    public static final String CERT_ALIAS_ALLOWED = "allowed_by_ca";
    public static final String CERT_ALIAS_REVOKED = "revoked_by_ca";
    public static final String CERT_ALIAS_REVOKED_EMPTY_CRL = "revoked_by_ca_empty_crl";
    public static final String CERT_ALIAS_REVOKED_INVALID_CRL_PATH = "revoked_by_ca_invalid_crl_path";
    public static final String CERT_ALIAS_ALLOWED_WITH_INTERMEDIATE = "allowed_by_ca_with_intermediate";
    public static final String CERT_ALIAS_UNTRUSTED_CLIENT = "untrusted_client";

    public static final String BROKER_KEYSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "broker_keystore.jks").toString();
    public static final String BROKER_CRT =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "broker.crt").toString();
    public static final String BROKER_CSR =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "broker.csr").toString();
    public static final String BROKER_TRUSTSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "broker_truststore.jks").toString();
    public static final String BROKER_PEERSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "broker_peerstore.jks").toString();
    public static final String BROKER_EXPIRED_TRUSTSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "broker_expired_truststore.jks").toString();
    public static final String BROKER_KEYSTORE_ALIAS = "broker";

    public static final String TEST_EMPTY_KEYSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "test_empty_keystore.jks").toString();
    public static final String TEST_KEYSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "test_keystore.jks").toString();
    public static final String TEST_CERT_ONLY_KEYSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "test_cert_only_keystore.jks").toString();
    public static final String TEST_PK_ONLY_KEYSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "test_pk_only_keystore.jks").toString();
    public static final String TEST_SYMMETRIC_KEY_KEYSTORE =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "test_symmetric_key_keystore.jks").toString();

    public static final String CA_CRL_EMPTY =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "MyRootCA.empty.crl").toString();
    public static final String CA_CRL =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "MyRootCA.crl").toString();
    public static final String INTERMEDIATE_CA_CRL =
            Paths.get(TEST_CERTIFICATES_DIRECTORY, "intermediate_ca.crl").toString();
}
