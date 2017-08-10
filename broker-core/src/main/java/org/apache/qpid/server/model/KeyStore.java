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
package org.apache.qpid.server.model;

import java.security.GeneralSecurityException;
import javax.net.ssl.KeyManager;

@ManagedObject( defaultType = "FileKeyStore" )
public interface KeyStore<X extends KeyStore<X>> extends ConfiguredObject<X>
{
    String CERTIFICATE_EXPIRY_WARN_PERIOD = "qpid.keystore.certificateExpiryWarnPeriod";

    @ManagedContextDefault(name = CERTIFICATE_EXPIRY_WARN_PERIOD,
            description = "The number of days before the certificate expiry date"
                          + " to issue the operational log warning about the certificate expiry")
    int DEFAULT_CERTIFICATE_EXPIRY_WARN_PERIOD = 30;

    String CERTIFICATE_EXPIRY_CHECK_FREQUENCY = "qpid.keystore.certificateExpiryCheckFrequency";

    @ManagedContextDefault(name = CERTIFICATE_EXPIRY_CHECK_FREQUENCY,
            description = "The interval of number of days to check certificate expiry")
    int DEFAULT_CERTIFICATE_EXPIRY_CHECK_FREQUENCY = 1;

    @DerivedAttribute
    int getCertificateExpiryWarnPeriod();

    @DerivedAttribute
    int getCertificateExpiryCheckFrequency();

    KeyManager[] getKeyManagers() throws GeneralSecurityException;
}
