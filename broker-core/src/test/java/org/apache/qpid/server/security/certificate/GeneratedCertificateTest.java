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


package org.apache.qpid.server.security.certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;

class GeneratedCertificateTest extends UnitTestBase
{
    @Test
    void toStringLeavesOutPrivateKey() throws Exception
    {
        final KeyCertificatePair pair = TlsResourceBuilder.createSelfSigned("CN=Qpid");

        final GeneratedCertificate generated = new GeneratedCertificate(pair.privateKey(), pair.certificate());

        assertEquals("GeneratedCertificate[subject=CN=Qpid, serialNumber="
                     + pair.certificate().getSerialNumber().toString(16) + "]", generated.toString());
    }

    @Test
    void missingPartsRejected() throws Exception
    {
        final KeyCertificatePair pair = TlsResourceBuilder.createSelfSigned("CN=Qpid");

        assertThrows(NullPointerException.class, () -> new GeneratedCertificate(null, pair.certificate()));
        assertThrows(NullPointerException.class, () -> new GeneratedCertificate(pair.privateKey(), null));
    }
}
