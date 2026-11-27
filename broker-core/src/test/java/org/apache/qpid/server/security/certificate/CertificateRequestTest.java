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

import java.net.InetAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.x500.X500Principal;

import org.junit.jupiter.api.Test;

import org.apache.qpid.test.utils.UnitTestBase;

class CertificateRequestTest extends UnitTestBase
{
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    private static final X500Principal SUBJECT = new X500Principal("CN=Qpid");

    @Test
    void build() throws Exception
    {
        final Instant notAfter = NOW.plus(1, ChronoUnit.DAYS);
        final Set<InetAddress> addresses = Set.of(InetAddress.getByName("127.0.0.1"));

        final CertificateRequest request = valid().validity(NOW, notAfter)
                .dnsNames(Set.of("localhost"))
                .addresses(addresses)
                .build();

        assertEquals("RSA", request.getKeyAlgorithm());
        assertEquals(2048, request.getKeyLength());
        assertEquals("SHA256withRSA", request.getSignatureAlgorithm());
        assertEquals(SUBJECT, request.getSubject());
        assertEquals(NOW, request.getNotBefore());
        assertEquals(notAfter, request.getNotAfter());
        assertEquals(Set.of("localhost"), request.getDnsNames());
        assertEquals(addresses, request.getAddresses());
    }

    @Test
    void invalidDnsNamesLeftOut()
    {
        final CertificateRequest request = valid()
                .dnsNames(Set.of("localhost", "broker.example.com", "not a name", "-invalid", ""))
                .build();

        assertEquals(Set.of("localhost", "broker.example.com"), request.getDnsNames());
    }

    @Test
    void namesDefaultToEmpty()
    {
        final CertificateRequest request = valid().build();

        assertEquals(Set.of(), request.getDnsNames());
        assertEquals(Set.of(), request.getAddresses());
    }

    @Test
    void namesAreCopied()
    {
        final Set<String> dnsNames = new HashSet<>(Set.of("localhost"));
        final CertificateRequest request = valid().dnsNames(dnsNames).build();

        dnsNames.add("other");

        assertEquals(Set.of("localhost"), request.getDnsNames());
        assertThrows(UnsupportedOperationException.class, () -> request.getDnsNames().add("other"));
    }

    @Test
    void nullNamesRejected()
    {
        assertThrows(NullPointerException.class, () -> valid().dnsNames(null));
        assertThrows(NullPointerException.class, () -> valid().addresses(null));
        final Set<String> withNull = new HashSet<>();
        withNull.add(null);
        assertThrows(NullPointerException.class, () -> valid().dnsNames(withNull));
    }

    @Test
    void missingParametersRejected()
    {
        assertThrows(IllegalArgumentException.class, () -> valid().keyAlgorithm(null, 2048).build());
        assertThrows(IllegalArgumentException.class, () -> valid().keyAlgorithm(" ", 2048).build());
        assertThrows(IllegalArgumentException.class, () -> valid().signatureAlgorithm(null).build());
        assertThrows(IllegalArgumentException.class, () -> valid().subject(null).build());
        assertThrows(IllegalArgumentException.class, () -> valid().validity(null, NOW).build());
        assertThrows(IllegalArgumentException.class, () -> valid().validity(NOW, null).build());
    }

    @Test
    void invalidKeyLengthRejected()
    {
        assertThrows(IllegalArgumentException.class, () -> valid().keyAlgorithm("RSA", 0).build());
        assertThrows(IllegalArgumentException.class, () -> valid().keyAlgorithm("RSA", -1).build());
    }

    @Test
    void validityMustEndAfterItStarts()
    {
        assertThrows(IllegalArgumentException.class, () -> valid().validity(NOW, NOW).build());
        assertThrows(IllegalArgumentException.class,
                     () -> valid().validity(NOW, NOW.minus(1, ChronoUnit.DAYS)).build());
    }

    @Test
    void validityTruncatedToTheSecond()
    {
        final Instant start = Instant.ofEpochSecond(1_000, 0);

        assertThrows(IllegalArgumentException.class,
                     () -> valid().validity(start, start.plusMillis(500)).build());
        assertThrows(IllegalArgumentException.class,
                     () -> valid().validity(start.plusMillis(500), start.plusMillis(999)).build());
        final CertificateRequest request = valid().validity(start.plusMillis(500), start.plusMillis(1_500)).build();
        assertEquals(start, request.getNotBefore());
        assertEquals(start.plusSeconds(1), request.getNotAfter());
    }

    private static CertificateRequest.Builder valid()
    {
        return CertificateRequest.builder()
                .keyAlgorithm("RSA", 2048)
                .signatureAlgorithm("SHA256withRSA")
                .subject(SUBJECT)
                .validity(NOW, NOW.plus(365, ChronoUnit.DAYS));
    }
}
