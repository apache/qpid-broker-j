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

import java.net.InetAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.security.auth.x500.X500Principal;

/**
 * Immutable parameters of a self-signed certificate, created with {@link #builder()}.
 * <p>
 * The issuer and subject are both {@link #getSubject()}. The certificate must carry a critical basic constraints
 * extension marking it as an end entity (not a CA) and, only when there is at least one name, a non-critical subject
 * alternative name extension built from {@link #getDnsNames()} and {@link #getAddresses()}. A generator may add
 * further extensions.
 */
public final class CertificateRequest
{
    // names that are not DNS names, such as names containing spaces, are left out of the certificate
    private static final Pattern DNS_NAME_PATTERN = Pattern.compile("[\\w&&[^\\d]][\\w\\d.-]*");

    private final String _keyAlgorithm;
    private final int _keyLength;
    private final String _signatureAlgorithm;
    private final X500Principal _subject;
    private final Instant _notBefore;
    private final Instant _notAfter;
    private final Set<String> _dnsNames;
    private final Set<InetAddress> _addresses;

    private CertificateRequest(final Builder builder)
    {
        _keyAlgorithm = requireNonBlank(builder._keyAlgorithm, "Key algorithm");
        _signatureAlgorithm = requireNonBlank(builder._signatureAlgorithm, "Signature algorithm");
        _subject = requireSet(builder._subject, "Subject");
        // X.509 stores validity with a precision of one second
        _notBefore = requireSet(builder._notBefore, "Start of the validity period").truncatedTo(ChronoUnit.SECONDS);
        _notAfter = requireSet(builder._notAfter, "End of the validity period").truncatedTo(ChronoUnit.SECONDS);
        _dnsNames = builder._dnsNames.stream()
                .filter(dnsName -> DNS_NAME_PATTERN.matcher(dnsName).matches())
                .collect(Collectors.toUnmodifiableSet());
        _addresses = builder._addresses;
        _keyLength = builder._keyLength;
        if (_keyLength <= 0)
        {
            throw new IllegalArgumentException("Key length must be positive: " + _keyLength);
        }
        if (!_notBefore.isBefore(_notAfter))
        {
            throw new IllegalArgumentException(String.format(
                    "Start of the validity period %s must be at least one whole second before its end %s",
                    _notBefore, _notAfter));
        }
    }

    /**
     * @return new builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * @return JCA name of the key algorithm
     */
    public String getKeyAlgorithm()
    {
        return _keyAlgorithm;
    }

    /**
     * @return key length in bits
     */
    public int getKeyLength()
    {
        return _keyLength;
    }

    /**
     * @return name of the signature algorithm; generators must accept the JCA standard names, such as
     * SHA256withRSA, and may accept other names
     */
    public String getSignatureAlgorithm()
    {
        return _signatureAlgorithm;
    }

    /**
     * @return subject, which is also the issuer of the self-signed certificate
     */
    public X500Principal getSubject()
    {
        return _subject;
    }

    /**
     * @return start of the validity period, in whole seconds
     */
    public Instant getNotBefore()
    {
        return _notBefore;
    }

    /**
     * @return end of the validity period, in whole seconds
     */
    public Instant getNotAfter()
    {
        return _notAfter;
    }

    /**
     * @return DNS names for the subject alternative name extension, all of which must be included; names that are
     * not DNS names, for example names containing spaces, have already been left out
     */
    public Set<String> getDnsNames()
    {
        return _dnsNames;
    }

    /**
     * @return IP addresses for the subject alternative name extension, all of which must be included
     */
    public Set<InetAddress> getAddresses()
    {
        return _addresses;
    }

    private static String requireNonBlank(final String value, final String name)
    {
        if (requireSet(value, name).isBlank())
        {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static <T> T requireSet(final T value, final String name)
    {
        if (value == null)
        {
            throw new IllegalArgumentException(name + " must be set");
        }
        return value;
    }

    public static final class Builder
    {
        private String _keyAlgorithm;
        private int _keyLength;
        private String _signatureAlgorithm;
        private X500Principal _subject;
        private Instant _notBefore;
        private Instant _notAfter;
        private Set<String> _dnsNames = Set.of();
        private Set<InetAddress> _addresses = Set.of();

        private Builder()
        {
        }

        /**
         * @param keyAlgorithm JCA name of the key algorithm, for example RSA
         * @param keyLength key length in bits
         * @return this builder
         */
        public Builder keyAlgorithm(final String keyAlgorithm, final int keyLength)
        {
            _keyAlgorithm = keyAlgorithm;
            _keyLength = keyLength;
            return this;
        }

        /**
         * @param signatureAlgorithm name of the signature algorithm; JCA standard names, such as SHA256withRSA,
         * are accepted by every generator
         * @return this builder
         */
        public Builder signatureAlgorithm(final String signatureAlgorithm)
        {
            _signatureAlgorithm = signatureAlgorithm;
            return this;
        }

        /**
         * @param subject subject, which is also the issuer of the self-signed certificate
         * @return this builder
         */
        public Builder subject(final X500Principal subject)
        {
            _subject = subject;
            return this;
        }

        /**
         * @param notBefore start of the validity period, truncated to whole seconds
         * @param notAfter end of the validity period, truncated to whole seconds
         * @return this builder
         */
        public Builder validity(final Instant notBefore, final Instant notAfter)
        {
            _notBefore = notBefore;
            _notAfter = notAfter;
            return this;
        }

        /**
         * @param dnsNames candidate DNS names for the subject alternative name extension; names that are not
         * DNS names are left out
         * @return this builder
         * @throws NullPointerException if the set or one of its names is null
         */
        public Builder dnsNames(final Set<String> dnsNames)
        {
            _dnsNames = Set.copyOf(dnsNames);
            return this;
        }

        /**
         * @param addresses IP addresses for the subject alternative name extension
         * @return this builder
         * @throws NullPointerException if the set or one of its addresses is null
         */
        public Builder addresses(final Set<InetAddress> addresses)
        {
            _addresses = Set.copyOf(addresses);
            return this;
        }

        /**
         * @return certificate parameters
         * @throws IllegalArgumentException if a parameter is missing or invalid
         */
        public CertificateRequest build()
        {
            return new CertificateRequest(this);
        }
    }
}
