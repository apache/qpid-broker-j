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
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.security.auth.x500.X500Principal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable, and therefore thread-safe, parameters of a self-signed certificate, created with {@link #builder()}. The
 * certificate a generator makes from them is described by {@link CertificateGenerator#generate(CertificateRequest)}.
 */
public class CertificateRequest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CertificateRequest.class);

    // a name is kept only if it starts with an ASCII letter or an underscore and continues with ASCII letters,
    // digits, underscores, dots and hyphens, which leaves out textual IP addresses and names containing spaces
    private static final Pattern DNS_NAME_PATTERN = Pattern.compile("[\\w&&[^\\d]][\\w\\d.-]*");

    private final String _keyAlgorithm;
    private final int _keyLength;
    private final String _signatureAlgorithm;
    private final X500Principal _subject;
    private final Instant _notBefore;
    private final Instant _notAfter;
    private final Set<String> _dnsNames;
    private final Set<InetAddress> _addresses;

    // private, so that the class cannot be subclassed and stays immutable without being declared final
    private CertificateRequest(final Builder builder)
    {
        _keyAlgorithm = requireNonBlank(builder._keyAlgorithm, "Key algorithm");
        _signatureAlgorithm = requireNonBlank(builder._signatureAlgorithm, "Signature algorithm");
        _subject = requireSet(builder._subject, "Subject");
        // RFC 5280 does not allow an empty issuer, and the issuer of a self-signed certificate is its subject
        if (_subject.getName().isEmpty())
        {
            throw new IllegalArgumentException("Subject must not be empty");
        }
        // X.509 stores validity with a precision of one second
        _notBefore = requireSet(builder._notBefore, "Start of the validity period").truncatedTo(ChronoUnit.SECONDS);
        _notAfter = requireSet(builder._notAfter, "End of the validity period").truncatedTo(ChronoUnit.SECONDS);
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
        _addresses = builder._addresses;
        final Map<Boolean, Set<String>> dnsNames = builder._dnsNames.stream()
                .collect(Collectors.partitioningBy(dnsName -> DNS_NAME_PATTERN.matcher(dnsName).matches(),
                                                   Collectors.toUnmodifiableSet()));
        _dnsNames = dnsNames.get(true);
        // logged at debug only, as a host name lookup returns a textual IP address when there is no reverse DNS
        // entry, so leaving out names is normal
        if (!dnsNames.get(false).isEmpty())
        {
            LOGGER.debug("Names left out of the certificate for '{}', as they do not match the DNS name pattern: {}",
                         _subject.getName(), dnsNames.get(false));
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
     * @return DNS names for the subject alternative name extension, which must contain exactly these names; names
     * that do not start with an ASCII letter or an underscore, or that contain characters other than ASCII letters,
     * digits, underscores, dots and hyphens, such as textual IP addresses and names containing spaces, have already
     * been left out
     */
    public Set<String> getDnsNames()
    {
        return _dnsNames;
    }

    /**
     * @return IP addresses for the subject alternative name extension, which must contain exactly these addresses;
     * the scope of an IPv6 address is local to the host and is not encoded
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

    /**
     * Builder of a {@link CertificateRequest}. It is not thread-safe.
     */
    public static class Builder
    {
        private String _keyAlgorithm;
        private int _keyLength;
        private String _signatureAlgorithm;
        private X500Principal _subject;
        private Instant _notBefore;
        private Instant _notAfter;
        private Set<String> _dnsNames = Set.of();
        private Set<InetAddress> _addresses = Set.of();

        // private, so that the class cannot be subclassed without being declared final
        private Builder()
        {
        }

        /**
         * @param keyAlgorithm JCA name of the key algorithm, for example RSA
         * @param keyLength key length in bits, which must be positive
         * @return this builder
         */
        public Builder keyAlgorithm(final String keyAlgorithm, final int keyLength)
        {
            _keyAlgorithm = keyAlgorithm;
            _keyLength = keyLength;
            return this;
        }

        /**
         * @param signatureAlgorithm name of the signature algorithm; generators must accept the JCA standard names,
         * such as SHA256withRSA, and may accept other names
         * @return this builder
         */
        public Builder signatureAlgorithm(final String signatureAlgorithm)
        {
            _signatureAlgorithm = signatureAlgorithm;
            return this;
        }

        /**
         * @param subject subject, which is also the issuer of the self-signed certificate; it must not be empty
         * @return this builder
         */
        public Builder subject(final X500Principal subject)
        {
            _subject = subject;
            return this;
        }

        /**
         * @param notBefore start of the validity period, truncated to whole seconds; after truncation it must be at
         * least one second before the end
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
         * @param dnsNames candidate DNS names for the subject alternative name extension; names that do not start
         * with an ASCII letter or an underscore, or that contain characters other than ASCII letters, digits,
         * underscores, dots and hyphens, such as textual IP addresses, are left out, as described by
         * {@link #getDnsNames()}
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
