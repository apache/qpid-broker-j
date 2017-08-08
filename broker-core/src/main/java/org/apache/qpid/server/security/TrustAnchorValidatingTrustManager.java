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

package org.apache.qpid.server.security;

import java.security.GeneralSecurityException;
import java.security.cert.CertPathBuilder;
import java.security.cert.CertStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.PKIXCertPathBuilderResult;
import java.security.cert.TrustAnchor;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TrustAnchorValidatingTrustManager implements X509TrustManager
{
    private static Logger LOGGER = LoggerFactory.getLogger(TrustAnchorValidatingTrustManager.class);

    private String _trustStoreName;
    private final X509TrustManager _x509TrustManager;
    private final Set<TrustAnchor> _trustAnchors;
    private final Set<Certificate> _otherCerts;

    TrustAnchorValidatingTrustManager(final String trustStoreName, final X509TrustManager x509TrustManager,
                                      final Set<TrustAnchor> trustAnchors,
                                      final Set<Certificate> otherCerts)
    {
        _trustStoreName = trustStoreName;
        _x509TrustManager = x509TrustManager;
        _trustAnchors = trustAnchors;
        _otherCerts = otherCerts;
    }

    @Override
    public void checkClientTrusted(final X509Certificate[] x509Certificates, final String authType)
            throws CertificateException
    {
        _x509TrustManager.checkClientTrusted(x509Certificates, authType);

        X509Certificate peerCertificate = x509Certificates[0];
        PKIXCertPathBuilderResult pkixCertPathBuilderResult;
        try
        {
            pkixCertPathBuilderResult = getPkixCertPathBuilderResult(x509Certificates, _trustAnchors, _otherCerts);
        }
        catch (GeneralSecurityException e)
        {
            throw new CertificateException("Unexpected error whilst validating trust-anchor", e);
        }

        X509Certificate trustAnchorCert = pkixCertPathBuilderResult.getTrustAnchor().getTrustedCert();
        try
        {
            trustAnchorCert.checkValidity();
        }
        catch (CertificateExpiredException | CertificateNotYetValidException e)
        {
            LOGGER.warn("Authentication failed for peer bearing certificate (subject DN '{}') "
                        + "as the trust anchor (subject DN '{}') within truststore '{}' "
                        + "is either expired or not yet valid. Validity range {} - {}",
                        peerCertificate.getSubjectDN(),
                        trustAnchorCert.getSubjectDN(),
                        _trustStoreName,
                        trustAnchorCert.getNotBefore(),
                        trustAnchorCert.getNotAfter());
            throw e;
        }
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] x509Certificates, final String authType)
            throws CertificateException
    {
        _x509TrustManager.checkServerTrusted(x509Certificates, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers()
    {
        return _x509TrustManager.getAcceptedIssuers();
    }

    private PKIXCertPathBuilderResult getPkixCertPathBuilderResult(final X509Certificate[] x509Certificates,
                                                                   final Set<TrustAnchor> trustAnchors,
                                                                   final Set<Certificate> otherCerts)
            throws GeneralSecurityException
    {
        Set<Certificate> storeCerts = new HashSet<>();
        storeCerts.addAll(otherCerts);

        Iterator<X509Certificate> iterator = Arrays.asList(x509Certificates).iterator();

        if (!iterator.hasNext())
        {
            throw new IllegalArgumentException("Peer certificate not found");
        }

        final X509Certificate peerCertificate = iterator.next();
        while (iterator.hasNext())
        {
            X509Certificate intermediate = iterator.next();
            storeCerts.add(intermediate);
        }


        X509CertSelector selector = new X509CertSelector();
        selector.setCertificate(peerCertificate);
        // IBM JDK seems to require that the peer's certficate exists in the Collection too
        storeCerts.add(peerCertificate);

        PKIXBuilderParameters pkixParams = new PKIXBuilderParameters(trustAnchors, selector);
        pkixParams.setRevocationEnabled(false);

        CertStore intermediateCertStore = CertStore.getInstance("Collection",
                                                                new CollectionCertStoreParameters(storeCerts));
        pkixParams.addCertStore(intermediateCertStore);

        CertPathBuilder builder = CertPathBuilder.getInstance("PKIX");

        return (PKIXCertPathBuilderResult) builder.build(pkixParams);
    }
}
