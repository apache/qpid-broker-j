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

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.qpid.server.model.ManagedAttributeValue;

public class CertificateDetailsImpl implements CertificateDetails, ManagedAttributeValue
{
    private final X509Certificate _x509cert;

    public CertificateDetailsImpl(final X509Certificate x509cert)
    {
        _x509cert = x509cert;
    }

    @Override
    public String getSerialNumber()
    {
        return _x509cert.getSerialNumber().toString();
    }

    @Override
    public int getVersion()
    {
        return _x509cert.getVersion();
    }

    @Override
    public String getSignatureAlgorithm()
    {
        return _x509cert.getSigAlgName();
    }

    @Override
    public String getIssuerName()
    {
        return _x509cert.getIssuerX500Principal().getName();
    }

    @Override
    public String getSubjectName()
    {
        return _x509cert.getSubjectX500Principal().getName();
    }

    @Override
    public List<String> getSubjectAltNames()
    {
        try
        {
            List<String> altNames = new ArrayList<>();
            final Collection<List<?>> altNameObjects = _x509cert.getSubjectAlternativeNames();
            if (altNameObjects != null)
            {
                for (List<?> entry : altNameObjects)
                {
                    final int type = (Integer) entry.get(0);
                    if (type == 1 || type == 2)
                    {
                        altNames.add(entry.get(1).toString().trim());
                    }
                }
            }
            return altNames;
        }
        catch (CertificateParsingException e)
        {

            return Collections.emptyList();
        }
    }

    @Override
    public Date getValidFrom()
    {
        return _x509cert.getNotBefore();
    }

    @Override
    public Date getValidUntil()
    {
        return _x509cert.getNotAfter();
    }
}
