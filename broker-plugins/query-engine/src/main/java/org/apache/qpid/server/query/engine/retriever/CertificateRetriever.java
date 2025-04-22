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
package org.apache.qpid.server.query.engine.retriever;

import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.security.CertificateDetails;

/**
 * Retrieves CertificateDetails entities
 *
 * @param <C> Descendant of ConfiguredObject
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class CertificateRetriever<C extends ConfiguredObject<?>> extends ConfiguredObjectRetriever<C> implements EntityRetriever<C>
{
    /**
     * Target type for a KeyStore
     */
    @SuppressWarnings({"java:S1170", "rawtypes", "RedundantCast", "unchecked"})
    private final Class<C> _keyStoreType = (Class<C>) (Class<? extends ConfiguredObject>) KeyStore.class;

    /**
     * Target type for a TrustStore
     */
    @SuppressWarnings({"java:S1170", "rawtypes", "RedundantCast", "unchecked"})
    private final Class<C> _trustStoreType = (Class<C>) (Class<? extends ConfiguredObject>) TrustStore.class;

    /**
     * List of entity field names
     */
    private final List<String> _fieldNames = Stream.of("store", "alias", "issuerName", "serialNumber", "hexSerialNumber",
            "signatureAlgorithm", "subjectAltNames", "subjectName", "validFrom", "validUntil", "version")
            .collect(Collectors.toList());

    /**
     * Mapping function for a CertificateDetails
     */
    private final BiFunction<ConfiguredObject<?>, CertificateDetails, Map<String, Object>> certificateMapping =
        (ConfiguredObject<?> parent, CertificateDetails certificate) ->
        {
            final Map<String, Object> map = new LinkedHashMap<>();
            map.put(_fieldNames.get(0), parent.getName());
            map.put(_fieldNames.get(1), certificate.getAlias() == null ? "null" : certificate.getAlias());
            map.put(_fieldNames.get(2), certificate.getIssuerName());
            map.put(_fieldNames.get(3), certificate.getSerialNumber());
            map.put(_fieldNames.get(4), toHex(certificate.getSerialNumber()));
            map.put(_fieldNames.get(5), certificate.getSignatureAlgorithm());
            map.put(_fieldNames.get(6), certificate.getSubjectAltNames());
            map.put(_fieldNames.get(7), certificate.getSubjectName());
            map.put(_fieldNames.get(8), certificate.getValidFrom());
            map.put(_fieldNames.get(9), certificate.getValidUntil());
            map.put(_fieldNames.get(10), certificate.getVersion());
            return Collections.unmodifiableMap(map);
        };

    /**
     * Returns stream of CertificateDetails entities
     *
     * @param broker Broker instance
     *
     * @return Stream of entities
     */
    @Override()
    public Stream<Map<String, ?>> retrieve(final C broker)
    {
        final Stream<KeyStore<?>> keyStoreStream = retrieve(broker, _keyStoreType).map(keystore -> ((KeyStore<?>) keystore));
        final Stream<TrustStore<?>> trustStoreStream = retrieve(broker, _trustStoreType).map(truststore -> ((TrustStore<?>) truststore));
        return Stream.concat(
            keyStoreStream.flatMap(keyStore -> keyStore.getCertificateDetails().stream().map(certificate -> certificateMapping.apply(keyStore, certificate))),
            trustStoreStream.flatMap(trustStore -> trustStore.getCertificateDetails().stream().map(certificate -> certificateMapping.apply(trustStore, certificate)))
       );
    }

    /**
     * Returns list of entity field names
     *
     * @return List of field names
     */
    @Override()
    @SuppressWarnings("findbugs:EI_EXPOSE_REP")
    // List of field names already is an immutable collection
    public List<String> getFieldNames()
    {
        return _fieldNames;
    }

    /**
     * Converts serial number to hex
     *
     * @param serialNumber Serial number
     *
     * @return Hexadecimal serial number
     */
    private String toHex(String serialNumber)
    {
        try
        {
            if (serialNumber.contains(":"))
            {
                return format(new BigInteger(serialNumber.replace(":", ""), 16));
            }
            else
            {
                return format(new BigInteger(serialNumber));
            }
        }
        catch (NumberFormatException e)
        {
            return serialNumber;
        }
    }

    /**
     * Formats serial number to hex
     *
     * @param serialNumber Serial number
     *
     * @return Hexadecimal serial number
     */
    private String format(BigInteger serialNumber)
    {
        return "0x" + serialNumber.toString(16);
    }
}
