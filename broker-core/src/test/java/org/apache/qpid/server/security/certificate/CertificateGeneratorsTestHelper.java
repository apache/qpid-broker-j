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

import java.util.Objects;

/** Lets tests outside this package control the certificate generator selection. */
public final class CertificateGeneratorsTestHelper
{
    private CertificateGeneratorsTestHelper()
    {
    }

    /**
     * @return a handle that clears the selection when closed
     */
    public static Reset useGenerator(final String type, final CertificateGenerator generator)
    {
        Objects.requireNonNull(generator, "Generator must not be null, use useNoGenerator instead");
        CertificateGenerators.setSelection(new CertificateGenerators.Selection(type, generator, null));
        return CertificateGeneratorsTestHelper::reset;
    }

    /**
     * @return a handle that clears the selection when closed
     */
    public static Reset useNoGenerator(final String reason)
    {
        CertificateGenerators.setSelection(CertificateGenerators.Selection.unavailable(reason));
        return CertificateGeneratorsTestHelper::reset;
    }

    /** Clears the selection, so that the next use discovers the installed generators again. */
    public static void reset()
    {
        CertificateGenerators.setSelection(null);
    }

    /** Clears the selection when closed. */
    @FunctionalInterface
    public interface Reset extends AutoCloseable
    {
        @Override
        void close();
    }
}
