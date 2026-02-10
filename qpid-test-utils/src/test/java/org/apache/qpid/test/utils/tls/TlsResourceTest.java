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

package org.apache.qpid.test.utils.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

import org.junit.jupiter.api.Test;

class TlsResourceTest
{
    @Test
    void secretIsUnavailableAfterClose()
    {
        final TlsResource tls = new TlsResource("private", "cert", "secret",
                KeyStore.getDefaultType());
        try
        {
            final char[] before = tls.getSecretAsCharacters();
            assertEquals("secret", new String(before));
        }
        finally
        {
            tls.close();
        }

        assertThrows(IllegalStateException.class, tls::getSecretAsCharacters);
    }

    @Test
    void deletesTempDirectoryOnClose() throws Exception
    {
        final TlsResource tls = new TlsResource();
        Path directory = null;
        try
        {
            final Path file = tls.createFile(".tmp");
            directory = file.getParent();
            assertTrue(Files.exists(directory));
        }
        finally
        {
            tls.close();
        }

        assertNotNull(directory);
        assertFalse(Files.exists(directory));
    }

    @Test
    void cannotReuseAfterClose()
    {
        final TlsResource tls = new TlsResource();
        tls.close();

        assertThrows(IllegalStateException.class, () -> tls.createFile(".tmp"));
    }
}
