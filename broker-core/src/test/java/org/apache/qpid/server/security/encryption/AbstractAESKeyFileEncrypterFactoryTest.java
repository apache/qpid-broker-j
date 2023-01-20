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
package org.apache.qpid.server.security.encryption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.test.utils.UnitTestBase;

@SuppressWarnings({"rawtypes", "unchecked"})
public class AbstractAESKeyFileEncrypterFactoryTest extends UnitTestBase
{
    private Broker _broker;
    private Path _tmpDir;
    private AbstractAESKeyFileEncrypterFactory _factory;

    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(isStrongEncryptionEnabled());

        _broker = mock(Broker.class);
        _tmpDir = Files.createTempDirectory(getTestName());

        when(_broker.getContextKeys(eq(false))).thenReturn(Set.of());
        when(_broker.getContextValue(eq(String.class), eq(SystemConfig.QPID_WORK_DIR))).thenReturn(_tmpDir.toString());
        when(_broker.getCategoryClass()).thenReturn(Broker.class);
        when(_broker.getName()).thenReturn(getTestName());
        final ArgumentCaptor<Map> attributesCaptor = ArgumentCaptor.forClass(Map.class);

        doAnswer((Answer<Void>) invocationOnMock ->
        {
            if (attributesCaptor.getValue().containsKey("context"))
            {
                Map replacementContext = (Map) attributesCaptor.getValue().get("context");
                when(_broker.getContext()).thenReturn(replacementContext);
            }
            return null;
        }).when(_broker).setAttributes(attributesCaptor.capture());

        _factory = new AbstractAESKeyFileEncrypterFactory()
        {
            @Override
            public String getType()
            {
                return null;
            }

            @Override
            protected ConfigurationSecretEncrypter createEncrypter(final SecretKeySpec keySpec)
            {
                return new AESKeyFileEncrypter(keySpec);
            }
        };
    }

    @Test
    public void testCreateKeyInDefaultLocation() throws Exception
    {
        assumeTrue(supportsPosixFileAttributes());
        final ConfigurationSecretEncrypter encrypter = _factory.createEncrypter(_broker);

        final KeyFilePathChecker keyFilePathChecker = new KeyFilePathChecker();

        doChecks(encrypter, keyFilePathChecker);

        final String pathName = (String) _broker.getContext().get(AESKeyFileEncrypterFactory.ENCRYPTER_KEY_FILE);

        // check the context variable was set
        assertEquals(keyFilePathChecker.getKeyFile().toString(), pathName);
    }

    private void doChecks(final ConfigurationSecretEncrypter encrypter,
                          final KeyFilePathChecker keyFilePathChecker) throws IOException
    {
        // walk the directory to find the file
        Files.walkFileTree(_tmpDir, keyFilePathChecker);

        // check the file was actually found
        assertNotNull(keyFilePathChecker.getKeyFile());

        final String secret = "notasecret";

        // check the encrypter works
        assertEquals(secret, encrypter.decrypt(encrypter.encrypt(secret)));
    }

    @Test
    public void testSettingContextKeyLeadsToFileCreation() throws Exception
    {
        assumeTrue(supportsPosixFileAttributes());
        final String filename = randomUUID() + ".key";
        final String subdirName = getTestName() + File.separator + "test";
        final String fileLocation = _tmpDir.toString() + File.separator + subdirName + File.separator + filename;

        when(_broker.getContextKeys(eq(false))).thenReturn(Set.of(AESKeyFileEncrypterFactory.ENCRYPTER_KEY_FILE));
        when(_broker.getContextValue(eq(String.class),
                                     eq(AESKeyFileEncrypterFactory.ENCRYPTER_KEY_FILE))).thenReturn(fileLocation);

        final ConfigurationSecretEncrypter encrypter = _factory.createEncrypter(_broker);

        final KeyFilePathChecker keyFilePathChecker = new KeyFilePathChecker(subdirName, filename);

        doChecks(encrypter, keyFilePathChecker);
    }


    @Test
    public void testUnableToCreateFileInSpecifiedLocation() throws Exception
    {
        final String filename = randomUUID() + ".key";
        final String subdirName = getTestName() + File.separator + "test";
        final String fileLocation = _tmpDir.toString() + File.separator + subdirName + File.separator + filename;

        when(_broker.getContextKeys(eq(false))).thenReturn(Set.of(AESKeyFileEncrypterFactory.ENCRYPTER_KEY_FILE));
        when(_broker.getContextValue(eq(String.class),
                                     eq(AESKeyFileEncrypterFactory.ENCRYPTER_KEY_FILE))).thenReturn(fileLocation);

        Files.createDirectories(Paths.get(fileLocation));

        assertThrows(IllegalConfigurationException.class,
                () -> _factory.createEncrypter(_broker),
                "Should not be able to create a key file where a directory currently is");
    }


    @Test
    public void testPermissionsAreChecked() throws Exception
    {
        assumeTrue(supportsPosixFileAttributes());
        final String filename = randomUUID() + ".key";
        final String subdirName = getTestName() + File.separator + "test";
        final String fileLocation = _tmpDir.toString() + File.separator + subdirName + File.separator + filename;

        when(_broker.getContextKeys(eq(false))).thenReturn(Set.of(AESKeyFileEncrypterFactory.ENCRYPTER_KEY_FILE));
        when(_broker.getContextValue(eq(String.class),
                                     eq(AESKeyFileEncrypterFactory.ENCRYPTER_KEY_FILE))).thenReturn(fileLocation);

        Files.createDirectories(Paths.get(_tmpDir.toString(), subdirName));

        final File file = new File(fileLocation);
        file.createNewFile();
        Files.setPosixFilePermissions(file.toPath(),
                                      EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ));

        assertThrows(IllegalArgumentException.class,
                () -> _factory.createEncrypter(_broker),
                "Should not be able to create a key file where the file is readable");
    }

    @Test
    public void testInvalidKey() throws Exception
    {
        assumeTrue(supportsPosixFileAttributes());
        final String filename = randomUUID() + ".key";
        final String subdirName = getTestName() + File.separator + "test";
        final String fileLocation = _tmpDir.toString() + File.separator + subdirName + File.separator + filename;

        when(_broker.getContextKeys(eq(false))).thenReturn(Set.of(AESKeyFileEncrypterFactory.ENCRYPTER_KEY_FILE));
        when(_broker.getContextValue(eq(String.class),
                                     eq(AESKeyFileEncrypterFactory.ENCRYPTER_KEY_FILE))).thenReturn(fileLocation);

        Files.createDirectories(Paths.get(_tmpDir.toString(), subdirName));

        final File file = new File(fileLocation);
        try (final FileOutputStream fos = new FileOutputStream(file))
        {
            fos.write("This is not an AES key.  It is a string saying it is not an AES key".getBytes(
                    StandardCharsets.US_ASCII));
        }
        Files.setPosixFilePermissions(file.toPath(), EnumSet.of(PosixFilePermission.OWNER_READ));

        assertThrows(IllegalConfigurationException.class,
                () -> _factory.createEncrypter(_broker),
                "Should not be able to start where the key is not a valid key");
    }

    private boolean supportsPosixFileAttributes()
    {
        return Files.getFileAttributeView(_tmpDir, PosixFileAttributeView.class) != null;
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        try (final Stream<Path> stream = Files.walk(_tmpDir))
        {
            stream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    static boolean isStrongEncryptionEnabled() throws NoSuchAlgorithmException
    {
        return Cipher.getMaxAllowedKeyLength("AES") >= 256;
    }

    private class KeyFilePathChecker extends SimpleFileVisitor<Path>
    {
        private final String _fileName;
        private final String _subdirName;
        private Path _keyFile;
        private boolean _inKeysSubdir;

        public KeyFilePathChecker()
        {
            this(AESKeyFileEncrypterFactory.DEFAULT_KEYS_SUBDIR_NAME, "Broker_" + getTestName() + ".key");
        }

        public KeyFilePathChecker(final String subdirName, final String fileName)
        {
            _subdirName = subdirName;
            _fileName = fileName;
        }

        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException
        {
            if (!_inKeysSubdir && dir.endsWith(_subdirName))
            {
                _inKeysSubdir = true;
                assertFalse(Files.getPosixFilePermissions(dir).contains(PosixFilePermission.OTHERS_READ));
                assertFalse(Files.getPosixFilePermissions(dir).contains(PosixFilePermission.OTHERS_WRITE));
                assertFalse(Files.getPosixFilePermissions(dir).contains(PosixFilePermission.OTHERS_EXECUTE));

                assertFalse(Files.getPosixFilePermissions(dir).contains(PosixFilePermission.GROUP_READ));
                assertFalse(Files.getPosixFilePermissions(dir).contains(PosixFilePermission.GROUP_WRITE));
                assertFalse(Files.getPosixFilePermissions(dir).contains(PosixFilePermission.GROUP_EXECUTE));
                return FileVisitResult.CONTINUE;
            }
            else
            {
                return _inKeysSubdir ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
            }
        }

        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException
        {
            if (_inKeysSubdir)
            {
                if (file.endsWith(_fileName))
                {
                    _keyFile = file;

                    assertFalse(Files.getPosixFilePermissions(file).contains(PosixFilePermission.OTHERS_READ));
                    assertFalse(Files.getPosixFilePermissions(file).contains(PosixFilePermission.OTHERS_WRITE));
                    assertFalse(Files.getPosixFilePermissions(file).contains(PosixFilePermission.OTHERS_EXECUTE));

                    assertFalse(Files.getPosixFilePermissions(file).contains(PosixFilePermission.GROUP_READ));
                    assertFalse(Files.getPosixFilePermissions(file).contains(PosixFilePermission.GROUP_WRITE));
                    assertFalse(Files.getPosixFilePermissions(file).contains(PosixFilePermission.GROUP_EXECUTE));

                    return FileVisitResult.TERMINATE;
                }
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(final Path dir, final IOException exc)
        {
            _inKeysSubdir = false;
            return FileVisitResult.CONTINUE;
        }

        public Path getKeyFile()
        {
            return _keyFile;
        }
    }
}
