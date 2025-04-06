/*
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

import static org.apache.qpid.server.security.encryption.AbstractAESKeyFileEncrypterFactoryTest.isStrongEncryptionEnabled;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.JsonSystemConfigImpl;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.User;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.UnitTestBase;

public class AESGCMKeyFileEncrypterTest extends UnitTestBase
{
    public static final int BROKER_START_TIMEOUT = 10;
    private static final String SECRET = "secret";
    private static SecretKeySpec secretKey;

    private final SecureRandom _random = new SecureRandom();

    private Path _configurationLocation;
    private Path _workDir;
    private Broker<?> _broker;
    private SystemLauncher _systemLauncher;

    @BeforeEach
    public void setUp() throws Exception
    {
        assumeTrue(isStrongEncryptionEnabled());
        final byte[] keyData = new byte[32];
        _random.nextBytes(keyData);
        secretKey = new SecretKeySpec(keyData, "AES");
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if (_systemLauncher != null)
        {
            _systemLauncher.shutdown();
        }
        if (_workDir != null)
        {
            FileUtils.deleteDirectory(_workDir.toFile().getAbsolutePath());
        }
    }

    @Test
    public void testRepeatedEncryptionsReturnDifferentValues()
    {
        final AESGCMKeyFileEncrypter encrypter = new AESGCMKeyFileEncrypter(secretKey);
        final Set<String> encryptions = new HashSet<>();
        final int iterations = 10;
        for (int i = 0; i < iterations; i++)
        {
            encryptions.add(encrypter.encrypt(SECRET));
        }

        assertEquals(iterations, encryptions.size(), "Not all encryptions were distinct");

        for (final String encrypted : encryptions)
        {
            assertEquals(SECRET, encrypter.decrypt(encrypted), "Not all encryptions decrypt correctly");
        }
    }

    @Test
    public void testCreationFailsOnInvalidSecret() throws Exception
    {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new AESGCMKeyFileEncrypter(null),
                "An encrypter should not be creatable from a null key");
        assertTrue(thrown.getMessage().contains("A non null secret key must be supplied"),
                "Unexpected exception message:" + thrown.getMessage());


        final PBEKeySpec keySpec = new PBEKeySpec(SECRET.toCharArray());
        final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");

        thrown = assertThrows(IllegalArgumentException.class,
                () -> new AESGCMKeyFileEncrypter(factory.generateSecret(keySpec)),
                "An encrypter should not be creatable from the wrong type of secret key");
        assertTrue(thrown.getMessage()
                        .contains("Provided secret key was for the algorithm: PBEWithMD5AndDES when AES was needed."),
                "Unexpected exception message:" + thrown.getMessage());
    }

    @Test
    public void testEncryptionOfEmptyString()
    {
        doTestSimpleEncryptDecrypt("");
    }

    @Test
    public void testEncryptingNullFails()
    {
        final AESGCMKeyFileEncrypter encrypter = new AESGCMKeyFileEncrypter(secretKey);
        assertThrows(NullPointerException.class,
                () -> encrypter.encrypt(null),
                "Attempting to encrypt null should fail");
    }

    @Test
    public void testEncryptingVeryLargeSecret()
    {
        final Random random = new Random();
        final byte[] data = new byte[4096];
        random.nextBytes(data);
        for (int i = 0; i < data.length; i++)
        {
            data[i] = (byte) (data[i] & 0xEF);
        }
        doTestSimpleEncryptDecrypt(new String(data, StandardCharsets.US_ASCII));
    }

    @Test
    public void testDecryptNonsense()
    {
        final AESGCMKeyFileEncrypter encrypter = new AESGCMKeyFileEncrypter(secretKey);
        assertThrows(NullPointerException.class, () -> encrypter.decrypt(null), "Should not decrypt a null value");
        assertThrows(IllegalArgumentException.class,
                () -> encrypter.decrypt(""),
                "Should not decrypt the empty String");
        assertThrows(IllegalArgumentException.class,
                () -> encrypter.decrypt("thisisnonsense"),
                "Should not decrypt a small amount of nonsense");
        assertThrows(IllegalArgumentException.class,
                () -> encrypter.decrypt("thisisn'tvalidBase64!soitshouldfailwithanIllegalArgumentException"),
                "Should not decrypt a larger amount of nonsense");
    }

    @Test
    public void testChangeOfEncryptionToGCM() throws Exception
    {
        createBrokerAndAuthenticationProviderWithEncrypterPassword(AESKeyFileEncrypterFactory.TYPE);
        final String aesEncryptedPassword = getEncryptedPasswordFromConfig();
        final SecretKeySpec aesSecretKey = new SecretKeySpec(getBrokerSecretKey(), "AES");
        final AESKeyFileEncrypter cbcEncrypter = new AESKeyFileEncrypter(aesSecretKey);
        final String aesDecryptedPassword = cbcEncrypter.decrypt(aesEncryptedPassword);
        assertEquals(SECRET, aesDecryptedPassword, "Decrypted text doesnt match original");
        _broker.setAttributes(Map.of(
                Broker.CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER, AESGCMKeyFileEncrypterFactory.TYPE));
        final String gcmEncryptedPassword = getEncryptedPasswordFromConfig();
        final AESGCMKeyFileEncrypter gcmEncrypter = new AESGCMKeyFileEncrypter(aesSecretKey);
        final String gcmDecryptedPassword = gcmEncrypter.decrypt(gcmEncryptedPassword);
        assertEquals(SECRET, gcmDecryptedPassword, "Decrypted text doesnt match original");
    }

    @Test
    public void testSetKeyLocationAsExpression() throws Exception
    {
        final Path workDir = Files.createTempDirectory("qpid_work_dir");
        final File keyFile = new File(workDir.toFile(), "test.key");
        AbstractAESKeyFileEncrypterFactory.createAndPopulateKeyFile(keyFile);
        final Map<String, String> context = Map.of(
                AbstractAESKeyFileEncrypterFactory.ENCRYPTER_KEY_FILE,
                "${qpid.work_dir}" + File.separator + keyFile.getName());
        createBrokerAndAuthenticationProviderWithEncrypterPassword(AESGCMKeyFileEncrypterFactory.TYPE,
                                                                   workDir,
                                                                   context);
        final String encryptedPassword = getEncryptedPasswordFromConfig();
        final SecretKeySpec aesSecretKey = new SecretKeySpec(Files.readAllBytes(keyFile.toPath()), "AES");
        final AESGCMKeyFileEncrypter encrypter = new AESGCMKeyFileEncrypter(aesSecretKey);
        final String decryptedPassword = encrypter.decrypt(encryptedPassword);
        assertEquals(SECRET, decryptedPassword, "Decrypted text doesnt match original");
    }

    @Test
    public void testChangeOfEncryptionToAES() throws Exception
    {
        createBrokerAndAuthenticationProviderWithEncrypterPassword(AESGCMKeyFileEncrypterFactory.TYPE);
        final String gcmEncryptedPassword = getEncryptedPasswordFromConfig();
        final SecretKeySpec aesSecretKey = new SecretKeySpec(getBrokerSecretKey(), "AES");
        final AESGCMKeyFileEncrypter gcmEncrypter = new AESGCMKeyFileEncrypter(aesSecretKey);
        final String gcmDecryptedPassword = gcmEncrypter.decrypt(gcmEncryptedPassword);
        assertEquals(SECRET, gcmDecryptedPassword, "Decrypted text doesnt match original");
        _broker.setAttributes(Map.of(
                Broker.CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER, "AESKeyFile"));
        final String cbcEncryptedPassword = getEncryptedPasswordFromConfig();
        final AESKeyFileEncrypter cbcEncrypter = new AESKeyFileEncrypter(aesSecretKey);
        final String cbcDecryptedPassword = cbcEncrypter.decrypt(cbcEncryptedPassword);
        assertEquals(SECRET, cbcDecryptedPassword, "Decrypted text doesnt match original");
    }

    private void doTestSimpleEncryptDecrypt(final String text)
    {
        final AESGCMKeyFileEncrypter encrypter = new AESGCMKeyFileEncrypter(secretKey);

        final String encrypted = encrypter.encrypt(text);
        assertNotNull(encrypted, "Encrypter did not return a result from encryption");
        assertNotEquals(text, encrypted, "Plain text and encrypted version are equal");
        final String decrypted = encrypter.decrypt(encrypted);
        assertNotNull(decrypted, "Encrypter did not return a result from decryption");
        assertEquals(text, decrypted, "Encryption was not reversible");
    }

    private void createBrokerAndAuthenticationProviderWithEncrypterPassword(final Object encryptionType)
            throws Exception
    {
        createBrokerAndAuthenticationProviderWithEncrypterPassword(encryptionType,
                                                                   Files.createTempDirectory("qpid_work_dir"),
                                                                   Map.of());
    }

    private void createBrokerAndAuthenticationProviderWithEncrypterPassword(final Object encryptionType,
                                                                            final Path workDir,
                                                                            final Map<String, String> brokerContext)
            throws Exception
    {
        _workDir = workDir;
        final Map<String, String> context = Map.of("qpid.work_dir", workDir.toFile().getAbsolutePath());

        _configurationLocation = Files.createTempFile(_workDir, "config", ".json");
        final Map<String, Object> config = Map.of(
                ConfiguredObject.NAME, getTestName(),
                Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION,
                Broker.CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER, encryptionType,
                Broker.CONTEXT, brokerContext);
        new ObjectMapper().writeValue(_configurationLocation.toFile(), config);

        final Map<String, Object> attributes = Map.of(
                "storePath", _configurationLocation.toFile().getAbsolutePath(),
                "preferenceStoreAttributes", "{\"type\": \"Noop\"}",
                SystemConfig.TYPE, JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE,
                SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, Boolean.FALSE,
                SystemConfig.CONTEXT, context);
        final CompletableFuture<SystemConfig<?>> configFuture = new CompletableFuture<>();
        _systemLauncher = new SystemLauncher(new SystemLauncherListener.DefaultSystemLauncherListener()
        {
            @Override
            public void onContainerResolve(final SystemConfig<?> systemConfig)
            {
                configFuture.complete(systemConfig);
            }
        });
        _systemLauncher.startup(attributes);
        final SystemConfig<?> systemConfig = configFuture.get(BROKER_START_TIMEOUT, TimeUnit.SECONDS);
        _broker = (Broker<?>) systemConfig.getContainer();

        final Map<String, Object> authProviderAttributes = Map.of(
                ConfiguredObject.NAME, "testAuthProvider",
                ConfiguredObject.TYPE, "Plain");

        final AuthenticationProvider<?>
                authProvider = _broker.createChild(AuthenticationProvider.class, authProviderAttributes);

        final Map<String, Object> userAttrs = new HashMap<>();
        userAttrs.put(User.TYPE, "managed");
        userAttrs.put(User.NAME, "guest");
        userAttrs.put(User.PASSWORD, SECRET);
        authProvider.createChild(User.class, userAttrs);
    }

    private byte[] getBrokerSecretKey() throws IOException
    {
        final String keyFile = AbstractAESKeyFileEncrypterFactory.getSecretKeyLocation(_broker);
        return Files.readAllBytes(Paths.get(keyFile));
    }

    @SuppressWarnings("unchecked")
    private String getEncryptedPasswordFromConfig() throws java.io.IOException
    {
        final File configFile = _configurationLocation.toFile();
        final Map<String, Object> configEntry =
                new ObjectMapper().readValue(configFile, new TypeReference<>(){});
        final List<Object> configAuthProvider = (List<Object>) configEntry.get("authenticationproviders");
        final Map<String, Object> users = (Map<String, Object>) configAuthProvider.get(0);
        final List<Object> configUsers = (List<Object>) users.get("users");
        final Map<String, Object> configUser = (HashMap<String, Object>) configUsers.get(0);
        return (String) configUser.get("password");
    }
}
