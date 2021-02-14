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
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    private static final String SECRET = "secret";
    public static final int BROKER_START_TIMEOUT = 10;
    private final SecureRandom _random = new SecureRandom();
    private Path _configurationLocation;
    private Path _workDir;
    private Broker<?> _broker;
    private SystemLauncher _systemLauncher;
    private static SecretKeySpec secretKey;

    @Before
    public void setUp() throws Exception
    {
        assumeThat(isStrongEncryptionEnabled(), is(true));
        final byte[] keyData = new byte[32];
        _random.nextBytes(keyData);
        secretKey = new SecretKeySpec(keyData, "AES");
    }

    @After
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

        assertEquals("Not all encryptions were distinct", iterations, encryptions.size());

        for (String encrypted : encryptions)
        {
            assertEquals("Not all encryptions decrypt correctly", SECRET, encrypter.decrypt(encrypted));
        }
    }

    @Test
    public void testCreationFailsOnInvalidSecret() throws Exception
    {
        try
        {
            new AESGCMKeyFileEncrypter(null);
            fail("An encrypter should not be creatable from a null key");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue("Unexpected exception message:" + e.getMessage(),
                       e.getMessage().contains("A non null secret key must be supplied"));
        }

        PBEKeySpec keySpec = new PBEKeySpec(SECRET.toCharArray());
        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
        try
        {
            new AESGCMKeyFileEncrypter(factory.generateSecret(keySpec));
            fail("An encrypter should not be creatable from the wrong type of secret key");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue("Unexpected exception message:" + e.getMessage(),
                       e.getMessage()
                        .contains("Provided secret key was for the algorithm: PBEWithMD5AndDES when AES was needed."));
        }
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
        try
        {
            encrypter.encrypt(null);
            fail("Attempting to encrypt null should fail");
        }
        catch (NullPointerException e)
        {
            //pass
        }
    }

    @Test
    public void testEncryptingVeryLargeSecret()
    {
        Random random = new Random();
        byte[] data = new byte[4096];
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

        try
        {
            encrypter.decrypt(null);
            fail("Should not decrypt a null value");
        }
        catch (NullPointerException e)
        {
            // pass
        }

        try
        {
            encrypter.decrypt("");
            fail("Should not decrypt the empty String");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        try
        {
            encrypter.decrypt("thisisnonsense");
            fail("Should not decrypt a small amount of nonsense");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }

        try
        {
            encrypter.decrypt("thisisn'tvalidBase64!soitshouldfailwithanIllegalArgumentException");
            fail("Should not decrypt a larger amount of nonsense");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testChangeOfEncryptionToGCM() throws Exception
    {
        createBrokerAndAuthenticationProviderWithEncrypterPassword(AESKeyFileEncrypterFactory.TYPE);
        final String aesEncryptedPassword = getEncryptedPasswordFromConfig();
        final SecretKeySpec aesSecretKey = new SecretKeySpec(getBrokerSecretKey(), "AES");
        final AESKeyFileEncrypter cbcEncrypter = new AESKeyFileEncrypter(aesSecretKey);
        final String aesDecryptedPassword = cbcEncrypter.decrypt(aesEncryptedPassword);
        assertEquals("Decrypted text doesnt match original", SECRET, aesDecryptedPassword);
        _broker.setAttributes(Collections.singletonMap(Broker.CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER,
                                                       AESGCMKeyFileEncrypterFactory.TYPE));
        final String gcmEncryptedPassword = getEncryptedPasswordFromConfig();
        final AESGCMKeyFileEncrypter gcmEncrypter = new AESGCMKeyFileEncrypter(aesSecretKey);
        final String gcmDecryptedPassword = gcmEncrypter.decrypt(gcmEncryptedPassword);
        assertEquals("Decrypted text doesnt match original", SECRET, gcmDecryptedPassword);
    }

    @Test
    public void testSetKeyLocationAsExpression() throws Exception
    {
        final Path workDir = Files.createTempDirectory("qpid_work_dir");
        final File keyFile = new File(workDir.toFile(), "test.key");
        AbstractAESKeyFileEncrypterFactory.createAndPopulateKeyFile(keyFile);
        final Map<String, String> context = Collections.singletonMap(
                AbstractAESKeyFileEncrypterFactory.ENCRYPTER_KEY_FILE,
                "${qpid.work_dir}" + File.separator + keyFile.getName());
        createBrokerAndAuthenticationProviderWithEncrypterPassword(AESGCMKeyFileEncrypterFactory.TYPE,
                                                                   workDir,
                                                                   context);
        final String encryptedPassword = getEncryptedPasswordFromConfig();
        final SecretKeySpec aesSecretKey = new SecretKeySpec(Files.readAllBytes(keyFile.toPath()), "AES");
        final AESGCMKeyFileEncrypter encrypter = new AESGCMKeyFileEncrypter(aesSecretKey);
        final String decryptedPassword = encrypter.decrypt(encryptedPassword);
        assertEquals("Decrypted text doesnt match original", SECRET, decryptedPassword);
    }

    @Test
    public void testChangeOfEncryptionToAES() throws Exception
    {
        createBrokerAndAuthenticationProviderWithEncrypterPassword(AESGCMKeyFileEncrypterFactory.TYPE);
        final String gcmEncryptedPassword = getEncryptedPasswordFromConfig();
        final SecretKeySpec aesSecretKey = new SecretKeySpec(getBrokerSecretKey(), "AES");
        final AESGCMKeyFileEncrypter gcmEncrypter = new AESGCMKeyFileEncrypter(aesSecretKey);
        final String gcmDecryptedPassword = gcmEncrypter.decrypt(gcmEncryptedPassword);
        assertEquals("Decrypted text doesnt match original", SECRET, gcmDecryptedPassword);
        _broker.setAttributes(Collections.singletonMap(Broker.CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER,
                                                       "AESKeyFile"));
        final String cbcEncryptedPassword = getEncryptedPasswordFromConfig();
        final AESKeyFileEncrypter cbcEncrypter = new AESKeyFileEncrypter(aesSecretKey);
        final String cbcDecryptedPassword = cbcEncrypter.decrypt(cbcEncryptedPassword);
        assertEquals("Decrypted text doesnt match original", SECRET, cbcDecryptedPassword);
    }

    private void doTestSimpleEncryptDecrypt(final String text)
    {
        AESGCMKeyFileEncrypter encrypter = new AESGCMKeyFileEncrypter(secretKey);

        String encrypted = encrypter.encrypt(text);
        assertNotNull("Encrypter did not return a result from encryption", encrypted);
        assertNotEquals("Plain text and encrypted version are equal", text, encrypted);
        String decrypted = encrypter.decrypt(encrypted);
        assertNotNull("Encrypter did not return a result from decryption", decrypted);
        assertEquals("Encryption was not reversible", text, decrypted);
    }

    private void createBrokerAndAuthenticationProviderWithEncrypterPassword(final Object encryptionType)
            throws Exception
    {
        createBrokerAndAuthenticationProviderWithEncrypterPassword(encryptionType,
                                                                   Files.createTempDirectory("qpid_work_dir"),
                                                                   Collections.emptyMap());
    }

    private void createBrokerAndAuthenticationProviderWithEncrypterPassword(final Object encryptionType,
                                                                            final Path workDir,
                                                                            final Map<String, String> brokerContext)
            throws Exception
    {
        _workDir = workDir;
        final Map<String, String> context = new HashMap<>();
        context.put("qpid.work_dir", workDir.toFile().getAbsolutePath());

        _configurationLocation = Files.createTempFile(_workDir, "config", ".json");
        final Map<String, Object> config = new HashMap<>();
        config.put(ConfiguredObject.NAME, getTestName());
        config.put(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        config.put(Broker.CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER, encryptionType);
        config.put(Broker.CONTEXT, brokerContext);
        new ObjectMapper().writeValue(_configurationLocation.toFile(), config);

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put("storePath", _configurationLocation.toFile().getAbsolutePath());
        attributes.put("preferenceStoreAttributes", "{\"type\": \"Noop\"}");
        attributes.put(SystemConfig.TYPE, JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE);
        attributes.put(SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, Boolean.FALSE);
        attributes.put(SystemConfig.CONTEXT, context);
        final SettableFuture<SystemConfig<?>> configFuture = SettableFuture.create();
        _systemLauncher = new SystemLauncher(new SystemLauncherListener.DefaultSystemLauncherListener()
        {
            @Override
            public void onContainerResolve(final SystemConfig<?> systemConfig)
            {
                configFuture.set(systemConfig);
            }
        });
        _systemLauncher.startup(attributes);
        final SystemConfig<?> systemConfig = configFuture.get(BROKER_START_TIMEOUT, TimeUnit.SECONDS);
        _broker = (Broker<?>) systemConfig.getContainer();

        final Map<String, Object> authProviderAttributes = new HashMap<>();
        authProviderAttributes.put(ConfiguredObject.NAME, "testAuthProvider");
        authProviderAttributes.put(ConfiguredObject.TYPE, "Plain");

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
        final Map<String, Object> configEntry = new ObjectMapper().readValue(configFile,
                                                                             new TypeReference<Map<String, Object>>()
                                                                             {
                                                                             });
        final List<Object> configAuthProvider = (List<Object>) configEntry.get("authenticationproviders");
        final Map<String, Object> users = (Map<String, Object>) configAuthProvider.get(0);
        final List<Object> configUsers = (List<Object>) users.get("users");
        final Map<String, Object> configUser = (HashMap<String, Object>) configUsers.get(0);
        return (String) configUser.get("password");
    }
}
