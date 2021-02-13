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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.junit.AfterClass;
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
    private static final String PLAIN_SECRET = "secret";
    public static final int TIMEOUT = 10;
    private final SecureRandom _random = new SecureRandom();
    private static Path _configurationLocation;
    private static Path _workDir;
    private Broker<?> _broker;
    private static SystemLauncher _systemLauncher;
    private static SecretKeySpec secretKey;

    @Before
    public void setUp()
    {
        final byte[] keyData = new byte[32];
        _random.nextBytes(keyData);
        secretKey = new SecretKeySpec(keyData, "AES");
    }

    private void createBrokerWithEncrypter(final Object encrptionType) throws Exception
    {
        _workDir = Files.createTempDirectory("qpid_work_dir");
        _configurationLocation = Files.createTempFile(_workDir, "config", ".json");
        Map<String, Object> config = new HashMap<>();
        config.put(ConfiguredObject.NAME, getTestName());
        config.put(Broker.MODEL_VERSION, BrokerModel.MODEL_VERSION);
        config.put(Broker.CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER, encrptionType);
        new ObjectMapper().writeValue(_configurationLocation.toFile(), config);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("storePath", _configurationLocation.toFile().getAbsolutePath());
        attributes.put("preferenceStoreAttributes", "{\"type\": \"Noop\"}");
        attributes.put(SystemConfig.TYPE, JsonSystemConfigImpl.SYSTEM_CONFIG_TYPE);
        attributes.put(SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, Boolean.FALSE);
        attributes.put(SystemConfig.CONTEXT,
                       Collections.singletonMap("qpid.work_dir", _workDir.toFile().getAbsolutePath()));
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
        final SystemConfig<?> systemConfig = configFuture.get(TIMEOUT, TimeUnit.SECONDS);
        _broker = (Broker<?>) systemConfig.getContainer();

        final Map<String, Object> authProviderAttributes = new HashMap<>();
        authProviderAttributes.put(ConfiguredObject.NAME, "testAuthProvider");
        authProviderAttributes.put(ConfiguredObject.TYPE, "Plain");

        final AuthenticationProvider
                authProvider = _broker.createChild(AuthenticationProvider.class, authProviderAttributes);

        Map<String, Object> userAttrs = new HashMap<>();
        userAttrs.put(User.TYPE, "managed");
        userAttrs.put(User.NAME, "guest");
        userAttrs.put(User.PASSWORD, PLAIN_SECRET);
        User user = (User) authProvider.createChild(User.class, userAttrs);
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        _systemLauncher.shutdown();
        FileUtils.deleteDirectory(_workDir.toFile().getAbsolutePath());
        FileUtils.deleteDirectory(_configurationLocation.toFile().getAbsolutePath());
    }

    @Test
    public void testRepeatedEncryptionsReturnDifferentValues() throws Exception
    {
        AESGCMKeyFileEncrypter encrypter = new AESGCMKeyFileEncrypter(secretKey);

        Set<String> encryptions = new HashSet<>();

        int iterations = 10;

        for (int i = 0; i < iterations; i++)
        {
            encryptions.add(encrypter.encrypt(PLAIN_SECRET));
        }

        assertEquals("Not all encryptions were distinct", (long) iterations, (long) encryptions.size());

        for (String encrypted : encryptions)
        {
            assertEquals("Not all encryptions decrypt correctly", PLAIN_SECRET, encrypter.decrypt(encrypted));
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

        try
        {
            PBEKeySpec keySpec = new PBEKeySpec(PLAIN_SECRET.toCharArray());
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
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
    public void testEncryptionOfEmptyString() throws Exception
    {
        doTestSimpleEncryptDecrypt("");
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

    @Test
    public void testEncryptingNullFails() throws Exception
    {
        try
        {
            AESGCMKeyFileEncrypter encrypter = new AESGCMKeyFileEncrypter(secretKey);

            String encrypted = encrypter.encrypt(null);
            fail("Attempting to encrypt null should fail");
        }
        catch (NullPointerException e)
        {
            //pass
        }
    }

    @Test
    public void testEncryptingVeryLargeSecret() throws Exception
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
    public void testDecryptNonsense() throws Exception
    {
        AESGCMKeyFileEncrypter encrypter = new AESGCMKeyFileEncrypter(secretKey);

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
            String answer = encrypter.decrypt("thisisn'tvalidBase64!soitshouldfailwithanIllegalArgumentException");
            fail("Should not decrypt a larger amount of nonsense");
        }
        catch (IllegalArgumentException e)
        {
            // pass
        }
    }

    @Test
    public void testChangeOfEncryptiontoGCM() throws Exception
    {
        createBrokerWithEncrypter(AESKeyFileEncrypterFactory.TYPE);
        String aesEncryptedPassword = getEncryptedPasswordFromConfig();
        byte[] keyBytes = getSecretKey();
        final SecretKeySpec aesSecretKey = new SecretKeySpec(keyBytes, "AES");
        AESKeyFileEncrypter encrypter = new AESKeyFileEncrypter(aesSecretKey);
        String aesDecryptedPassword = encrypter.decrypt(aesEncryptedPassword);
        assertEquals("Decrpted text doesnt match original", PLAIN_SECRET, aesDecryptedPassword);
        _broker.setAttributes(Collections.singletonMap(Broker.CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER,
                                                       AESGCMKeyFileEncrypterFactory.TYPE));
        String gcmencryptedPassword = getEncryptedPasswordFromConfig();
        AESGCMKeyFileEncrypter gcmEncrypter = new AESGCMKeyFileEncrypter(aesSecretKey);
        String gcmDecryptedPassword = gcmEncrypter.decrypt(gcmencryptedPassword);
        assertEquals("Decrpted text doesnt match original", PLAIN_SECRET, gcmDecryptedPassword);
    }

    @Test
    public void testChangeOfEncryptiontoAES() throws Exception
    {
        createBrokerWithEncrypter(AESGCMKeyFileEncrypterFactory.TYPE);
        String gcmEncryptedPassword = getEncryptedPasswordFromConfig();
        byte[] keyBytes = getSecretKey();
        final SecretKeySpec aesSecretKey = new SecretKeySpec(keyBytes, "AES");
        AESGCMKeyFileEncrypter gcmEncrypter = new AESGCMKeyFileEncrypter(aesSecretKey);
        String gcmDecryptedPassword = gcmEncrypter.decrypt(gcmEncryptedPassword);
        assertEquals("Decrpted text doesnt match original", PLAIN_SECRET, gcmDecryptedPassword);
        _broker.setAttributes(Collections.singletonMap(Broker.CONFIDENTIAL_CONFIGURATION_ENCRYPTION_PROVIDER,
                                                       "AESKeyFile"));
        String aesencryptedPassword = getEncryptedPasswordFromConfig();
        AESKeyFileEncrypter aesEncrypter = new AESKeyFileEncrypter(aesSecretKey);
        String aesDecryptedPassword = aesEncrypter.decrypt(aesencryptedPassword);
        assertEquals("Decrpted text doesnt match original", PLAIN_SECRET, aesDecryptedPassword);
    }

    private byte[] getSecretKey() throws IOException
    {
        byte[] keyBytes = new byte[0];
        boolean isSecretFound = false;
        File[] workDir = _workDir.toFile().listFiles();

        for (File file : workDir)
        {
            if (file.isDirectory())
            {
                File[] keysDir = file.listFiles();
                for (File keysFile : keysDir)
                {
                    File secretKeyFile = keysFile;
                    keyBytes = Files.readAllBytes(Paths.get(String.valueOf(secretKeyFile)));
                    isSecretFound = true;
                    break;
                }
                if (isSecretFound)
                {
                    break;
                }
            }
        }
        return keyBytes;
    }

    private String getEncryptedPasswordFromConfig() throws java.io.IOException
    {
        ObjectMapper objectMapper = new ObjectMapper();
        File configFile = _configurationLocation.toFile();
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>()
        {
        };
        HashMap<String, Object> configEntry = objectMapper.readValue(configFile, typeRef);
        ArrayList configAuthProvider = (ArrayList) configEntry.get("authenticationproviders");
        HashMap<String, Object> users = (HashMap<String, Object>) configAuthProvider.get(0);
        ArrayList configUsers = (ArrayList) users.get("users");
        HashMap<String, Object> configUser = (HashMap<String, Object>) configUsers.get(0);
        return (String) configUser.get("password");
    }
}
