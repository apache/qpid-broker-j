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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.plugin.ConditionallyAvailable;
import org.apache.qpid.server.plugin.ConfigurationSecretEncrypterFactory;
import org.apache.qpid.server.plugin.PluggableService;

@PluggableService
public class AESGCMKeyFileEncrypterFactory extends AbstractAESKeyFileEncrypterFactory
        implements ConfigurationSecretEncrypterFactory, ConditionallyAvailable
{
    public static final String TYPE = "AESGCMKeyFile";
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAESKeyFileEncrypterFactory.class);

    @Override
    public String getType()
    {
        return TYPE;
    }

    @Override
    public ConfigurationSecretEncrypter createEncrypter(final ConfiguredObject<?> object)
    {
        String fileLocation;
        if (object.getContextKeys(false).contains(ENCRYPTER_KEY_FILE))
        {
            fileLocation = object.getContextValue(String.class, ENCRYPTER_KEY_FILE);
        }
        else
        {

            fileLocation = object.getContextValue(String.class, SystemConfig.QPID_WORK_DIR)
                           + File.separator + DEFAULT_KEYS_SUBDIR_NAME + File.separator
                           + object.getCategoryClass().getSimpleName() + "_"
                           + object.getName() + ".key";

            Map<String, String> context = object.getContext();
            Map<String, String> modifiedContext = new LinkedHashMap<>(context);
            modifiedContext.put(ENCRYPTER_KEY_FILE, fileLocation);

            object.setAttributes(Collections.<String, Object>singletonMap(ConfiguredObject.CONTEXT, modifiedContext));
        }
        File file = new File(fileLocation);
        if (!file.exists())
        {
            LOGGER.warn(
                    "Configuration encryption is enabled, but no configuration secret was found. A new configuration secret will be created at '{}'.",
                    fileLocation);
            try
            {
                createAndPopulateKeyFile(file);
            }
            catch (IllegalAccessException e)
            {
                e.printStackTrace();
            }
        }
        if (!file.isFile())
        {
            throw new IllegalArgumentException(String.format("File '%s' is not a regular file.", fileLocation));
        }
        try
        {
            checkFilePermissions(fileLocation, file);
            if (Files.size(file.toPath()) != AES_KEY_SIZE_BYTES)
            {
                throw new IllegalArgumentException(String.format("Key file '%s' contains an incorrect about of data",
                                                                 fileLocation));
            }

            try (FileInputStream inputStream = new FileInputStream(file))
            {
                byte[] key = new byte[AES_KEY_SIZE_BYTES];
                int pos = 0;
                int read;
                while (pos < key.length && -1 != (read = inputStream.read(key, pos, key.length - pos)))
                {
                    pos += read;
                }
                if (pos != key.length)
                {
                    throw new IllegalConfigurationException(String.format(
                            "Key file '%s' contained an incorrect about of data",
                            fileLocation));
                }
                SecretKeySpec keySpec = new SecretKeySpec(key, AES_ALGORITHM);
                return new AESGCMKeyFileEncrypter(keySpec);
            }
        }
        catch (IOException e)
        {
            throw new IllegalConfigurationException("Unable to get file permissions: " + e.getMessage(), e);
        }
    }
}
