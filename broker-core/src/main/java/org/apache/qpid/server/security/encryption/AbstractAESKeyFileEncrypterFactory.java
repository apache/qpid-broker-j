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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;

public abstract class AbstractAESKeyFileEncrypterFactory
{

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAESKeyFileEncrypterFactory.class);

    static final String ENCRYPTER_KEY_FILE = "encrypter.key.file";

    private static final int AES_KEY_SIZE_BITS = 256;
    protected static final int AES_KEY_SIZE_BYTES = AES_KEY_SIZE_BITS / 8;
    protected static final String AES_ALGORITHM = "AES";

    public static final String TYPE = "AESGCMKeyFile";

    static final String DEFAULT_KEYS_SUBDIR_NAME = ".keys";

    private static final boolean IS_AVAILABLE;

    private static final String GENERAL_EXCEPTION_MESSAGE =
            "Unable to determine a mechanism to protect access to the key file on this filesystem";

    static
    {
        boolean isAvailable;
        try
        {
            final int allowedKeyLength = Cipher.getMaxAllowedKeyLength(AES_ALGORITHM);
            isAvailable = allowedKeyLength >= AES_KEY_SIZE_BITS;
            if (!isAvailable)
            {
                LOGGER.warn("The {} configuration encryption encryption mechanism is not available. "
                            + "Maximum available AES key length is {} but {} is required. "
                            + "Ensure the full strength JCE policy has been installed into your JVM.",
                            TYPE,
                            allowedKeyLength,
                            AES_KEY_SIZE_BITS);
            }
        }
        catch (NoSuchAlgorithmException e)
        {
            isAvailable = false;

            LOGGER.error("The "
                         + TYPE
                         + " configuration encryption encryption mechanism is not available. "
                         + "The "
                         + AES_ALGORITHM
                         + " algorithm is not available within the JVM (despite it being a requirement).");
        }

        IS_AVAILABLE = isAvailable;
    }

    protected void checkFilePermissions(String fileLocation, File file) throws IOException
    {
        if (isPosixFileSystem(file))
        {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file.toPath());

            if (permissions.contains(PosixFilePermission.OTHERS_READ)
                || permissions.contains(PosixFilePermission.OTHERS_WRITE))
            {
                throw new IllegalArgumentException("Key file '"
                                                   + fileLocation
                                                   + "' has incorrect permissions.  Only the owner "
                                                   + "should be able to read or write this file.");
            }
        }
        else if (isAclFileSystem(file))
        {
            AclFileAttributeView attributeView = Files.getFileAttributeView(file.toPath(), AclFileAttributeView.class);
            ArrayList<AclEntry> acls = new ArrayList<>(attributeView.getAcl());
            ListIterator<AclEntry> iter = acls.listIterator();
            UserPrincipal owner = Files.getOwner(file.toPath());
            while (iter.hasNext())
            {
                AclEntry acl = iter.next();
                if (acl.type() == AclEntryType.ALLOW)
                {
                    Set<AclEntryPermission> originalPermissions = acl.permissions();
                    Set<AclEntryPermission> updatedPermissions = EnumSet.copyOf(originalPermissions);

                    if (!owner.equals(acl.principal())
                        && updatedPermissions.removeAll(EnumSet.of(AclEntryPermission.READ_DATA)))
                    {
                        throw new IllegalArgumentException(String.format(
                                "Key file '%s' has incorrect permissions.  Only the owner should be able to read from the file.",
                                fileLocation));
                    }
                }
            }
        }
        else
        {
            throw new IllegalArgumentException(GENERAL_EXCEPTION_MESSAGE);
        }
    }

    private boolean isPosixFileSystem(File file) throws IOException
    {
        return Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class) != null;
    }

    private boolean isAclFileSystem(File file) throws IOException
    {
        return Files.getFileAttributeView(file.toPath(), AclFileAttributeView.class) != null;
    }


    protected void createAndPopulateKeyFile(final File file) throws IllegalAccessException
    {
        try
        {
            createEmptyKeyFile(file);

            KeyGenerator keyGenerator = KeyGenerator.getInstance(AES_ALGORITHM);
            keyGenerator.init(AES_KEY_SIZE_BITS);
            SecretKey key = keyGenerator.generateKey();
            try (FileOutputStream os = new FileOutputStream(file))
            {
                os.write(key.getEncoded());
            }

            makeKeyFileReadOnly(file);
        }
        catch (NoSuchAlgorithmException | IOException e)
        {
            throw new IllegalConfigurationException("Cannot create key file: " + e.getMessage(), e);
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalAccessException("Cannot access key file: " + e);
        }
    }

    private void makeKeyFileReadOnly(File file) throws IOException, IllegalAccessException
    {
        if (isPosixFileSystem(file))
        {
            Files.setPosixFilePermissions(file.toPath(), EnumSet.of(PosixFilePermission.OWNER_READ));
        }
        else if (isAclFileSystem(file))
        {
            AclFileAttributeView attributeView = Files.getFileAttributeView(file.toPath(), AclFileAttributeView.class);
            ArrayList<AclEntry> acls = new ArrayList<>(attributeView.getAcl());
            ListIterator<AclEntry> iter = acls.listIterator();
            file.setReadOnly();
            while (iter.hasNext())
            {
                AclEntry acl = iter.next();
                Set<AclEntryPermission> originalPermissions = acl.permissions();
                Set<AclEntryPermission> updatedPermissions = EnumSet.copyOf(originalPermissions);

                if (updatedPermissions.removeAll(EnumSet.of(AclEntryPermission.APPEND_DATA,
                                                            AclEntryPermission.DELETE,
                                                            AclEntryPermission.EXECUTE,
                                                            AclEntryPermission.WRITE_ACL,
                                                            AclEntryPermission.WRITE_DATA,
                                                            AclEntryPermission.WRITE_ATTRIBUTES,
                                                            AclEntryPermission.WRITE_NAMED_ATTRS,
                                                            AclEntryPermission.WRITE_OWNER)))
                {
                    AclEntry.Builder builder = AclEntry.newBuilder(acl);
                    builder.setPermissions(updatedPermissions);
                    iter.set(builder.build());
                }
            }
            attributeView.setAcl(acls);
        }
        else
        {
            throw new IllegalAccessException(GENERAL_EXCEPTION_MESSAGE);
        }
    }

    private void createEmptyKeyFile(File file) throws IOException
    {
        final Path parentFilePath = file.getAbsoluteFile().getParentFile().toPath();

        if (isPosixFileSystem(file))
        {
            Set<PosixFilePermission> ownerOnly = EnumSet.of(PosixFilePermission.OWNER_READ,
                                                            PosixFilePermission.OWNER_WRITE,
                                                            PosixFilePermission.OWNER_EXECUTE);
            Files.createDirectories(parentFilePath, PosixFilePermissions.asFileAttribute(ownerOnly));

            Files.createFile(file.toPath(), PosixFilePermissions.asFileAttribute(
                    EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)));
        }
        else if (isAclFileSystem(file))
        {
            Files.createDirectories(parentFilePath);
            final UserPrincipal owner = Files.getOwner(parentFilePath);
            AclFileAttributeView attributeView = Files.getFileAttributeView(parentFilePath, AclFileAttributeView.class);
            List<AclEntry> acls = new ArrayList<>(attributeView.getAcl());
            ListIterator<AclEntry> iter = acls.listIterator();
            boolean found = false;
            while (iter.hasNext())
            {
                AclEntry acl = iter.next();
                if (!owner.equals(acl.principal()))
                {
                    iter.remove();
                }
                else if (acl.type() == AclEntryType.ALLOW)
                {
                    found = true;
                    AclEntry.Builder builder = AclEntry.newBuilder(acl);
                    Set<AclEntryPermission> permissions = acl.permissions().isEmpty()
                            ? new HashSet<AclEntryPermission>()
                            : EnumSet.copyOf(acl.permissions());
                    permissions.addAll(Arrays.asList(AclEntryPermission.ADD_FILE,
                                                     AclEntryPermission.ADD_SUBDIRECTORY,
                                                     AclEntryPermission.LIST_DIRECTORY));
                    builder.setPermissions(permissions);
                    iter.set(builder.build());
                }
            }
            if (!found)
            {
                AclEntry.Builder builder = AclEntry.newBuilder();
                builder.setPermissions(AclEntryPermission.ADD_FILE,
                                       AclEntryPermission.ADD_SUBDIRECTORY,
                                       AclEntryPermission.LIST_DIRECTORY);
                builder.setType(AclEntryType.ALLOW);
                builder.setPrincipal(owner);
                acls.add(builder.build());
            }
            attributeView.setAcl(acls);

            Files.createFile(file.toPath(), new FileAttribute<List<AclEntry>>()
            {
                @Override
                public String name()
                {
                    return "acl:acl";
                }

                @Override
                public List<AclEntry> value()
                {
                    AclEntry.Builder builder = AclEntry.newBuilder();
                    builder.setType(AclEntryType.ALLOW);
                    builder.setPermissions(EnumSet.allOf(AclEntryPermission.class));
                    builder.setPrincipal(owner);
                    return Collections.singletonList(builder.build());
                }
            });
        }
        else
        {
            throw new IllegalArgumentException(GENERAL_EXCEPTION_MESSAGE);
        }
    }

    public boolean isAvailable()
    {
        return IS_AVAILABLE;
    }

    public abstract String getType();
}
