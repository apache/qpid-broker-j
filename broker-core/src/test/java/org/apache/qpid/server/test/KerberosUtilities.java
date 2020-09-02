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
 */

package org.apache.qpid.server.test;

import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.kerberos.KerberosKey;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.google.common.io.ByteStreams;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.test.utils.JvmVendor;
import org.apache.qpid.test.utils.SystemPropertySetter;

public class KerberosUtilities
{
    public static final String REALM = "QPID.ORG";
    public static final String HOST_NAME = InetAddress.getLoopbackAddress().getCanonicalHostName();
    public static final String CLIENT_PRINCIPAL_NAME = "client";
    public static final String CLIENT_PRINCIPAL_FULL_NAME = CLIENT_PRINCIPAL_NAME + "@" + REALM;
    public static final String SERVER_PROTOCOL = "AMQP";
    public static final String SERVICE_PRINCIPAL_NAME = SERVER_PROTOCOL + "/" + HOST_NAME;
    public static final String ACCEPT_SCOPE = isIBM() ? "com.ibm.security.jgss.krb5.accept" : "com.sun.security.jgss.accept";
    private static final String USE_SUBJECT_CREDS_ONLY = "javax.security.auth.useSubjectCredsOnly";
    public static final String LOGIN_CONFIG = "java.security.auth.login.config";

    private static final String INITIATE_SCOPE = isIBM() ? "com.ibm.security.jgss.krb5.initiate" : "com.sun.security.jgss.initiate";
    private static final Logger LOGGER = LoggerFactory.getLogger(KerberosUtilities.class);
    private static final String IBM_LOGIN_MODULE_CLASS = "com.ibm.security.auth.module.Krb5LoginModule";
    private static final String SUN_LOGIN_MODULE_CLASS = "com.sun.security.auth.module.Krb5LoginModule";
    private static final String KERBEROS_LOGIN_MODULE_CLASS = isIBM() ? IBM_LOGIN_MODULE_CLASS : SUN_LOGIN_MODULE_CLASS;
    private static final String LOGIN_CONFIG_RESOURCE = "login.config";
    private static final String LOGIN_IBM_CONFIG_RESOURCE = "login.ibm.config";
    private static final String SERVICE_PRINCIPAL_FULL_NAME = SERVICE_PRINCIPAL_NAME + "@" + REALM;
    private static final String BROKER_KEYTAB = "broker.keytab";
    private static final String CLIENT_KEYTAB = "client.keytab";


    public File prepareKeyTabs(final EmbeddedKdcResource kdc) throws Exception
    {
        final File clientKeyTabFile;
        kdc.createPrincipal(BROKER_KEYTAB, SERVICE_PRINCIPAL_FULL_NAME);
        clientKeyTabFile = kdc.createPrincipal(CLIENT_KEYTAB, CLIENT_PRINCIPAL_FULL_NAME);
        return clientKeyTabFile;
    }

    public void prepareConfiguration(final String hostName, final SystemPropertySetter systemPropertySetter)
            throws IOException
    {
        final Path loginConfig = transformLoginConfig(hostName);
        systemPropertySetter.setSystemProperty(LOGIN_CONFIG,
                                               URLDecoder.decode(loginConfig.toFile().getAbsolutePath(), UTF_8.name()));
        systemPropertySetter.setSystemProperty(USE_SUBJECT_CREDS_ONLY, "false");
    }

    public byte[] buildToken(String clientPrincipalName, File clientKeyTabFile, String targetServerPrincipalName)
            throws Exception
    {
        final LoginContext lc = createKerberosKeyTabLoginContext(INITIATE_SCOPE,
                                                                 clientPrincipalName,
                                                                 clientKeyTabFile);

        Subject clientSubject = null;
        String useSubjectCredsOnly = System.getProperty(USE_SUBJECT_CREDS_ONLY);
        try
        {
            debug("Before login");
            lc.login();
            clientSubject = lc.getSubject();
            debug("LoginContext subject {}", clientSubject);
            System.setProperty(USE_SUBJECT_CREDS_ONLY, "true");
            return Subject.doAs(clientSubject,
                                (PrivilegedExceptionAction<byte[]>) () -> buildTokenWithinSubjectWithKerberosTicket(
                                        clientPrincipalName,
                                        targetServerPrincipalName));
        }
        finally
        {
            if (useSubjectCredsOnly == null)
            {
                System.clearProperty(USE_SUBJECT_CREDS_ONLY);
            }
            else
            {
                System.setProperty(USE_SUBJECT_CREDS_ONLY, useSubjectCredsOnly);
            }
            if (clientSubject != null)
            {
                lc.logout();
            }
        }
    }

    private byte[] buildTokenWithinSubjectWithKerberosTicket(String clientPrincipalName,
                                                             String targetServerPrincipalName) throws GSSException
    {
        debug("Building token for client principal '{}' and server principal '{}'",
              clientPrincipalName,
              targetServerPrincipalName);

        final GSSManager manager = GSSManager.getInstance();
        final GSSName clientName = manager.createName(clientPrincipalName, GSSName.NT_USER_NAME);
        final GSSCredential credential;
        try
        {
            credential = manager.createCredential(clientName,
                                                  GSSCredential.DEFAULT_LIFETIME,
                                                  new Oid("1.2.840.113554.1.2.2"),
                                                  GSSCredential.INITIATE_ONLY);
        }
        catch (GSSException e)
        {
            debug("Failure to create credential for {}", clientName, e);
            throw e;
        }

        debug("Client credential '{}'", credential);

        final GSSName serverName = manager.createName(targetServerPrincipalName, GSSName.NT_USER_NAME);
        final Oid spnegoMechOid = new Oid("1.3.6.1.5.5.2");
        final GSSContext clientContext = manager.createContext(serverName.canonicalize(spnegoMechOid),
                                                               spnegoMechOid,
                                                               credential,
                                                               GSSContext.DEFAULT_LIFETIME);

        debug("Requesting ticket using initiator's credentials");

        try
        {
            clientContext.requestCredDeleg(true);
            debug("Requesting ticket");
            return clientContext.initSecContext(new byte[]{}, 0, 0);
        }
        catch (GSSException e)
        {
            debug("Failure to request token", e);
            throw e;
        }
        finally
        {
            clientContext.dispose();
        }
    }

    public LoginContext createKerberosKeyTabLoginContext(final String scopeName,
                                                         final String principalName,
                                                         final File keyTabFile)
            throws LoginException
    {
        final KerberosPrincipal principal = new KerberosPrincipal(principalName);
        final KeyTab keyTab = getKeyTab(principal, keyTabFile);
        final Subject subject = new Subject(false,
                                            Collections.singleton(principal),
                                            Collections.emptySet(),
                                            Collections.singleton(keyTab));

        return createLoginContext(scopeName,
                                  subject,
                                  createKeyTabConfiguration(scopeName, keyTabFile, principal.getName()));
    }

    public KerberosKeyTabLoginConfiguration createKeyTabConfiguration(final String scopeName,
                                                                      final File keyTabFile,
                                                                      final String name)
    {
        return new KerberosKeyTabLoginConfiguration(scopeName, name, keyTabFile);
    }


    private LoginContext createLoginContext(final String serviceName, final Subject subject, final Configuration config)
            throws LoginException
    {
        return new LoginContext(serviceName, subject, callbacks -> {
            for (Callback callback : callbacks)
            {
                if (callback instanceof TextOutputCallback)
                {
                    LOGGER.error(((TextOutputCallback) callback).getMessage());
                }
            }
        }, config);
    }


    private KeyTab getKeyTab(final KerberosPrincipal principal, final File keyTabFile)
    {
        if (!keyTabFile.exists() || !keyTabFile.canRead())
        {
            throw new IllegalArgumentException("Specified file does not exist or is not readable.");
        }

        final KeyTab keytab = KeyTab.getInstance(principal, keyTabFile);
        if (!keytab.exists())
        {
            throw new IllegalArgumentException("Specified file is not a keyTab file.");
        }

        final KerberosKey[] keys = keytab.getKeys(principal);
        if (keys.length == 0)
        {
            throw new IllegalArgumentException("Specified file does not contain at least one key for this principal.");
        }

        for (final KerberosKey key : keys)
        {
            try
            {
                key.destroy();
            }
            catch (DestroyFailedException e)
            {
                LOGGER.debug("Unable to destroy key", e);
            }
        }

        return keytab;
    }

    public static class KerberosKeyTabLoginConfiguration extends Configuration
    {
        private final String _scopeName;
        private final AppConfigurationEntry _entry;

        KerberosKeyTabLoginConfiguration(final String scopeName,
                                         final String principalName,
                                         final File keyTabFile)
        {
            final Map<String, String> options = new HashMap<>();
            options.put("principal", principalName);

            if (isIBM())
            {
                options.put("useKeytab", keyTabFile.getAbsolutePath());
                options.put("credsType", "both");
            }
            else
            {
                options.put("keyTab", keyTabFile.getAbsolutePath());
                options.put("useKeyTab", TRUE.toString());
                options.put("doNotPrompt", TRUE.toString());
                options.put("refreshKrb5Config", TRUE.toString());
            }
            _entry = new AppConfigurationEntry(KERBEROS_LOGIN_MODULE_CLASS,
                                               AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                               options);
            _scopeName = scopeName;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name)
        {
            if (_scopeName.equals(name))
            {
                return new AppConfigurationEntry[]{_entry};
            }
            return new AppConfigurationEntry[0];
        }
    }

    public void debug(String message, Object... args)
    {
        LOGGER.debug(message, args);
        if (Boolean.TRUE.toString().equalsIgnoreCase(System.getProperty("sun.security.krb5.debug")))
        {
            System.out.println(String.format(message.replace("{}", "%s"), args));
        }
    }

    private Path transformLoginConfig(String hostName) throws IOException
    {
        final String resourceName = isIBM() ? LOGIN_IBM_CONFIG_RESOURCE : LOGIN_CONFIG_RESOURCE;
        final URL resource = KerberosUtilities.class.getClassLoader().getResource(resourceName);
        if (resource == null)
        {
            throw new IllegalArgumentException(String.format("Unknown resource '%s'", resourceName));
        }
        final String config;
        try (InputStream is = resource.openStream())
        {
            config = new String(ByteStreams.toByteArray(is), UTF_8);
        }
        catch (IOException e)
        {
            throw new IOException(String.format("Failed to load resource '%s'", resource.toExternalForm()), e);
        }
        final String newConfig = config.replace("AMQP/localhost", "AMQP/" + hostName)
                                       .replace("target/" + BROKER_KEYTAB, toAbsolutePath(BROKER_KEYTAB))
                                       .replace("target/" + CLIENT_KEYTAB, toAbsolutePath(CLIENT_KEYTAB));

        final Path file = Paths.get("target", LOGIN_CONFIG_RESOURCE);
        Files.write(file,
                    newConfig.getBytes(UTF_8),
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        return file.toRealPath(LinkOption.NOFOLLOW_LINKS);
    }

    private String toAbsolutePath(String fileName)
    {
        final Path path = Paths.get("target", fileName)
                               .toAbsolutePath()
                               .normalize();
        return path.toUri().getPath();
    }

    private static boolean isIBM()
    {
        return JvmVendor.getJvmVendor() == JvmVendor.IBM;
    }

}
