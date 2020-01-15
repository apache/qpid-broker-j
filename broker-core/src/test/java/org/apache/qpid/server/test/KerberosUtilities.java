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

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

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

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.util.FileUtils;

public class KerberosUtilities
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KerberosUtilities.class);
    private static final String IBM_LOGIN_MODULE_CLASS = "com.ibm.security.auth.module.Krb5LoginModule";
    private static final String SUN_LOGIN_MODULE_CLASS = "com.sun.security.auth.module.Krb5LoginModule";
    public static final String KERBEROS_LOGIN_MODULE_CLASS =
            System.getProperty("java.vendor").contains("IBM") ? IBM_LOGIN_MODULE_CLASS : SUN_LOGIN_MODULE_CLASS;

    public byte[] buildToken(String clientPrincipalName, String targetServerPrincipalName) throws GSSException
    {
        debugConfig();

        debug("Building token for client principal '{}' and server principal '{}'",
              clientPrincipalName,
              targetServerPrincipalName);

        final GSSManager manager = GSSManager.getInstance();
        final GSSName clientName = manager.createName(clientPrincipalName, GSSName.NT_USER_NAME);
        final GSSCredential credential = manager.createCredential(clientName,
                                                                  GSSCredential.DEFAULT_LIFETIME,
                                                                  new Oid("1.2.840.113554.1.2.2"),
                                                                  GSSCredential.INITIATE_ONLY);

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
            options.put("useKeyTab", TRUE.toString());
            options.put("keyTab", keyTabFile.getAbsolutePath());
            options.put("refreshKrb5Config", TRUE.toString());
            options.put("doNotPrompt", TRUE.toString());
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

    public static void debugConfig()
    {
        if (LOGGER.isDebugEnabled())
        {
            final String krb5Conf = System.getProperty("java.security.krb5.conf");
            if (krb5Conf != null)
            {
                final File file = new File(krb5Conf);
                if (file.exists())
                {
                    String config = FileUtils.readFileAsString(file);
                    debug("Kerberos config: {}", config);
                }
                else
                {
                    LOGGER.warn("Kerberos config file was not found in the expected location at '{}'", krb5Conf);
                }
            }
            else
            {
                LOGGER.warn("JVM system property 'java.security.krb5.conf' is not set");
            }

            Map<String, String> env = new LinkedHashMap<>(System.getenv());
            env.forEach((k, v) -> debug("Environment setting: {}={}", k ,v));

            Properties properties = new Properties(System.getProperties());
            properties.forEach((k, v) -> debug("JVM setting: {}={}", k ,v));
        }
    }

    public static void debug(String message, Object... args)
    {
        LOGGER.debug(message, args);
        if (Boolean.TRUE.toString().equalsIgnoreCase(System.getProperty("sun.security.krb5.debug")))
        {
            System.out.println(String.format(message.replace("{}", "%s"), args));
        }
    }

}
