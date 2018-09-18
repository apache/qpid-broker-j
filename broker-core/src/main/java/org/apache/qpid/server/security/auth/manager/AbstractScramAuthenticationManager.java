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
package org.apache.qpid.server.security.auth.manager;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.SaslException;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.UsernamePrincipal;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.plain.PlainNegotiator;
import org.apache.qpid.server.security.auth.sasl.scram.ScramNegotiator;
import org.apache.qpid.server.security.auth.sasl.scram.ScramSaslServerSource;
import org.apache.qpid.server.util.Strings;

public abstract class AbstractScramAuthenticationManager<X extends AbstractScramAuthenticationManager<X>>
        extends ConfigModelPasswordManagingAuthenticationProvider<X>
        implements PasswordCredentialManagingAuthenticationProvider<X>, ScramSaslServerSource
{

    public static final String PLAIN = "PLAIN";
    private final SecureRandom _random = new SecureRandom();

    public static final String QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT = "qpid.auth.scram.iteration_count";
    @ManagedContextDefault(name = QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT)
    public static final int DEFAULT_ITERATION_COUNT = 4096;

    private int _iterationCount = DEFAULT_ITERATION_COUNT;
    private boolean _doNotCreateStoredPasswordBecauseItIsBeingUpgraded;


    protected AbstractScramAuthenticationManager(final Map<String, Object> attributes, final Broker broker)
    {
        super(attributes, broker);
    }

    @Override
    @StateTransition( currentState = { State.UNINITIALIZED, State.QUIESCED, State.QUIESCED }, desiredState = State.ACTIVE )
    protected ListenableFuture<Void> activate()
    {
        _iterationCount = getContextValue(Integer.class, QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT);
        for(ManagedUser user : getUserMap().values())
        {
            updateStoredPasswordFormatIfNecessary(user);
        }
        return super.activate();
    }

    @Override
    public List<String> getMechanisms()
    {
        return Collections.unmodifiableList(Arrays.asList(getMechanismName(), PLAIN));
    }

    protected abstract String getMechanismName();

    @Override
    public SaslNegotiator createSaslNegotiator(String mechanism,
                                               final SaslSettings saslSettings,
                                               final NamedAddressSpace addressSpace)
    {
        if(getMechanismName().equals(mechanism))
        {
            return new ScramNegotiator(this, this, getMechanismName());
        }
        else if(PLAIN.equals(mechanism))
        {
            return new PlainNegotiator(this);
        }
        else
        {
            return null;
        }
    }

    @Override
    public AuthenticationResult authenticate(final String username, final String password)
    {
        ManagedUser user = getUser(username);
        if(user != null)
        {
            updateStoredPasswordFormatIfNecessary(user);
            SaltAndPasswordKeys saltAndPasswordKeys = getSaltAndPasswordKeys(username);
            try
            {
                byte[] saltedPassword = createSaltedPassword(saltAndPasswordKeys.getSalt(), password, saltAndPasswordKeys.getIterationCount());
                byte[] clientKey = computeHmac(saltedPassword, "Client Key");

                byte[] storedKey = MessageDigest.getInstance(getDigestName()).digest(clientKey);

                byte[] serverKey = computeHmac(saltedPassword, "Server Key");

                if(Arrays.equals(saltAndPasswordKeys.getStoredKey(), storedKey)
                   && Arrays.equals(saltAndPasswordKeys.getServerKey(), serverKey))
                {
                    return new AuthenticationResult(new UsernamePrincipal(username, this));
                }
            }
            catch (IllegalArgumentException | NoSuchAlgorithmException | SaslException e)
            {
                return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR,e);
            }
        }

        return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
    }

    @Override
    public int getIterationCount()
    {
        return _iterationCount;
    }

    private static final byte[] INT_1 = new byte[]{0, 0, 0, 1};

    private void updateStoredPasswordFormatIfNecessary(final ManagedUser user)
    {
        final int oldDefaultIterationCount = 4096;
        final String[] passwordFields = user.getPassword().split(",");
        if (passwordFields.length == 2)
        {
            byte[] saltedPassword = Strings.decodeBase64(passwordFields[PasswordField.SALTED_PASSWORD.ordinal()]);

            try
            {
                byte[] clientKey = computeHmac(saltedPassword, "Client Key");

                byte[] storedKey = MessageDigest.getInstance(getDigestName()).digest(clientKey);

                byte[] serverKey = computeHmac(saltedPassword, "Server Key");

                String password = passwordFields[PasswordField.SALT.ordinal()] + ","
                                  + "," // remove previously insecure salted password field
                                  + Base64.getEncoder().encodeToString(storedKey) + ","
                                  + Base64.getEncoder().encodeToString(serverKey) + ","
                                  + oldDefaultIterationCount;
                upgradeUserPassword(user, password);
            }
            catch (NoSuchAlgorithmException e)
            {
                throw new IllegalArgumentException(e);
            }
        }
        else if (passwordFields.length == 4)
        {
            String password = passwordFields[PasswordField.SALT.ordinal()] + ","
                    + "," // remove previously insecure salted password field
                    + passwordFields[PasswordField.STORED_KEY.ordinal()] + ","
                    + passwordFields[PasswordField.SERVER_KEY.ordinal()] + ","
                    + oldDefaultIterationCount;
            upgradeUserPassword(user, password);
        }
        else if (passwordFields.length != 5)
        {
            throw new IllegalConfigurationException("password field for user '" + user.getName() + "' has unrecognised format.");
        }
    }

    private void upgradeUserPassword(final ManagedUser user, final String password)
    {
        try
        {
            _doNotCreateStoredPasswordBecauseItIsBeingUpgraded = true;
            user.setPassword(password);
        }
        finally
        {
            _doNotCreateStoredPasswordBecauseItIsBeingUpgraded = false;
        }
    }

    private byte[] createSaltedPassword(byte[] salt, String password, int iterationCount)
    {
        Mac mac = createShaHmac(password.getBytes(ASCII));

        mac.update(salt);
        mac.update(INT_1);
        byte[] result = mac.doFinal();

        byte[] previous = null;
        for(int i = 1; i < iterationCount; i++)
        {
            mac.update(previous != null? previous: result);
            previous = mac.doFinal();
            for(int x = 0; x < result.length; x++)
            {
                result[x] ^= previous[x];
            }
        }

        return result;

    }

    private byte[] computeHmac(final byte[] key, final String string)
    {
        Mac mac = createShaHmac(key);
        mac.update(string.getBytes(StandardCharsets.US_ASCII));
        return mac.doFinal();
    }

    private Mac createShaHmac(final byte[] keyBytes)
    {
        try
        {
            SecretKeySpec key = new SecretKeySpec(keyBytes, getHmacName());
            Mac mac = Mac.getInstance(getHmacName());
            mac.init(key);
            return mac;
        }
        catch (NoSuchAlgorithmException | InvalidKeyException e)
        {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    protected String createStoredPassword(final String password)
    {
        if (_doNotCreateStoredPasswordBecauseItIsBeingUpgraded)
        {
            return password;
        }

        try
        {
            final int iterationCount = getIterationCount();
            byte[] salt = generateSalt();
            byte[] saltedPassword = createSaltedPassword(salt, password, iterationCount);
            byte[] clientKey = computeHmac(saltedPassword, "Client Key");

            byte[] storedKey = MessageDigest.getInstance(getDigestName()).digest(clientKey);
            byte[] serverKey = computeHmac(saltedPassword, "Server Key");

            return Base64.getEncoder().encodeToString(salt) + ","
                   + "," // leave insecure salted password field blank
                   + Base64.getEncoder().encodeToString(storedKey) + ","
                   + Base64.getEncoder().encodeToString(serverKey) + ","
                   + iterationCount;
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    void validateUser(final ManagedUser managedUser)
    {
        if(!ASCII.newEncoder().canEncode(managedUser.getName()))
        {
            throw new IllegalArgumentException("User names are restricted to characters in the ASCII charset");
        }
    }

    @Override
    public SaltAndPasswordKeys getSaltAndPasswordKeys(final String username)
    {
        ManagedUser user = getUser(username);

        final byte[] salt;
        final byte[] storedKey;
        final byte[] serverKey;
        final int iterationCount;
        final SaslException exception;

        if(user == null)
        {
            // don't disclose that the user doesn't exist, just generate random data so the failure is indistinguishable
            // from the "wrong password" case.
            salt = generateSalt();
            storedKey = null;
            serverKey = null;
            iterationCount = getIterationCount();
            exception = new SaslException("Authentication Failed");
        }
        else
        {
            updateStoredPasswordFormatIfNecessary(user);
            final String[] passwordFields = user.getPassword().split(",");
            salt = Strings.decodeBase64(passwordFields[PasswordField.SALT.ordinal()]);
            storedKey = Strings.decodeBase64(passwordFields[PasswordField.STORED_KEY.ordinal()]);
            serverKey = Strings.decodeBase64(passwordFields[PasswordField.SERVER_KEY.ordinal()]);
            iterationCount = Integer.parseInt(passwordFields[PasswordField.ITERATION_COUNT.ordinal()]);
            exception = null;
        }

        return new SaltAndPasswordKeys()
        {
            @Override
            public byte[] getSalt()
            {
                return salt;
            }

            @Override
            public byte[] getStoredKey() throws SaslException
            {
                if(storedKey == null)
                {
                    throw exception;
                }
                return storedKey;
            }

            @Override
            public byte[] getServerKey() throws SaslException
            {
                if(serverKey == null)
                {
                    throw exception;
                }
                return serverKey;
            }

            @Override
            public int getIterationCount() throws SaslException
            {
                if(iterationCount < 0)
                {
                    throw exception;
                }
                return iterationCount;
            }
        };
    }

    private byte[] generateSalt()
    {
        byte[] tmpSalt = new byte[32];
        _random.nextBytes(tmpSalt);
        return tmpSalt;
    }

    private enum PasswordField
    {
        SALT, SALTED_PASSWORD, STORED_KEY, SERVER_KEY, ITERATION_COUNT
    }
}
