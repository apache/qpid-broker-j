/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.    
 *
 * 
 */
package org.apache.qpid.server.security.auth.database;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.security.auth.login.AccountNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.PasswordCredentialManagingAuthenticationProvider;
import org.apache.qpid.server.security.auth.manager.AbstractScramAuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ScramSHA1AuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ScramSHA256AuthenticationManager;
import org.apache.qpid.server.security.auth.sasl.PasswordSource;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Negotiator;
import org.apache.qpid.server.security.auth.sasl.plain.PlainNegotiator;
import org.apache.qpid.server.security.auth.sasl.scram.ScramNegotiator;
import org.apache.qpid.server.security.auth.sasl.scram.ScramSaslServerSourceAdapter;

/**
 * Represents a user database where the account information is stored in a simple flat file.
 *
 * The file is expected to be in the form: username:password username1:password1 ... usernamen:passwordn
 *
 * where a carriage return separates each username/password pair. Passwords are assumed to be in plain text.
 */
public class PlainPasswordFilePrincipalDatabase extends AbstractPasswordFilePrincipalDatabase<PlainUser>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(PlainPasswordFilePrincipalDatabase.class);
    private final List<String> _mechanisms = Collections.unmodifiableList(Arrays.asList(PlainNegotiator.MECHANISM,
                                                                                        CramMd5Negotiator.MECHANISM,
                                                                                        ScramSHA1AuthenticationManager.MECHANISM,
                                                                                        ScramSHA256AuthenticationManager.MECHANISM));
    private final ScramSaslServerSourceAdapter _scramSha1Adapter;
    private final ScramSaslServerSourceAdapter _scramSha256Adapter;


    public PlainPasswordFilePrincipalDatabase(PasswordCredentialManagingAuthenticationProvider<?> authenticationProvider)
    {
        super(authenticationProvider);

        PasswordSource passwordSource = getPasswordSource();
        final int scramIterationCount = authenticationProvider.getContextValue(Integer.class,
                                                                               AbstractScramAuthenticationManager.QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT);
        _scramSha1Adapter = new ScramSaslServerSourceAdapter(scramIterationCount,
                                                             ScramSHA1AuthenticationManager.HMAC_NAME,
                                                             ScramSHA1AuthenticationManager.DIGEST_NAME,
                                                             passwordSource);
        _scramSha256Adapter = new ScramSaslServerSourceAdapter(scramIterationCount,
                                                               ScramSHA256AuthenticationManager.HMAC_NAME,
                                                               ScramSHA256AuthenticationManager.DIGEST_NAME,
                                                               passwordSource);
    }


    /**
     * Used to verify that the presented Password is correct. Currently only used by Management Console
     *
     * @param principal The principal to authenticate
     * @param password  The plaintext password to check
     *
     * @return true if password is correct
     *
     * @throws AccountNotFoundException if the principal cannot be found
     */
    @Override
    public boolean verifyPassword(String principal, char[] password) throws AccountNotFoundException
    {

        char[] pwd = lookupPassword(principal);

        if (pwd == null)
        {
            throw new AccountNotFoundException("Unable to lookup the specified users password");
        }

        return compareCharArray(pwd, password);

    }

    @Override
    protected PlainUser createUserFromPassword(Principal principal, char[] passwd)
    {
        return new PlainUser(principal.getName(), passwd, getAuthenticationProvider());
    }


    @Override
    protected PlainUser createUserFromFileData(String[] result)
    {
        return new PlainUser(result, getAuthenticationProvider());
    }


    @Override
    protected Logger getLogger()
    {
        return LOGGER;
    }


    @Override
    public List<String> getMechanisms()
    {
        return _mechanisms;
    }

    @Override
    public SaslNegotiator createSaslNegotiator(final String mechanism, final SaslSettings saslSettings)
    {
        if (CramMd5Negotiator.MECHANISM.equals(mechanism))
        {
            return new CramMd5Negotiator(getAuthenticationProvider(),
                                         saslSettings.getLocalFQDN(),
                                         getPasswordSource());
        }
        else if (PlainNegotiator.MECHANISM.equals(mechanism))
        {
            return new PlainNegotiator(getAuthenticationProvider());
        }
        else if (ScramSHA1AuthenticationManager.MECHANISM.equals(mechanism))
        {
            return new ScramNegotiator(getAuthenticationProvider(), _scramSha1Adapter, ScramSHA1AuthenticationManager.MECHANISM);
        }
        else if (ScramSHA256AuthenticationManager.MECHANISM.equals(mechanism))
        {
            return new ScramNegotiator(getAuthenticationProvider(), _scramSha256Adapter, ScramSHA256AuthenticationManager.MECHANISM);
        }
        return null;
    }
}
