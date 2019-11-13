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
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5HashedNegotiator;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5HexNegotiator;
import org.apache.qpid.server.security.auth.sasl.plain.PlainNegotiator;

/**
 * Represents a user database where the account information is stored in a simple flat file.
 *
 * The file is expected to be in the form: username:password username1:password1 ... usernamen:passwordn
 *
 * where a carriage return separates each username/password pair. Passwords are assumed to be in plain text.
 */
public class Base64MD5PasswordFilePrincipalDatabase extends AbstractPasswordFilePrincipalDatabase<HashedUser>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Base64MD5PasswordFilePrincipalDatabase.class);
    private List<String> _mechanisms = Collections.unmodifiableList(Arrays.asList(CramMd5HashedNegotiator.MECHANISM,
                                                                                  CramMd5HexNegotiator.MECHANISM,
                                                                                  PlainNegotiator.MECHANISM));

    public Base64MD5PasswordFilePrincipalDatabase(final PasswordCredentialManagingAuthenticationProvider<?> authenticationProvider)
    {
        super(authenticationProvider);
    }


    /**
     * Used to verify that the presented Password is correct. Currently only used by Management Console
     *
     * @param principal The principal to authenticate
     * @param password  The password to check
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
        
        byte[] byteArray = new byte[password.length];
        int index = 0;
        for (char c : password)
        {
            byteArray[index++] = (byte) c;
        }
        
        byte[] md5byteArray;
        try
        {
            md5byteArray = HashedUser.getMD5(byteArray);
        }
        catch (Exception e1)
        {
            getLogger().warn("Unable to hash password for user '{}' for comparison", principal);
            return false;
        }
        
        char[] hashedPassword = new char[md5byteArray.length];

        index = 0;
        for (byte c : md5byteArray)
        {
            hashedPassword[index++] = (char) c;
        }

        return compareCharArray(pwd, hashedPassword);
    }

    @Override
    protected HashedUser createUserFromPassword(Principal principal, char[] passwd)
    {
        return new HashedUser(principal.getName(), passwd, getAuthenticationProvider());
    }


    @Override
    protected HashedUser createUserFromFileData(String[] result)
    {
        return new HashedUser(result, getAuthenticationProvider());
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
        if(CramMd5HashedNegotiator.MECHANISM.equals(mechanism))
        {
            return new CramMd5HashedNegotiator(getAuthenticationProvider(),
                                               saslSettings.getLocalFQDN(),
                                               getPasswordSource());
        }
        else if(CramMd5HexNegotiator.MECHANISM.equals(mechanism))
        {
            return new CramMd5HexNegotiator(getAuthenticationProvider(),
                                                 saslSettings.getLocalFQDN(),
                                                 getPasswordSource());
        }
        else if(PlainNegotiator.MECHANISM.equals(mechanism))
        {
            return new PlainNegotiator(getAuthenticationProvider());
        }
        return null;
    }

}
