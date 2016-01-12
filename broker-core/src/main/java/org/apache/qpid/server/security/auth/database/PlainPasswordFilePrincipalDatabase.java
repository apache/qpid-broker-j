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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.AccountNotFoundException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.security.auth.manager.ScramSHA1AuthenticationManager;
import org.apache.qpid.server.security.auth.manager.ScramSHA256AuthenticationManager;
import org.apache.qpid.server.security.auth.sasl.crammd5.CRAMMD5Initialiser;
import org.apache.qpid.server.security.auth.sasl.plain.PlainInitialiser;
import org.apache.qpid.server.security.auth.sasl.plain.PlainSaslServer;
import org.apache.qpid.server.security.auth.sasl.scram.ScramSaslServer;
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

    private final Logger _logger = LoggerFactory.getLogger(PlainPasswordFilePrincipalDatabase.class);
    private final Map<String, CallbackHandler> _callbackHandlerMap = new HashMap<String, CallbackHandler>();
    private final List<String> _mechanisms = Collections.unmodifiableList(Arrays.asList(PlainSaslServer.MECHANISM,
                                                                                        CRAMMD5Initialiser.MECHANISM,
                                                                                        ScramSHA1AuthenticationManager.MECHANISM,
                                                                                        ScramSHA256AuthenticationManager.MECHANISM));
    private final ScramSaslServerSourceAdapter _scramSha1Adapter;
    private final ScramSaslServerSourceAdapter _scramSha256Adapter;


    public PlainPasswordFilePrincipalDatabase()
    {
        PlainInitialiser plainInitialiser = new PlainInitialiser();
        plainInitialiser.initialise(this);
        _callbackHandlerMap.put(PlainSaslServer.MECHANISM, plainInitialiser.getCallbackHandler());
        _callbackHandlerMap.put(ScramSHA1AuthenticationManager.MECHANISM, plainInitialiser.getCallbackHandler());
        _callbackHandlerMap.put(ScramSHA256AuthenticationManager.MECHANISM, plainInitialiser.getCallbackHandler());


        CRAMMD5Initialiser crammd5Initialiser = new CRAMMD5Initialiser();
        crammd5Initialiser.initialise(this);
        _callbackHandlerMap.put(CRAMMD5Initialiser.MECHANISM, crammd5Initialiser.getCallbackHandler());

        ScramSaslServerSourceAdapter.PasswordSource passwordSource =
                new ScramSaslServerSourceAdapter.PasswordSource()
                {
                    @Override
                    public char[] getPassword(final String username)
                    {
                        return lookupPassword(username);
                    }
                };

        _scramSha1Adapter = new ScramSaslServerSourceAdapter(4096, "HmacSHA1", "SHA-1", passwordSource);
        _scramSha256Adapter = new ScramSaslServerSourceAdapter(4096, "HmacSHA256", "SHA-256", passwordSource);


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
    public boolean verifyPassword(String principal, char[] password) throws AccountNotFoundException
    {

        char[] pwd = lookupPassword(principal);
        
        if (pwd == null)
        {
            throw new AccountNotFoundException("Unable to lookup the specified users password");
        }

        return compareCharArray(pwd, password);

    }

    protected PlainUser createUserFromPassword(Principal principal, char[] passwd)
    {
        return new PlainUser(principal.getName(), passwd);
    }


    @Override
    protected PlainUser createUserFromFileData(String[] result)
    {
        return new PlainUser(result);
    }


    protected Logger getLogger()
    {
        return _logger;
    }


    @Override
    public List<String> getMechanisms()
    {
        return _mechanisms;
    }

    @Override
    public SaslServer createSaslServer(String mechanism, String localFQDN, Principal externalPrincipal) throws SaslException
    {
        final CallbackHandler callbackHandler = _callbackHandlerMap.get(mechanism);
        if(callbackHandler == null)
        {
            throw new SaslException("Unsupported mechanism: " + mechanism);
        }

        if(CRAMMD5Initialiser.MECHANISM.equals(mechanism))
        {
            //simply delegate to the built in CRAM-MD5 SaslServer
            return Sasl.createSaslServer(mechanism, "AMQP", localFQDN, null, callbackHandler);
        }
        else if(PlainSaslServer.MECHANISM.equals(mechanism))
        {
            return new PlainSaslServer(callbackHandler);
        }
        else if(ScramSHA1AuthenticationManager.MECHANISM.equals(mechanism))
        {
            return new ScramSaslServer(_scramSha1Adapter, mechanism, "HmacSHA1", "SHA-1");
        }
        else if(ScramSHA256AuthenticationManager.MECHANISM.equals(mechanism))
        {
            return new ScramSaslServer(_scramSha256Adapter, mechanism, "HmacSHA256", "SHA-256");
        }

        throw new SaslException("Unsupported mechanism: " + mechanism);
    }

}
