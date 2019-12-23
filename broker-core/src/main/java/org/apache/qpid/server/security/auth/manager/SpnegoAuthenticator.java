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

package org.apache.qpid.server.security.auth.manager;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.google.common.base.StandardSystemProperty;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.security.TokenCarryingPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;

public class SpnegoAuthenticator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SpnegoAuthenticator.class);
    public static final String REQUEST_AUTH_HEADER_NAME = "Authorization";
    public static final String RESPONSE_AUTH_HEADER_NAME = "WWW-Authenticate";
    public static final String RESPONSE_AUTH_HEADER_VALUE_NEGOTIATE = "Negotiate";
    public static final String AUTH_TYPE = "SPNEGO";
    static final String NEGOTIATE_PREFIX = "Negotiate ";
    private final KerberosAuthenticationManager _kerberosProvider;

    SpnegoAuthenticator(final KerberosAuthenticationManager kerberosProvider)
    {
        _kerberosProvider = kerberosProvider;
    }

    public AuthenticationResult authenticate(String authorizationHeader)
    {
        if (authorizationHeader == null)
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("'Authorization' header is not set");
            }
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
        }
        else
        {
            if (!hasNegotiatePrefix(authorizationHeader))
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("'Authorization' header value does not start with '{}'", NEGOTIATE_PREFIX);
                }
                return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
            }
            else
            {
                final String negotiateToken = authorizationHeader.substring(NEGOTIATE_PREFIX.length());
                final byte[] decodedNegotiateHeader;
                try
                {
                    decodedNegotiateHeader = Base64.getDecoder().decode(negotiateToken);
                }
                catch (RuntimeException e)
                {
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Ticket decoding failed", e);
                    }
                    return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
                }

                if (decodedNegotiateHeader.length == 0)
                {
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Empty ticket");
                    }
                    return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
                }
                else
                {
                    return authenticate(decodedNegotiateHeader);
                }
            }
        }
    }

    private boolean hasNegotiatePrefix(final String authorizationHeader)
    {
        if (authorizationHeader.length() > NEGOTIATE_PREFIX.length() )
        {
            return NEGOTIATE_PREFIX.equalsIgnoreCase(authorizationHeader.substring(0, NEGOTIATE_PREFIX.length()));
        }
        return false;
    }

    public AuthenticationResult authenticate(byte[] negotiateToken)
    {
        LoginContext loginContext = null;
        try
        {
            loginContext = new LoginContext(_kerberosProvider.getSpnegoLoginConfigScope());
            loginContext.login();
            Subject subject = loginContext.getSubject();

            return doAuthenticate(subject, negotiateToken);
        }
        catch (LoginException e)
        {
            LOGGER.error("JASS login failed", e);
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
        finally
        {
            if (loginContext != null)
            {
                try
                {
                    loginContext.logout();
                }
                catch (LoginException e)
                {
                    // Ignore
                }
            }
        }
    }

    private AuthenticationResult doAuthenticate(final Subject subject, final byte[] negotiateToken)
    {
        GSSContext context = null;
        try
        {

            final int credentialLifetime;
            if (String.valueOf(System.getProperty(StandardSystemProperty.JAVA_VENDOR.key()))
                      .toUpperCase()
                      .contains("IBM"))
            {
                credentialLifetime = GSSCredential.INDEFINITE_LIFETIME;
            }
            else
            {
                credentialLifetime = GSSCredential.DEFAULT_LIFETIME;
            }

            final GSSManager manager = GSSManager.getInstance();
            final PrivilegedExceptionAction<GSSCredential> credentialsAction =
                    () -> manager.createCredential(null,
                                                   credentialLifetime,
                                                   new Oid("1.3.6.1.5.5.2"),
                                                   GSSCredential.ACCEPT_ONLY);
            final GSSContext gssContext = manager.createContext(Subject.doAs(subject, credentialsAction));
            context = gssContext;

            final PrivilegedExceptionAction<byte[]> acceptAction =
                    () -> gssContext.acceptSecContext(negotiateToken, 0, negotiateToken.length);
            final byte[] outToken = Subject.doAs(subject, acceptAction);

            if (outToken == null)
            {
                LOGGER.debug("Ticket validation failed");
                return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
            }

            final PrivilegedAction<String> authenticationAction = () -> {
                if (gssContext.isEstablished())
                {
                    GSSName gssName = null;
                    try
                    {
                        gssName = gssContext.getSrcName();
                    }
                    catch (final GSSException e)
                    {
                        LOGGER.error("Unable to get src name from gss context", e);
                    }

                    if (gssName != null)
                    {
                        return stripRealmNameIfRequired(gssName.toString());
                    }
                }
                return null;
            };
            final String principalName = Subject.doAs(subject, authenticationAction);
            if (principalName != null)
            {
                TokenCarryingPrincipal principal = new TokenCarryingPrincipal()
                {

                    private Map<String, String> _tokens = Collections.singletonMap(RESPONSE_AUTH_HEADER_NAME,
                                                                                   NEGOTIATE_PREFIX + Base64.getEncoder()
                                                                                                            .encodeToString(outToken));

                    @Override
                    public Map<String, String> getTokens()
                    {
                        return _tokens;
                    }

                    @Override
                    public ConfiguredObject<?> getOrigin()
                    {
                        return _kerberosProvider;
                    }

                    @Override
                    public String getName()
                    {
                        return principalName;
                    }

                    @Override
                    public boolean equals(final Object o)
                    {
                        if (this == o)
                        {
                            return true;
                        }
                        if (!(o instanceof TokenCarryingPrincipal))
                        {
                            return false;
                        }

                        final TokenCarryingPrincipal that = (TokenCarryingPrincipal) o;

                        if (!getName().equals(that.getName()))
                        {
                            return false;
                        }

                        if (!getTokens().equals(that.getTokens()))
                        {
                            return false;
                        }
                        return getOrigin() != null ? getOrigin().equals(that.getOrigin()) : that.getOrigin() == null;
                    }

                    @Override
                    public int hashCode()
                    {
                        int result = getName().hashCode();
                        result = 31 * result + (getOrigin() != null ? getOrigin().hashCode() : 0);
                        result = 31 * result + getTokens().hashCode();
                        return result;
                    }

                };
                return new AuthenticationResult(principal);
            }
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
        }
        catch (GSSException e)
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Ticket validation failed", e);
            }
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
        catch (PrivilegedActionException e)
        {
            final Exception cause = e.getException();
            if (cause instanceof GSSException)
            {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Service login failed", e);
                }
            }
            else
            {
                LOGGER.error("Service login failed", e);
            }
            return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, e);
        }
        finally
        {
            if (context != null)
            {
                try
                {
                    context.dispose();
                }
                catch (GSSException e)
                {
                    // Ignore
                }
            }
        }
    }

    private String stripRealmNameIfRequired(String name)
    {
        if (_kerberosProvider.isStripRealmFromPrincipalName() && name != null)
        {
            final int i = name.indexOf('@');
            if (i > 0)
            {
                name = name.substring(0, i);
            }
        }
        return name;
    }
}
