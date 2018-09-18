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
package org.apache.qpid.server.management.plugin.servlet.rest;

import java.io.IOException;
import java.security.AccessController;
import java.security.Principal;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.security.auth.Subject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.SessionInvalidatedException;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.server.util.Strings;

public class SaslServlet extends AbstractServlet
{
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslServlet.class);

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String ATTR_RANDOM = "SaslServlet.Random";
    private static final String ATTR_ID = "SaslServlet.ID";
    private static final String ATTR_SASL_NEGOTIATOR = "SaslServlet.SaslNegotiator";
    private static final String ATTR_EXPIRY = "SaslServlet.Expiry";

    public SaslServlet()
    {
        super();
    }




    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response,
                         final ConfiguredObject<?> managedObject) throws ServletException, IOException
    {
        getRandom(request);

        AuthenticationProvider<?> authenticationProvider = getAuthenticationProvider(request);
        List<String> mechanismsList = authenticationProvider.getAvailableMechanisms(request.isSecure());
        String[] mechanisms = mechanismsList.toArray(new String[mechanismsList.size()]);
        Map<String, Object> outputObject = new LinkedHashMap<String, Object>();

        final Subject subject = Subject.getSubject(AccessController.getContext());
        final Principal principal = AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(subject);
        if(principal != null)
        {
            outputObject.put("user", principal.getName());
        }
        else if (request.getRemoteUser() != null)
        {
            outputObject.put("user", request.getRemoteUser());
        }

        outputObject.put("mechanisms", (Object) mechanisms);

        sendJsonResponse(outputObject, request, response);

    }

    private Random getRandom(final HttpServletRequest request)
    {
        HttpSession session = request.getSession();
        Random rand = (Random) HttpManagementUtil.getSessionAttribute(ATTR_RANDOM, session, request);

        if(rand == null)
        {
            synchronized (SECURE_RANDOM)
            {
                rand = new Random(SECURE_RANDOM.nextLong());
            }
            HttpManagementUtil.setSessionAttribute(ATTR_RANDOM, rand, session, request);
        }
        return rand;
    }


    @Override
    protected void doPost(final HttpServletRequest request,
                          final HttpServletResponse response,
                          final ConfiguredObject<?> managedObject) throws IOException
    {
        checkSaslAuthEnabled(request);

        final HttpSession session = request.getSession();
        try
        {
            String mechanism = request.getParameter("mechanism");
            String id = request.getParameter("id");
            String saslResponse = request.getParameter("response");

            SubjectCreator subjectCreator = getSubjectCreator(request);
            AuthenticationProvider<?> authenticationProvider = getAuthenticationProvider(request);
            SaslNegotiator saslNegotiator = null;
            if(mechanism != null)
            {
                if(id == null && authenticationProvider.getAvailableMechanisms(request.isSecure()).contains(mechanism))
                {
                    LOGGER.debug("Creating SaslServer for mechanism: {}", mechanism);

                    saslNegotiator = subjectCreator.createSaslNegotiator(mechanism, new SaslSettings()
                    {
                        @Override
                        public String getLocalFQDN()
                        {
                            return request.getServerName();
                        }

                        @Override
                        public Principal getExternalPrincipal()
                        {
                            return null/*TODO*/;
                        }
                    });
                }
            }
            else
            {
                if(id != null)
                {
                    if (id.equals(HttpManagementUtil.getSessionAttribute(ATTR_ID, session, request))
                        && System.currentTimeMillis() < (Long) HttpManagementUtil.getSessionAttribute(ATTR_EXPIRY, session, request))
                    {
                        saslNegotiator = (SaslNegotiator) HttpManagementUtil.getSessionAttribute(ATTR_SASL_NEGOTIATOR,
                                                                                                 session,
                                                                                                 request);
                    }
                }
            }

            if (saslNegotiator != null)
            {
                evaluateSaslResponse(request, response, session, saslResponse, saslNegotiator, subjectCreator);
            }
            else
            {
                cleanup(request, session);
                response.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
            }
        }
        catch (SessionInvalidatedException e)
        {
            response.setStatus(HttpServletResponse.SC_PRECONDITION_FAILED);
        }
        finally
        {
            if (response.getStatus() != HttpServletResponse.SC_OK)
            {
                HttpManagementUtil.invalidateSession(session);
            }
        }
    }

    private void cleanup(final HttpServletRequest request, final HttpSession session)
    {
        final SaslNegotiator negotiator =
                (SaslNegotiator) HttpManagementUtil.getSessionAttribute(ATTR_SASL_NEGOTIATOR, session, request);
        if (negotiator != null)
        {
            negotiator.dispose();
        }
        HttpManagementUtil.removeAttribute(ATTR_ID, session, request);
        HttpManagementUtil.removeAttribute(ATTR_SASL_NEGOTIATOR, session, request);
        HttpManagementUtil.removeAttribute(ATTR_EXPIRY, session, request);
    }

    private void checkSaslAuthEnabled(HttpServletRequest request)
    {
        boolean saslAuthEnabled = false;
        HttpManagementConfiguration management = getManagementConfiguration();
        if (request.isSecure())
        {
            saslAuthEnabled = management.isHttpsSaslAuthenticationEnabled();
        }
        else
        {
            saslAuthEnabled = management.isHttpSaslAuthenticationEnabled();
        }
        if (!saslAuthEnabled)
        {
            throw new ConnectionScopedRuntimeException("Sasl authentication disabled.");
        }
    }

    private void evaluateSaslResponse(final HttpServletRequest request,
                                      final HttpServletResponse response,
                                      final HttpSession session,
                                      final String saslResponse,
                                      final SaslNegotiator saslNegotiator,
                                      SubjectCreator subjectCreator) throws IOException
    {
        byte[] saslResponseBytes = saslResponse == null
                ? new byte[0]
                : Strings.decodeBase64(saslResponse);
        SubjectAuthenticationResult authenticationResult = subjectCreator.authenticate(saslNegotiator, saslResponseBytes);
        byte[] challenge = authenticationResult.getChallenge();
        Map<String, Object> outputObject = new LinkedHashMap<>();
        int responseStatus = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

        if (authenticationResult.getStatus() == AuthenticationResult.AuthenticationStatus.SUCCESS)
        {
            Subject original = authenticationResult.getSubject();
            Broker broker = getBroker();
            try
            {
                Subject subject = HttpManagementUtil.createServletConnectionSubject(request, original);
                HttpManagementUtil.assertManagementAccess(broker, subject);
                HttpManagementUtil.saveAuthorisedSubject(request, subject);
                if(challenge != null && challenge.length != 0)
                {
                    outputObject.put("additionalData", Base64.getEncoder().encodeToString(challenge));
                }
                responseStatus = HttpServletResponse.SC_OK;
            }
            catch(SecurityException e)
            {
                responseStatus = HttpServletResponse.SC_FORBIDDEN;
            }
            finally
            {
                cleanup(request, session);
            }
        }
        else if (authenticationResult.getStatus() == AuthenticationResult.AuthenticationStatus.CONTINUE)
        {
            Random rand = getRandom(request);
            String id = String.valueOf(rand.nextLong());
            HttpManagementUtil.setSessionAttribute(ATTR_ID, id, session, request);
            HttpManagementUtil.setSessionAttribute(ATTR_SASL_NEGOTIATOR, saslNegotiator, session, request);
            long saslExchangeExpiry = getManagementConfiguration().getSaslExchangeExpiry();
            HttpManagementUtil.setSessionAttribute(ATTR_EXPIRY,
                                                   System.currentTimeMillis() + saslExchangeExpiry,
                                                   session, request);

            outputObject.put("id", id);
            outputObject.put("challenge", Base64.getEncoder().encodeToString(challenge));
            responseStatus = HttpServletResponse.SC_OK;
        }
        else
        {
            responseStatus = HttpServletResponse.SC_UNAUTHORIZED;
            cleanup(request, session);
        }

        sendJsonResponse(outputObject, request, response, responseStatus, false);
    }

    private SubjectCreator getSubjectCreator(HttpServletRequest request)
    {
        final Port<?> port = HttpManagementUtil.getPort(request);

        return port.getSubjectCreator(request.isSecure(), request.getServerName());
    }

    private AuthenticationProvider<?> getAuthenticationProvider(final HttpServletRequest request)
    {
        return HttpManagementUtil.getManagementConfiguration(getServletContext()).getAuthenticationProvider(request);
    }
}
