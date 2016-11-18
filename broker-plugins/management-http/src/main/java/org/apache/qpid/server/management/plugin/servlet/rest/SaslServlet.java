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
import java.security.Principal;
import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.security.auth.Subject;
import javax.security.sasl.SaslServer;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.security.SubjectCreator;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.SubjectAuthenticationResult;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;
import org.apache.qpid.util.Strings;

public class SaslServlet extends AbstractServlet
{
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslServlet.class);

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String ATTR_RANDOM = "SaslServlet.Random";
    private static final String ATTR_ID = "SaslServlet.ID";
    private static final String ATTR_SASL_SERVER = "SaslServlet.SaslServer";
    private static final String ATTR_EXPIRY = "SaslServlet.Expiry";
    private static final long SASL_EXCHANGE_EXPIRY = 3000L;

    public SaslServlet()
    {
        super();
    }

    protected void doGetWithSubjectAndActor(HttpServletRequest request, HttpServletResponse response) throws
                                                                                   ServletException,
                                                                                   IOException
    {
        getRandom(request);

        SubjectCreator subjectCreator = getSubjectCreator(request);
        List<String> mechanismsList = subjectCreator.getMechanisms();
        String[] mechanisms = mechanismsList.toArray(new String[mechanismsList.size()]);
        Map<String, Object> outputObject = new LinkedHashMap<String, Object>();

        final Subject subject = getAuthorisedSubject(request);
        if(subject != null)
        {
            Principal principal = AuthenticatedPrincipal.getAuthenticatedPrincipalFromSubject(subject);
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
        Random rand = (Random) session.getAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_RANDOM,
                                                                                                       request));
        if(rand == null)
        {
            synchronized (SECURE_RANDOM)
            {
                rand = new Random(SECURE_RANDOM.nextLong());
            }
            session.setAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_RANDOM, request), rand);
        }
        return rand;
    }


    @Override
    protected void doPostWithSubjectAndActor(final HttpServletRequest request, final HttpServletResponse response) throws IOException
    {
        checkSaslAuthEnabled(request);

        final HttpSession session = request.getSession();
        try
        {
            String mechanism = request.getParameter("mechanism");
            String id = request.getParameter("id");
            String saslResponse = request.getParameter("response");

            SubjectCreator subjectCreator = getSubjectCreator(request);

            if(mechanism != null)
            {
                if(id == null)
                {
                    LOGGER.debug("Creating SaslServer for mechanism: {}", mechanism);

                    SaslServer saslServer = subjectCreator.createSaslServer(mechanism, request.getServerName(), null/*TODO*/);
                    evaluateSaslResponse(request, response, session, saslResponse, saslServer, subjectCreator);
                }
                else
                {
                    response.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
                    session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_ID, request));
                    session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_SASL_SERVER,
                                                                                               request));
                    session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_EXPIRY, request));
                }
            }
            else
            {
                if(id != null)
                {
                    if(id.equals(session.getAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_ID,
                                                                                                         request))) && System.currentTimeMillis() < (Long) session.getAttribute(
                            HttpManagementUtil.getRequestSpecificAttributeName(ATTR_EXPIRY, request)))
                    {
                        SaslServer saslServer = (SaslServer) session.getAttribute(HttpManagementUtil.getRequestSpecificAttributeName(
                                ATTR_SASL_SERVER,
                                request));
                        evaluateSaslResponse(request, response, session, saslResponse, saslServer, subjectCreator);
                    }
                    else
                    {
                        response.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
                        session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_ID, request));
                        session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_SASL_SERVER,
                                                                                                   request));
                        session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_EXPIRY,
                                                                                                   request));
                    }
                }
                else
                {
                    response.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
                    session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_ID, request));
                    session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_SASL_SERVER,
                                                                                               request));
                    session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_EXPIRY, request));
                }
            }
        }
        finally
        {
            if (response.getStatus() != HttpServletResponse.SC_OK)
            {
                session.invalidate();
            }
        }
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
                                      final HttpSession session, final String saslResponse, final SaslServer saslServer, SubjectCreator subjectCreator) throws IOException
    {
        byte[] saslResponseBytes = saslResponse == null
                ? new byte[0]
                : Strings.decodeBase64(saslResponse);
        SubjectAuthenticationResult authenticationResult = subjectCreator.authenticate(saslServer, saslResponseBytes);
        byte[] challenge = authenticationResult.getChallenge();
        Map<String, Object> outputObject = new LinkedHashMap<>();
        int responseStatus;

        if (authenticationResult.getStatus() == AuthenticationResult.AuthenticationStatus.SUCCESS)
        {
            Subject original = authenticationResult.getSubject();
            Broker broker = getBroker();
            try
            {
                HttpManagementUtil.assertManagementAccess(broker, original);
                Subject subject = HttpManagementUtil.createServletConnectionSubject(request, original);

                HttpManagementUtil.saveAuthorisedSubject(request, subject);
                session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_ID, request));
                session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_SASL_SERVER, request));
                session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_EXPIRY, request));
                if(challenge != null && challenge.length != 0)
                {
                    outputObject.put("challenge", DatatypeConverter.printBase64Binary(challenge));
                }
                responseStatus = HttpServletResponse.SC_OK;
            }
            catch(SecurityException e)
            {
                responseStatus = HttpServletResponse.SC_FORBIDDEN;
            }
        }
        else if (authenticationResult.getStatus() == AuthenticationResult.AuthenticationStatus.CONTINUE)
        {
            Random rand = getRandom(request);
            String id = String.valueOf(rand.nextLong());
            session.setAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_ID, request), id);
            session.setAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_SASL_SERVER, request), saslServer);
            session.setAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_EXPIRY, request), System.currentTimeMillis() + SASL_EXCHANGE_EXPIRY);

            outputObject.put("id", id);
            outputObject.put("challenge", DatatypeConverter.printBase64Binary(challenge));
            responseStatus = HttpServletResponse.SC_OK;
        }
        else
        {
            session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_ID, request));
            session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_SASL_SERVER, request));
            session.removeAttribute(HttpManagementUtil.getRequestSpecificAttributeName(ATTR_EXPIRY, request));
            responseStatus = HttpServletResponse.SC_UNAUTHORIZED;
        }

        sendJsonResponse(outputObject, request, response, responseStatus, false);
    }

    private SubjectCreator getSubjectCreator(HttpServletRequest request)
    {
        return HttpManagementUtil.getManagementConfiguration(getServletContext()).getAuthenticationProvider(request).getSubjectCreator(
                request.isSecure());
    }

    @Override
    protected Subject getAuthorisedSubject(HttpServletRequest request)
    {
        Subject subject = HttpManagementUtil.getAuthorisedSubject(request);
        if(subject == null)
        {
            subject = HttpManagementUtil.tryToAuthenticate(request, HttpManagementUtil.getManagementConfiguration(getServletContext()));
        }
        return subject;
    }

}
