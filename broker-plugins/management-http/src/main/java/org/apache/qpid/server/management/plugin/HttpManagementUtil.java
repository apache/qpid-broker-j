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
package org.apache.qpid.server.management.plugin;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import javax.security.auth.Subject;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.qpid.server.management.plugin.servlet.ServletConnectionPrincipal;
import org.apache.qpid.server.management.plugin.session.LoginLogoutReporter;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.security.access.Operation;

public class HttpManagementUtil
{

    /**
     * Servlet context attribute holding a reference to a broker instance
     */
    public static final String ATTR_BROKER = "Qpid.broker";

    /**
     * Servlet context attribute holding a reference to plugin configuration
     */
    public static final String ATTR_MANAGEMENT_CONFIGURATION = "Qpid.managementConfiguration";

    private static final String ATTR_LOGIN_LOGOUT_REPORTER = "Qpid.loginLogoutReporter";
    private static final String ATTR_SUBJECT = "Qpid.subject";
    private static final String ATTR_LOG_ACTOR = "Qpid.logActor";

    public static final String ACCEPT_ENCODING_HEADER = "Accept-Encoding";
    public static final String CONTENT_ENCODING_HEADER = "Content-Encoding";
    public static final String GZIP_CONTENT_ENCODING = "gzip";

    private static final Collection<HttpRequestPreemptiveAuthenticator> AUTHENTICATORS;
    private static final Operation MANAGE_ACTION = Operation.ACTION("manage");

    static
    {
        List<HttpRequestPreemptiveAuthenticator> authenticators = new ArrayList<>();
        for(HttpRequestPreemptiveAuthenticator authenticator : (new QpidServiceLoader()).instancesOf(HttpRequestPreemptiveAuthenticator.class))
        {
            authenticators.add(authenticator);
        }
        AUTHENTICATORS = Collections.unmodifiableList(authenticators);
    }

    public static String getRequestSpecificAttributeName(String name, HttpServletRequest request)
    {
        return name + "." + HttpManagement.getPort(request).getId();
    }

    public static Broker<?> getBroker(ServletContext servletContext)
    {
        return (Broker<?>) servletContext.getAttribute(ATTR_BROKER);
    }

    public static HttpManagementConfiguration getManagementConfiguration(ServletContext servletContext)
    {
        return (HttpManagementConfiguration) servletContext.getAttribute(ATTR_MANAGEMENT_CONFIGURATION);
    }

    public static Subject getAuthorisedSubject(HttpServletRequest request)
    {
        HttpSession session = request.getSession(false);
        return (session == null ? null : (Subject) session.getAttribute(getRequestSpecificAttributeName(ATTR_SUBJECT,request)));
    }

    public static Subject createServletConnectionSubject(final HttpServletRequest request, Subject original)
    {
        Subject subject = new Subject(false,
                              original.getPrincipals(),
                              original.getPublicCredentials(),
                              original.getPrivateCredentials());
        subject.getPrincipals().add(new ServletConnectionPrincipal(request));
        subject.setReadOnly();
        return subject;
    }

    public static void assertManagementAccess(final Broker<?> broker, Subject subject)
    {
        Subject.doAs(subject, new PrivilegedAction<Void>()
        {
            @Override
            public Void run()
            {
                broker.authorise(MANAGE_ACTION);
                return null;
            }
        });
    }

    public static void saveAuthorisedSubject(HttpServletRequest request, Subject subject)
    {
        HttpSession session = request.getSession();
        session.setAttribute(getRequestSpecificAttributeName(ATTR_SUBJECT, request), subject);

        // Cause the user logon to be logged.
        session.setAttribute(getRequestSpecificAttributeName(ATTR_LOGIN_LOGOUT_REPORTER, request),
                             new LoginLogoutReporter(subject, getBroker(session.getServletContext())));
    }

    public static Subject tryToAuthenticate(HttpServletRequest request, HttpManagementConfiguration managementConfig)
    {
        Subject subject = null;
        for(HttpRequestPreemptiveAuthenticator authenticator : AUTHENTICATORS)
        {
            subject = authenticator.attemptAuthentication(request, managementConfig);
            if(subject != null)
            {
                break;
            }
        }
        return subject;
    }

    public static OutputStream getOutputStream(final HttpServletRequest request, final HttpServletResponse response)
            throws IOException
    {
        return getOutputStream(request, response, getManagementConfiguration(request.getServletContext()));
    }

    public static OutputStream getOutputStream(final HttpServletRequest request, final HttpServletResponse response, HttpManagementConfiguration managementConfiguration)
            throws IOException
    {
        OutputStream outputStream;
        if(isCompressingAccepted(request, managementConfiguration))
        {
            outputStream = new GZIPOutputStream(response.getOutputStream());
            response.setHeader(CONTENT_ENCODING_HEADER, GZIP_CONTENT_ENCODING);
        }
        else
        {
            outputStream = response.getOutputStream();
        }
        return outputStream;
    }

    public static boolean isCompressingAccepted(final HttpServletRequest request,
                                                final HttpManagementConfiguration managementConfiguration)
    {
        return managementConfiguration.isCompressResponses()
               && Collections.list(request.getHeaderNames()).contains(ACCEPT_ENCODING_HEADER)
               && request.getHeader(ACCEPT_ENCODING_HEADER).contains(GZIP_CONTENT_ENCODING);
    }

    public static String ensureFilenameIsRfc2183(final String requestedFilename)
    {
        return requestedFilename.replaceAll("[\\P{InBasic_Latin}\\\\:/\\p{Cntrl}]", "");
    }

    public static List<String> getPathInfoElements(final String servletPath, final String pathInfo)
    {
        if (pathInfo == null || pathInfo.length() == 0)
        {
            return Collections.emptyList();
        }

        String[] pathInfoElements = pathInfo.substring(1).split("/");
        for (int i = 0; i < pathInfoElements.length; i++)
        {
            try
            {
                // double decode to allow slashes in object names. first decoding happens in request.getPathInfo().
                pathInfoElements[i] = URLDecoder.decode(pathInfoElements[i], StandardCharsets.UTF_8.name());
            }
            catch (UnsupportedEncodingException e)
            {
                throw new IllegalArgumentException("Servlet at " + servletPath
                                                   + " could not decode path element: " + pathInfoElements[i], e);
            }
        }
        return Arrays.asList(pathInfoElements);
    }

    public static String getRequestURL(HttpServletRequest httpRequest)
    {
        String url;
        StringBuilder urlBuilder = new StringBuilder(httpRequest.getRequestURL());
        String queryString = httpRequest.getQueryString();
        if (queryString != null)
        {
            urlBuilder.append('?').append(queryString);
        }
        url = urlBuilder.toString();
        return url;
    }

    public static String getRequestPrincipals(HttpServletRequest httpRequest)
    {
        HttpSession session = httpRequest.getSession(false);
        if (session != null)
        {
            Subject subject = HttpManagementUtil.getAuthorisedSubject(httpRequest);
            if (subject != null)
            {

                Set<Principal> principalSet = subject.getPrincipals();
                if (!principalSet.isEmpty())
                {
                    TreeSet<String> principalNames = new TreeSet();
                    for (Principal principal : principalSet)
                    {
                        principalNames.add(principal.getName());
                    }
                    return principalNames.toString();
                }
            }
        }
        return null;
    }
}
