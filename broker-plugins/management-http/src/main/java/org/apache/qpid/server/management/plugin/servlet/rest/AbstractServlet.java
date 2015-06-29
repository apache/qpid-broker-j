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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.ContentHeader;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public abstract class AbstractServlet extends HttpServlet
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractServlet.class);
    public static final String CONTENT_DISPOSITION = "Content-disposition";

    private Broker<?> _broker;
    private HttpManagementConfiguration _managementConfiguration;

    protected AbstractServlet()
    {
        super();
    }

    @Override
    public void init() throws ServletException
    {
        ServletConfig servletConfig = getServletConfig();
        ServletContext servletContext = servletConfig.getServletContext();
        _broker = HttpManagementUtil.getBroker(servletContext);
        _managementConfiguration = HttpManagementUtil.getManagementConfiguration(servletContext);
        super.init();
    }

    @Override
    protected final void doGet(final HttpServletRequest request, final HttpServletResponse resp)
    {
        doWithSubjectAndActor(
            new PrivilegedExceptionAction<Void>()
            {
                @Override
                public Void run() throws Exception
                {
                    doGetWithSubjectAndActor(request, resp);
                    return null;
                }
            },
            request,
            resp
        );
    }

    /**
     * Performs the GET action as the logged-in {@link Subject}.
     * Subclasses commonly override this method
     */
    protected void doGetWithSubjectAndActor(HttpServletRequest request, HttpServletResponse resp) throws ServletException, IOException
    {
        throw new UnsupportedOperationException("GET not supported by this servlet");
    }


    @Override
    protected final void doPost(final HttpServletRequest request, final HttpServletResponse resp)
    {
        doWithSubjectAndActor(
            new PrivilegedExceptionAction<Void>()
            {
                @Override
                public Void run()  throws Exception
                {
                    doPostWithSubjectAndActor(request, resp);
                    return null;
                }
            },
            request,
            resp
        );
    }

    /**
     * Performs the POST action as the logged-in {@link Subject}.
     * Subclasses commonly override this method
     */
    protected void doPostWithSubjectAndActor(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        throw new UnsupportedOperationException("POST not supported by this servlet");
    }

    @Override
    protected final void doPut(final HttpServletRequest request, final HttpServletResponse resp)
    {
        doWithSubjectAndActor(
            new PrivilegedExceptionAction<Void>()
            {
                @Override
                public Void run() throws Exception
                {
                    doPutWithSubjectAndActor(request, resp);
                    return null;
                }
            },
            request,
            resp
        );
    }

    public OutputStream getOutputStream(final HttpServletRequest request, final HttpServletResponse response)
            throws IOException
    {
        return HttpManagementUtil.getOutputStream(request, response, _managementConfiguration);
    }

    /**
     * Performs the PUT action as the logged-in {@link Subject}.
     * Subclasses commonly override this method
     */
    protected void doPutWithSubjectAndActor(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        throw new UnsupportedOperationException("PUT not supported by this servlet");
    }

    @Override
    protected final void doDelete(final HttpServletRequest request, final HttpServletResponse resp)
            throws ServletException, IOException
    {
        doWithSubjectAndActor(
            new PrivilegedExceptionAction<Void>()
            {
                @Override
                public Void run() throws Exception
                {
                    doDeleteWithSubjectAndActor(request, resp);
                    return null;
                }
            },
            request,
            resp
        );
    }

    /**
     * Performs the PUT action as the logged-in {@link Subject}.
     * Subclasses commonly override this method
     */
    protected void doDeleteWithSubjectAndActor(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        throw new UnsupportedOperationException("DELETE not supported by this servlet");
    }

    private void doWithSubjectAndActor(
                    PrivilegedExceptionAction<Void> privilegedExceptionAction,
                    final HttpServletRequest request,
                    final HttpServletResponse resp)
    {
        Subject subject;
        try
        {
            subject = getAuthorisedSubject(request);
        }
        catch (SecurityException e)
        {
            sendError(resp, HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }

        try
        {
            Subject.doAs(subject, privilegedExceptionAction);
        }
        catch(RuntimeException e)
        {
            LOGGER.error("Unable to perform action", e);
            throw e;
        }
        catch (PrivilegedActionException e)
        {
            LOGGER.error("Unable to perform action", e);
            Throwable cause = e.getCause();
            if(cause instanceof RuntimeException)
            {
                throw (RuntimeException)cause;
            }
            if(cause instanceof Error)
            {
                throw (Error)cause;
            }
            throw new ConnectionScopedRuntimeException(e.getCause());
        }
    }

    protected Subject getAuthorisedSubject(HttpServletRequest request)
    {
        Subject subject = HttpManagementUtil.getAuthorisedSubject(request.getSession());
        if (subject == null)
        {
            throw new SecurityException("Access to management rest interfaces is denied for un-authorised user");
        }
        return subject;
    }

    protected Broker<?> getBroker()
    {
        return _broker;
    }

    protected HttpManagementConfiguration getManagementConfiguration()
    {
        return _managementConfiguration;
    }

    protected void sendJsonResponse(Object object, HttpServletRequest request, HttpServletResponse response) throws IOException
    {
        sendJsonResponse(object, request, response, HttpServletResponse.SC_OK, true);
    }

    protected void sendJsonResponse(Object object, HttpServletRequest request, HttpServletResponse response, int responseCode, boolean sendCachingHeaders) throws IOException
    {
        response.setStatus(responseCode);
        response.setContentType("application/json");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());

        if (sendCachingHeaders)
        {
            sendCachingHeadersOnResponse(response);
        }

        writeObjectToResponse(object, request, response);
    }

    protected void sendJsonErrorResponse(HttpServletRequest request,
                                         HttpServletResponse response,
                                         int responseCode,
                                         String message) throws IOException
    {
        response.setStatus(responseCode);
        response.setContentType("application/json");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());

        writeObjectToResponse(Collections.singletonMap("errorMessage", message), request, response);
    }

    protected void sendError(final HttpServletResponse resp, int responseCode)
    {
        try
        {
            resp.sendError(responseCode);
        }
        catch (IOException e)
        {
            throw new ConnectionScopedRuntimeException("Failed to send error response code " + responseCode, e);
        }
    }

    private void writeObjectToResponse(Object object, HttpServletRequest request,  HttpServletResponse response) throws IOException
    {
        OutputStream stream = getOutputStream(request, response);
        ObjectMapper mapper = ConfiguredObjectJacksonModule.newObjectMapper();
        mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        mapper.writeValue(stream, object);
    }

    protected void sendCachingHeadersOnResponse(HttpServletResponse response)
    {
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Pragma", "no-cache");
        response.setDateHeader("Expires", 0);
    }

    protected String[] getPathInfoElements(HttpServletRequest request)
    {
        String pathInfo = request.getPathInfo();
        if (pathInfo != null && pathInfo.length() > 0)
        {
            String[] pathInfoElements = pathInfo.substring(1).split("/");
            for (int i = 0; i < pathInfoElements.length; i++)
            {
                try
                {
                    // double decode to allow slashes in object names. first decoding happens in request.getPathInfo().
                    pathInfoElements[i] = URLDecoder.decode(pathInfoElements[i], "UTF-8");
                }
                catch (UnsupportedEncodingException e)
                {
                    throw new IllegalArgumentException("REST servlet " + getServletName() + " could not decode path element: " + pathInfoElements[i], e);
                }
            }
            return pathInfoElements;
        }
        return null;
    }

    protected void writeTypedContent(Content content, HttpServletRequest request, HttpServletResponse response) throws IOException
    {
        Map<ContentHeader, Method> contentHeaderGetters = getContentHeaderMethods(content);
        if (contentHeaderGetters != null)
        {
            for (Map.Entry<ContentHeader, Method> entry: contentHeaderGetters.entrySet())
            {
                final String headerName = entry.getKey().value();
                try
                {
                    response.setHeader(headerName, String.valueOf(entry.getValue().invoke(content)));
                }
                catch (Exception e)
                {
                    LOGGER.warn("Unexpected exception whilst setting response header " + headerName, e);
                }
            }

        }
        try(OutputStream os = getOutputStream(request, response))
        {
            content.write(os);
            response.setStatus(HttpServletResponse.SC_OK);
        }
        catch (FileNotFoundException e)
        {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }
        catch(IOException e)
        {
            LOGGER.warn("Unexpected exception processing request", e);
            sendJsonErrorResponse(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private Map<ContentHeader, Method> getContentHeaderMethods(Content content)
    {
        Map<ContentHeader, Method> results = new HashMap<>();
        Method[] methods = content.getClass().getMethods();
        for (Method method: methods)
        {
            if (method.isAnnotationPresent(ContentHeader.class))
            {
                final Class<?>[] parameterTypes = method.getParameterTypes();
                if (parameterTypes.length >0)
                {
                    LOGGER.warn("Parameters are found on method " + method.getName()
                            + " annotated with ContentHeader annotation. No parameter is allowed. Ignoring ContentHeader annotation.");
                }
                else
                {
                    results.put(method.getAnnotation(ContentHeader.class), method);
                }
            }
        }
        return results;
    }

}
