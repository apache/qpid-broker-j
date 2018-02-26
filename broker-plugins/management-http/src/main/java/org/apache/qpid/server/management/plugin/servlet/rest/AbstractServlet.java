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

import static org.apache.qpid.server.management.plugin.HttpManagementUtil.CONTENT_ENCODING_HEADER;
import static org.apache.qpid.server.management.plugin.HttpManagementUtil.GZIP_CONTENT_ENCODING;
import static org.apache.qpid.server.management.plugin.HttpManagementUtil.ensureFilenameIsRfc2183;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.management.plugin.GunzipOutputStream;
import org.apache.qpid.server.management.plugin.HttpManagementConfiguration;
import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFinder;
import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.CustomRestHeaders;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.model.RestContentHeader;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

public abstract class AbstractServlet extends HttpServlet
{
    public static final int SC_UNPROCESSABLE_ENTITY = 422;
    /**
     * Signifies that the agent wishes the servlet to set the Content-Disposition on the
     * response with the value attachment.  This filename will be derived from the parameter value.
     */
    public static final String CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM = "contentDispositionAttachmentFilename";
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractServlet.class);
    public static final String CONTENT_DISPOSITION = "Content-Disposition";

    private transient Broker<?> _broker;
    private transient HttpManagementConfiguration _managementConfiguration;
    private transient final ConcurrentMap<ConfiguredObject<?>, ConfiguredObjectFinder> _configuredObjectFinders = new ConcurrentHashMap<>();


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

    private ConfiguredObject<?> getManagedObject(final HttpServletRequest request, final HttpServletResponse resp)
    {
        HttpPort<?> port =  HttpManagementUtil.getPort(request);
        final NamedAddressSpace addressSpace = port.getAddressSpace(request.getServerName());
        if(addressSpace == null)
        {
            if(port.isManageBrokerOnNoAliasMatch())
            {
                return getBroker();
            }
            LOGGER.info("No HTTP Management alias mapping found for host '{}' on port {}", request.getServerName(), port);
            sendError(resp, HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
        else if(addressSpace instanceof VirtualHost<?>)
        {
            return (VirtualHost<?>)addressSpace;
        }
        else
        {
            return getBroker();
        }
    }

    @Override
    protected final void doGet(final HttpServletRequest request, final HttpServletResponse resp) throws ServletException, IOException
    {
        ConfiguredObject<?> managedObject = getManagedObject(request, resp);
        if(managedObject != null)
        {
            doGet(request, resp, managedObject);
        }
    }

    protected void doGet(HttpServletRequest request,
                         HttpServletResponse resp,
                         ConfiguredObject<?> managedObject) throws ServletException, IOException
    {
        throw new UnsupportedOperationException("GET not supported by this servlet");
    }


    @Override
    protected final void doPost(final HttpServletRequest request, final HttpServletResponse resp) throws ServletException, IOException
    {
        ConfiguredObject<?> managedObject = getManagedObject(request, resp);
        if(managedObject != null)
        {
            doPost(request, resp, managedObject);
        }
    }

    protected void doPost(HttpServletRequest req,
                          HttpServletResponse resp,
                          ConfiguredObject<?> managedObject) throws ServletException, IOException
    {
        throw new UnsupportedOperationException("POST not supported by this servlet");
    }

    @Override
    protected final void doPut(final HttpServletRequest request, final HttpServletResponse resp) throws ServletException, IOException
    {
        ConfiguredObject<?> managedObject = getManagedObject(request, resp);
        if(managedObject != null)
        {
            doPut(request, resp, managedObject);
        }
    }

    protected void setContentDispositionHeaderIfNecessary(final HttpServletResponse response,
                                                        final String attachmentFilename)
    {
        if (attachmentFilename != null)
        {
            String filenameRfc2183 = ensureFilenameIsRfc2183(attachmentFilename);
            if (filenameRfc2183.length() > 0)
            {
                response.setHeader(CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", filenameRfc2183));
            }
            else
            {
                response.setHeader(CONTENT_DISPOSITION, "attachment");  // Agent will allow user to choose a name
            }
        }
    }

    protected void doPut(HttpServletRequest req,
                         HttpServletResponse resp,
                         final ConfiguredObject<?> managedObject) throws ServletException, IOException
    {
        throw new UnsupportedOperationException("PUT not supported by this servlet");
    }

    @Override
    protected final void doDelete(final HttpServletRequest request, final HttpServletResponse resp)
            throws ServletException, IOException
    {
        ConfiguredObject<?> managedObject = getManagedObject(request, resp);
        if(managedObject != null)
        {
            doDelete(request, resp, managedObject);
        }
    }

    protected void doDelete(HttpServletRequest req,
                            HttpServletResponse resp,
                            ConfiguredObject<?> managedObject) throws ServletException, IOException
    {
        throw new UnsupportedOperationException("DELETE not supported by this servlet");
    }

    protected OutputStream getOutputStream(final HttpServletRequest request, final HttpServletResponse response)
            throws IOException
    {
        return HttpManagementUtil.getOutputStream(request, response, _managementConfiguration);
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

    protected final void sendJsonResponse(Object object, HttpServletRequest request, HttpServletResponse response, int responseCode, boolean sendCachingHeaders) throws IOException
    {
        response.setStatus(responseCode);
        response.setContentType("application/json");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());

        if (sendCachingHeaders)
        {
            sendCachingHeadersOnResponse(response);
        }


        if(object instanceof CustomRestHeaders)
        {
            setResponseHeaders(response, (CustomRestHeaders) object);
        }
        writeObjectToResponse(object, request, response);
    }

    protected final void sendJsonErrorResponse(HttpServletRequest request,
                                               HttpServletResponse response,
                                               int responseCode,
                                               String message) throws IOException
    {
        sendJsonResponse(Collections.singletonMap("errorMessage", message), request, response, responseCode, false);
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
        ObjectMapper mapper = ConfiguredObjectJacksonModule.newObjectMapper(false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.writeValue(stream, object);
    }

    protected void sendCachingHeadersOnResponse(HttpServletResponse response)
    {
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Pragma", "no-cache");
        response.setDateHeader("Expires", 0);
    }

    protected void writeTypedContent(Content content, HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        Map<String, Object> headers = getResponseHeaders(content);

        try (OutputStream os = getOutputStream(request, response, headers))
        {
            response.setStatus(HttpServletResponse.SC_OK);
            for (Map.Entry<String, Object> entry : headers.entrySet())
            {
                response.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
            }
            content.write(os);
        }
        catch (IOException e)
        {
            LOGGER.warn("Unexpected exception processing request", e);
            sendJsonErrorResponse(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private OutputStream getOutputStream(final HttpServletRequest request,
                                         final HttpServletResponse response,
                                         Map<String, Object> headers) throws IOException
    {
        final boolean isGzipCompressed = GZIP_CONTENT_ENCODING.equals(headers.get(CONTENT_ENCODING_HEADER.toUpperCase()));
        final boolean isCompressingAccepted = HttpManagementUtil.isCompressingAccepted(request, _managementConfiguration);

        OutputStream stream = response.getOutputStream();

        if (isGzipCompressed)
        {
            if (!isCompressingAccepted)
            {
                stream = new GunzipOutputStream(stream);
                headers.remove(CONTENT_ENCODING_HEADER.toUpperCase());
            }
        }
        else
        {
            if (isCompressingAccepted)
            {
                stream = new GZIPOutputStream(stream);
                headers.put(CONTENT_ENCODING_HEADER.toUpperCase(), GZIP_CONTENT_ENCODING);
            }
        }

        return stream;
    }

    private Map<String, Object> getResponseHeaders(final Object content)
    {
        Map<String, Object> headers = Collections.emptyMap();
        if (content instanceof CustomRestHeaders)
        {
            CustomRestHeaders customRestHeaders = (CustomRestHeaders) content;
            Map<RestContentHeader, Method> contentHeaderGetters = getContentHeaderMethods(customRestHeaders);
            if (contentHeaderGetters != null)
            {
                headers = new HashMap<>();
                for (Map.Entry<RestContentHeader, Method> entry : contentHeaderGetters.entrySet())
                {
                    final String headerName = entry.getKey().value();
                    try
                    {
                        final Object headerValue = entry.getValue().invoke(customRestHeaders);
                        if (headerValue != null)
                        {
                            headers.put(headerName.toUpperCase(), headerValue);
                        }
                    }
                    catch (Exception e)
                    {
                        LOGGER.warn("Unexpected exception whilst setting response header " + headerName, e);
                    }
                }
            }
        }
        return headers;
    }

    private void setResponseHeaders(final HttpServletResponse response, final CustomRestHeaders customRestHeaders)
    {
        Map<String, Object> headers = getResponseHeaders(customRestHeaders);
        for(Map.Entry<String,Object> entry : headers.entrySet())
        {
            response.setHeader(entry.getKey(), String.valueOf(entry.getValue()));
        }
    }

    private Map<RestContentHeader, Method> getContentHeaderMethods(CustomRestHeaders content)
    {
        Map<RestContentHeader, Method> results = new HashMap<>();
        Method[] methods = content.getClass().getMethods();
        for (Method method: methods)
        {
            if (method.isAnnotationPresent(RestContentHeader.class))
            {
                final Class<?>[] parameterTypes = method.getParameterTypes();
                if (parameterTypes.length >0)
                {
                    LOGGER.warn("Parameters are found on method " + method.getName()
                            + " annotated with ContentHeader annotation. No parameter is allowed. Ignoring ContentHeader annotation.");
                }
                else
                {
                    method.setAccessible(true);
                    results.put(method.getAnnotation(RestContentHeader.class), method);
                }
            }
        }
        return results;
    }


    protected final ConfiguredObjectFinder getConfiguredObjectFinder(final ConfiguredObject<?> root)
    {
        ConfiguredObjectFinder finder = _configuredObjectFinders.get(root);
        if(finder == null)
        {
            finder = new ConfiguredObjectFinder(root);
            final ConfiguredObjectFinder existingValue = _configuredObjectFinders.putIfAbsent(root, finder);
            if(existingValue != null)
            {
                finder = existingValue;
            }
            else
            {
                final AbstractConfigurationChangeListener deletionListener =
                        new AbstractConfigurationChangeListener()
                        {
                            @Override
                            public void stateChanged(final ConfiguredObject<?> object,
                                                     final State oldState,
                                                     final State newState)
                            {
                                if (newState == State.DELETED)
                                {
                                    _configuredObjectFinders.remove(root);
                                }
                            }
                        };
                root.addChangeListener(deletionListener);
                if(root.getState() == State.DELETED)
                {
                    _configuredObjectFinders.remove(root);
                    root.removeChangeListener(deletionListener);
                }
            }
        }
        return finder;
    }


}
