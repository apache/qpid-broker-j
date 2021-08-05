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

package org.apache.qpid.server.management.plugin.servlet.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Strings;

import org.apache.qpid.server.management.plugin.HttpManagementUtil;
import org.apache.qpid.server.management.plugin.ManagementController;
import org.apache.qpid.server.management.plugin.ManagementException;
import org.apache.qpid.server.management.plugin.ManagementRequest;
import org.apache.qpid.server.management.plugin.ManagementResponse;
import org.apache.qpid.server.management.plugin.ResponseType;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.util.DataUrlUtils;

public class RestServlet extends AbstractServlet
{
    private static final long serialVersionUID = 1L;
    private static final String APPLICATION_JSON = "application/json";

    private transient ManagementController _managementController;

    @SuppressWarnings("unused")
    public RestServlet()
    {
        super();
    }

    @Override
    public void init() throws ServletException
    {
        super.init();

        final ServletConfig servletConfig = getServletConfig();
        final ServletContext servletContext = servletConfig.getServletContext();

        final String modelVersion = servletConfig.getInitParameter("qpid.controller.version");
        if (modelVersion == null)
        {
            throw new ServletException("Controller version is not specified");
        }

        @SuppressWarnings("uncjecked")
        ManagementController controller = (ManagementController) servletContext.getAttribute("qpid.controller.chain");
        do
        {
            if (controller.getVersion().equals(modelVersion))
            {
                _managementController = controller;
                break;
            }
            controller = controller.getNextVersionManagementController();
        }
        while (controller != null);

        if (_managementController == null)
        {
            throw new ServletException("Controller is not found");
        }
    }

    @Override
    protected void doGet(final HttpServletRequest httpServletRequest,
                         final HttpServletResponse httpServletResponse,
                         final ConfiguredObject<?> managedObject)
            throws IOException
    {
        try
        {
            final ManagementRequest request = new ServletManagementRequest(managedObject, httpServletRequest);
            final ManagementController controller = getManagementController();
            final ManagementResponse response = controller.handleGet(request);

            sendResponse(request, response, httpServletRequest, httpServletResponse, controller);
        }
        catch (ManagementException e)
        {
            sendResponse(e, httpServletRequest, httpServletResponse);
        }
    }

    @Override
    protected void doPost(final HttpServletRequest httpServletRequest,
                          final HttpServletResponse httpServletResponse,
                          final ConfiguredObject<?> managedObject) throws IOException
    {
        try
        {
            final ManagementRequest request = new ServletManagementRequest(managedObject, httpServletRequest);
            final ManagementController controller = getManagementController();
            final ManagementResponse response = controller.handlePost(request);

            sendResponse(request, response, httpServletRequest, httpServletResponse, controller);
        }
        catch (ManagementException e)
        {
            sendResponse(e, httpServletRequest, httpServletResponse);
        }
    }

    @Override
    protected void doPut(final HttpServletRequest httpServletRequest,
                         final HttpServletResponse httpServletResponse,
                         final ConfiguredObject<?> managedObject) throws IOException
    {
        try
        {
            final ManagementRequest request = new ServletManagementRequest(managedObject, httpServletRequest);
            final ManagementController controller = getManagementController();
            final ManagementResponse response = controller.handlePut(request);

            sendResponse(request, response, httpServletRequest, httpServletResponse, controller);
        }
        catch (ManagementException e)
        {
            sendResponse(e, httpServletRequest, httpServletResponse);
        }
    }

    @Override
    protected void doDelete(final HttpServletRequest httpServletRequest,
                            final HttpServletResponse httpServletResponse,
                            final ConfiguredObject<?> managedObject) throws IOException
    {
        try
        {
            final ManagementRequest request = new ServletManagementRequest(managedObject, httpServletRequest);
            final ManagementController controller = getManagementController();
            final ManagementResponse response = controller.handleDelete(request);

            sendResponse(request, response, httpServletRequest, httpServletResponse, controller);
        }
        catch (ManagementException e)
        {
            sendResponse(e, httpServletRequest, httpServletResponse);
        }
    }

    private ManagementController getManagementController()
    {
        return _managementController;
    }

    private void sendResponse(final ManagementException managementException,
                              final HttpServletRequest request,
                              final HttpServletResponse response) throws IOException
    {
        setHeaders(response);
        setExceptionHeaders(managementException, response);
        response.setStatus(managementException.getStatusCode());
        writeJsonResponse(Collections.singletonMap("errorMessage", managementException.getMessage()),
                          request,
                          response);
    }

    private void setExceptionHeaders(final ManagementException managementException, final HttpServletResponse response)
    {
        Map<String, String> headers = managementException.getHeaders();
        if (headers != null)
        {
            headers.forEach(response::setHeader);
        }
    }

    private String toContentDispositionHeader(final String attachmentFilename)
    {
        String filenameRfc2183 = HttpManagementUtil.ensureFilenameIsRfc2183(attachmentFilename);
        if (filenameRfc2183.length() > 0)
        {
            return String.format("attachment; filename=\"%s\"", filenameRfc2183);
        }
        else
        {
            // Agent will allow user to choose a name
            return "attachment";
        }
    }

    private void sendResponse(final ManagementRequest managementRequest,
                              final ManagementResponse managementResponse,
                              final HttpServletRequest request,
                              final HttpServletResponse response,
                              final ManagementController controller) throws IOException
    {
        setHeaders(response);
        Map<String, String> headers = managementResponse.getHeaders();
        if (!headers.isEmpty())
        {
            headers.forEach(response::setHeader);
        }

        Map<String, List<String>> parameters = managementRequest.getParameters();
        if (parameters.containsKey(CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM))
        {
            String attachmentFilename = managementRequest.getParameter(CONTENT_DISPOSITION_ATTACHMENT_FILENAME_PARAM);
            response.setHeader(CONTENT_DISPOSITION, toContentDispositionHeader(attachmentFilename));
        }
        response.setStatus(managementResponse.getResponseCode());

        Object body = managementResponse.getBody();
        if (body instanceof Content)
        {
            Content content = (Content) body;
            try
            {
                writeTypedContent(content, request, response);
            }
            finally
            {
                content.release();
            }
        }
        else
        {
            response.setContentType(APPLICATION_JSON);
            if (body != null && managementResponse.getType() == ResponseType.MODEL_OBJECT)
            {
                body = controller.formatConfiguredObject(
                        managementResponse.getBody(),
                        parameters,
                        managementRequest.isSecure()
                        || managementRequest.isConfidentialOperationAllowedOnInsecureChannel());
            }
            writeJsonResponse(body, request, response);
        }
    }

    private void setHeaders(final HttpServletResponse response)
    {
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Pragma", "no-cache");
        response.setDateHeader("Expires", 0);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    }

    private void writeJsonResponse(final Object formattedResponse,
                                   final HttpServletRequest request,
                                   final HttpServletResponse response) throws IOException
    {
        try (OutputStream stream = HttpManagementUtil.getOutputStream(request,
                                                                      response,
                                                                      getManagementConfiguration()))
        {
            ObjectMapper mapper = ConfiguredObjectJacksonModule.newObjectMapper(false);
            mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
            mapper.writeValue(stream, formattedResponse);
        }
    }

    private static Map<String, List<String>> parseQueryString(String queryString)
    {
        if (Strings.isNullOrEmpty(queryString))
        {
            return Collections.emptyMap();
        }
        Map<String, List<String>> query = new LinkedHashMap<>();
        final String[] pairs = queryString.split("&");
        for (String pairString : pairs)
        {
            List<String> pair = new ArrayList<>(Arrays.asList(pairString.split("=")));
            if (pair.size() == 1)
            {
                pair.add(null);
            }
            else if (pair.size() != 2)
            {
                throw new IllegalArgumentException(String.format("could not parse query string '%s'", queryString));
            }

            String key;
            String value;
            try
            {
                key = URLDecoder.decode(pair.get(0), "UTF-8");
                value = pair.get(1) == null ? null : URLDecoder.decode(pair.get(1), "UTF-8");
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException(e);
            }
            if (!query.containsKey(key))
            {
                query.put(key, new ArrayList<>());
            }
            query.get(key).add(value);
        }
        return query;
    }

    private static class ServletManagementRequest implements ManagementRequest
    {
        private final HttpPort<?> _port;
        private final HttpServletRequest _request;
        private final Map<String, List<String>> _query;
        private final List<String> _path;
        private final ConfiguredObject<?> _root;
        private final String _category;
        private final Map<String, String> _headers;

        ServletManagementRequest(final ConfiguredObject<?> root,
                                 final HttpServletRequest request)
        {
            _root = root;
            _request = request;
            _port = HttpManagementUtil.getPort(request);
            _query = Collections.unmodifiableMap(parseQueryString(request.getQueryString()));
            String pathInfo = _request.getPathInfo() == null ? "" : _request.getPathInfo();
            String servletPath = request.getServletPath();
            _path = Collections.unmodifiableList(HttpManagementUtil.getPathInfoElements(servletPath, pathInfo));
            final String[] servletPathElements = servletPath.split("/");
            _category = servletPathElements[servletPathElements.length - 1];
            final Map<String, String> headers = Collections.list(request.getHeaderNames())
                                                           .stream()
                                                           .collect(Collectors.toMap(name -> name, request::getHeader));
            _headers = Collections.unmodifiableMap(headers);
        }

        public ConfiguredObject<?> getRoot()
        {
            return _root;
        }

        public boolean isSecure()
        {
            return _request.isSecure();
        }

        public boolean isConfidentialOperationAllowedOnInsecureChannel()
        {
            return _port.isAllowConfidentialOperationsOnInsecureChannels();
        }

        public List<String> getPath()
        {
            return _path;
        }

        public String getMethod()
        {
            return _request.getMethod();
        }

        public Map<String, List<String>> getParameters()
        {
            return Collections.unmodifiableMap(_query);
        }

        @Override
        public String getParameter(final String name)
        {
            final List<String> values = _query.get(name);
            return values == null || values.isEmpty() ? null : values.get(0);
        }

        public Map<String, String> getHeaders()
        {
            return _headers;
        }

        @SuppressWarnings("unchecked")
        public <T> T getBody(Class<T> type)
        {
            try
            {
                return parse(type);
            }
            catch (IOException | ServletException e)
            {
                throw ManagementException.createBadRequestManagementException("Cannot parse body", e);
            }
        }

        @Override
        public String getRequestURL()
        {
            return _request.getRequestURL().toString();
        }

        @SuppressWarnings("unchecked")
        private <T> T parse(Class<T> type) throws IOException, ServletException
        {
            T providedObject;
            final ObjectMapper mapper = new ObjectMapper();

            if (_headers.containsKey("Content-Type") && _request.getHeader("Content-Type")
                                                                .startsWith("multipart/form-data"))
            {
                Map<String, Object> items = new LinkedHashMap<>();
                Map<String, String> fileUploads = new HashMap<>();
                Collection<Part> parts = _request.getParts();
                for (Part part : parts)
                {
                    if ("data".equals(part.getName()) && "application/json".equals(part.getContentType()))
                    {
                        items = mapper.readValue(part.getInputStream(), LinkedHashMap.class);
                    }
                    else
                    {
                        byte[] data = new byte[(int) part.getSize()];
                        try (InputStream inputStream = part.getInputStream())
                        {
                            inputStream.read(data);
                        }
                        fileUploads.put(part.getName(), DataUrlUtils.getDataUrlForBytes(data));
                    }
                }
                items.putAll(fileUploads);

                providedObject = (T) items;
            }
            else
            {
                providedObject = mapper.readValue(_request.getInputStream(), type);
            }
            return providedObject;
        }

        @Override
        public Map<String, Object> getParametersAsFlatMap()
        {
            final Map<String, Object> providedObject = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : _query.entrySet())
            {
                final List<String> value = entry.getValue();
                if (value != null)
                {
                    if (value.size() == 1)
                    {
                        providedObject.put(entry.getKey(), value.get(0));
                    }
                    else
                    {
                        providedObject.put(entry.getKey(), value);
                    }
                }
            }
            return providedObject;
        }

        @Override
        public String getCategory()
        {
            return _category;
        }
    }
}
