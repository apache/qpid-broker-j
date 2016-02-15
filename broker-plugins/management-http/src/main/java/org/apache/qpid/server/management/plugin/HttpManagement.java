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
import java.io.Writer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.DispatcherType;
import javax.servlet.MultipartConfigElement;
import javax.servlet.http.HttpServletRequest;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.management.plugin.filter.ExceptionHandlingFilter;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.messages.ManagementConsoleMessages;
import org.apache.qpid.server.management.plugin.connector.TcpAndSslSelectChannelConnector;
import org.apache.qpid.server.management.plugin.filter.ForbiddingAuthorisationFilter;
import org.apache.qpid.server.management.plugin.filter.ForbiddingTraceFilter;
import org.apache.qpid.server.management.plugin.filter.LoggingFilter;
import org.apache.qpid.server.management.plugin.filter.RedirectingAuthorisationFilter;
import org.apache.qpid.server.management.plugin.servlet.DefinedFileServlet;
import org.apache.qpid.server.management.plugin.servlet.FileServlet;
import org.apache.qpid.server.management.plugin.servlet.RootServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.ApiDocsServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.JsonValueServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.LoggedOnUserPreferencesServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.LogoutServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.MetaDataServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.QueueReportServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.RestServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.SaslServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.StructureServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.TimeZoneServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.UserPreferencesServlet;
import org.apache.qpid.server.model.*;
import org.apache.qpid.server.model.adapter.AbstractPluginAdapter;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.model.port.PortManager;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.transport.network.security.ssl.QpidMultipleTrustManager;
import org.apache.qpid.transport.network.security.ssl.SSLUtil;

@ManagedObject( category = false, type = HttpManagement.PLUGIN_TYPE )
public class HttpManagement extends AbstractPluginAdapter<HttpManagement> implements HttpManagementConfiguration<HttpManagement>, PortManager
{
    private static final String PORT_SERVLET_ATTRIBUTE = "org.apache.qpid.server.model.Port";
    private final Logger _logger = LoggerFactory.getLogger(HttpManagement.class);
    
    // 10 minutes by default
    public static final int DEFAULT_TIMEOUT_IN_SECONDS = 60 * 10;
    public static final String TIME_OUT = "sessionTimeout";
    public static final String HTTP_BASIC_AUTHENTICATION_ENABLED = "httpBasicAuthenticationEnabled";
    public static final String HTTPS_BASIC_AUTHENTICATION_ENABLED = "httpsBasicAuthenticationEnabled";
    public static final String HTTP_SASL_AUTHENTICATION_ENABLED = "httpSaslAuthenticationEnabled";
    public static final String HTTPS_SASL_AUTHENTICATION_ENABLED = "httpsSaslAuthenticationEnabled";

    public static final String PLUGIN_TYPE = "MANAGEMENT-HTTP";

    public static final String DEFAULT_LOGOUT_URL = "/logout.html";

    private static final String OPERATIONAL_LOGGING_NAME = "Web";

    private static final String JSESSIONID_COOKIE_PREFIX = "JSESSIONID_";

    private static final String[] STATIC_FILE_TYPES = { "*.js", "*.css", "*.html", "*.png", "*.gif", "*.jpg",
                                                        "*.jpeg", "*.json", "*.txt", "*.xsl", "*.svg" };

    private Server _server;

    @ManagedAttributeField
    private boolean _httpsSaslAuthenticationEnabled;

    @ManagedAttributeField
    private boolean _httpSaslAuthenticationEnabled;

    @ManagedAttributeField
    private boolean _httpsBasicAuthenticationEnabled;

    @ManagedAttributeField
    private boolean _httpBasicAuthenticationEnabled;

    @ManagedAttributeField
    private int _sessionTimeout;

    @ManagedAttributeField
    private boolean _compressResponses;

    private boolean _allowPortActivation;
    private Map<HttpPort<?>, Connector> _portConnectorMap = new HashMap<>();

    @ManagedObjectFactoryConstructor
    public HttpManagement(Map<String, Object> attributes, Broker broker)
    {
        super(attributes, broker);
    }

    @StateTransition(currentState = {State.UNINITIALIZED,State.ERRORED}, desiredState = State.ACTIVE)
    @SuppressWarnings("unused")
    private ListenableFuture<Void> doStart()
    {

        Collection<HttpPort<?>> httpPorts = getEligibleHttpPorts(getBroker().getPorts());
        if (httpPorts.isEmpty())
        {
            _logger.warn("HttpManagement plugin is configured but no suitable HTTP ports are available.");
        }
        else
        {
            getBroker().getEventLogger().message(ManagementConsoleMessages.STARTUP(OPERATIONAL_LOGGING_NAME));

            _server = createServer(httpPorts);
            try
            {
                _server.start();
                logOperationalListenMessages();
            }
            catch (Exception e)
            {
                throw new ServerScopedRuntimeException("Failed to start HTTP management on ports : " + httpPorts, e);
            }

            getBroker().getEventLogger().message(ManagementConsoleMessages.READY(OPERATIONAL_LOGGING_NAME));
        }
        setState(State.ACTIVE);
        return Futures.immediateFuture(null);
    }

    @Override
    protected void onClose()
    {
        if (_server != null)
        {
            try
            {
                logOperationalShutdownMessage();
                _server.stop();
            }
            catch (Exception e)
            {
                throw new ServerScopedRuntimeException("Failed to stop HTTP management", e);
            }
        }

        getBroker().getEventLogger().message(ManagementConsoleMessages.STOPPED(OPERATIONAL_LOGGING_NAME));
    }

    public int getSessionTimeout()
    {
        return _sessionTimeout;
    }

    private Server createServer(Collection<HttpPort<?>> ports)
    {
        _logger.debug("Starting up web server on {}", ports);
        _allowPortActivation = true;

        Server server = new Server();
        // All connectors will have their own thread pool, so we expect the server to need none.
        server.setThreadPool(new ZeroSizedThreadPool());

        int lastPort = -1;
        for (HttpPort<?> port : ports)
        {
            SelectChannelConnector connector = createConnector(port);
            server.addConnector(connector);
            _portConnectorMap.put(port, connector);
            lastPort = port.getPort();
        }

        _allowPortActivation = false;

        ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
        root.setContextPath("/");
        root.setCompactPath(true);
        server.setHandler(root);
        server.setSendServerVersion(false);
        final ErrorHandler errorHandler = new ErrorHandler()
        {
            @Override
            protected void writeErrorPageBody(HttpServletRequest request, Writer writer, int code, String message, boolean showStacks)
                    throws IOException
            {
                String uri= request.getRequestURI();

                writeErrorPageMessage(request,writer,code,message,uri);

                for (int i= 0; i < 20; i++)
                    writer.write("<br/>                                                \n");
            }
        };
        root.setErrorHandler(errorHandler);

        // set servlet context attributes for broker and configuration
        root.getServletContext().setAttribute(HttpManagementUtil.ATTR_BROKER, getBroker());
        root.getServletContext().setAttribute(HttpManagementUtil.ATTR_MANAGEMENT_CONFIGURATION, this);

        root.addFilter(new FilterHolder(new ExceptionHandlingFilter()), "/*", EnumSet.allOf(DispatcherType.class));

        FilterHolder loggingFilter = new FilterHolder(new LoggingFilter());
        root.addFilter(loggingFilter, "/api/*", EnumSet.of(DispatcherType.REQUEST));
        root.addFilter(loggingFilter, "/service/*", EnumSet.of(DispatcherType.REQUEST));

        root.addFilter(new FilterHolder(new ForbiddingTraceFilter()), "/*", EnumSet.of(DispatcherType.REQUEST));
        FilterHolder restAuthorizationFilter = new FilterHolder(new ForbiddingAuthorisationFilter());
        restAuthorizationFilter.setInitParameter(ForbiddingAuthorisationFilter.INIT_PARAM_ALLOWED, "/service/sasl");
        root.addFilter(restAuthorizationFilter, "/api/*", EnumSet.of(DispatcherType.REQUEST));
        root.addFilter(restAuthorizationFilter, "/apidocs/*", EnumSet.of(DispatcherType.REQUEST));
        root.addFilter(restAuthorizationFilter, "/service/*", EnumSet.of(DispatcherType.REQUEST));

        root.addFilter(new FilterHolder(new RedirectingAuthorisationFilter()), "/index.html", EnumSet.of(DispatcherType.REQUEST));
        root.addFilter(new FilterHolder(new RedirectingAuthorisationFilter()), "/", EnumSet.of(DispatcherType.REQUEST));

        addRestServlet(root, Broker.class);

        ServletHolder apiDocsServlet = new ServletHolder(new ApiDocsServlet(getModel()));
        final ServletHolder rewriteSerlvet = new ServletHolder(new RewriteServlet("^(.*)$", "$1/"));
        for(String path : new String[]{"/apidocs", "/apidocs/latest", "/apidocs/"+getLatestSupportedVersion()})
        {
            root.addServlet(rewriteSerlvet, path);
            root.addServlet(apiDocsServlet, path + "/");
        }

        root.addServlet(new ServletHolder(new UserPreferencesServlet()), "/service/userpreferences/*");
        root.addServlet(new ServletHolder(new LoggedOnUserPreferencesServlet()), "/service/preferences");
        root.addServlet(new ServletHolder(new StructureServlet()), "/service/structure");
        root.addServlet(new ServletHolder(new QueueReportServlet()), "/service/queuereport/*");

        root.addServlet(new ServletHolder(new MetaDataServlet(getModel())), "/service/metadata");

        root.addServlet(new ServletHolder(new SaslServlet()), "/service/sasl");

        root.addServlet(new ServletHolder(new RootServlet("/","/apidocs/","index.html")), "/");
        root.addServlet(new ServletHolder(new LogoutServlet()), "/logout");

        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDojoPath(), true)), "/dojo/dojo/*");
        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDijitPath(), true)), "/dojo/dijit/*");
        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDojoxPath(), true)), "/dojo/dojox/*");

        for(String pattern : STATIC_FILE_TYPES)
        {
            root.addServlet(new ServletHolder(new FileServlet()), pattern);
        }

        root.addServlet(new ServletHolder(new TimeZoneServlet()), "/service/timezones");

        final SessionManager sessionManager = root.getSessionHandler().getSessionManager();
        sessionManager.getSessionCookieConfig().setName(JSESSIONID_COOKIE_PREFIX + lastPort);
        sessionManager.setMaxInactiveInterval((Integer)getAttribute(TIME_OUT));

        return server;
    }

    private SelectChannelConnector createConnector(final HttpPort<?> port)
    {
        port.setPortManager(this);

        if(port.getState() != State.ACTIVE)
        {
            // TODO - RG - probably does nothing
            port.startAsync();
        }
        SelectChannelConnector connector = null;

        Collection<Transport> transports = port.getTransports();
        if (!transports.contains(Transport.SSL))
        {
            final Port thePort = port;
            connector = new SelectChannelConnector()
                        {
                            @Override
                            public void customize(final EndPoint endpoint, final Request request) throws IOException
                            {
                                super.customize(endpoint, request);
                                request.setAttribute(PORT_SERVLET_ATTRIBUTE, thePort);
                            }
                        };
        }
        else if (transports.contains(Transport.SSL))
        {
            connector = createSslConnector(port);
        }
        else
        {
            throw new IllegalArgumentException("Unexpected transport on port "
                                               + port.getName()
                                               + ":"
                                               + transports);
        }
        String bindingAddress = port.getBindingAddress();
        if (bindingAddress != null && !bindingAddress.trim().equals("") && !bindingAddress.trim().equals("*"))
        {
            connector.setHost(bindingAddress.trim());
        }
        connector.setPort(port.getPort());


        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setName("HttpManagement-" + port.getName());

        int additionalInternalThreads = port.getContextValue(Integer.class,
                                                             HttpPort.PORT_HTTP_ADDITIONAL_INTERNAL_THREADS);
        int maximumQueueRequests = port.getContextValue(Integer.class, HttpPort.PORT_HTTP_MAXIMUM_QUEUED_REQUESTS);

        int threadPoolMaximum = port.getThreadPoolMaximum();
        int threadPoolMinimum = port.getThreadPoolMinimum();

        threadPool.setMaxQueued(maximumQueueRequests);
        threadPool.setMaxThreads(threadPoolMaximum + additionalInternalThreads);
        threadPool.setMinThreads(threadPoolMinimum + additionalInternalThreads);

        int jettyAcceptorLimit = 2 * Runtime.getRuntime().availableProcessors();
        connector.setAcceptors(Math.min(Math.max(1, threadPoolMaximum / 2), jettyAcceptorLimit));
        connector.setThreadPool(threadPool);
        return connector;
    }

    private SelectChannelConnector createSslConnector(final HttpPort<?> port)
    {
        final SelectChannelConnector connector;
        KeyStore keyStore = port.getKeyStore();
        Collection<TrustStore> trustStores = port.getTrustStores();
        if (keyStore == null)
        {
            throw new IllegalConfigurationException("Key store is not configured. Cannot start management on HTTPS port without keystore");
        }
        SslContextFactory factory = new SslContextFactory()
                                    {
                                        public String[] selectProtocols(String[] enabledProtocols, String[] supportedProtocols)
                                        {
                                            List<String> selectedProtocols = new ArrayList<>(Arrays.asList(enabledProtocols));
                                            SSLUtil.updateEnabledProtocols(selectedProtocols, supportedProtocols);

                                            return selectedProtocols.toArray(new String[selectedProtocols.size()]);
                                        }

                                    };

        if(port.getDisabledCipherSuites() != null)
        {
            factory.addExcludeCipherSuites(port.getDisabledCipherSuites().toArray(new String[port.getDisabledCipherSuites().size()]));
        }

        if(port.getEnabledCipherSuites() != null && !port.getEnabledCipherSuites().isEmpty())
        {
            factory.setIncludeCipherSuites(port.getEnabledCipherSuites().toArray(new String[port.getEnabledCipherSuites().size()]));
        }

        boolean needClientCert = port.getNeedClientAuth() || port.getWantClientAuth();

        if (needClientCert && trustStores.isEmpty())
        {
            throw new IllegalConfigurationException("Client certificate authentication is enabled on AMQP port '"
                                                    + this.getName() + "' but no trust store defined");
        }

        try
        {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            KeyManager[] keyManagers = keyStore.getKeyManagers();

            TrustManager[] trustManagers;
            if(trustStores == null || trustStores.isEmpty())
            {
                trustManagers = null;
            }
            else if(trustStores.size() == 1)
            {
                trustManagers = trustStores.iterator().next().getTrustManagers();
            }
            else
            {
                Collection<TrustManager> trustManagerList = new ArrayList<>();
                final QpidMultipleTrustManager mulTrustManager = new QpidMultipleTrustManager();

                for(TrustStore ts : trustStores)
                {
                    TrustManager[] managers = ts.getTrustManagers();
                    if(managers != null)
                    {
                        for(TrustManager manager : managers)
                        {
                            if(manager instanceof X509TrustManager)
                            {
                                mulTrustManager.addTrustManager((X509TrustManager)manager);
                            }
                            else
                            {
                                trustManagerList.add(manager);
                            }
                        }
                    }
                }
                if(!mulTrustManager.isEmpty())
                {
                    trustManagerList.add(mulTrustManager);
                }
                trustManagers = trustManagerList.toArray(new TrustManager[trustManagerList.size()]);
            }
            sslContext.init(keyManagers, trustManagers, null);

            factory.setSslContext(sslContext);
            if(port.getNeedClientAuth())
            {
                factory.setNeedClientAuth(true);
            }
            else if(port.getWantClientAuth())
            {
                factory.setWantClientAuth(true);
            }
        }
        catch (GeneralSecurityException e)
        {
            throw new ServerScopedRuntimeException("Cannot configure port " + port.getName() + " for transport " + Transport.SSL, e);
        }
        connector = port.getTransports().contains(Transport.TCP)
                ? new TcpAndSslSelectChannelConnector(factory)
                    {
                        @Override
                        public void customize(final EndPoint endpoint, final Request request) throws IOException
                        {
                            super.customize(endpoint, request);
                            request.setAttribute(PORT_SERVLET_ATTRIBUTE, port);
                        }
                    }
                : new SslSelectChannelConnector(factory)
                    {
                        @Override
                        public void customize(final EndPoint endpoint, final Request request) throws IOException
                        {
                            super.customize(endpoint, request);
                            request.setAttribute(PORT_SERVLET_ATTRIBUTE, port);
                        }
                    };
        return connector;
    }

    private void addRestServlet(ServletContextHandler root, final Class<? extends ConfiguredObject> rootClass)
    {
        Set<Class<? extends ConfiguredObject>> categories = new HashSet<>(getModel().getDescendantCategories(rootClass));
        categories.add(rootClass);
        for(Class<? extends ConfiguredObject> category : categories)
        {
                String name = category.getSimpleName().toLowerCase();
                List<Class<? extends ConfiguredObject>> hierarchyList = new ArrayList<>();

                if(category != rootClass)
                {
                    Collection<Class<? extends ConfiguredObject>> parentCategories;

                    hierarchyList.add(category);

                    while (!(parentCategories = getModel().getParentTypes(category)).contains(rootClass))
                    {
                        hierarchyList.addAll(parentCategories);
                        category = parentCategories.iterator().next();
                    }

                    Collections.reverse(hierarchyList);

                }
                Class<? extends ConfiguredObject>[] hierarchyArray =
                        hierarchyList.toArray(new Class[hierarchyList.size()]);

                ServletHolder servletHolder = new ServletHolder(name, new RestServlet(hierarchyArray));
                servletHolder.getRegistration().setMultipartConfig(
                        new MultipartConfigElement("",
                                                   getContextValue(Long.class, MAX_HTTP_FILE_UPLOAD_SIZE_CONTEXT_NAME),
                                                   -1l,
                                                   getContextValue(Integer.class,
                                                                   MAX_HTTP_FILE_UPLOAD_SIZE_CONTEXT_NAME)));

                List<String> paths = Arrays.asList("/api/latest/" + name,
                                                   "/api/v" + BrokerModel.MODEL_MAJOR_VERSION + "/" + name);

                for (String path : paths)
                {
                    root.addServlet(servletHolder, path + "/*");
                }
                ServletHolder docServletHolder = new ServletHolder(name + "docs", new ApiDocsServlet(getModel(),
                                                                                                     paths,
                                                                                                     hierarchyArray));
                root.addServlet(docServletHolder, "/apidocs/latest/" + name + "/");
                root.addServlet(docServletHolder, "/apidocs/v" + BrokerModel.MODEL_MAJOR_VERSION + "/" + name + "/");
                root.addServlet(docServletHolder, "/apidocs/latest/" + name);
                root.addServlet(docServletHolder, "/apidocs/v" + BrokerModel.MODEL_MAJOR_VERSION + "/" + name);


        }

        final ServletHolder versionsServletHolder = new ServletHolder(new JsonValueServlet(getApiProperties()));
        root.addServlet(versionsServletHolder,"/api");
        root.addServlet(versionsServletHolder,"/api/");

    }

    private Map<String, Object> getApiProperties()
    {
        return Collections.<String,Object>singletonMap("supportedVersions", getSupportedRestApiVersions());
    }

    private List<String> getSupportedRestApiVersions()
    {
        // TODO - actually support multiple versions and add those versions to the list
        return Collections.singletonList(getLatestSupportedVersion());
    }

    private String getLatestSupportedVersion()
    {
        return "v"+String.valueOf(BrokerModel.MODEL_MAJOR_VERSION);
    }

    private void logOperationalListenMessages()
    {
        for (Map.Entry<HttpPort<?>, Connector> portConnector : _portConnectorMap.entrySet())
        {
            HttpPort<?> port = portConnector.getKey();
            Connector connector = portConnector.getValue();
            Set<Transport> transports = port.getTransports();
            for (Transport transport: transports)
            {
                getBroker().getEventLogger().message(ManagementConsoleMessages.LISTENING(Protocol.HTTP.name(),
                                                                                         transport.name(),
                                                                                         connector.getLocalPort()));
            }
        }
    }

    private void logOperationalShutdownMessage()
    {
        for (Connector connector : _portConnectorMap.values())
        {
            getBroker().getEventLogger().message(ManagementConsoleMessages.SHUTTING_DOWN(Protocol.HTTP.name(), connector.getLocalPort()));
        }
    }

    private Collection<HttpPort<?>> getEligibleHttpPorts(Collection<Port<?>> ports)
    {
        Collection<HttpPort<?>> httpPorts = new HashSet<>();
        for (Port<?> port : ports)
        {
            if (State.ACTIVE == port.getDesiredState() &&
                State.ERRORED != port.getState() &&
                port.getProtocols().contains(Protocol.HTTP))
            {
                httpPorts.add((HttpPort<?>) port);
            }
        }
        return Collections.unmodifiableCollection(httpPorts);
    }

    @Override
    public boolean isActivationAllowed(final Port<?> port)
    {
        return _allowPortActivation;
    }

    @Override
    public boolean isHttpsSaslAuthenticationEnabled()
    {
        return _httpsSaslAuthenticationEnabled;
    }

    @Override
    public boolean isHttpSaslAuthenticationEnabled()
    {
        return _httpSaslAuthenticationEnabled;
    }

    @Override
    public boolean isHttpsBasicAuthenticationEnabled()
    {
        return _httpsBasicAuthenticationEnabled;
    }

    @Override
    public boolean isHttpBasicAuthenticationEnabled()
    {
        return _httpBasicAuthenticationEnabled;
    }

    @Override
    public boolean isCompressResponses()
    {
        return _compressResponses;
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider(HttpServletRequest request)
    {
        HttpPort<?> port = (HttpPort<?>)request.getAttribute(PORT_SERVLET_ATTRIBUTE);
        return port == null ? null : port.getAuthenticationProvider();
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);

        HttpManagementConfiguration<?> updated = (HttpManagementConfiguration<?>)proxyForValidation;
        if(changedAttributes.contains(HttpManagement.NAME))
        {
            if(!getName().equals(updated.getName()))
            {
                throw new IllegalConfigurationException("Changing the name of http management plugin is not allowed");
            }
        }
        if (changedAttributes.contains(TIME_OUT))
        {
            int value = updated.getSessionTimeout();
            if (value < 0)
            {
                throw new IllegalConfigurationException("Only positive integer value can be specified for the session time out attribute");
            }
        }
    }

    private static class ZeroSizedThreadPool implements ThreadPool
    {
        @Override
        public boolean dispatch(final Runnable job)
        {
            throw new IllegalStateException("Job unexpectedly dispatched to server thread pool. Cannot dispatch");
        }

        @Override
        public void join() throws InterruptedException
        {
        }

        @Override
        public int getThreads()
        {
            return 0;
        }

        @Override
        public int getIdleThreads()
        {
            return 0;
        }

        @Override
        public boolean isLowOnThreads()
        {
            return false;
        }
    }
}
