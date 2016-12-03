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
import java.io.StringWriter;
import java.io.Writer;
import java.net.BindException;
import java.net.InetSocketAddress;
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
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.DispatcherType;
import javax.servlet.MultipartConfigElement;
import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.logging.messages.ManagementConsoleMessages;
import org.apache.qpid.server.logging.messages.PortMessages;
import org.apache.qpid.server.management.plugin.connector.TcpAndSslSelectChannelConnector;
import org.apache.qpid.server.management.plugin.filter.ExceptionHandlingFilter;
import org.apache.qpid.server.management.plugin.filter.ForbiddingAuthorisationFilter;
import org.apache.qpid.server.management.plugin.filter.ForbiddingTraceFilter;
import org.apache.qpid.server.management.plugin.filter.LoggingFilter;
import org.apache.qpid.server.management.plugin.filter.RedirectingAuthorisationFilter;
import org.apache.qpid.server.management.plugin.filter.PreemptiveSessionInvalidationFilter;
import org.apache.qpid.server.management.plugin.filter.RewriteRequestForUncompressedJavascript;
import org.apache.qpid.server.management.plugin.servlet.FileServlet;
import org.apache.qpid.server.management.plugin.servlet.RootServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.ApiDocsServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.BrokerQueryServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.JsonValueServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.LogoutServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.MetaDataServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.QueueReportServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.RestServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.SaslServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.StructureServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.TimeZoneServlet;
import org.apache.qpid.server.management.plugin.servlet.rest.VirtualHostQueryServlet;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.KeyStore;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.StateTransition;
import org.apache.qpid.server.model.Transport;
import org.apache.qpid.server.model.TrustStore;
import org.apache.qpid.server.model.adapter.AbstractPluginAdapter;
import org.apache.qpid.server.model.port.HttpPort;
import org.apache.qpid.server.model.port.PortManager;
import org.apache.qpid.server.transport.PortBindFailureException;
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
    public String _corsAllowOrigins;

    @ManagedAttributeField
    public Set<String> _corsAllowMethods;

    @ManagedAttributeField
    public String _corsAllowHeaders;

    @ManagedAttributeField
    public boolean _corsAllowCredentials;

    @ManagedAttributeField
    private boolean _compressResponses;

    private boolean _allowPortActivation;
    private Map<HttpPort<?>, Connector> _portConnectorMap = new HashMap<>();

    private volatile boolean _serveUncompressedDojo;

    @ManagedObjectFactoryConstructor
    public HttpManagement(Map<String, Object> attributes, Broker broker)
    {
        super(attributes, broker);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        _serveUncompressedDojo =   Boolean.TRUE.equals(getContextValue(Boolean.class, "qpid.httpManagement.serveUncompressedDojo"));
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
            catch (PortBindFailureException e)
            {
                getBroker().getEventLogger().message(PortMessages.BIND_FAILED("HTTP", e.getAddress().getPort()));
                throw e;
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
    protected ListenableFuture<Void> onClose()
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
        return Futures.immediateFuture(null);
    }

    public int getSessionTimeout()
    {
        return _sessionTimeout;
    }

    public String getCorsAllowOrigins()
    {
        return _corsAllowOrigins;
    }

    public Set<String> getCorsAllowMethods()
    {
        return _corsAllowMethods;
    }

    public String getCorsAllowHeaders()
    {
        return _corsAllowHeaders;
    }

    public boolean getCorsAllowCredentials()
    {
        return _corsAllowCredentials;
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

        FilterHolder corsFilter = new FilterHolder(new CrossOriginFilter());
        corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, getCorsAllowOrigins());
        corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, Joiner.on(",").join(getCorsAllowMethods()));
        corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, getCorsAllowHeaders());
        corsFilter.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM, String.valueOf(getCorsAllowCredentials()));
        root.addFilter(corsFilter, "/*", EnumSet.of(DispatcherType.REQUEST));

        root.addFilter(new FilterHolder(new PreemptiveSessionInvalidationFilter()), "/api/*", EnumSet.of(DispatcherType.REQUEST));

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

        if (_serveUncompressedDojo)
        {
            root.addFilter(RewriteRequestForUncompressedJavascript.class, "/dojo/dojo/*", EnumSet.of(DispatcherType.REQUEST));
            root.addFilter(RewriteRequestForUncompressedJavascript.class, "/dojo/dojox/*", EnumSet.of(DispatcherType.REQUEST));
        }

        addRestServlet(root);

        ServletHolder queryServlet = new ServletHolder(new BrokerQueryServlet());
        root.addServlet(queryServlet, "/api/latest/querybroker/*");
        root.addServlet(queryServlet, "/api/v" + BrokerModel.MODEL_VERSION + "/querybroker/*");

        ServletHolder vhQueryServlet = new ServletHolder(new VirtualHostQueryServlet());
        root.addServlet(vhQueryServlet, "/api/latest/queryvhost/*");
        root.addServlet(vhQueryServlet, "/api/v" + BrokerModel.MODEL_VERSION + "/queryvhost/*");


        ServletHolder apiDocsServlet = new ServletHolder(new ApiDocsServlet());
        final ServletHolder rewriteSerlvet = new ServletHolder(new RewriteServlet("^(.*)$", "$1/"));
        for(String path : new String[]{"/apidocs", "/apidocs/latest", "/apidocs/"+getLatestSupportedVersion()})
        {
            root.addServlet(rewriteSerlvet, path);
            root.addServlet(apiDocsServlet, path + "/");
        }

        root.addServlet(new ServletHolder(new StructureServlet()), "/service/structure");
        root.addServlet(new ServletHolder(new QueueReportServlet()), "/service/queuereport/*");

        root.addServlet(new ServletHolder(new MetaDataServlet()), "/service/metadata");

        root.addServlet(new ServletHolder(new SaslServlet()), "/service/sasl");

        root.addServlet(new ServletHolder(new RootServlet("/","/apidocs/","index.html")), "/");
        root.addServlet(new ServletHolder(new LogoutServlet()), "/logout");

        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDojoPath(), true)), "/dojo/dojo/*");
        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDijitPath(), true)), "/dojo/dijit/*");
        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDojoxPath(), true)), "/dojo/dojox/*");
        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDgridPath(), true)), "/dojo/dgrid/*");
        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDstorePath(), true)), "/dojo/dstore/*");

        for(String pattern : STATIC_FILE_TYPES)
        {
            root.addServlet(new ServletHolder(new FileServlet()), pattern);
        }

        root.addServlet(new ServletHolder(new TimeZoneServlet()), "/service/timezones");

        final SessionManager sessionManager = root.getSessionHandler().getSessionManager();
        sessionManager.getSessionCookieConfig().setName(JSESSIONID_COOKIE_PREFIX + lastPort);
        sessionManager.getSessionCookieConfig().setHttpOnly(true);
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

                            public void open() throws IOException
                            {
                                try
                                {
                                    super.open();
                                }
                                catch (BindException e)
                                {
                                    InetSocketAddress addr = getHost() == null ? new InetSocketAddress(getPort())
                                                                               : new InetSocketAddress(getHost(), getPort());
                                    throw new PortBindFailureException(addr);
                                }
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
                                        @Override
                                        public String[] selectProtocols(String[] enabledProtocols, String[] supportedProtocols)
                                        {
                                            return SSLUtil.filterEnabledProtocols(enabledProtocols, supportedProtocols,
                                                                                  port.getTlsProtocolWhiteList(),
                                                                                  port.getTlsProtocolBlackList());
                                        }

                                        @Override
                                        public String[] selectCipherSuites(String[] enabledCipherSuites, String[] supportedCipherSuites)
                                        {
                                            return SSLUtil.filterEnabledCipherSuites(enabledCipherSuites, supportedCipherSuites,
                                                                                     port.getTlsCipherSuiteWhiteList(),
                                                                                     port.getTlsCipherSuiteBlackList());
                                        }

                                        @Override
                                        public void customize(final SSLEngine sslEngine)
                                        {
                                            super.customize(sslEngine);
                                            useCipherOrderIfPossible(sslEngine);
                                        }

                                        private void useCipherOrderIfPossible(final SSLEngine sslEngine)
                                        {
                                            if(port.getTlsCipherSuiteWhiteList() != null
                                               && !port.getTlsCipherSuiteWhiteList().isEmpty())
                                            {
                                                SSLUtil.useCipherOrderIfPossible(sslEngine);
                                            }
                                        }
                                    };

        boolean needClientCert = port.getNeedClientAuth() || port.getWantClientAuth();

        if (needClientCert && trustStores.isEmpty())
        {
            throw new IllegalConfigurationException("Client certificate authentication is enabled on AMQP port '"
                                                    + this.getName() + "' but no trust store defined");
        }

        try
        {
            SSLContext sslContext = SSLUtil.tryGetSSLContext();
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

                        public void open() throws IOException
                        {
                            try
                            {
                                super.open();
                            }
                            catch (BindException e)
                            {
                                InetSocketAddress addr = getHost() == null ? new InetSocketAddress(getPort())
                                        : new InetSocketAddress(getHost(), getPort());
                                throw new PortBindFailureException(addr);
                            }
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

                        public void open() throws IOException
                        {
                            try
                            {
                                super.open();
                            }
                            catch (BindException e)
                            {
                                InetSocketAddress addr = getHost() == null ? new InetSocketAddress(getPort())
                                        : new InetSocketAddress(getHost(), getPort());
                                throw new PortBindFailureException(addr);
                            }
                        }
                    };
        return connector;
    }

    private void addRestServlet(ServletContextHandler root)
    {
        Set<Class<? extends ConfiguredObject>> categories = new HashSet<>(getModel().getSupportedCategories());
        final RestServlet restServlet = new RestServlet();
        final ApiDocsServlet apiDocsServlet = new ApiDocsServlet();

        for (Class<? extends ConfiguredObject> category : categories)
        {
            String name = category.getSimpleName().toLowerCase();

            ServletHolder servletHolder = new ServletHolder(name, restServlet);
            servletHolder.getRegistration().setMultipartConfig(
                    new MultipartConfigElement("",
                                               getContextValue(Long.class, MAX_HTTP_FILE_UPLOAD_SIZE_CONTEXT_NAME),
                                               -1l,
                                               getContextValue(Integer.class,
                                                               MAX_HTTP_FILE_UPLOAD_SIZE_CONTEXT_NAME)));

            List<String> paths = Arrays.asList("/api/latest/" + name,
                                               "/api/v" + BrokerModel.MODEL_VERSION + "/" + name);

            for (String path : paths)
            {
                root.addServlet(servletHolder, path + "/*");
            }
            ServletHolder docServletHolder = new ServletHolder(name + "docs", apiDocsServlet);
            root.addServlet(docServletHolder, "/apidocs/latest/" + name + "/");
            root.addServlet(docServletHolder, "/apidocs/v" + BrokerModel.MODEL_VERSION + "/" + name + "/");
            root.addServlet(docServletHolder, "/apidocs/latest/" + name);
            root.addServlet(docServletHolder, "/apidocs/v" + BrokerModel.MODEL_VERSION + "/" + name);


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
        return "v"+String.valueOf(BrokerModel.MODEL_VERSION);
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
        HttpPort<?> port = getPort(request);
        return port == null ? null : port.getAuthenticationProvider();
    }

    @SuppressWarnings("unused")
    public static Set<String> getAllAvailableCorsMethodCombinations()
    {
        List<String> methods = Arrays.asList("OPTION", "HEAD", "GET", "POST", "PUT", "DELETE");
        Set<Set<String>> combinations = new HashSet();
        int n = methods.size();
        assert n < 31 : "Too many combination to calculate";
        // enumerate all 2**n combinations
        for (int i = 0; i < (1 << n); ++i) {
            Set<String> currentCombination = new HashSet();
            // each bit in the variable i represents an item of the sequence
            // if the bit is set the item should appear in this particular combination
            for (int index = 0; index < n; ++index) {
                if ((i & (1 << index)) != 0) {
                    currentCombination.add(methods.get(index));
                }
            }
            combinations.add(currentCombination);
        }

        Set<String> combinationsAsString = new HashSet<>(combinations.size());
        ObjectMapper mapper = new ObjectMapper();
        for(Set<String> combination : combinations)
        {
            try(StringWriter writer = new StringWriter())
            {
                mapper.writeValue(writer, combination);
                combinationsAsString.add(writer.toString());
            }
            catch (IOException e)
            {
                throw new IllegalArgumentException("Unexpected IO Exception generating JSON string", e);
            }
        }
        return Collections.unmodifiableSet(combinationsAsString);
    }

    public static HttpPort<?> getPort(final HttpServletRequest request)
    {
        return (HttpPort<?>)request.getAttribute(PORT_SERVLET_ATTRIBUTE);
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
