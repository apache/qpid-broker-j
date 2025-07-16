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
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSessionContext;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.MultipartConfigElement;
import jakarta.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.ee10.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.ee10.servlet.ServletHandler;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.ssl.SslHandshakeListener;
import org.eclipse.jetty.rewrite.handler.CompactPathRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.DetectorConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.handler.CrossOriginHandler;
import org.eclipse.jetty.util.annotation.Name;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.messages.ManagementConsoleMessages;
import org.apache.qpid.server.logging.messages.PortMessages;
import org.apache.qpid.server.management.plugin.filter.AuthenticationCheckFilter;
import org.apache.qpid.server.management.plugin.filter.ExceptionHandlingFilter;
import org.apache.qpid.server.management.plugin.filter.InteractiveAuthenticationFilter;
import org.apache.qpid.server.management.plugin.filter.LoggingFilter;
import org.apache.qpid.server.management.plugin.filter.MethodFilter;
import org.apache.qpid.server.management.plugin.filter.RedirectFilter;
import org.apache.qpid.server.management.plugin.filter.RewriteRequestForUncompressedJavascript;
import org.apache.qpid.server.management.plugin.servlet.FileServlet;
import org.apache.qpid.server.management.plugin.servlet.RootServlet;
import org.apache.qpid.server.management.plugin.servlet.ContentServlet;
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
import org.apache.qpid.server.model.AbstractConfigurationChangeListener;
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
import org.apache.qpid.server.plugin.ContentFactory;
import org.apache.qpid.server.plugin.QpidServiceLoader;
import org.apache.qpid.server.transport.PortBindFailureException;
import org.apache.qpid.server.transport.network.security.ssl.SSLUtil;
import org.apache.qpid.server.util.DaemonThreadFactory;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

@ManagedObject( category = false, type = HttpManagement.PLUGIN_TYPE )
public class HttpManagement extends AbstractPluginAdapter<HttpManagement> implements HttpManagementConfiguration<HttpManagement>, PortManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpManagement.class);
    
    // 10 minutes by default
    public static final int DEFAULT_TIMEOUT_IN_SECONDS = 60 * 10;
    public static final String TIME_OUT = "sessionTimeout";
    public static final String HTTP_BASIC_AUTHENTICATION_ENABLED = "httpBasicAuthenticationEnabled";
    public static final String HTTPS_BASIC_AUTHENTICATION_ENABLED = "httpsBasicAuthenticationEnabled";
    public static final String HTTP_SASL_AUTHENTICATION_ENABLED = "httpSaslAuthenticationEnabled";
    public static final String HTTPS_SASL_AUTHENTICATION_ENABLED = "httpsSaslAuthenticationEnabled";

    public static final String PLUGIN_TYPE = "MANAGEMENT-HTTP";

    public static final String DEFAULT_LOGOUT_URL = "/logout.html";
    public static final String DEFAULT_LOGIN_URL = "/index.html";

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
    public Set<String> _corsAllowOrigins;

    @ManagedAttributeField
    public Set<String> _corsAllowMethods;

    @ManagedAttributeField
    public Set<String> _corsAllowHeaders;

    @ManagedAttributeField
    public Set<String> _allowedResponseHeaders;

    @ManagedAttributeField
    public boolean _corsAllowCredentials;

    @ManagedAttributeField
    private boolean _compressResponses;

    @ManagedAttributeField
    private boolean _useLegacyUriCompliance;

    private final Map<HttpPort<?>, ServerConnector> _portConnectorMap = new ConcurrentHashMap<>();
    private final Map<HttpPort<?>, SslContextFactory.Server> _sslContextFactoryMap = new ConcurrentHashMap<>();
    private final BrokerChangeListener _brokerChangeListener = new BrokerChangeListener();

    private volatile boolean _serveUncompressedDojo;
    private volatile Long _saslExchangeExpiry;
    private volatile ThreadPoolExecutor _jettyServerExecutor;

    @ManagedObjectFactoryConstructor
    public HttpManagement(final Map<String, Object> attributes, final Broker<?> broker)
    {
        super(attributes, broker);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        _serveUncompressedDojo = Boolean.TRUE.equals(getContextValue(Boolean.class, "qpid.httpManagement.serveUncompressedDojo"));
        _saslExchangeExpiry = getContextValue(Long.class, SASL_EXCHANGE_EXPIRY_CONTEXT_NAME);
        getBroker().addChangeListener(_brokerChangeListener);
    }

    @StateTransition(currentState = {State.UNINITIALIZED,State.ERRORED}, desiredState = State.ACTIVE)
    @SuppressWarnings("unused")
    private CompletableFuture<Void> doStart()
    {
        final Collection<HttpPort<?>> httpPorts = getEligibleHttpPorts(getBroker().getPorts());
        if (httpPorts.isEmpty())
        {
            LOGGER.warn("HttpManagement plugin is configured but no suitable HTTP ports are available.");
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
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<Void> onClose()
    {
        getBroker().removeChangeListener(_brokerChangeListener);
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

        if (_jettyServerExecutor != null)
        {
            _jettyServerExecutor.shutdown();
        }
        getBroker().getEventLogger().message(ManagementConsoleMessages.STOPPED(OPERATIONAL_LOGGING_NAME));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public int getSessionTimeout()
    {
        return _sessionTimeout;
    }

    @Override
    public Set<String> getCorsAllowOrigins()
    {
        return _corsAllowOrigins;
    }

    @Override
    public Set<String> getCorsAllowMethods()
    {
        return _corsAllowMethods;
    }

    @Override
    public Set<String> getCorsAllowHeaders()
    {
        return _corsAllowHeaders;
    }

    @Override
    public Set<String> getAllowedResponseHeaders()
    {
        return _allowedResponseHeaders;
    }

    @Override
    public boolean getCorsAllowCredentials()
    {
        return _corsAllowCredentials;
    }

    private Server createServer(final Collection<HttpPort<?>> ports)
    {
        LOGGER.debug("Starting up web server on {}", ports);

        _jettyServerExecutor = new ScheduledThreadPoolExecutor(1, new DaemonThreadFactory("Jetty-Server-Thread"));
        final Server server = new Server(new ExecutorThreadPool(_jettyServerExecutor));
        int lastPort = -1;
        for (final HttpPort<?> port : ports)
        {
            final ServerConnector connector = createConnector(port, server);
            connector.addBean(new ConnectionTrackingListener());
            server.addConnector(connector);
            _portConnectorMap.put(port, connector);
            lastPort = port.getPort();
        }

        final ContextHandlerCollection contextCollection = new ContextHandlerCollection();

        final RewriteHandler rewriteHandler = new RewriteHandler();
        rewriteHandler.setHandler(contextCollection);
        rewriteHandler.addRule(new CompactPathRule());

        final CrossOriginHandler corsHandler = new CrossOriginHandler();
        corsHandler.setHandler(rewriteHandler);
        corsHandler.setAllowedOriginPatterns(getCorsAllowOrigins());
        corsHandler.setAllowedMethods(getCorsAllowMethods());
        corsHandler.setAllowedHeaders(getCorsAllowHeaders());
        corsHandler.setAllowCredentials(getCorsAllowCredentials());

        final ServletContextHandler root = new ServletContextHandler("/",  ServletContextHandler.SESSIONS);
        root.setHandler(corsHandler);
        server.setHandler(root);

        final ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler()
        {
            @Override
            protected void writeErrorPageBody(HttpServletRequest request, Writer writer, int code, String message, boolean showStacks)
                    throws IOException
            {
                final String uri = request.getRequestURI();

                writeErrorPageMessage(request,writer,code,message,uri);

                for (int i= 0; i < 20; i++)
                {
                    writer.write("<br/>                                                \n");
                }
            }
        };
        root.setErrorHandler(errorHandler);

        // set servlet context attributes for broker and configuration
        root.getServletContext().setAttribute(HttpManagementUtil.ATTR_BROKER, getBroker());
        root.getServletContext().setAttribute(HttpManagementUtil.ATTR_MANAGEMENT_CONFIGURATION, this);

        root.addFilter(new FilterHolder(new ExceptionHandlingFilter()), "/*", EnumSet.allOf(DispatcherType.class));
        root.addFilter(new FilterHolder(new MethodFilter()), "/*", EnumSet.of(DispatcherType.REQUEST));

        addFiltersAndServletsForRest(root);
        if (!Boolean.TRUE.equals(getContextValue(Boolean.class, DISABLE_UI_CONTEXT_NAME)))
        {
            addFiltersAndServletsForUserInterfaces(root);
        }

        root.getSessionHandler().getSessionCookieConfig().setName(JSESSIONID_COOKIE_PREFIX + lastPort);
        root.getSessionHandler().getSessionCookieConfig().setHttpOnly(true);
        root.getSessionHandler().setMaxInactiveInterval(getSessionTimeout());

        if (_useLegacyUriCompliance)
        {
            server.getContainedBeans(ServletHandler.class)
                    .forEach(handler -> handler.setDecodeAmbiguousURIs(true));
        }

        return server;
    }

    private void addFiltersAndServletsForRest(final ServletContextHandler root)
    {
        final FilterHolder loggingFilter = new FilterHolder(new LoggingFilter());
        root.addFilter(loggingFilter, "/api/*", EnumSet.of(DispatcherType.REQUEST));
        root.addFilter(loggingFilter, "/service/*", EnumSet.of(DispatcherType.REQUEST));

        final FilterHolder restAuthorizationFilter = new FilterHolder(new AuthenticationCheckFilter());
        restAuthorizationFilter.setInitParameter(AuthenticationCheckFilter.INIT_PARAM_ALLOWED, "/service/sasl");
        root.addFilter(restAuthorizationFilter, "/api/*", EnumSet.of(DispatcherType.REQUEST));
        root.addFilter(restAuthorizationFilter, "/service/*", EnumSet.of(DispatcherType.REQUEST));

        addRestServlet(root);

        final ServletHolder queryServlet = new ServletHolder(new BrokerQueryServlet());
        root.addServlet(queryServlet, "/api/latest/querybroker/*");
        root.addServlet(queryServlet, "/api/v" + BrokerModel.MODEL_VERSION + "/querybroker/*");

        final ServletHolder vhQueryServlet = new ServletHolder(new VirtualHostQueryServlet());
        root.addServlet(vhQueryServlet, "/api/latest/queryvhost/*");
        root.addServlet(vhQueryServlet, "/api/v" + BrokerModel.MODEL_VERSION + "/queryvhost/*");

        root.addServlet(new ServletHolder(new StructureServlet()), "/service/structure");
        root.addServlet(new ServletHolder(new QueueReportServlet()), "/service/queuereport/*");
        root.addServlet(new ServletHolder(new MetaDataServlet()), "/service/metadata");
        root.addServlet(new ServletHolder(new TimeZoneServlet()), "/service/timezones");

        final Iterable<ContentFactory> contentFactories = new QpidServiceLoader().instancesOf(ContentFactory.class);
        contentFactories.forEach(f ->
        {
            final ServletHolder metricsServlet = new ServletHolder(new ContentServlet(f));
            final String path = f.getType().toLowerCase();
            root.addServlet(metricsServlet, "/" + path);
            root.addServlet(metricsServlet, "/" + path  + "/*");

            if (getContextValue(Boolean.class, HTTP_MANAGEMENT_ENABLE_CONTENT_AUTHENTICATION))
            {
                root.addFilter(restAuthorizationFilter, "/" + path, EnumSet.of(DispatcherType.REQUEST));
                root.addFilter(restAuthorizationFilter, "/" + path  + "/*", EnumSet.of(DispatcherType.REQUEST));
            }
        });
    }

    private void addFiltersAndServletsForUserInterfaces(final ServletContextHandler root)
    {
        root.addFilter(new FilterHolder(new AuthenticationCheckFilter()), "/apidocs/*", EnumSet.of(DispatcherType.REQUEST));
        root.addFilter(new FilterHolder(new InteractiveAuthenticationFilter()), DEFAULT_LOGIN_URL, EnumSet.of(DispatcherType.REQUEST));
        root.addFilter(new FilterHolder(new InteractiveAuthenticationFilter()), "/", EnumSet.of(DispatcherType.REQUEST));

        final FilterHolder redirectFilter = new FilterHolder(new RedirectFilter());
        redirectFilter.setInitParameter(RedirectFilter.INIT_PARAM_REDIRECT_URI, DEFAULT_LOGIN_URL);
        root.addFilter(redirectFilter, "/login.html", EnumSet.of(DispatcherType.REQUEST));

        if (_serveUncompressedDojo)
        {
            root.addFilter(RewriteRequestForUncompressedJavascript.class, "/dojo/dojo/*", EnumSet.of(DispatcherType.REQUEST));
            root.addFilter(RewriteRequestForUncompressedJavascript.class, "/dojo/dojox/*", EnumSet.of(DispatcherType.REQUEST));
        }

        addApiDocsServlets(root);

        root.addServlet(new ServletHolder(new SaslServlet()), "/service/sasl");
        root.addServlet(new ServletHolder(new RootServlet("/", "/apidocs/", "index.html")), "/");
        root.addServlet(new ServletHolder(new LogoutServlet()), "/logout");
        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDojoPath(), true)), "/dojo/dojo/*");
        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDijitPath(), true)), "/dojo/dijit/*");
        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDojoxPath(), true)), "/dojo/dojox/*");
        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDgridPath(), true)), "/dojo/dgrid/*");
        root.addServlet(new ServletHolder(new FileServlet(DojoHelper.getDstorePath(), true)), "/dojo/dstore/*");

        for (final String pattern : STATIC_FILE_TYPES)
        {
            root.addServlet(new ServletHolder(new FileServlet()), pattern);
        }
    }

    private void addApiDocsServlets(final ServletContextHandler root)
    {
        final ApiDocsServlet apiDocsServlet = new ApiDocsServlet();
        final ServletHolder apiDocsServletHolder = new ServletHolder(apiDocsServlet);
        final String version = "v" + BrokerModel.MODEL_VERSION;
        for (final String path : new String[]{"/apidocs", "/apidocs/latest", "/apidocs/" + version})
        {
            root.addServlet(apiDocsServletHolder, path);
            root.addServlet(apiDocsServletHolder, path + "/");
        }

        getModel().getSupportedCategories()
                  .stream()
                  .map(Class::getSimpleName)
                  .map(String::toLowerCase)
                  .forEach(name ->
                  {
                      root.addServlet(apiDocsServletHolder, "/apidocs/latest/" + name + "/");
                      root.addServlet(apiDocsServletHolder, "/apidocs/" + version + "/" + name + "/");
                      root.addServlet(apiDocsServletHolder, "/apidocs/latest/" + name);
                      root.addServlet(apiDocsServletHolder, "/apidocs/" + version + "/" + name);
                  });
    }

    @Override
    public int getBoundPort(final HttpPort httpPort)
    {
        final NetworkConnector networkConnector = _portConnectorMap.get(httpPort);
        return networkConnector != null ? networkConnector.getLocalPort() : -1;
    }

    @Override
    public int getNumberOfAcceptors(final HttpPort httpPort)
    {
        final ServerConnector serverConnector = _portConnectorMap.get(httpPort);
        return serverConnector != null ? serverConnector.getAcceptors() : -1;
    }

    @Override
    public int getNumberOfSelectors(final HttpPort httpPort)
    {
        final ServerConnector serverConnector = _portConnectorMap.get(httpPort);
        return serverConnector != null ? serverConnector.getSelectorManager().getSelectorCount() : -1;
    }

    @Override
    public SSLContext getSSLContext(final HttpPort httpPort)
    {
        final SslContextFactory sslContextFactory = getSslContextFactory(httpPort);
        return sslContextFactory != null ? sslContextFactory.getSslContext() : null;
    }

    @Override
    public boolean updateSSLContext(final HttpPort httpPort)
    {
        final SslContextFactory.Server sslContextFactory = getSslContextFactory(httpPort);
        if (sslContextFactory == null)
        {
            return false;
        }
        try
        {
            final SSLContext sslContext = createSslContext(httpPort);
            sslContextFactory.reload(sslContextFactory1 ->
            {
                final SslContextFactory.Server server = (SslContextFactory.Server) sslContextFactory1;
                server.setSslContext(sslContext);
                server.setNeedClientAuth(httpPort.getNeedClientAuth());
                server.setWantClientAuth(httpPort.getWantClientAuth());
            });
            return true;
        }
        catch (Exception e)
        {
            throw new IllegalConfigurationException("Unexpected exception on reload of ssl context factory", e);
        }
    }

    private SslContextFactory.Server getSslContextFactory(final HttpPort httpPort)
    {
        return _sslContextFactoryMap.get(httpPort);
    }

    private ServerConnector createConnector(final HttpPort<?> port, final Server server)
    {
        port.setPortManager(this);

        if (port.getState() != State.ACTIVE)
        {
            // TODO - RG - probably does nothing
            port.startAsync();
        }

        final HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory();
        httpConnectionFactory.getHttpConfiguration().setSendServerVersion(false);
        httpConnectionFactory.getHttpConfiguration().setSendXPoweredBy(false);
        HttpConfiguration.Customizer requestAttributeCustomizer = (Request request, HttpFields.Mutable responseHeaders) ->
        {
            HttpManagementUtil.getPortAttributeAction(port).performAction(request);
            return request;
        };

        httpConnectionFactory.getHttpConfiguration().addCustomizer(requestAttributeCustomizer);

        httpConnectionFactory.getHttpConfiguration().addCustomizer(new SecureRequestCustomizer());

        if (_useLegacyUriCompliance)
        {
            final Set<UriCompliance.Violation> violations = Set.of(UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR,
                    UriCompliance.Violation.AMBIGUOUS_PATH_ENCODING);
            final UriCompliance uriCompliance = UriCompliance.from(violations);
            httpConnectionFactory.getHttpConfiguration().setUriCompliance(uriCompliance);
        }

        ConnectionFactory[] connectionFactories;
        Collection<Transport> transports = port.getTransports();
        SslContextFactory.Server sslContextFactory = null;
        if (!transports.contains(Transport.SSL))
        {
            connectionFactories = new ConnectionFactory[]{ httpConnectionFactory };
        }
        else if (transports.contains(Transport.SSL))
        {
            sslContextFactory = createSslContextFactory(port);
            ConnectionFactory.Detecting sslConnectionFactory =
                    new SslConnectionFactory(sslContextFactory, httpConnectionFactory.getProtocol());
            if (port.getTransports().contains(Transport.TCP))
            {
                sslConnectionFactory = new DetectorConnectionFactory(sslConnectionFactory);
            }
            connectionFactories = new ConnectionFactory[]{ sslConnectionFactory, httpConnectionFactory };
        }
        else
        {
            throw new IllegalArgumentException("Unexpected transport on port " + port.getName() + ":" + transports);
        }

        final ServerConnector connector = new ServerConnector(server,
                new QBBTrackingThreadPool(port.getThreadPoolMaximum(), port.getThreadPoolMinimum()),
                null,
                null,
                port.getDesiredNumberOfAcceptors(),
                port.getDesiredNumberOfSelectors(),
                connectionFactories)
        {
            @Override
            public void open() throws IOException
            {
                try
                {
                    super.open();
                }
                catch (BindException e)
                {
                    _sslContextFactoryMap.remove(port);
                    InetSocketAddress addr = getHost() == null ? new InetSocketAddress(getPort())
                            : new InetSocketAddress(getHost(), getPort());
                    throw new PortBindFailureException(addr);
                }
            }
        };

        connector.setAcceptQueueSize(port.getAcceptBacklogSize());
        final String bindingAddress = port.getBindingAddress();
        if (bindingAddress != null && !bindingAddress.trim().isEmpty() && !bindingAddress.trim().equals("*"))
        {
            connector.setHost(bindingAddress.trim());
        }
        connector.setPort(port.getPort());

        if (transports.contains(Transport.SSL))
        {
            connector.addBean(new SslHandshakeListener()
            {
                @Override
                public void handshakeFailed(final Event event, final Throwable failure)
                {
                    final SSLEngine sslEngine = event.getSSLEngine();
                    final String hostname = sslEngine.getPeerHost();
                    final int port = sslEngine.getPeerPort();
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.info("TLS handshake failed: host='{}', port={}", hostname, port, failure);
                    }
                    else
                    {
                        LOGGER.info("TLS handshake failed: host='{}', port={}: {}", hostname, port, String.valueOf(failure));
                    }
                }
            });
        }

        final int acceptors = connector.getAcceptors();
        final int selectors = connector.getSelectorManager().getSelectorCount();
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug(
                    "Created connector for http port {} with maxThreads={}, minThreads={}, acceptors={}, selectors={}, acceptBacklog={}",
                    port.getName(),
                    port.getThreadPoolMaximum(),
                    port.getThreadPoolMinimum(),
                    acceptors,
                    selectors,
                    port.getAcceptBacklogSize());
        }

        final int requiredNumberOfConnections = acceptors + 2 * selectors + 1;
        if (port.getThreadPoolMaximum() < requiredNumberOfConnections)
        {
            throw new IllegalConfigurationException(String.format(
                    "Insufficient number of threads is configured on http port '%s': max=%d < needed(acceptors=%d + selectors=2*%d + request=1)",
                    port.getName(),
                    port.getThreadPoolMaximum(),
                    acceptors,
                    selectors));
        }
        if (sslContextFactory != null)
        {
            _sslContextFactoryMap.put(port, sslContextFactory);
        }

        return connector;
    }

    private SslContextFactory.Server createSslContextFactory(final HttpPort<?> port)
    {
        final SslContextFactory.Server factory = new SslContextFactory.Server()
        {
            @Override
            public void customize(final SSLEngine sslEngine)
            {
                super.customize(sslEngine);
                if (port.getTlsCipherSuiteAllowList() != null && !port.getTlsCipherSuiteAllowList().isEmpty())
                {
                    final SSLParameters sslParameters = sslEngine.getSSLParameters();
                    sslParameters.setUseCipherSuitesOrder(true);
                    sslEngine.setSSLParameters(sslParameters);
                }
                SSLUtil.updateEnabledCipherSuites(sslEngine, port.getTlsCipherSuiteAllowList(), port.getTlsCipherSuiteDenyList());
                SSLUtil.updateEnabledTlsProtocols(sslEngine, port.getTlsProtocolAllowList(), port.getTlsProtocolDenyList());
            }
        };
        factory.setSslContext(createSslContext(port));
        if (port.getNeedClientAuth())
        {
            factory.setNeedClientAuth(true);
        }
        else if (port.getWantClientAuth())
        {
            factory.setWantClientAuth(true);
        }
        return factory;
    }

    private SSLContext createSslContext(final HttpPort<?> port)
    {
        final KeyStore<?> keyStore = port.getKeyStore();
        if (keyStore == null)
        {
            throw new IllegalConfigurationException(
                    "Key store is not configured. Cannot start management on HTTPS port without keystore");
        }

        final boolean needClientCert = port.getNeedClientAuth() || port.getWantClientAuth();
        final Collection<TrustStore> trustStores = port.getTrustStores();

        if (needClientCert && trustStores.isEmpty())
        {
            throw new IllegalConfigurationException(String.format(
                    "Client certificate authentication is enabled on HTTPS port '%s' but no trust store defined",
                    this.getName()));
        }

        final SSLContext sslContext = SSLUtil.createSslContext(port.getKeyStore(), trustStores, port.getName());
        final SSLSessionContext serverSessionContext = sslContext.getServerSessionContext();
        if (port.getTLSSessionCacheSize() > 0)
        {
            serverSessionContext.setSessionCacheSize(port.getTLSSessionCacheSize());
        }
        if (port.getTLSSessionTimeout() > 0)
        {
            serverSessionContext.setSessionTimeout(port.getTLSSessionTimeout());
        }
        return sslContext;
    }

    private void addRestServlet(final ServletContextHandler root)
    {
        final Map<String, ManagementControllerFactory> factories = ManagementControllerFactory.loadFactories();
        final long maxFileSize = getContextValue(Long.class, MAX_HTTP_FILE_UPLOAD_SIZE_CONTEXT_NAME);
        final int maxRequestSize = getContextValue(Integer.class, MAX_HTTP_FILE_UPLOAD_SIZE_CONTEXT_NAME);
        final List<String> supportedVersions = new ArrayList<>();
        supportedVersions.add("latest");
        String currentVersion = BrokerModel.MODEL_VERSION;
        ManagementController managementController = null;
        ManagementControllerFactory factory;
        do
        {
            factory = factories.get(currentVersion);
            if (factory != null)
            {
                managementController = factory.createManagementController(this, managementController);
                final RestServlet managementServlet = new RestServlet();
                final Collection<String> categories = managementController.getCategories();
                for (final String category : categories)
                {
                    final String name = category.toLowerCase();
                    final String path = managementController.getCategoryMapping(name);
                    final ServletHolder servletHolder = new ServletHolder(path, managementServlet);
                    servletHolder.setInitParameter("qpid.controller.version", managementController.getVersion());

                    servletHolder.getRegistration().setMultipartConfig(
                            new MultipartConfigElement("", maxFileSize, -1L, maxRequestSize));

                    root.addServlet(servletHolder, path + (path.endsWith("/") ? "*" : "/*"));

                    if (BrokerModel.MODEL_VERSION.equals(managementController.getVersion()))
                    {
                        root.addServlet(servletHolder, "/api/latest/" + name + "/*");
                    }
                }
                supportedVersions.add("v" + currentVersion);
                currentVersion = factory.getPreviousVersion();
            }
        }
        while (factory != null);
        root.getServletContext().setAttribute("qpid.controller.chain", managementController);
        final Map<String, List<String>> supported = Map.of("supportedVersions", supportedVersions);
        final ServletHolder versionsServletHolder = new ServletHolder(new JsonValueServlet(supported));
        root.addServlet(versionsServletHolder, "/api");
        root.addServlet(versionsServletHolder, "/api/");
    }

    private void logOperationalListenMessages()
    {
        for (final Map.Entry<HttpPort<?>, ServerConnector> portConnector : _portConnectorMap.entrySet())
        {
            final HttpPort<?> port = portConnector.getKey();
            final NetworkConnector connector = portConnector.getValue();
            logOperationalListenMessages(port, connector.getLocalPort());
        }
    }

    private void logOperationalListenMessages(final HttpPort<?> port, final int localPort)
    {
        final Set<Transport> transports = port.getTransports();
        for (final Transport transport: transports)
        {
            getBroker().getEventLogger().message(ManagementConsoleMessages.LISTENING(Protocol.HTTP.name(),
                                                                                     transport.name(),
                                                                                     localPort));
        }
    }

    private void logOperationalShutdownMessage()
    {
        for (final NetworkConnector connector : _portConnectorMap.values())
        {
            logOperationalShutdownMessage(connector.getLocalPort());
        }
    }

    private void logOperationalShutdownMessage(final int localPort)
    {
        getBroker().getEventLogger().message(ManagementConsoleMessages.SHUTTING_DOWN(Protocol.HTTP.name(), localPort));
    }

    private Collection<HttpPort<?>> getEligibleHttpPorts(final Collection<Port<?>> ports)
    {
        final Collection<HttpPort<?>> httpPorts = new HashSet<>();
        for (final Port<?> port : ports)
        {
            if (State.ACTIVE == port.getDesiredState() && State.ERRORED != port.getState() &&
                    port.getProtocols().contains(Protocol.HTTP))
            {
                httpPorts.add((HttpPort<?>) port);
            }
        }
        return Collections.unmodifiableCollection(httpPorts);
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
    public boolean isUseLegacyUriCompliance()
    {
        return _useLegacyUriCompliance;
    }

    @Override
    public boolean isCompressResponses()
    {
        return _compressResponses;
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider(final HttpServletRequest request)
    {
        final HttpPort<?> port = getPort(request);
        return port == null ? null : port.getAuthenticationProvider();
    }

    @SuppressWarnings("unused")
    public static Set<String> getAllAvailableCorsMethodCombinations()
    {
        final List<String> methods = List.of("OPTIONS", "HEAD", "GET", "POST", "PUT", "DELETE");
        final Set<Set<String>> combinations = new HashSet<>();
        final int n = methods.size();
        assert n < 31 : "Too many combination to calculate";
        // enumerate all 2**n combinations
        for (int i = 0; i < (1 << n); ++i) {
            final Set<String> currentCombination = new HashSet<>();
            // each bit in the variable i represents an item of the sequence
            // if the bit is set the item should appear in this particular combination
            for (int index = 0; index < n; ++index)
            {
                if ((i & (1 << index)) != 0)
                {
                    currentCombination.add(methods.get(index));
                }
            }
            combinations.add(currentCombination);
        }

        final Set<String> combinationsAsString = new HashSet<>(combinations.size());
        ObjectMapper mapper = new ObjectMapper();
        for (Set<String> combination : combinations)
        {
            try
            {
                combinationsAsString.add(mapper.writeValueAsString(combination));
            }
            catch (IOException e)
            {
                throw new IllegalArgumentException("Unexpected IO Exception generating JSON string", e);
            }
        }
        return Collections.unmodifiableSet(combinationsAsString);
    }

    @Override
    public HttpPort<?> getPort(final HttpServletRequest request)
    {
        return HttpManagementUtil.getPort(request);
    }

    @Override
    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);

        HttpManagementConfiguration<?> updated = (HttpManagementConfiguration<?>)proxyForValidation;
        if (changedAttributes.contains(HttpManagement.NAME) && !getName().equals(updated.getName()))
        {
            throw new IllegalConfigurationException("Changing the name of http management plugin is not allowed");
        }
        if (changedAttributes.contains(TIME_OUT) && updated.getSessionTimeout() < 0)
        {
            throw new IllegalConfigurationException("Only positive integer value can be specified for the session time out attribute");
        }
    }

    @Override
    public long getSaslExchangeExpiry()
    {
        return _saslExchangeExpiry;
    }

    private static class QBBTrackingThreadPool extends QueuedThreadPool
    {
        private final ThreadFactory _threadFactory;

        public QBBTrackingThreadPool(@Name("maxThreads") final int maxThreads, @Name("minThreads") final int minThreads)
        {
            super(maxThreads, minThreads);
            _threadFactory = QpidByteBuffer.createQpidByteBufferTrackingThreadFactory(QBBTrackingThreadPool.super::newThread);
        }

        @Override
        public Thread newThread(final Runnable runnable)
        {
            return _threadFactory.newThread(runnable);
        }
    }

    private class BrokerChangeListener extends AbstractConfigurationChangeListener
    {
        @Override
        public void childAdded(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            if (child instanceof HttpPort)
            {
                final HttpPort<?> port = (HttpPort<?>) child;
                Server server = _server;
                if (server != null)
                {
                    ServerConnector connector = null;
                    try
                    {
                        connector = createConnector(port, server);
                        connector.addBean(new ConnectionTrackingListener());
                        server.addConnector(connector);
                        connector.start();
                        _portConnectorMap.put(port, connector);
                        logOperationalListenMessages(port, connector.getLocalPort());
                    }
                    catch (Exception e)
                    {
                        if (connector != null)
                        {
                            server.removeConnector(connector);
                        }
                        LOGGER.warn("HTTP management connector creation failed for http port {}", port, e);
                    }
                }
            }
        }

        @Override
        public void childRemoved(final ConfiguredObject<?> object, final ConfiguredObject<?> child)
        {
            if (child instanceof HttpPort)
            {
                final HttpPort<?> port = (HttpPort<?>) child;
                Server server = _server;
                if (server != null)
                {
                    _sslContextFactoryMap.remove(port);
                    final ServerConnector connector = _portConnectorMap.remove(port);
                    if (connector != null)
                    {
                        final int localPort = connector.getLocalPort();

                        final ConnectionTrackingListener tracker = connector.getBean(ConnectionTrackingListener.class);
                        // closes the server socket - we will see no new connections arriving
                        connector.close();
                        // minimise the timeout of endpoints so they close in a timely fashion
                        connector.setIdleTimeout(1);
                        connector.getConnectedEndPoints().forEach(endPoint -> endPoint.setIdleTimeout(1));
                        LOGGER.debug("Connector has {} connection(s)", tracker.getConnectionCount());

                        final TaskExecutor taskExecutor = getBroker().getTaskExecutor();
                        tracker.getAllClosedFuture().whenCompleteAsync(new BiConsumer<Void, Throwable>()
                        {
                            @Override
                            public void accept(final Void unused, final Throwable throwable)
                            {
                                final int connectionCount = tracker.getConnectionCount();
                                if (connectionCount == 0)
                                {
                                    LOGGER.debug("Stopping connector for http port {}", localPort);
                                    try
                                    {
                                        connector.stop();
                                    }
                                    catch (Exception e)
                                    {
                                        LOGGER.warn("Failed to stop connector for http port {}", localPort, e);
                                    }
                                    finally
                                    {
                                        logOperationalShutdownMessage(localPort);
                                        _server.removeConnector(connector);
                                    }
                                }
                                else
                                {
                                    LOGGER.debug("Connector still has {} connection(s)", tracker.getConnectionCount());
                                    connector.getConnectedEndPoints().forEach(endPoint -> endPoint.setIdleTimeout(1));
                                    tracker.getAllClosedFuture().whenCompleteAsync(this, taskExecutor);
                                }
                            }
                        }, taskExecutor);
                    }
                }
            }
        }
    }

    private static class ConnectionTrackingListener implements Connection.Listener
    {
        private final Map<Connection, CompletableFuture<Void>> _closeFutures = new ConcurrentHashMap<>();

        @Override
        public void onOpened(final Connection connection)
        {
            _closeFutures.put(connection, new CompletableFuture<>());
        }

        @Override
        public void onClosed(final Connection connection)
        {
            CompletableFuture<Void> closeFuture = _closeFutures.remove(connection);
            if (closeFuture != null)
            {
                closeFuture.complete(null);
            }
        }

        public CompletableFuture<Void> getAllClosedFuture()
        {
            return CompletableFuture.allOf(_closeFutures.values().toArray(new CompletableFuture[0]));
        }

        public int getConnectionCount()
        {
            return _closeFutures.size();
        }
    }
}
