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
 *
 */

package org.apache.qpid.tests.utils;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedAction;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.SystemLauncherListener;
import org.apache.qpid.server.logging.logback.LogbackLoggingSystemLauncherListener;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.IllegalStateTransitionException;
import org.apache.qpid.server.model.ManageableMessage;
import org.apache.qpid.server.model.NotFoundException;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.SystemConfig;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.plugin.PluggableService;
import org.apache.qpid.server.security.auth.TaskPrincipal;
import org.apache.qpid.server.security.auth.manager.oauth2.cloudfoundry.CloudFoundryOAuth2IdentityResolverService;
import org.apache.qpid.server.store.MemoryConfigurationStore;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.apache.qpid.server.virtualhostnode.JsonVirtualHostNode;
import org.apache.qpid.test.utils.tls.TlsResource;

@SuppressWarnings({"java:S116", "unchecked", "unused"})
// sonar complains about variable names
@PluggableService
public class EmbeddedBrokerPerClassAdminImpl implements BrokerAdmin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedBrokerPerClassAdminImpl.class);
    public static final String TYPE = "EMBEDDED_BROKER_PER_CLASS";
    private final Map<String, Integer> _ports = new HashMap<>();
    private String _tempAuthProvider;
    private SystemLauncher _systemLauncher;
    private Broker<?> _broker;
    private VirtualHostNode<?> _currentVirtualHostNode;
    private String _currentWorkDirectory;
    private boolean _isPersistentStore;
    private Map<String, String> _preservedProperties;

    @Override
    public void beforeTestClass(final Class testClass)
    {
        _preservedProperties = new HashMap<>();
        try
        {
            String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis()));
            _currentWorkDirectory = Files.createTempDirectory(String.format("qpid-work-%s-%s-", timestamp, testClass.getSimpleName())).toString();

            ConfigItem[] configItems = (ConfigItem[]) testClass.getAnnotationsByType(ConfigItem.class);
            Arrays.stream(configItems).filter(ConfigItem::jvm).forEach(i ->
            {
                _preservedProperties.put(i.name(), System.getProperty(i.name()));
                System.setProperty(i.name(), i.value());
            });
            Map<String, String> context = new HashMap<>();
            context.put("qpid.work_dir", _currentWorkDirectory);
            context.put("qpid.port.protocol_handshake_timeout", "1000000");
            context.putAll(Arrays.stream(configItems)
                    .filter(i -> !i.jvm())
                    .collect(Collectors.toMap(ConfigItem::name, ConfigItem::value, (name, value) -> value)));

            Map<String,Object> systemConfigAttributes = new HashMap<>();
            systemConfigAttributes.put(ConfiguredObject.CONTEXT, context);
            systemConfigAttributes.put(ConfiguredObject.TYPE, System.getProperty("broker.config-store-type", "JSON"));
            systemConfigAttributes.put(SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, Boolean.FALSE);

            if (Thread.getDefaultUncaughtExceptionHandler() == null)
            {
                Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());
            }

            LOGGER.info("Starting internal broker (same JVM)");

            List<SystemLauncherListener> systemLauncherListeners = new ArrayList<>();
            systemLauncherListeners.add(new LogbackLoggingSystemLauncherListener());
            systemLauncherListeners.add(new ShutdownLoggingSystemLauncherListener());
            systemLauncherListeners.add(new PortExtractingLauncherListener());
            _systemLauncher = new SystemLauncher(systemLauncherListeners.toArray(new SystemLauncherListener[0]));

            _systemLauncher.startup(systemConfigAttributes);
        }
        catch (Exception e)
        {
            throw new BrokerAdminException("Failed to start broker for test class", e);
        }
    }

    @Override
    public void beforeTestMethod(final Class testClass, final Method method)
    {
        final String virtualHostNodeName = testClass.getSimpleName() + "_" + method.getName();
        final String storeType = System.getProperty("virtualhostnode.type");
        _isPersistentStore = !"Memory".equals(storeType);

        String storeDir = null;
        if (System.getProperty("profile", "").startsWith("java-dby-mem"))
        {
            storeDir = ":memory:";
        }
        else if (!MemoryConfigurationStore.TYPE.equals(storeType))
        {
            storeDir = "${qpid.work_dir}" + File.separator + virtualHostNodeName;
        }

        String blueprint = System.getProperty("virtualhostnode.context.blueprint");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, virtualHostNodeName);
        attributes.put(ConfiguredObject.TYPE, storeType);
        attributes.put(ConfiguredObject.CONTEXT, Collections.singletonMap("virtualhostBlueprint", blueprint));
        attributes.put(VirtualHostNode.DEFAULT_VIRTUAL_HOST_NODE, true);
        attributes.put(VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION, blueprint);
        if (storeDir != null)
        {
            attributes.put(JsonVirtualHostNode.STORE_PATH, storeDir);
        }

        if (method.getAnnotation(AddOAuth2MockProvider.class) != null)
        {
            createOAuth2AuthenticationManager();
        }

        _currentVirtualHostNode = _broker.createChild(VirtualHostNode.class, attributes);
    }

    @Override
    public void afterTestMethod(final Class testClass, final Method method)
    {
        Subject deleteSubject = new Subject(true,
                                            new HashSet<>(Arrays.asList(_systemLauncher.getSystemPrincipal(),
                                                                        new TaskPrincipal("afterTestMethod"))),
                                            Collections.emptySet(),
                                            Collections.emptySet());
        Subject.doAs(deleteSubject, (PrivilegedAction<Object>) () -> {
            if (Boolean.getBoolean("broker.clean.between.tests"))
            {
                _currentVirtualHostNode.delete();
            }
            else
            {
                _currentVirtualHostNode.setAttributes(Collections.singletonMap(VirtualHostNode.DEFAULT_VIRTUAL_HOST_NODE,
                                                                               false));
            }
            return null;
        });

        if (method.getAnnotation(AddOAuth2MockProvider.class) != null)
        {
            final AmqpPort<?> port = (AmqpPort<?>) _broker.getPorts().stream().filter(p -> "AMQP".equals(p.getName()))
                    .findFirst().orElse(null);
            if (port != null)
            {
                port.setAttributes(Map.of(Port.AUTHENTICATION_PROVIDER, _tempAuthProvider));
            }
        }
    }

    @Override
    public void afterTestClass(final Class testClass)
    {
        _systemLauncher.shutdown();
        _ports.clear();
        if (Boolean.getBoolean("broker.clean.between.tests"))
        {
            FileUtils.delete(new File(_currentWorkDirectory), true);
        }

        _preservedProperties.forEach((k,v)-> {
            if (v == null)
            {
                System.clearProperty(k);
            }
            else
            {
                System.setProperty(k, v);
            }
        });
    }

    @Override
    public InetSocketAddress getBrokerAddress(final PortType portType)
    {
        Integer port = _ports.get(portType.name());
        if (port == null)
        {
            throw new IllegalStateException(String.format("Could not find port with name '%s' on the Broker", portType.name()));
        }
        return InetSocketAddress.createUnresolved("localhost", port);
    }

    @Override
    public void createQueue(final String queueName)
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(ConfiguredObject.NAME, queueName);
        attributes.put(ConfiguredObject.TYPE, "standard");
        final Queue<?> queue = _currentVirtualHostNode.getVirtualHost().createChild(Queue.class, attributes);
        final Exchange<?> exchange = _currentVirtualHostNode.getVirtualHost().getChildByName(Exchange.class, "amq.direct");
        exchange.bind(queueName, queueName, Collections.emptyMap(), false);
    }

    @Override
    public void deleteQueue(final String queueName)
    {
        getQueue(queueName).delete();
    }

    @Override
    public void putMessageOnQueue(final String queueName, final String... messages)
    {
        for (String message : messages)
        {
            ((QueueManagingVirtualHost<?>) _currentVirtualHostNode.getVirtualHost()).publishMessage(new ManageableMessage()
            {
                @Override
                public String getAddress()
                {
                    return queueName;
                }

                @Override
                public boolean isPersistent()
                {
                    return false;
                }

                @Override
                public Date getExpiration()
                {
                    return null;
                }

                @Override
                public String getCorrelationId()
                {
                    return null;
                }

                @Override
                public String getAppId()
                {
                    return null;
                }

                @Override
                public String getMessageId()
                {
                    return null;
                }

                @Override
                public String getMimeType()
                {
                    return "text/plain";
                }

                @Override
                public String getEncoding()
                {
                    return null;
                }

                @Override
                public int getPriority()
                {
                    return 0;
                }

                @Override
                public Date getNotValidBefore()
                {
                    return null;
                }

                @Override
                public String getReplyTo()
                {
                    return null;
                }

                @Override
                public Map<String, Object> getHeaders()
                {
                    return Map.of();
                }

                @Override
                public Object getContent()
                {
                    return message;
                }

                @Override
                public String getContentTransferEncoding()
                {
                    return null;
                }
            });
        }

    }

    @Override
    public int getQueueDepthMessages(final String testQueueName)
    {
        Queue<?> queue = _currentVirtualHostNode.getVirtualHost().getChildByName(Queue.class, testQueueName);
        return queue.getQueueDepthMessages();
    }

    @Override
    public boolean supportsRestart()
    {
        return _isPersistentStore;
    }

    @Override
    public CompletableFuture<Void> restart()
    {
        try
        {
            LOGGER.info("Stopping VirtualHostNode for restart");
            _currentVirtualHostNode.stop();
            LOGGER.info("Starting VirtualHostNode for restart");
            _currentVirtualHostNode.start();
            LOGGER.info("Restarting VirtualHostNode completed");
        }
        catch (Exception e)
        {
            return CompletableFuture.failedFuture(e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isAnonymousSupported()
    {
        return true;
    }

    @Override
    public boolean isSASLSupported()
    {
        return true;
    }

    @Override
    public boolean isSASLMechanismSupported(final String mechanismName)
    {
        return true;
    }

    @Override
    public boolean isWebSocketSupported()
    {
        return true;
    }

    @Override
    public boolean isQueueDepthSupported()
    {
        return true;
    }

    @Override
    public boolean isManagementSupported()
    {
        return true;
    }

    @Override
    public boolean isPutMessageOnQueueSupported()
    {
        return true;
    }

    @Override
    public boolean isDeleteQueueSupported()
    {
        return true;
    }

    @Override
    public String getValidUsername()
    {
        return "guest";
    }

    @Override
    public String getValidPassword()
    {
        return "guest";
    }

    @Override
    public String getKind()
    {
        return KIND_BROKER_J;
    }

    @Override
    public String getType()
    {
        return TYPE;
    }

    private Queue<?> getQueue(final String queueName)
    {
        return _currentVirtualHostNode.getVirtualHost().getChildren(Queue.class).stream()
                .filter(queue -> queue.getName().equals(queueName))
                .findFirst()
                .orElseThrow(() -> new NotFoundException(String.format("Queue '%s' not found", queueName)));
    }

    private void createOAuth2AuthenticationManager()
    {
        final String TEST_CLIENT_ID = "testClientId";
        final String TEST_CLIENT_SECRET = "testClientSecret";
        final String TEST_IDENTITY_RESOLVER_TYPE = CloudFoundryOAuth2IdentityResolverService.TYPE;
        final String TEST_URI_PATTERN = "https://%s:%d%s";
        final String TEST_AUTHORIZATION_ENDPOINT_NEEDS_AUTH = "true";
        final String TEST_SCOPE = "testScope";
        final String TEST_TRUST_STORE_NAME = null;
        final String TEST_ENDPOINT_HOST = "localhost";
        final String TEST_AUTHORIZATION_ENDPOINT_PATH = "/testauth";
        final String TEST_TOKEN_ENDPOINT_PATH = "/testtoken";
        final String TEST_IDENTITY_RESOLVER_ENDPOINT_PATH = "/testidresolver";
        final String TEST_POST_LOGOUT_PATH = "/testpostlogout";

        OAuth2MockEndpointHolder server;
        try
        {
            final TlsResource tlsResource = new TlsResource();
            tlsResource.beforeAll(null);
            final Path keyStore = tlsResource.createSelfSignedKeyStore("CN=127.0.0.1");
            server = new OAuth2MockEndpointHolder(keyStore.toFile().getAbsolutePath(), tlsResource.getSecret(), tlsResource.getKeyStoreType());
            final OAuth2MockEndpoint identityResolverEndpoint = new OAuth2MockEndpoint();
            identityResolverEndpoint.putExpectedParameter("token", "A".repeat(10_0000));
            identityResolverEndpoint.setExpectedMethod("POST");
            identityResolverEndpoint.setNeedsAuth(true);
            identityResolverEndpoint.setResponse(200, String.format("{\"user_name\":\"%s\"}", "xxx"));

            server.start();
            server.setEndpoints(Map.of(TEST_IDENTITY_RESOLVER_ENDPOINT_PATH, identityResolverEndpoint));

            final TrustManager[] trustingTrustManager = new TrustManager[]{new TrustingTrustManager()};

            final SSLContext sc = SSLContext.getInstance("TLSv1.3");
            sc.init(null, trustingTrustManager, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier(new BlindHostnameVerifier());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        final Map<String, Object> authProviderAttributes = new HashMap<>();
        String testOAuthProvider = "testOAuthProvider";
        authProviderAttributes.put(ConfiguredObject.NAME, testOAuthProvider);
        authProviderAttributes.put(ConfiguredObject.TYPE, "OAuth2");
        authProviderAttributes.put("clientId", TEST_CLIENT_ID);
        authProviderAttributes.put("clientSecret", TEST_CLIENT_SECRET);
        authProviderAttributes.put("identityResolverType", TEST_IDENTITY_RESOLVER_TYPE);
        authProviderAttributes.put("authorizationEndpointURI",
                String.format(TEST_URI_PATTERN, TEST_ENDPOINT_HOST, server.getPort(), TEST_AUTHORIZATION_ENDPOINT_PATH));
        authProviderAttributes.put("tokenEndpointURI",
                String.format(TEST_URI_PATTERN, TEST_ENDPOINT_HOST, server.getPort(), TEST_TOKEN_ENDPOINT_PATH));
        authProviderAttributes.put("tokenEndpointNeedsAuth", TEST_AUTHORIZATION_ENDPOINT_NEEDS_AUTH);
        authProviderAttributes.put("identityResolverEndpointURI",
                String.format(TEST_URI_PATTERN, TEST_ENDPOINT_HOST, server.getPort(), TEST_IDENTITY_RESOLVER_ENDPOINT_PATH));
        authProviderAttributes.put("postLogoutURI",
                String.format(TEST_URI_PATTERN, TEST_ENDPOINT_HOST, server.getPort(), TEST_POST_LOGOUT_PATH));
        authProviderAttributes.put("scope", TEST_SCOPE);
        authProviderAttributes.put("trustStore", TEST_TRUST_STORE_NAME);
        authProviderAttributes.put("secureOnlyMechanisms", List.of());
        final AuthenticationProvider<?> auth = _broker.createChild(AuthenticationProvider.class, authProviderAttributes);
        final AmqpPort<?> port = (AmqpPort<?>) _broker.getPorts().stream().filter(p -> "AMQP".equals(p.getName()))
                .findFirst().orElse(null);
        if (port != null)
        {
            final AuthenticationProvider<?> authProvider = (AuthenticationProvider<?>) port
                    .getAttribute(Port.AUTHENTICATION_PROVIDER);
            if (authProvider != null)
            {
                _tempAuthProvider = authProvider.getName();
            }
            port.setAttributes(Map.of(Port.AUTHENTICATION_PROVIDER, testOAuthProvider));
        }
    }

    private class PortExtractingLauncherListener implements SystemLauncherListener
    {
        private SystemConfig<?> _systemConfig;

        @Override
        public void beforeStartup()
        {
            // logic not used in tests
        }

        @Override
        public void errorOnStartup(final RuntimeException e)
        {
            // logic not used in tests
        }

        @Override
        public void afterStartup()
        {
            if (_systemConfig == null)
            {
                throw new IllegalStateException("System config is required");
            }

            _broker = (Broker<?>) _systemConfig.getContainer();
            _broker.getChildren(Port.class).forEach(port -> _ports.put(port.getName(), port.getBoundPort()));
        }

        @Override
        public void onContainerResolve(final SystemConfig<?> systemConfig)
        {
            _systemConfig = systemConfig;
        }

        @Override
        public void onContainerClose(final SystemConfig<?> systemConfig)
        {
            // logic not used in tests
        }

        @Override
        public void onShutdown(final int exitCode)
        {
            // logic not used in tests
        }

        @Override
        public void exceptionOnShutdown(final Exception e)
        {
            // logic not used in tests
        }
    }

    private static class UncaughtExceptionHandler implements Thread.UncaughtExceptionHandler
    {
        private final AtomicInteger _count = new AtomicInteger(0);

        @Override
        public void uncaughtException(final Thread t, final Throwable e)
        {
            System.err.print("Thread terminated due to uncaught exception");
            e.printStackTrace();

            LOGGER.error("Uncaught exception from thread {}", t.getName(), e);
            _count.getAndIncrement();
        }

        public int getAndResetCount()
        {
            int count;
            do
            {
                count = _count.get();
            }
            while (!_count.compareAndSet(count, 0));
            return count;
        }
    }

    private class ShutdownLoggingSystemLauncherListener extends SystemLauncherListener.DefaultSystemLauncherListener
    {
        @Override
        public void onShutdown(final int exitCode)
        {
            _systemLauncher = null;
        }

        @Override
        public void exceptionOnShutdown(final Exception e)
        {
            if (e instanceof IllegalStateException || e instanceof IllegalStateTransitionException)
            {
                LOGGER.error("IllegalStateException occurred on broker shutdown in test");
            }
        }
    }

    // sonar: hostname verifier is used for test purposes
    @SuppressWarnings("java:S4830")
    private static final class TrustingTrustManager implements X509TrustManager
    {
        @Override
        public void checkClientTrusted(final X509Certificate[] certs, final String authType)
        {
            // trust manager is used for test purposes, always trusts client
        }

        @Override
        public void checkServerTrusted(final X509Certificate[] certs, final String authType)
        {
            // trust manager is used for test purposes, always trusts server
        }

        @Override
        public X509Certificate[] getAcceptedIssuers()
        {
            return new X509Certificate[0];
        }
    }

    // sonar: hostname verifier is used for test purposes
    @SuppressWarnings("java:S5527")
    private static final class BlindHostnameVerifier implements HostnameVerifier
    {
        @Override
        public boolean verify(final String arg0, final SSLSession arg1)
        {
            return true;
        }
    }
}
