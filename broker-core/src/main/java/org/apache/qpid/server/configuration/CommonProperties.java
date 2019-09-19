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
package org.apache.qpid.server.configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralised record of Qpid common properties.
 *
 * Qpid build specific information like project name, version number, and source code repository revision number
 * are captured by this class and exposed via public static methods.
 *
 */
public class CommonProperties
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonProperties.class);

    /**
     * The timeout used by the IO layer for timeouts such as send timeout in IoSender, and the close timeout for IoSender and IoReceiver
     */
    public static final String IO_NETWORK_TRANSPORT_TIMEOUT_PROP_NAME = "qpid.io_network_transport_timeout";
    public static final int IO_NETWORK_TRANSPORT_TIMEOUT_DEFAULT = 60000;

    public static final String QPID_SECURITY_TLS_PROTOCOL_WHITE_LIST = "qpid.security.tls.protocolWhiteList";
    public static final String QPID_SECURITY_TLS_PROTOCOL_WHITE_LIST_DEFAULT = "TLSv1\\.[0-9]+";
    public static final String QPID_SECURITY_TLS_PROTOCOL_BLACK_LIST = "qpid.security.tls.protocolBlackList";
    public static final String QPID_SECURITY_TLS_PROTOCOL_BLACK_LIST_DEFAULT = "TLSv1\\.[0-1]";

    public static final String QPID_SECURITY_TLS_CIPHER_SUITE_WHITE_LIST = "qpid.security.tls.cipherSuiteWhiteList";
    public static final String QPID_SECURITY_TLS_CIPHER_SUITE_WHITE_LIST_DEFAULT = "";
    public static final String QPID_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST = "qpid.security.tls.cipherSuiteBlackList";
    public static final String QPID_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST_DEFAULT = "";

    private static final String MANIFEST_HEADER_IMPLEMENTATION_BUILD = "Implementation-Build";

    /** Defines the name of the version suffix property. */
    private static final String RELEASE_VERSION_SUFFIX = "qpid.version.suffix";

    /** Defines the default value for all properties that cannot be loaded. */
    private static final String DEFAULT = "unknown";

    /** Holds the product name. */
    private static final String productName;

    /** Holds the product version. */
    private static final String releaseVersion;

    /** Holds the source code revision. */
    private static final String buildVersion;

    private static final Properties properties = new Properties();

    private static final String QPID_VERSION = "qpid.version";

    static
    {
        Manifest jarManifest = getJarManifestFor(CommonProperties.class);
        Attributes mainAttributes = jarManifest.getMainAttributes();

        Package p = CommonProperties.class.getPackage();

        buildVersion = mainAttributes.getValue(MANIFEST_HEADER_IMPLEMENTATION_BUILD) != null ? mainAttributes.getValue(MANIFEST_HEADER_IMPLEMENTATION_BUILD) : DEFAULT;
        productName = p.getImplementationTitle() != null ? p.getImplementationTitle() : DEFAULT;

        String version = getImplementationVersion(p);
        System.setProperty(QPID_VERSION, version);

        boolean loadFromFile = true;
        String initialProperties = System.getProperty("qpid.common_properties_file");
        if (initialProperties == null)
        {
            initialProperties = "qpid-common.properties";
            loadFromFile = false;
        }

        loadProperties(properties, initialProperties, loadFromFile);

        String versionSuffix = properties.getProperty(RELEASE_VERSION_SUFFIX);
        releaseVersion = versionSuffix == null || "".equals(versionSuffix) ? version : version + ";" + versionSuffix;

        Set<String> propertyNames = new HashSet<>(properties.stringPropertyNames());
        propertyNames.removeAll(System.getProperties().stringPropertyNames());
        for (String propName : propertyNames)
        {
            System.setProperty(propName, properties.getProperty(propName));
        }

    }

    public static void ensureIsLoaded()
    {
        //noop; to call from static initialization blocks of other classes to provoke CommonProperties class initialization
    }

    public static Properties asProperties()
    {
        return new Properties(properties);
    }

    /**
     * Gets the product name.
     *
     * @return The product name.
     */
    public static String getProductName()
    {
        return productName;
    }

    /**
     * Gets the product version.
     *
     * @return The product version.
     */
    public static String getReleaseVersion()
    {
        return releaseVersion;
    }

    /**
     * Gets the source code revision.
     *
     * @return The source code revision.
     */
    public static String getBuildVersion()
    {
        return buildVersion;
    }

    /**
     * Extracts all of the version information as a printable string.
     *
     * @return All of the version information as a printable string.
     */
    public static String getVersionString()
    {
        return getProductName() + " - " + getReleaseVersion() + " build: " + getBuildVersion();
    }

    private CommonProperties()
    {
        //no instances
    }

    private static void loadProperties(Properties properties, String resourceLocation, boolean loadFromFile)
    {
        try
        {
            URL propertiesResource;
            if (loadFromFile)
            {
                propertiesResource = (new File(resourceLocation)).toURI().toURL();
            }
            else
            {
                propertiesResource = CommonProperties.class.getClassLoader().getResource(resourceLocation);
            }

            if (propertiesResource != null)
            {
                try (InputStream propertyStream = propertiesResource.openStream())
                {
                    if (propertyStream != null)
                    {
                        properties.load(propertyStream);
                    }
                }
                catch (IOException e)
                {
                    LOGGER.warn("Could not load properties file '{}'.", resourceLocation, e);
                }
            }
        }
        catch (MalformedURLException e)
        {
            LOGGER.warn("Could not open properties file '{}'.", resourceLocation, e);
        }
    }

    private static String getImplementationVersion(final Package p)
    {
        String version = p.getImplementationVersion();
        if (version == null)
        {
            version = DEFAULT;
            final String path = CommonProperties.class.getPackage().getName().replace(".", "/");
            final String fallbackPath = "/" + path + "/fallback-version.txt";
            final InputStream in = CommonProperties.class.getResourceAsStream(fallbackPath);
            if (in != null)
            {
                try(BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII)))
                {
                    version = reader.readLine();
                }
                catch (Exception e)
                {
                    LOGGER.trace("Problem reading version from fallback resource : {} ", fallbackPath, e);
                }
            }
        }
        return version;
    }

    private static Manifest getJarManifestFor(final Class<?> clazz)
    {
        final Manifest emptyManifest = new Manifest();
        String className = clazz.getSimpleName() + ".class";
        String classPath = clazz.getResource(className).toString();
        if (!classPath.startsWith("jar"))
        {
            return emptyManifest;
        }

        String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) +
                              "/META-INF/MANIFEST.MF";
        try (InputStream is = new URL(manifestPath).openStream())
        {
            return new Manifest(is);
        }
        catch (IOException e)
        {
            // Ignore
        }
        return emptyManifest;
    }
}
