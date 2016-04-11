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
package org.apache.qpid.configuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralised record of Qpid common properties.
 *
 * Qpid build specific information like  project name, version number, and source code repository revision number
 * are captured by this class and exposed via public static methods.
 *
 * @see ClientProperties
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
    public static final String QPID_SECURITY_TLS_PROTOCOL_BLACK_LIST_DEFAULT = "TLSv1\\.0";

    public static final String QPID_SECURITY_TLS_CIPHER_SUITE_WHITE_LIST = "qpid.security.tls.cipherSuiteWhiteList";
    public static final String QPID_SECURITY_TLS_CIPHER_SUITE_WHITE_LIST_DEFAULT = "";
    public static final String QPID_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST = "qpid.security.tls.cipherSuiteBlackList";
    public static final String QPID_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST_DEFAULT = "";

    /** The name of the version properties file to load from the class path. */
    public static final String VERSION_RESOURCE = "qpidversion.properties";

    /** Defines the name of the product property. */
    public static final String PRODUCT_NAME_PROPERTY = "qpid.name";

    /** Defines the name of the version property. */
    public static final String RELEASE_VERSION_PROPERTY = "qpid.version";

    /** Defines the name of the version suffix property. */
    public static final String RELEASE_VERSION_SUFFIX = "qpid.version.suffix";

    /** Defines the name of the source code revision property. */
    public static final String BUILD_VERSION_PROPERTY = "qpid.svnversion";

    /** Defines the default value for all properties that cannot be loaded. */
    private static final String DEFAULT = "unknown";

    /** Holds the product name. */
    private static final String productName;

    /** Holds the product version. */
    private static final String releaseVersion;

    /** Holds the product major version - derived from the releaseVersion */
    private static final int releaseVersionMajor;

    /** Holds the product minor version - derived from the releaseVersion */
    private static final int releaseVersionMinor;

    /** Holds the source code revision. */
    private static final String buildVersion;

    private static final Properties properties = new Properties();

    // Loads the values from the version properties file and common properties file.
    static
    {

        loadProperties(properties, VERSION_RESOURCE, false);

        buildVersion =  properties.getProperty(BUILD_VERSION_PROPERTY, DEFAULT);
        productName = properties.getProperty(PRODUCT_NAME_PROPERTY, DEFAULT);

        String version = properties.getProperty(RELEASE_VERSION_PROPERTY, DEFAULT);
        Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+).*");
        Matcher m = pattern.matcher(version);
        if (m.matches())
        {
            releaseVersionMajor = Integer.parseInt(m.group(1));
            releaseVersionMinor = Integer.parseInt(m.group(2));
        }
        else
        {
            LOGGER.warn("Failed to parse major and minor release number from '{}')", version);
            releaseVersionMajor = -1;
            releaseVersionMinor = -1;
        }

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
     * Gets the product major version.
     *
     * @return The product major version.
     */
    public static int getReleaseVersionMajor()
    {
        return releaseVersionMajor;
    }

    /**
     * Gets the product minor version.
     *
     * @return The product version.
     */
    public static int getReleaseVersionMinor()
    {
        return releaseVersionMinor;
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


}
