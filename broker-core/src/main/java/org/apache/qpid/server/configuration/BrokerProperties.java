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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.server.SystemLauncher;

/**
 * Declares broker system property names
 */
public class BrokerProperties
{
    public static final int  DEFAULT_HEARTBEAT_TIMEOUT_FACTOR = 2;
    public static final String PROPERTY_HEARTBEAT_TIMEOUT_FACTOR = "qpid.broker_heartbeat_timeout_factor";
    public static final int HEARTBEAT_TIMEOUT_FACTOR = Integer.getInteger(PROPERTY_HEARTBEAT_TIMEOUT_FACTOR, DEFAULT_HEARTBEAT_TIMEOUT_FACTOR);

    public static final String PROPERTY_DEAD_LETTER_EXCHANGE_SUFFIX = "qpid.broker_dead_letter_exchange_suffix";
    public static final String PROPERTY_DEAD_LETTER_QUEUE_SUFFIX = "qpid.broker_dead_letter_queue_suffix";

    public static final String PROPERTY_STATUS_UPDATES = "qpid.broker_status_updates";
    public static final String PROPERTY_LOCALE = "qpid.broker_locale";
    public static final String PROPERTY_DEFAULT_SUPPORTED_PROTOCOL_REPLY = "qpid.broker_default_supported_protocol_version_reply";
    public static final String PROPERTY_DISABLED_FEATURES = "qpid.broker_disabled_features";

    public static final String PROPERTY_QPID_HOME = "QPID_HOME";
    public static final String PROPERTY_QPID_WORK = "QPID_WORK";
    public static final String POSIX_FILE_PERMISSIONS = "qpid.default_posix_file_permissions";
    public static final String PROPERTY_STARTUP_FAILOVER_CONSOLE_LOG_LEVEL = "qpid.startup_failover_console_log_level";
    /**
     * Configuration property name for the absolute path to use for the broker work directory.
     *
     * If not otherwise set, the value for this configuration property defaults to the location
     * set in the "QPID_WORK" system property if that was set, or the 'work' sub-directory of
     * the JVM working directory ("user.dir" property) for the Java process if it was not.
     */
    public static final String QPID_WORK_DIR  = "qpid.work_dir";
    /**
     * Configuration property name for the absolute path to use for the broker home directory.
     *
     * If not otherwise set, the value for this configuration property defaults to the location
     * set in the "QPID_HOME" system property if that was set, or remains unset if it was not.
     */
    public static final String QPID_HOME_DIR  = "qpid.home_dir";

    private BrokerProperties()
    {
    }

    public static Locale getLocale()
    {
        Locale locale = Locale.US;
        String localeSetting = System.getProperty(BrokerProperties.PROPERTY_LOCALE);
        if (localeSetting != null)
        {
            String[] localeParts = localeSetting.split("_");
            String language = (localeParts.length > 0 ? localeParts[0] : "");
            String country = (localeParts.length > 1 ? localeParts[1] : "");
            String variant = "";
            if (localeParts.length > 2)
            {
                variant = localeSetting.substring(language.length() + 1 + country.length() + 1);
            }
            locale = new Locale(language, country, variant);
        }
        return locale;
    }

    public static void populateSystemPropertiesFromDefaults(final String initialProperties) throws IOException
    {
        URL initialPropertiesLocation;
        if(initialProperties == null)
        {
            initialPropertiesLocation = SystemLauncher.class.getClassLoader().getResource("system.properties");
        }
        else
        {
            initialPropertiesLocation = (new File(initialProperties)).toURI().toURL();
        }

        Properties props = new Properties(CommonProperties.asProperties());
        if(initialPropertiesLocation != null)
        {

            try(InputStream inStream = initialPropertiesLocation.openStream())
            {
                props.load(inStream);
            }
        }

        Set<String> propertyNames = new HashSet<>(props.stringPropertyNames());
        propertyNames.removeAll(System.getProperties().stringPropertyNames());
        for (String propName : propertyNames)
        {
            System.setProperty(propName, props.getProperty(propName));
        }
    }
}
