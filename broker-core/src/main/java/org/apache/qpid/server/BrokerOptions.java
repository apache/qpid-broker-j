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
package org.apache.qpid.server;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.qpid.server.configuration.BrokerProperties;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.SystemConfig;

public class BrokerOptions
{
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

    public static final String DEFAULT_INITIAL_CONFIG_NAME = "initial-config.json";
    public static final String DEFAULT_STORE_TYPE = "JSON";
    public static final String DEFAULT_CONFIG_NAME_PREFIX = "config";
    public static final String DEFAULT_INITIAL_CONFIG_LOCATION =
        BrokerOptions.class.getClassLoader().getResource(DEFAULT_INITIAL_CONFIG_NAME).toExternalForm();

    public static final String MANAGEMENT_MODE_USER_NAME = "mm_admin";

    private static final File FALLBACK_WORK_DIR = new File(System.getProperty("user.dir"), "work");

    private String _configurationStoreLocation;
    private String _configurationStoreType;

    private String _initialConfigurationLocation;

    private boolean _managementMode;
    private boolean _managementModeQuiesceVhosts;
    private int _managementModeHttpPortOverride;
    private String _managementModePassword;
    private boolean _overwriteConfigurationStore;
    private Map<String, String> _configProperties = new HashMap<String,String>();
    private boolean _startupLoggedToSystemOut = true;
    private String _initialSystemProperties;

    public Map<String, Object> convertToSystemConfigAttributes()
    {
        Map<String,Object> attributes = new HashMap<String, Object>();

        attributes.put("storePath", getConfigurationStoreLocation());
        attributes.put(ConfiguredObject.CONTEXT, getConfigProperties());

        attributes.put(SystemConfig.MANAGEMENT_MODE, _managementMode);
        attributes.put(SystemConfig.MANAGEMENT_MODE_QUIESCE_VIRTUAL_HOSTS, _managementModeQuiesceVhosts);
        attributes.put(SystemConfig.MANAGEMENT_MODE_HTTP_PORT_OVERRIDE, _managementModeHttpPortOverride);
        attributes.put(SystemConfig.MANAGEMENT_MODE_PASSWORD, _managementModePassword);
        attributes.put(SystemConfig.INITIAL_CONFIGURATION_LOCATION, getInitialConfigurationLocation());
        attributes.put(SystemConfig.STARTUP_LOGGED_TO_SYSTEM_OUT, isStartupLoggedToSystemOut());
        return attributes;
    }

    public String getManagementModePassword()
    {
        return _managementModePassword;
    }

    public void setManagementModePassword(String managementModePassword)
    {
        _managementModePassword = managementModePassword;
    }

    public boolean isManagementMode()
    {
        return _managementMode;
    }

    public void setManagementMode(boolean managementMode)
    {
        _managementMode = managementMode;
    }

    public boolean isManagementModeQuiesceVirtualHosts()
    {
        return _managementModeQuiesceVhosts;
    }

    public void setManagementModeQuiesceVirtualHosts(boolean managementModeQuiesceVhosts)
    {
        _managementModeQuiesceVhosts = managementModeQuiesceVhosts;
    }

    public int getManagementModeHttpPortOverride()
    {
        return _managementModeHttpPortOverride;
    }

    public void setManagementModeHttpPortOverride(int managementModeHttpPortOverride)
    {
        _managementModeHttpPortOverride = managementModeHttpPortOverride;
    }

    /**
     * Get the broker configuration store type.
     *
     * @return the previously set store type, or if none was set the default: {@value #DEFAULT_STORE_TYPE}
     */
    public String getConfigurationStoreType()
    {
        if(_configurationStoreType == null)
        {
            return  DEFAULT_STORE_TYPE;
        }

        return _configurationStoreType;
    }

    /**
     * Set the broker configuration store type.
     *
     * Passing null clears previously set values and returns to the default.
     */
    public void setConfigurationStoreType(String configurationStoreType)
    {
        _configurationStoreType = configurationStoreType;
    }

    /**
     * Get the broker configuration store location.
     *
     * Defaults to {@value #DEFAULT_CONFIG_NAME_PREFIX}.{@literal <store type>} (see {@link BrokerOptions#getConfigurationStoreType()})
     * within the broker work directory (gathered via config property {@link #QPID_WORK_DIR}).
     *
     * @return the previously set configuration store location, or the default location if none was set.
     */
    public String getConfigurationStoreLocation()
    {
        if(_configurationStoreLocation == null)
        {
            String workDir = getWorkDir();
            String storeType = getConfigurationStoreType();

            return new File(workDir, DEFAULT_CONFIG_NAME_PREFIX + "." + storeType.toLowerCase()).getAbsolutePath();
        }

        return _configurationStoreLocation;
    }

    /**
     * Set the absolute path to use for the broker configuration store.
     *
     * Passing null clears any previously set value and returns to the default.
     */
    public void setConfigurationStoreLocation(String configurationStore)
    {
        _configurationStoreLocation = configurationStore;
    }

    /**
     * Returns whether the existing broker configuration store should be overwritten with the current
     * initial configuration file (see {@link BrokerOptions#getInitialConfigurationLocation()}).
     */
    public boolean isOverwriteConfigurationStore()
    {
        return _overwriteConfigurationStore;
    }

    /**
     * Sets whether the existing broker configuration store should be overwritten with the current
     * initial configuration file (see {@link BrokerOptions#getInitialConfigurationLocation()}).
     */
    public void setOverwriteConfigurationStore(boolean overwrite)
    {
        _overwriteConfigurationStore = overwrite;
    }

    /**
     * Get the broker initial JSON configuration location.
     *
     * Defaults to an internal configuration file within the broker jar.
     *
     * @return the previously set configuration location, or the default location if none was set.
     */
    public String getInitialConfigurationLocation()
    {
        if(_initialConfigurationLocation == null)
        {
            String overriddenDefaultConfigurationLocation = System.getProperty("qpid.initialConfigurationLocation");
            if (overriddenDefaultConfigurationLocation != null)
            {
                URL resource = BrokerOptions.class.getClassLoader().getResource(overriddenDefaultConfigurationLocation);
                if (resource == null)
                {
                    throw new IllegalArgumentException(String.format("Initial configuration '%s' is not found",
                                                                          overriddenDefaultConfigurationLocation));
                }
                return resource.toExternalForm();
            }
            return DEFAULT_INITIAL_CONFIG_LOCATION;
        }

        return _initialConfigurationLocation;
    }

    /**
     * Set the absolute path or URL to use for the initial JSON configuration, which is loaded with the
     * in order to initialise any new store for the broker.
     *
     * Passing null clears any previously set value and returns to the default.
     */
    public void setInitialConfigurationLocation(String initialConfigurationLocation)
    {
        _initialConfigurationLocation = initialConfigurationLocation;
    }

    /**
     * Sets the named configuration property to the given value.
     *
     * Passing a null value causes removal of a previous value, and restores any default there may have been.
     */
    public void setConfigProperty(String name, String value)
    {
        if(value == null)
        {
            _configProperties.remove(name);
        }
        else
        {
            _configProperties.put(name, value);
        }
    }

    /**
     * Get an un-editable copy of the configuration properties, representing
     * the user-configured values as well as any defaults for properties
     * not otherwise configured.
     *
     * Subsequent property changes are not reflected in this map.
     */
    public Map<String,String> getConfigProperties()
    {
        ConcurrentMap<String, String> properties = new ConcurrentHashMap<String,String>();
        properties.putAll(_configProperties);

        properties.putIfAbsent(QPID_WORK_DIR, getWorkDir());

        String homeDir = getHomeDir();
        if(homeDir != null)
        {
            properties.putIfAbsent(QPID_HOME_DIR, homeDir);
        }

        return Collections.unmodifiableMap(properties);
    }

    private String getProperty(String propName, String altPropName, String defaultValue)
    {
        String value = getProperty(propName);
        if(value == null)
        {
            value = getProperty(altPropName);
            if(value == null)
            {
                value = defaultValue;
            }
        }
        return value;
    }

    private String getProperty(String propName)
    {
        return _configProperties.containsKey(propName)
                ? _configProperties.get(propName)
                : System.getProperties().containsKey(propName)
                        ? System.getProperty(propName)
                        : System.getenv(propName);
    }

    private String getWorkDir()
    {
        return getProperty(QPID_WORK_DIR, BrokerProperties.PROPERTY_QPID_WORK, FALLBACK_WORK_DIR.getAbsolutePath());
    }

    private String getHomeDir()
    {
        return getProperty(QPID_HOME_DIR, BrokerProperties.PROPERTY_QPID_HOME, null);
    }

    /*
     * Temporary method for test purposes
     */
    public boolean isStartupLoggedToSystemOut()
    {
        return _startupLoggedToSystemOut;
    }

    /*
     * Temporary method for test purposes
     */
    public void setStartupLoggedToSystemOut(boolean startupLoggedToSystemOut)
    {
        this._startupLoggedToSystemOut = startupLoggedToSystemOut;
    }

    /**
     * Get the location of initial JVM system properties to set. This can be URL or a file path
     *
     * @return the location of initial JVM system properties to set.
     */
    public String getInitialSystemProperties()
    {
        return _initialSystemProperties;
    }

    /**
     * Set the location of initial properties file to set as JVM system properties. This can be URL or a file path
     *
     * @param initialSystemProperties the location of initial JVM system properties.
     */
    public void setInitialSystemProperties(String initialSystemProperties)
    {
        _initialSystemProperties = initialSystemProperties;
    }

    @Override
    public String toString()
    {
        return "BrokerOptions{" +
               "_configurationStoreLocation='" + _configurationStoreLocation + '\'' +
               ", _configurationStoreType='" + _configurationStoreType + '\'' +
               ", _initialConfigurationLocation='" + _initialConfigurationLocation + '\'' +
               ", _managementMode=" + _managementMode +
               ", _managementModeQuiesceVhosts=" + _managementModeQuiesceVhosts +
               ", _managementModeHttpPortOverride=" + _managementModeHttpPortOverride +
               ", _overwriteConfigurationStore=" + _overwriteConfigurationStore +
               ", _configProperties=" + _configProperties +
               ", _startupLoggedToSystemOut=" + _startupLoggedToSystemOut +
               ", _initialSystemProperties='" + _initialSystemProperties + '\'' +
               '}';
    }
}
