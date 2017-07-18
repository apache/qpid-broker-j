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
package org.apache.qpid.server.model;

import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreAttributes;
import org.apache.qpid.server.store.preferences.PreferencesRoot;

@ManagedObject (creatable = false)
public interface SystemConfig<X extends SystemConfig<X>> extends ConfiguredObject<X>,
                                                                 ModelRoot,
                                                                 PreferencesRoot,
                                                                 EventLoggerProvider
{

    String MANAGEMENT_MODE = "managementMode";
    
    String MANAGEMENT_MODE_QUIESCE_VIRTUAL_HOSTS = "managementModeQuiesceVirtualHosts";
    String MANAGEMENT_MODE_HTTP_PORT_OVERRIDE = "managementModeHttpPortOverride";
    String MANAGEMENT_MODE_PASSWORD = "managementModePassword";
    String INITIAL_CONFIGURATION_LOCATION = "initialConfigurationLocation";
    String INITIAL_SYSTEM_PROPERTIES_LOCATION = "initialSystemPropertiesLocation";
    String STARTUP_LOGGED_TO_SYSTEM_OUT = "startupLoggedToSystemOut";


    String PROPERTY_QPID_WORK = "QPID_WORK";
    @ManagedContextDefault(name= SystemConfig.PROPERTY_QPID_WORK)
    String DEFAULT_QPID_WORK = "${user.dir}${file.separator}work";

    /**
     * Configuration property name for the absolute path to use for the broker work directory.
     *
     * If not otherwise set, the value for this configuration property defaults to the location
     * set in the "QPID_WORK" system property if that was set, or the 'work' sub-directory of
     * the JVM working directory ("user.dir" property) for the Java process if it was not.
     */
    String QPID_WORK_DIR  = "qpid.work_dir";

    @ManagedContextDefault(name= SystemConfig.QPID_WORK_DIR)
    String DEFAULT_QPID_WORK_DIR = "${QPID_WORK}";

    @ManagedContextDefault(name="qpid.broker.defaultPreferenceStoreAttributes")
    String DEFAULT_PREFERENCE_STORE_ATTRIBUTES = "{\"type\": \"JSON\", \"attributes\":{\"path\": \"${json:qpid.work_dir}${json:file.separator}preferences.json\"}}";

    String POSIX_FILE_PERMISSIONS = "qpid.default_posix_file_permissions";
    @ManagedContextDefault(name = SystemConfig.POSIX_FILE_PERMISSIONS)
    String DEFAULT_POSIX_FILE_PERMISSIONS = "rw-r-----";


    String MANAGEMENT_MODE_USER_NAME = "mm_admin";

    String PROPERTY_STATUS_UPDATES = "qpid.broker_status_updates";

    @ManagedAttribute(immutable = true, defaultValue = Broker.BROKER_TYPE)
    String getDefaultContainerType();

    @ManagedAttribute(defaultValue = "false")
    boolean isManagementMode();

    @ManagedAttribute(defaultValue = "0")
    int getManagementModeHttpPortOverride();

    @ManagedAttribute(defaultValue = "false")
    boolean isManagementModeQuiesceVirtualHosts();

    @ManagedAttribute(secure = true)
    String getManagementModePassword();

    String DEFAULT_INITIAL_CONFIG_NAME = "initial-config.json";

    @ManagedContextDefault(name="qpid.initialConfigurationLocation")
    String DEFAULT_INITIAL_CONFIG_LOCATION = "classpath:"+DEFAULT_INITIAL_CONFIG_NAME;

    @ManagedAttribute(defaultValue = "${qpid.initialConfigurationLocation}")
    String getInitialConfigurationLocation();

    @ManagedAttribute
    String getInitialSystemPropertiesLocation();

    @ManagedAttribute(defaultValue = "true")
    boolean isStartupLoggedToSystemOut();

    @ManagedAttribute( description = "Configuration for the preference store, e.g. type, path, etc.",
            defaultValue = "${qpid.broker.defaultPreferenceStoreAttributes}")
    PreferenceStoreAttributes getPreferenceStoreAttributes();

    @Override
    EventLogger getEventLogger();

    Container<?> getContainer();

    DurableConfigurationStore getConfigurationStore();

    Runnable getOnContainerResolveTask();

    void setOnContainerResolveTask(Runnable runnable);

    Runnable getOnContainerCloseTask();

    void setOnContainerCloseTask(Runnable runnable);

    <T extends Container<? extends T>> T getContainer(Class<T> clazz);
}
