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

import java.security.AccessControlException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.security.auth.Subject;

import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.preferences.UserPreferences;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.ConfiguredObjectRecord;

@ManagedObject( creatable = false, category = false )
/**
 * An object that can be "managed" (eg via the web interface) and usually read from configuration.
 */
public interface ConfiguredObject<X extends ConfiguredObject<X>> extends ContextProvider, TaskExecutorProvider, PermissionedObject
{
    String OVER_SIZED_ATTRIBUTE_ALTERNATIVE_TEXT = "Value is too long to display";

    String ID = "id";
    String NAME = "name";
    String TYPE = "type";
    String DESCRIPTION = "description";
    String DURABLE = "durable";
    String CONTEXT = "context";
    String LIFETIME_POLICY = "lifetimePolicy";

    String LAST_UPDATED_BY = "lastUpdatedBy";
    String LAST_UPDATED_TIME = "lastUpdatedTime";
    String STATE = "state";
    String DESIRED_STATE = "desiredState";
    String CREATED_BY = "createdBy";
    String CREATED_TIME = "createdTime";


    String AWAIT_ATTAINMENT_TIMEOUT = "awaitAttainmentTimeout";
    @ManagedContextDefault( name = AWAIT_ATTAINMENT_TIMEOUT)
    public static final int DEFAULT_AWAIT_ATTAINMENT_TIMEOUT = 5000;

    /**
     * Get the universally unique identifier for the object
     *
     * @return the objects id
     */
    @ManagedAttribute( mandatory = true, immutable = true )
    UUID getId();

    /**
     * Get the name of the object
     *
     * @return the name of the object
     */
    @Override
    @ManagedAttribute( mandatory = true )
    String getName();


    @ManagedAttribute
    String getDescription();

    @ManagedAttribute ( immutable = true )
    String getType();

    @ManagedAttribute
    Map<String, String> getContext();

    @DerivedAttribute( persist = true )
    String getLastUpdatedBy();

    @DerivedAttribute( persist = true )
    Date getLastUpdatedTime();

    @DerivedAttribute( persist = true )
    String getCreatedBy();

    @DerivedAttribute( persist = true )
    Date getCreatedTime();


    /**
     * Get the desired state of the object.
     *
     * This is the state set at the object itself, however the object
     * may not be able attain this state if one of its ancestors is in a different state (in particular a descendant
     * object may not be ACTIVE if all of its ancestors are not also ACTIVE).
     *
     * @return the desired state of the object
     */
    @ManagedAttribute( defaultValue = "ACTIVE" )
    State getDesiredState();

    /**
     * Get the actual state of the object.
     *
     * This state is derived from the desired state of the object itself and
     * the actual state of its parents. If an object "desires" to be ACTIVE, but one of its parents is STOPPED, then
     * the actual state of the object will be STOPPED
     *
     * @return the actual state of the object
     */
    @DerivedAttribute
    State getState();

    @DerivedAttribute
    Date getLastOpenedTime();

    /**
     * Add a listener which will be informed of all changes to this configuration object
     *
     * @param listener the listener to add
     */
    void addChangeListener(ConfigurationChangeListener listener);

    /**
     * Remove a change listener
     *
     *
     * @param listener the listener to remove
     * @return true iff a listener was removed
     */
    boolean removeChangeListener(ConfigurationChangeListener listener);

    /**
     * Get the parent of the given type for this object
     *
     * @return the objects parent
     */
    ConfiguredObject<?> getParent();


    /**
     * Returns whether the the object configuration is durably stored
     *
     * @return the durability
     */
    @ManagedAttribute( defaultValue = "true", immutable = true)
    boolean isDurable();

    /**
     * Return the lifetime policy for the object
     *
     * @return the lifetime policy
     */
    @ManagedAttribute( defaultValue = "PERMANENT" )
    LifetimePolicy getLifetimePolicy();

    @ManagedOperation(description = "Return the (selected) statistic values", nonModifying = true, changesConfiguredObjectState = false, skipAclCheck = true)
    Map<String, Object> getStatistics(@Param(name = "statistics", defaultValue = "[]",
            description = "Optional list of statistic values to retrieve") List<String> statistics);

    @ManagedOperation(description = "Set context variable with given name to given value."
                                    + " Previous value is returned or null if not set directly on configured object.",
            changesConfiguredObjectState = true,
            skipAclCheck = true)
    String setContextVariable(@Param(name = "name", description = "Context variable name") String name,
                              @Param(name = "value", description = "Context variable value") String value);

    @ManagedOperation(description = "Remove context variable with given name."
                                    + " Variable value is returned or null if not set directly on configured object.",
            changesConfiguredObjectState = true,
            skipAclCheck = true)
    String removeContextVariable(@Param(name = "name", description = "Context variable name") String name);

    /**
     * Get the names of attributes that are set on this object
     *
     * Note that the returned collection is correct at the time the method is called, but will not reflect future
     * additions or removals when they occur
     *
     * @return the collection of attribute names
     */
    Collection<String> getAttributeNames();


    /**
     * Return the value for the given attribute name. The actual attribute value
     * is returned if the configured object has such attribute set. If not, the
     * value is looked default attributes.
     *
     * @param name
     *            the name of the attribute
     * @return the value of the attribute at the object (or null if the
     *         attribute value is set neither on object itself no in defaults)
     */
    Object getAttribute(String name);

    /**
     * Return the map containing only explicitly set attributes
     *
     * @return the map with the attributes
     */
    Map<String, Object> getActualAttributes();


    /**
     * Return the statistics for the ConfiguredObject
     *
     * @return the current statistics for the ConfiguredObject
     */
    Map<String, Object> getStatistics();

    /**
     * Return children of the ConfiguredObject of the given class
     *
     * @param clazz the class of the children to return
     * @return the children
     *
     * @throws NullPointerException if the supplied class null
     *
     */
    <C extends ConfiguredObject> Collection<C> getChildren(Class<C> clazz);

    <C extends ConfiguredObject> C getChildById(Class<C> clazz, UUID id);

    <C extends ConfiguredObject> C getChildByName(Class<C> clazz, String name);


    <C extends ConfiguredObject> C createChild(Class<C> childClass,
                                               Map<String, Object> attributes);

    <C extends ConfiguredObject> CompletableFuture<C> getAttainedChildById(Class<C> childClass,
                                                                           UUID id);

    <C extends ConfiguredObject> CompletableFuture<C> getAttainedChildByName(Class<C> childClass, String name);

    <C extends ConfiguredObject> CompletableFuture<C> createChildAsync(Class<C> childClass,
                                                                       Map<String, Object> attributes);

    void setAttributes(Map<String, Object> attributes) throws IllegalStateException, AccessControlException, IllegalArgumentException;
    CompletableFuture<Void> setAttributesAsync(Map<String, Object> attributes) throws IllegalStateException, AccessControlException, IllegalArgumentException;


    @Override
    Class<? extends ConfiguredObject> getCategoryClass();
    Class<? extends ConfiguredObject> getTypeClass();

    boolean managesChildStorage();

    <C extends ConfiguredObject<C>> C findConfiguredObject(Class<C> clazz, String name);

    // TODO - remove this when objects become responsible for their own storage
    ConfiguredObjectRecord asObjectRecord();

    void open();
    CompletableFuture<Void> openAsync();

    void close();
    CompletableFuture<Void> closeAsync();

    CompletableFuture<Void> deleteAsync();

    TaskExecutor getChildExecutor();

    ConfiguredObjectFactory getObjectFactory();

    Model getModel();

    void delete();

    boolean hasEncrypter();
    void decryptSecrets();

    UserPreferences getUserPreferences();
    void setUserPreferences(UserPreferences userPreferences);

    void authorise(Operation operation) throws AccessControlException;
    void authorise(Operation operation, Map<String, Object> arguments) throws AccessControlException;
    void authorise(SecurityToken token, Operation operation, Map<String, Object> arguments) throws AccessControlException;
    SecurityToken newToken(Subject subject);
}
