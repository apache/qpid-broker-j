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

package org.apache.qpid.server.model.validator;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.ConfigurationChangeListener;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectFactory;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.State;
import org.apache.qpid.server.model.preferences.UserPreferences;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.store.ConfiguredObjectRecord;

import javax.security.auth.Subject;
import java.lang.reflect.Type;
import java.security.AccessControlException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class TestConfiguredObject implements ConfiguredObject<TestConfiguredObject>
{
    public static final String CONFIGURED_OBJECT = "ConfiguredObject";

    public static final String _NAME = "TestConfiguredObject";

    private final Date _now = new Date();

    private final Map<String, String> _context = new LinkedHashMap<>();

    private final Map<String, Object> _attributes = new LinkedHashMap<>();

    private final TaskExecutor _taskExecutor = CurrentThreadTaskExecutor.newStartedInstance();

    private final ConfiguredObject<?> _parent;

    private UUID _uuid = UUID.randomUUID();

    private UserPreferences _userPreferences;

    public TestConfiguredObject()
    {
        super();
        _parent = null;
    }

    public TestConfiguredObject(ConfiguredObject<?> parent, Map<String, Object> attributes)
    {
        super();
        _parent = parent;
        _attributes.putAll(attributes);
    }

    @Override
    public UUID getId()
    {
        return _uuid;
    }

    public TestConfiguredObject withId(UUID id)
    {
        this._uuid = id;
        return this;
    }

    @Override
    public String getName()
    {
        return _NAME;
    }

    @Override
    public String getDescription()
    {
        return getName();
    }

    @Override
    public String getType()
    {
        return CONFIGURED_OBJECT;
    }

    @Override
    public Map<String, String> getContext()
    {
        return _context;
    }

    @Override
    public String getLastUpdatedBy()
    {
        return "user";
    }

    @Override
    public Date getLastUpdatedTime()
    {
        return _now;
    }

    @Override
    public String getCreatedBy()
    {
        return "user";
    }

    @Override
    public Date getCreatedTime()
    {
        return _now;
    }

    @Override
    public State getDesiredState()
    {
        return State.ACTIVE;
    }

    @Override
    public State getState()
    {
        return State.ACTIVE;
    }

    @Override
    public Date getLastOpenedTime()
    {
        return _now;
    }

    @Override
    public void addChangeListener(ConfigurationChangeListener listener)
    {
    }

    @Override
    public boolean removeChangeListener(ConfigurationChangeListener listener)
    {
        return false;
    }

    @Override
    public ConfiguredObject<?> getParent()
    {
        return _parent;
    }

    @Override
    public boolean isDurable()
    {
        return false;
    }

    @Override
    public LifetimePolicy getLifetimePolicy()
    {
        return LifetimePolicy.IN_USE;
    }

    @Override
    public Map<String, Object> getStatistics(List<String> statistics)
    {
        return Collections.emptyMap();
    }

    @Override
    public String setContextVariable(String name, String value)
    {
        return _context.put(name, value);
    }

    @Override
    public String removeContextVariable(String name)
    {
        return _context.remove(name);
    }

    @Override
    public Collection<String> getAttributeNames()
    {
        return _attributes.keySet();
    }

    @Override
    public Object getAttribute(String name)
    {
        return _attributes.get(name);
    }

    @Override
    public Map<String, Object> getActualAttributes()
    {
        return _attributes;
    }

    @Override
    public Map<String, Object> getStatistics()
    {
        return Collections.emptyMap();
    }

    @Override
    public <C extends ConfiguredObject> Collection<C> getChildren(Class<C> clazz)
    {
        return Collections.emptyList();
    }

    @Override
    public <C extends ConfiguredObject> C getChildById(Class<C> clazz, UUID id)
    {
        return null;
    }

    @Override
    public <C extends ConfiguredObject> C getChildByName(Class<C> clazz, String name)
    {
        return null;
    }

    @Override
    public <C extends ConfiguredObject> C createChild(Class<C> childClass, Map<String, Object> attributes)
    {
        return null;
    }

    @Override
    public <C extends ConfiguredObject> ListenableFuture<C> getAttainedChildById(Class<C> childClass, UUID id)
    {
        final SettableFuture<C> returnVal = SettableFuture.create();
        returnVal.set(null);
        return returnVal;
    }

    @Override
    public <C extends ConfiguredObject> ListenableFuture<C> getAttainedChildByName(Class<C> childClass, String name)
    {
        final SettableFuture<C> returnVal = SettableFuture.create();
        returnVal.set(null);
        return returnVal;
    }

    @Override
    public <C extends ConfiguredObject> ListenableFuture<C> createChildAsync(Class<C> childClass, Map<String, Object> attributes)
    {
        final SettableFuture<C> returnVal = SettableFuture.create();
        returnVal.set(null);
        return returnVal;
    }

    @Override
    public void setAttributes(Map<String, Object> attributes) throws IllegalStateException, AccessControlException, IllegalArgumentException
    {
        _attributes.clear();
        _attributes.putAll(attributes);
    }

    @Override
    public ListenableFuture<Void> setAttributesAsync(Map<String, Object> attributes) throws IllegalStateException, AccessControlException, IllegalArgumentException
    {
        setAttributes(attributes);

        final SettableFuture<Void> returnVal = SettableFuture.create();
        returnVal.set(null);
        return returnVal;
    }

    @Override
    public Class<? extends ConfiguredObject> getCategoryClass()
    {
        return TestConfiguredObject.class;
    }

    @Override
    public Class<? extends ConfiguredObject> getTypeClass()
    {
        return TestConfiguredObject.class;
    }

    @Override
    public boolean managesChildStorage()
    {
        return false;
    }

    @Override
    public <C extends ConfiguredObject<C>> C findConfiguredObject(Class<C> clazz, String name)
    {
        if (getClass().equals(clazz) && Objects.equals(getName(), name))
        {
            return (C) this;
        }
        return null;
    }

    @Override
    public ConfiguredObjectRecord asObjectRecord()
    {
        final TestConfiguredObject me = this;
        return new ConfiguredObjectRecord()
        {
            @Override
            public UUID getId()
            {
                return me.getId();
            }

            @Override
            public String getType()
            {
                return me.getType();
            }

            @Override
            public Map<String, Object> getAttributes()
            {
                return me.getActualAttributes();
            }

            @Override
            public Map<String, UUID> getParents()
            {
                return Collections.emptyMap();
            }
        };
    }

    @Override
    public void open()
    {
    }

    @Override
    public ListenableFuture<Void> openAsync()
    {
        final SettableFuture<Void> returnVal = SettableFuture.create();
        returnVal.set(null);
        return returnVal;
    }

    @Override
    public void close()
    {
    }

    @Override
    public ListenableFuture<Void> closeAsync()
    {
        final SettableFuture<Void> returnVal = SettableFuture.create();
        returnVal.set(null);
        return returnVal;
    }

    @Override
    public ListenableFuture<Void> deleteAsync()
    {
        final SettableFuture<Void> returnVal = SettableFuture.create();
        returnVal.set(null);
        return returnVal;
    }

    @Override
    public TaskExecutor getChildExecutor()
    {
        return null;
    }

    @Override
    public ConfiguredObjectFactory getObjectFactory()
    {
        return TestModel.MODEL.getObjectFactory();
    }

    @Override
    public Model getModel()
    {
        return TestModel.MODEL;
    }

    @Override
    public void delete()
    {
    }

    @Override
    public boolean hasEncrypter()
    {
        return false;
    }

    @Override
    public void decryptSecrets()
    {
    }

    @Override
    public UserPreferences getUserPreferences()
    {
        return _userPreferences;
    }

    @Override
    public void setUserPreferences(UserPreferences userPreferences)
    {
        _userPreferences = userPreferences;
    }

    @Override
    public void authorise(Operation operation) throws AccessControlException
    {
    }

    @Override
    public void authorise(Operation operation, Map<String, Object> arguments) throws AccessControlException
    {
    }

    @Override
    public void authorise(SecurityToken token, Operation operation, Map<String, Object> arguments) throws AccessControlException
    {
    }

    @Override
    public SecurityToken newToken(Subject subject)
    {
        return null;
    }

    @Override
    public <T> T getContextValue(Class<T> clazz, String propertyName)
    {
        if (String.class.equals(clazz))
        {
            return (T) _context.get(propertyName);
        }
        return null;
    }

    @Override
    public <T> T getContextValue(Class<T> clazz, Type t, String propertyName)
    {
        return getContextValue(clazz, propertyName);
    }

    @Override
    public Set<String> getContextKeys(boolean excludeSystem)
    {
        return _context.keySet();
    }

    @Override
    public TaskExecutor getTaskExecutor()
    {
        return _taskExecutor;
    }
}
