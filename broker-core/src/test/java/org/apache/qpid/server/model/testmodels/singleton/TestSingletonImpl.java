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
 */
package org.apache.qpid.server.model.testmodels.singleton;

import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import com.google.common.collect.Sets;

import org.apache.qpid.server.configuration.updater.CurrentThreadTaskExecutor;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.store.preferences.NoopPreferenceStoreFactoryService;
import org.apache.qpid.server.store.preferences.PreferenceStore;

@ManagedObject( category = false, type = TestSingletonImpl.TEST_SINGLETON_TYPE)
public class TestSingletonImpl extends AbstractConfiguredObject<TestSingletonImpl>
        implements TestSingleton<TestSingletonImpl>
{
    public static final String TEST_SINGLETON_TYPE = "testsingleton";

    private static final Principal SYSTEM_PRINCIPAL = new Principal() {
        @Override
        public String getName()
        {
            return "TEST";
        }
    };
    private static final Subject SYSTEM_SUBJECT = new Subject(true,
                                                       Collections.singleton(SYSTEM_PRINCIPAL),
                                                       Collections.emptySet(),
                                                       Collections.emptySet());

    public static final int DERIVED_VALUE = -100;
    private final PreferenceStore _preferenceStore =
            new NoopPreferenceStoreFactoryService().createInstance(null, Collections.<String, Object>emptyMap());

    @ManagedAttributeField
    private String _automatedPersistedValue;

    @ManagedAttributeField
    private String _automatedNonPersistedValue;

    @ManagedAttributeField
    private String _defaultedValue;

    @ManagedAttributeField
    private String _stringValue;

    @ManagedAttributeField
    private int _intValue;

    @ManagedAttributeField
    private Map<String,String> _mapValue;

    @ManagedAttributeField
    private String _validValue;

    @ManagedAttributeField
    private TestEnum _enumValue;

    @ManagedAttributeField
    private Set<TestEnum> _enumSetValues;

    @ManagedAttributeField
    private String _secureValue;

    @ManagedAttributeField
    private String _immutableValue;

    @ManagedAttributeField
    private String _valueWithPattern;

    @ManagedAttributeField
    private List<String> _listValueWithPattern;

    @ManagedAttributeField
    private Date _dateValue;

    @ManagedAttributeField
    private String _attrWithDefaultFromContextNoInit;

    @ManagedAttributeField
    private String _attrWithDefaultFromContextCopyInit;

    @ManagedAttributeField
    private String _attrWithDefaultFromContextMaterializeInit;

    private Deque<HashSet<String>> _lastReportedSetAttributes = new ArrayDeque<>();

    @ManagedObjectFactoryConstructor
    public TestSingletonImpl(final Map<String, Object> attributes)
    {
        super(null, attributes, newTaskExecutor(), TestModel.getInstance());
    }

    private static CurrentThreadTaskExecutor newTaskExecutor()
    {
        CurrentThreadTaskExecutor currentThreadTaskExecutor = new CurrentThreadTaskExecutor();
        currentThreadTaskExecutor.start();
        return currentThreadTaskExecutor;
    }

    public TestSingletonImpl(final Map<String, Object> attributes,
                             final TaskExecutor taskExecutor)
    {
        super(null, attributes, taskExecutor);
    }


    @Override
    public String getAutomatedPersistedValue()
    {
        return _automatedPersistedValue;
    }

    @Override
    public String getAutomatedNonPersistedValue()
    {
        return _automatedNonPersistedValue;
    }

    @Override
    public String getDefaultedValue()
    {
        return _defaultedValue;
    }

    @Override
    public String getStringValue()
    {
        return _stringValue;
    }

    @Override
    public Map<String, String> getMapValue()
    {
        return _mapValue;
    }

    @Override
    public TestEnum getEnumValue()
    {
        return _enumValue;
    }

    @Override
    public Set<TestEnum> getEnumSetValues()
    {
        return _enumSetValues;
    }

    @Override
    public String getValidValue()
    {
        return _validValue;
    }

    @Override
    public int getIntValue()
    {
        return _intValue;
    }

    @Override
    public long getDerivedValue()
    {
        return DERIVED_VALUE;
    }

    @Override
    public String getSecureValue()
    {
        return _secureValue;
    }

    @Override
    public String getImmutableValue()
    {
        return _immutableValue;
    }

    @Override
    public String getValueWithPattern()
    {
        return _valueWithPattern;
    }

    @Override
    public List<String> getListValueWithPattern()
    {
        return _listValueWithPattern;
    }

    @Override
    public Date getDateValue()
    {
        return _dateValue;
    }

    @Override
    public Long getLongStatistic()
    {
        return System.currentTimeMillis();
    }

    @Override
    public String getAttrWithDefaultFromContextNoInit()
    {
        return _attrWithDefaultFromContextNoInit;
    }

    @Override
    public String getAttrWithDefaultFromContextCopyInit()
    {
        return _attrWithDefaultFromContextCopyInit;
    }

    @Override
    public String getAttrWithDefaultFromContextMaterializeInit()
    {
        return _attrWithDefaultFromContextMaterializeInit;
    }

    @Override
    protected Principal getSystemPrincipal()
    {
        return SYSTEM_PRINCIPAL;
    }

    @Override
    protected void logOperation(final String operation)
    {

    }

    @Override
    public <T> T doAsSystem(PrivilegedAction<T> action)
    {
        return Subject.doAs(SYSTEM_SUBJECT, action);
    }

    @Override
    protected void postSetAttributes(final Set<String> actualUpdatedAttributes)
    {
        super.postSetAttributes(actualUpdatedAttributes);
        _lastReportedSetAttributes.add(Sets.newHashSet(actualUpdatedAttributes));
    }

    @Override
    public Set<String> takeLastReportedSetAttributes()
    {
        return _lastReportedSetAttributes.removeFirst();
    }
}
