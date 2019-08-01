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

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.security.AccessControlContext;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import javax.security.auth.Subject;
import javax.security.auth.SubjectDomainCombiner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.configuration.updater.Task;
import org.apache.qpid.server.configuration.updater.TaskExecutor;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.logging.OperationLogMessage;
import org.apache.qpid.server.model.preferences.UserPreferences;
import org.apache.qpid.server.security.AccessControl;
import org.apache.qpid.server.security.Result;
import org.apache.qpid.server.security.SecurityToken;
import org.apache.qpid.server.security.access.Operation;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;
import org.apache.qpid.server.security.auth.TaskPrincipal;
import org.apache.qpid.server.security.encryption.ConfigurationSecretEncrypter;
import org.apache.qpid.server.store.ConfiguredObjectRecord;
import org.apache.qpid.server.store.preferences.UserPreferencesCreator;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.util.Strings;

public abstract class AbstractConfiguredObject<X extends ConfiguredObject<X>> implements ConfiguredObject<X>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConfiguredObject.class);

    private static final Map<Class, Object> SECURE_VALUES;

    public static final String SECURED_STRING_VALUE = "********";

    static
    {
        Map<Class,Object> secureValues = new HashMap<Class, Object>();
        secureValues.put(String.class, SECURED_STRING_VALUE);
        secureValues.put(Integer.class, 0);
        secureValues.put(Long.class, 0l);
        secureValues.put(Byte.class, (byte)0);
        secureValues.put(Short.class, (short)0);
        secureValues.put(Double.class, (double)0);
        secureValues.put(Float.class, (float)0);

        SECURE_VALUES = Collections.unmodifiableMap(secureValues);
    }

    private ConfigurationSecretEncrypter _encrypter;
    private AccessControl _parentAccessControl;
    private Principal _systemPrincipal;
    private UserPreferences _userPreferences;

    private enum DynamicState { UNINIT, OPENED, CLOSED };

    private static class DynamicStateWithFuture
    {
        private final DynamicState _dynamicState;
        private final ListenableFuture<Void> _future;

        private DynamicStateWithFuture(final DynamicState dynamicState, final ListenableFuture<Void> future)
        {
            _dynamicState = dynamicState;
            _future = future;
        }

        public DynamicState getDynamicState()
        {
            return _dynamicState;
        }

        public ListenableFuture<Void> getFuture()
        {
            return _future;
        }
    }

    private static final DynamicStateWithFuture UNINIT = new DynamicStateWithFuture(
            DynamicState.UNINIT,
            Futures.<Void>immediateFuture(null));
    private static final DynamicStateWithFuture OPENED = new DynamicStateWithFuture(
            DynamicState.OPENED,
            Futures.<Void>immediateFuture(null));


    private final AtomicReference<DynamicStateWithFuture> _dynamicState = new AtomicReference<>(UNINIT);



    private final Map<String,Object> _attributes = new HashMap<>();
    private final ConfiguredObject<?> _parent;
    private final Collection<ConfigurationChangeListener> _changeListeners =
            new ArrayList<>();

    private final Map<Class<? extends ConfiguredObject>, Collection<ConfiguredObject<?>>> _children =
            new ConcurrentHashMap<>();
    private final Map<Class<? extends ConfiguredObject>, ConcurrentMap<UUID,ConfiguredObject<?>>> _childrenById =
            new ConcurrentHashMap<>();
    private final Map<Class<? extends ConfiguredObject>, ConcurrentMap<String,ConfiguredObject<?>>> _childrenByName =
            new ConcurrentHashMap<>();


    @ManagedAttributeField
    private final UUID _id;

    private final TaskExecutor _taskExecutor;

    private final Class<? extends ConfiguredObject> _category;
    private final Class<? extends ConfiguredObject> _typeClass;
    private final Class<? extends ConfiguredObject> _bestFitInterface;
    private volatile Model _model;
    private final boolean _managesChildStorage;


    @ManagedAttributeField
    private Date _createdTime;

    @ManagedAttributeField
    private String _createdBy;

    @ManagedAttributeField
    private Date _lastUpdatedTime;

    @ManagedAttributeField
    private String _lastUpdatedBy;

    @ManagedAttributeField
    private String _name;

    @ManagedAttributeField
    private Map<String,String> _context;

    @ManagedAttributeField
    private boolean _durable;

    @ManagedAttributeField
    private String _description;

    @ManagedAttributeField
    private LifetimePolicy _lifetimePolicy;

    private final Map<String, ConfiguredObjectAttribute<?,?>> _attributeTypes;

    private final Map<String, ConfiguredObjectTypeRegistry.AutomatedField> _automatedFields;
    private final Map<State, Map<State, Method>> _stateChangeMethods;

    @ManagedAttributeField
    private String _type;

    private final OwnAttributeResolver _ownAttributeResolver = new OwnAttributeResolver(this);
    private final AncestorAttributeResolver _ancestorAttributeResolver = new AncestorAttributeResolver(this);


    @ManagedAttributeField
    private State _desiredState;


    private volatile SettableFuture<ConfiguredObject<X>> _attainStateFuture = SettableFuture.create();
    private boolean _openComplete;
    private boolean _openFailed;
    private volatile State _state = State.UNINITIALIZED;
    private volatile Date _lastOpenedTime;
    private volatile int _awaitAttainmentTimeout;

    protected AbstractConfiguredObject(final ConfiguredObject<?> parent,
                                       Map<String, Object> attributes)
    {
        this(parent, attributes, parent.getChildExecutor());
    }


    protected AbstractConfiguredObject(final ConfiguredObject<?> parent,
                                       Map<String, Object> attributes,
                                       TaskExecutor taskExecutor)
    {
        this(parent, attributes, taskExecutor, parent.getModel());
    }

    protected AbstractConfiguredObject(final ConfiguredObject<?> parent,
                                       Map<String, Object> attributes,
                                       TaskExecutor taskExecutor,
                                       Model model)
    {
        _taskExecutor = taskExecutor;
        if(taskExecutor == null)
        {
            throw new NullPointerException("task executor is null");
        }
        _model = model;
        _parent = parent;

        _category = ConfiguredObjectTypeRegistry.getCategory(getClass());
        Class<? extends ConfiguredObject> typeClass = model.getTypeRegistry().getTypeClass(getClass());
        _typeClass = typeClass == null ? _category : typeClass;

        _attributeTypes = model.getTypeRegistry().getAttributeTypes(getClass());
        _automatedFields = model.getTypeRegistry().getAutomatedFields(getClass());
        _stateChangeMethods = model.getTypeRegistry().getStateChangeMethods(getClass());


        if(_parent instanceof AbstractConfiguredObject && ((AbstractConfiguredObject)_parent)._encrypter != null)
        {
            _encrypter = ((AbstractConfiguredObject)_parent)._encrypter;
        }
        else if(_parent instanceof ConfigurationSecretEncrypterSource && ((ConfigurationSecretEncrypterSource)_parent).getEncrypter() != null)
        {
            _encrypter = ((ConfigurationSecretEncrypterSource)_parent).getEncrypter();
        }

        if(_parent instanceof AbstractConfiguredObject && ((AbstractConfiguredObject)_parent).getAccessControl() != null)
        {
            _parentAccessControl = ((AbstractConfiguredObject)_parent).getAccessControl();
        }
        else if(_parent instanceof AccessControlSource && ((AccessControlSource)_parent).getAccessControl()!=null)
        {
            _parentAccessControl = ((AccessControlSource)_parent).getAccessControl();
        }

        if(_parent instanceof AbstractConfiguredObject && ((AbstractConfiguredObject)_parent).getSystemPrincipal() != null)
        {
            _systemPrincipal = ((AbstractConfiguredObject)_parent).getSystemPrincipal();
        }
        else if(_parent instanceof SystemPrincipalSource && ((SystemPrincipalSource)_parent).getSystemPrincipal()!=null)
        {
            _systemPrincipal = ((SystemPrincipalSource)_parent).getSystemPrincipal();
        }


        Object idObj = attributes.get(ID);

        UUID uuid;
        if(idObj == null)
        {
            uuid = UUID.randomUUID();
            attributes = new LinkedHashMap<>(attributes);
            attributes.put(ID, uuid);
        }
        else
        {
            uuid = AttributeValueConverter.UUID_CONVERTER.convert(idObj, this);
        }
        _id = uuid;
        _name = AttributeValueConverter.STRING_CONVERTER.convert(attributes.get(NAME),this);
        if(_name == null)
        {
            throw new IllegalArgumentException("The name attribute is mandatory for " + getClass().getSimpleName() + " creation.");
        }

        _type = ConfiguredObjectTypeRegistry.getType(getClass());
        _managesChildStorage = managesChildren(_category) || managesChildren(_typeClass);
        _bestFitInterface = calculateBestFitInterface();

        if(attributes.get(TYPE) != null && !_type.equals(attributes.get(TYPE)))
        {
            throw new IllegalConfigurationException("Provided type is " + attributes.get(TYPE)
                                                    + " but calculated type is " + _type);
        }
        else if(attributes.get(TYPE) == null)
        {
            attributes = new LinkedHashMap<>(attributes);
            attributes.put(TYPE, _type);
        }

        populateChildTypeMaps();


        Object durableObj = attributes.get(DURABLE);
        _durable = AttributeValueConverter.BOOLEAN_CONVERTER.convert(durableObj == null
                                                                             ? ((ConfiguredSettableAttribute) (_attributeTypes
                .get(DURABLE))).defaultValue()
                                                                             : durableObj, this);

        for (String name : getAttributeNames())
        {
            if (attributes.containsKey(name))
            {
                final Object value = attributes.get(name);
                if (value != null)
                {
                    _attributes.put(name, value);
                }
            }
        }

        if(!_attributes.containsKey(CREATED_BY))
        {
            final AuthenticatedPrincipal currentUser = AuthenticatedPrincipal.getCurrentUser();
            if(currentUser != null)
            {
                _attributes.put(CREATED_BY, currentUser.getName());
            }
        }
        if(!_attributes.containsKey(CREATED_TIME))
        {
            _attributes.put(CREATED_TIME, System.currentTimeMillis());
        }
        for(ConfiguredObjectAttribute<?,?> attr : _attributeTypes.values())
        {
            if(!attr.isDerived())
            {
                ConfiguredSettableAttribute<?,?> autoAttr = (ConfiguredSettableAttribute<?,?>)attr;
                if (autoAttr.isMandatory() && !(_attributes.containsKey(attr.getName())
                                            || !"".equals(autoAttr.defaultValue())))
                {
                    deleted();
                    throw new IllegalArgumentException("Mandatory attribute "
                                                       + attr.getName()
                                                       + " not supplied for instance of "
                                                       + getClass().getName());
                }
            }
        }
    }

    protected final void updateModel(Model model)
    {
        if(this instanceof DynamicModel && _children.isEmpty() && _model.getChildTypes(getCategoryClass()).isEmpty() && Model.isSpecialization(_model, model, getCategoryClass()))
        {
            _model = model;
            populateChildTypeMaps();
        }
        else
        {
            throw new IllegalStateException("Cannot change the model of a class which does not implement DynamicModel, or has defined child types");
        }
    }

    private void populateChildTypeMaps()
    {
        if(!(_children.isEmpty() && _childrenById.isEmpty() && _childrenByName.isEmpty()))
        {
            throw new IllegalStateException("Cannot update the child type maps on a class with pre-existing child types");
        }
        for (Class<? extends ConfiguredObject> childClass : getModel().getChildTypes(getCategoryClass()))
        {
            _children.put(childClass, new CopyOnWriteArrayList<ConfiguredObject<?>>());
            _childrenById.put(childClass, new ConcurrentHashMap<UUID, ConfiguredObject<?>>());
            _childrenByName.put(childClass, new ConcurrentHashMap<String, ConfiguredObject<?>>());
        }
    }

    private boolean managesChildren(final Class<? extends ConfiguredObject> clazz)
    {
        return clazz.getAnnotation(ManagedObject.class).managesChildren();
    }

    private Class<? extends ConfiguredObject> calculateBestFitInterface()
    {
        Set<Class<? extends ConfiguredObject>> candidates = new HashSet<Class<? extends ConfiguredObject>>();
        findBestFitInterface(getClass(), candidates);
        switch(candidates.size())
        {
            case 0:
                throw new ServerScopedRuntimeException("The configured object class " + getClass().getSimpleName() + " does not seem to implement an interface");
            case 1:
                return candidates.iterator().next();
            default:
                ArrayList<Class<? extends ConfiguredObject>> list = new ArrayList<>(candidates);

                throw new ServerScopedRuntimeException("The configured object class " + getClass().getSimpleName()
                        + " implements no single common interface which extends ConfiguredObject"
                        + " Identified candidates were : " + Arrays.toString(list.toArray()));
        }
    }

    private static final void findBestFitInterface(Class<? extends ConfiguredObject> clazz, Set<Class<? extends ConfiguredObject>> candidates)
    {
        for(Class<?> interfaceClass : clazz.getInterfaces())
        {
            if(ConfiguredObject.class.isAssignableFrom(interfaceClass))
            {
                checkCandidate((Class<? extends ConfiguredObject>) interfaceClass, candidates);
            }
        }
        if(clazz.getSuperclass() != null && ConfiguredObject.class.isAssignableFrom(clazz.getSuperclass()))
        {
            findBestFitInterface((Class<? extends ConfiguredObject>) clazz.getSuperclass(), candidates);
        }
    }

    private static void checkCandidate(final Class<? extends ConfiguredObject> interfaceClass,
                                       final Set<Class<? extends ConfiguredObject>> candidates)
    {
        if(!candidates.contains(interfaceClass))
        {
            Iterator<Class<? extends ConfiguredObject>> candidateIterator = candidates.iterator();

            while(candidateIterator.hasNext())
            {
                Class<? extends ConfiguredObject> existingCandidate = candidateIterator.next();
                if(existingCandidate.isAssignableFrom(interfaceClass))
                {
                    candidateIterator.remove();
                }
                else if(interfaceClass.isAssignableFrom(existingCandidate))
                {
                    return;
                }
            }

            candidates.add(interfaceClass);

        }
    }

    private void automatedSetValue(final String name, Object value)
    {
        try
        {
            final ConfiguredAutomatedAttribute attribute = (ConfiguredAutomatedAttribute) _attributeTypes.get(name);
            if(value == null && !"".equals(attribute.defaultValue()))
            {
                value = attribute.defaultValue();
            }
            ConfiguredObjectTypeRegistry.AutomatedField field = _automatedFields.get(name);

            if(field.getPreSettingAction() != null)
            {
                field.getPreSettingAction().invoke(this);
            }

            Object desiredValue = attribute.convert(value, this);
            field.getField().set(this, desiredValue);

            if(field.getPostSettingAction() != null)
            {
                field.getPostSettingAction().invoke(this);
            }
        }
        catch (IllegalAccessException e)
        {
            throw new ServerScopedRuntimeException("Unable to set the automated attribute " + name + " on the configure object type " + getClass().getName(),e);
        }
        catch (InvocationTargetException e)
        {
            if(e.getCause() instanceof RuntimeException)
            {
                throw (RuntimeException) e.getCause();
            }
            throw new ServerScopedRuntimeException("Unable to set the automated attribute " + name + " on the configure object type " + getClass().getName(),e);
        }
    }

    private boolean checkValidValues(final ConfiguredSettableAttribute attribute, final Object desiredValue)
    {
        for (Object validValue : attribute.validValues())
        {
            Object convertedValidValue = attribute.getConverter().convert(validValue, this);

            if (convertedValidValue.equals(desiredValue))
            {
                return true;
            }
        }

        return false;
    }

    private boolean checkValidValuePattern(final ConfiguredSettableAttribute attribute, final Object desiredValue)
    {
        Collection<String> valuesToCheck;

        if(attribute.getType().equals(String.class))
        {
            valuesToCheck = Collections.singleton(desiredValue.toString());
        }
        else if(Collection.class.isAssignableFrom(attribute.getType()) && attribute.getGenericType() instanceof ParameterizedType)
        {
            ParameterizedType paramType = (ParameterizedType)attribute.getGenericType();
            if(paramType.getActualTypeArguments().length == 1 && paramType.getActualTypeArguments()[0] == String.class)
            {
                valuesToCheck = (Collection<String>)desiredValue;
            }
            else
            {
                valuesToCheck = Collections.emptySet();
            }
        }
        else
        {
            valuesToCheck = Collections.emptySet();
        }

        Pattern pattern = Pattern.compile(attribute.validValuePattern());
        for (String value : valuesToCheck)
        {
            if(!pattern.matcher(value).matches())
            {
                return false;
            }
        }

        return true;
    }


    @Override
    public final void open()
    {
        doSync(openAsync());
    }


    @Override
    public final ListenableFuture<Void> openAsync()
    {
        return doOnConfigThread(new Task<ListenableFuture<Void>, RuntimeException>()
                                {
                                    @Override
                                    public ListenableFuture<Void> execute()
                                    {
                                        if (_dynamicState.compareAndSet(UNINIT, OPENED))
                                        {
                                            _openFailed = false;
                                            OpenExceptionHandler exceptionHandler = new OpenExceptionHandler();
                                            try
                                            {
                                                doResolution(true, exceptionHandler);
                                                doValidation(true, exceptionHandler);
                                                doOpening(true, exceptionHandler);
                                                return doAttainState(exceptionHandler);
                                            }
                                            catch (RuntimeException e)
                                            {
                                                exceptionHandler.handleException(e, AbstractConfiguredObject.this);
                                                return Futures.immediateFuture(null);
                                            }
                                        }
                                        else
                                        {
                                            return Futures.immediateFuture(null);
                                        }

                                    }

                                    @Override
                                    public String getObject()
                                    {
                                        return AbstractConfiguredObject.this.toString();
                                    }

                                    @Override
                                    public String getAction()
                                    {
                                        return "open";
                                    }

                                    @Override
                                    public String getArguments()
                                    {
                                        return null;
                                    }
                                });

    }

    protected final <T, E extends Exception> ListenableFuture<T> doOnConfigThread(final Task<ListenableFuture<T>, E> task)
    {
        final SettableFuture<T> returnVal = SettableFuture.create();

        _taskExecutor.submit(new Task<Void, RuntimeException>()
        {

            @Override
            public Void execute()
            {
                try
                {
                    addFutureCallback(task.execute(), new FutureCallback<T>()
                    {
                        @Override
                        public void onSuccess(final T result)
                        {
                            returnVal.set(result);
                        }

                        @Override
                        public void onFailure(final Throwable t)
                        {
                            returnVal.setException(t);
                        }
                    }, getTaskExecutor());
                }
                catch(Throwable t)
                {
                    returnVal.setException(t);
                }
                return null;
            }

            @Override
            public String getObject()
            {
                return task.getObject();
            }

            @Override
            public String getAction()
            {
                return task.getAction();
            }

            @Override
            public String getArguments()
            {
                return task.getArguments();
            }
        });

        return returnVal;
    }



    public void registerWithParents()
    {
        if(_parent instanceof AbstractConfiguredObject<?>)
        {
            ((AbstractConfiguredObject<?>)_parent).registerChild(this);
        }
        else if(_parent instanceof AbstractConfiguredObjectProxy)
        {
            ((AbstractConfiguredObjectProxy)_parent).registerChild(this);
        }

    }

    protected final ListenableFuture<Void> closeChildren()
    {
        final List<ListenableFuture<Void>> childCloseFutures = new ArrayList<>();

        applyToChildren(new Action<ConfiguredObject<?>>()
        {
            @Override
            public void performAction(final ConfiguredObject<?> child)
            {
                ListenableFuture<Void> childCloseFuture = child.closeAsync();
                addFutureCallback(childCloseFuture, new FutureCallback<Void>()
                {
                    @Override
                    public void onSuccess(final Void result)
                    {
                    }

                    @Override
                    public void onFailure(final Throwable t)
                    {
                        LOGGER.error("Exception occurred while closing {} : {}",
                                     child.getClass().getSimpleName(), child.getName(), t);
                    }
                }, getTaskExecutor());
                childCloseFutures.add(childCloseFuture);
            }
        });

        ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(childCloseFutures);
        return doAfter(combinedFuture, new Runnable()
        {
            @Override
            public void run()
            {
                // TODO consider removing each child from the parent as each child close completes, rather
                // than awaiting the completion of the combined future.  This would make it easy to give
                // clearer debug that would highlight the children that have failed to closed.
                for(Collection<ConfiguredObject<?>> childList : _children.values())
                {
                    childList.clear();
                }

                for(Map<UUID,ConfiguredObject<?>> childIdMap : _childrenById.values())
                {
                    childIdMap.clear();
                }

                for(Map<String,ConfiguredObject<?>> childNameMap : _childrenByName.values())
                {
                    childNameMap.clear();
                }

                LOGGER.debug("All children closed {} : {}", AbstractConfiguredObject.this.getClass().getSimpleName(), getName());
            }
        });
    }


    @Override
    public void close()
    {
        doSync(closeAsync());
    }

    @Override
    public final ListenableFuture<Void> closeAsync()
    {
        return doOnConfigThread(new Task<ListenableFuture<Void>, RuntimeException>()
        {
            @Override
            public ListenableFuture<Void> execute()
            {
                LOGGER.debug("Closing " + AbstractConfiguredObject.this.getClass().getSimpleName() + " : " + getName());
                final SettableFuture<Void> returnFuture = SettableFuture.create();
                DynamicStateWithFuture desiredStateWithFuture = new DynamicStateWithFuture(DynamicState.CLOSED, returnFuture);
                DynamicStateWithFuture currentStateWithFuture;
                while((currentStateWithFuture = _dynamicState.get()) == OPENED)
                {
                    if(_dynamicState.compareAndSet(OPENED, desiredStateWithFuture))
                    {
                        final ChainedListenableFuture<Void> future =
                                doAfter(beforeClose(), new Callable<ListenableFuture<Void>>()
                                {
                                    @Override
                                    public ListenableFuture<Void> call() throws Exception
                                    {
                                        return closeChildren();
                                    }
                                }).then(new Callable<ListenableFuture<Void>>()
                                {
                                    @Override
                                    public ListenableFuture<Void> call() throws Exception
                                    {
                                        return onClose();
                                    }
                                }).then(new Callable<ListenableFuture<Void>>()
                                {
                                    @Override
                                    public ListenableFuture<Void> call() throws Exception
                                    {
                                        unregister(false);
                                        LOGGER.debug("Closed "
                                                     + AbstractConfiguredObject.this.getClass().getSimpleName()
                                                     + " : "
                                                     + getName());
                                        return Futures.immediateFuture(null);
                                    }
                                });
                        addFutureCallback(future, new FutureCallback<Void>()
                        {
                            @Override
                            public void onSuccess(final Void result)
                            {
                                returnFuture.set(null);
                            }

                            @Override
                            public void onFailure(final Throwable t)
                            {
                                returnFuture.setException(t);
                            }
                        }, MoreExecutors.directExecutor());

                        return returnFuture;
                    }
                }

                return currentStateWithFuture.getFuture();

            }

            @Override
            public String getObject()
            {
                return AbstractConfiguredObject.this.toString();
            }

            @Override
            public String getAction()
            {
                return "close";
            }

            @Override
            public String getArguments()
            {
                return null;
            }
        });

    }

    protected ListenableFuture<Void> beforeClose()
    {
        return Futures.immediateFuture(null);
    }

    protected ListenableFuture<Void> onClose()
    {
        return Futures.immediateFuture(null);
    }

    public final void create()
    {
        doSync(createAsync());
    }

    public final ListenableFuture<Void> createAsync()
    {
        return doOnConfigThread(new Task<ListenableFuture<Void>, RuntimeException>()
        {
            @Override
            public ListenableFuture<Void> execute()
            {
                if (_dynamicState.compareAndSet(UNINIT, OPENED))
                {
                    initializeAttributes();

                    CreateExceptionHandler createExceptionHandler = new CreateExceptionHandler();
                    try
                    {
                        doResolution(true, createExceptionHandler);
                        doValidation(true, createExceptionHandler);
                        validateOnCreate();
                        registerWithParents();
                        createUserPreferences();
                    }
                    catch (RuntimeException e)
                    {
                        createExceptionHandler.handleException(e, AbstractConfiguredObject.this);
                    }

                    final AbstractConfiguredObjectExceptionHandler unregisteringExceptionHandler =
                            new CreateExceptionHandler(true);

                    try
                    {
                        doCreation(true, unregisteringExceptionHandler);
                        doOpening(true, unregisteringExceptionHandler);
                        return doAttainState(unregisteringExceptionHandler);
                    }
                    catch (RuntimeException e)
                    {
                        unregisteringExceptionHandler.handleException(e, AbstractConfiguredObject.this);
                    }
                }
                return Futures.immediateFuture(null);

            }

            @Override
            public String getObject()
            {
                return AbstractConfiguredObject.this.toString();
            }

            @Override
            public String getAction()
            {
                return "create";
            }

            @Override
            public String getArguments()
            {
                return null;
            }
        });

    }

    private void createUserPreferences()
    {
        if (this instanceof UserPreferencesCreator)
        {
            return;
        }

        UserPreferencesCreator preferenceCreator = getAncestor(UserPreferencesCreator.class);
        if (preferenceCreator != null)
        {
            UserPreferences userPreferences = preferenceCreator.createUserPreferences(this);
            setUserPreferences(userPreferences);
        }
    }

    private void initializeAttributes()
    {
        final AuthenticatedPrincipal currentUser = AuthenticatedPrincipal.getCurrentUser();
        if (currentUser != null)
        {
            String currentUserName = currentUser.getName();
            _attributes.put(LAST_UPDATED_BY, currentUserName);
            _attributes.put(CREATED_BY, currentUserName);
        }

        final Date currentTime = new Date();
        _attributes.put(LAST_UPDATED_TIME, currentTime);
        _attributes.put(CREATED_TIME, currentTime);

        ConfiguredObject<?> proxyForInitialization = null;
        for(ConfiguredObjectAttribute<?,?> attr : _attributeTypes.values())
        {
            if(!attr.isDerived())
            {
                ConfiguredSettableAttribute autoAttr = (ConfiguredSettableAttribute)attr;
                final boolean isPresent = _attributes.containsKey(attr.getName());
                final boolean hasDefault = !"".equals(autoAttr.defaultValue());
                if(!isPresent && hasDefault)
                {
                    switch(autoAttr.getInitialization())
                    {
                        case copy:
                            _attributes.put(autoAttr.getName(), autoAttr.defaultValue());
                            break;
                        case materialize:

                            if(proxyForInitialization == null)
                            {
                                proxyForInitialization = createProxyForInitialization(_attributes);
                            }
                            _attributes.put(autoAttr.getName(), autoAttr.convert(autoAttr.defaultValue(),
                                                                                 proxyForInitialization));
                            break;
                    }
                }
            }
        }
    }

    protected void validateOnCreate()
    {
    }

    protected boolean rethrowRuntimeExceptionsOnOpen()
    {
        return false;
    }

    protected final void handleExceptionOnOpen(RuntimeException e)
    {
        if (rethrowRuntimeExceptionsOnOpen() || e instanceof ServerScopedRuntimeException)
        {
            throw e;
        }

        LOGGER.error("Failed to open object with name '" + getName() + "'.  Object will be put into ERROR state.", e);

        try
        {
            onExceptionInOpen(e);
        }
        catch (RuntimeException re)
        {
            LOGGER.error("Unexpected exception while handling exception on open for " + getName(), e);
        }

        if (!_openComplete)
        {
            _openFailed = true;
            _dynamicState.compareAndSet(OPENED, UNINIT);
        }

        //TODO: children of ERRORED CO will continue to remain in ACTIVE state
        setState(State.ERRORED);
    }

    /**
     * Callback method to perform ConfiguredObject specific exception handling on exception in open.
     * <p>
     * The method is not expected to throw any runtime exception.
     * @param e open exception
     */
    protected void onExceptionInOpen(RuntimeException e)
    {
    }

    private ListenableFuture<Void> doAttainState(final AbstractConfiguredObjectExceptionHandler exceptionHandler)
    {
        final List<ListenableFuture<Void>> childStateFutures = new ArrayList<>();

        applyToChildren(new Action<ConfiguredObject<?>>()
        {
            @Override
            public void performAction(final ConfiguredObject<?> child)
            {
                if (child instanceof AbstractConfiguredObject)
                {
                    AbstractConfiguredObject<?> abstractConfiguredChild = (AbstractConfiguredObject<?>) child;
                    if(abstractConfiguredChild._dynamicState.get().getDynamicState() == DynamicState.OPENED)
                    {
                        final AbstractConfiguredObject configuredObject = abstractConfiguredChild;
                        childStateFutures.add(configuredObject.doAttainState(exceptionHandler));
                    }
                }
                else if(child instanceof AbstractConfiguredObjectProxy
                    && ((AbstractConfiguredObjectProxy)child).getDynamicState() == DynamicState.OPENED)
                {
                    final AbstractConfiguredObjectProxy configuredObject = (AbstractConfiguredObjectProxy) child;
                    childStateFutures.add(configuredObject.doAttainState(exceptionHandler));
                }
            }
        });

        ListenableFuture<List<Void>> combinedChildStateFuture = Futures.allAsList(childStateFutures);

        final SettableFuture<Void> returnVal = SettableFuture.create();
        addFutureCallback(combinedChildStateFuture, new FutureCallback<List<Void>>()
        {
            @Override
            public void onSuccess(final List<Void> result)
            {
                try
                {
                    addFutureCallback(attainState(),
                                        new FutureCallback<Void>()
                                        {
                                            @Override
                                            public void onSuccess(final Void result1)
                                            {
                                                returnVal.set(null);
                                            }

                                            @Override
                                            public void onFailure(final Throwable t)
                                            {
                                                try
                                                {
                                                    if (t instanceof RuntimeException)
                                                    {
                                                        try
                                                        {
                                                            exceptionHandler.handleException((RuntimeException) t,
                                                                                             AbstractConfiguredObject.this);
                                                            returnVal.set(null);
                                                        }
                                                        catch (RuntimeException r)
                                                        {
                                                            returnVal.setException(r);
                                                        }
                                                    }
                                                }
                                                finally
                                                {
                                                    if (!returnVal.isDone())
                                                    {
                                                        returnVal.setException(t);
                                                    }
                                                }
                                            }
                                        }, getTaskExecutor());
                }
                catch (RuntimeException e)
                {
                    try
                    {
                        exceptionHandler.handleException(e, AbstractConfiguredObject.this);
                        returnVal.set(null);
                    }
                    catch (Throwable t)
                    {
                        returnVal.setException(t);
                    }
                }
            }

            @Override
            public void onFailure(final Throwable t)
            {
                // One or more children failed to attain state but the error could not be handled by the handler
                returnVal.setException(t);
            }
        }, getTaskExecutor());

        return returnVal;
    }

    protected final void doOpening(boolean skipCheck, final AbstractConfiguredObjectExceptionHandler exceptionHandler)
    {
        if(skipCheck || _dynamicState.compareAndSet(UNINIT, OPENED))
        {
            onOpen();
            notifyStateChanged(State.UNINITIALIZED, getState());
            applyToChildren(new Action<ConfiguredObject<?>>()
            {
                @Override
                public void performAction(final ConfiguredObject<?> child)
                {
                    if (child.getState() != State.ERRORED)
                    {

                        try
                        {
                            if(child instanceof AbstractConfiguredObject)
                            {
                                AbstractConfiguredObject configuredObject = (AbstractConfiguredObject) child;
                                configuredObject.doOpening(false, exceptionHandler);
                            }
                            else if(child instanceof AbstractConfiguredObjectProxy)
                            {
                                AbstractConfiguredObjectProxy configuredObject = (AbstractConfiguredObjectProxy) child;
                                configuredObject.doOpening(false, exceptionHandler);
                            }
                        }
                        catch (RuntimeException e)
                        {
                            exceptionHandler.handleException(e, child);
                        }
                    }
                }
            });
            _openComplete = true;
            _lastOpenedTime = new Date();
        }
    }

    protected final void doValidation(final boolean skipCheck, final AbstractConfiguredObjectExceptionHandler exceptionHandler)
    {
        if(skipCheck || _dynamicState.get().getDynamicState() != DynamicState.OPENED)
        {
            applyToChildren(new Action<ConfiguredObject<?>>()
            {
                @Override
                public void performAction(final ConfiguredObject<?> child)
                {
                    if (child.getState() != State.ERRORED)
                    {
                        try
                        {
                            if(child instanceof AbstractConfiguredObject)
                            {
                                AbstractConfiguredObject configuredObject = (AbstractConfiguredObject) child;
                                configuredObject.doValidation(false, exceptionHandler);
                            }
                            else if(child instanceof AbstractConfiguredObjectProxy)
                            {
                                AbstractConfiguredObjectProxy configuredObject = (AbstractConfiguredObjectProxy) child;
                                configuredObject.doValidation(false, exceptionHandler);
                            }
                        }
                        catch (RuntimeException e)
                        {
                            exceptionHandler.handleException(e, child);
                        }
                    }
                }
            });
            onValidate();
        }
    }

    protected final void doResolution(boolean skipCheck, final AbstractConfiguredObjectExceptionHandler exceptionHandler)
    {
        if(skipCheck || _dynamicState.get().getDynamicState() != DynamicState.OPENED)
        {
            onResolve();
            postResolve();
            applyToChildren(new Action()
            {
                @Override
                public void performAction(Object child)
                {
                        try
                        {
                            if (child instanceof AbstractConfiguredObject)
                            {
                                AbstractConfiguredObject configuredObject = (AbstractConfiguredObject) child;

                                configuredObject.doResolution(false, exceptionHandler);
                            }
                            else if (child instanceof AbstractConfiguredObjectProxy)
                            {
                                AbstractConfiguredObjectProxy configuredObject = (AbstractConfiguredObjectProxy) child;

                                configuredObject.doResolution(false, exceptionHandler);
                            }
                        }
                        catch (RuntimeException e)
                        {
                            exceptionHandler.handleException(e, (ConfiguredObject)child);
                        }

                }
            });
            postResolveChildren();
        }
    }

    protected void postResolveChildren()
    {

    }

    protected void postResolve()
    {
        if (getActualAttributes().get(CREATED_BY) != null)
        {
            _createdBy = (String) getActualAttributes().get(CREATED_BY);
        }
        if (getActualAttributes().get(CREATED_TIME) != null)
        {
            _createdTime = AttributeValueConverter.DATE_CONVERTER.convert(getActualAttributes().get(CREATED_TIME), this);
        }
        if (getActualAttributes().get(LAST_UPDATED_BY) != null)
        {
            _lastUpdatedBy = (String) getActualAttributes().get(LAST_UPDATED_BY);
        }
        if (getActualAttributes().get(LAST_UPDATED_TIME) != null)
        {
            _lastUpdatedTime = AttributeValueConverter.DATE_CONVERTER.convert(getActualAttributes().get(LAST_UPDATED_TIME), this);
        }
    }

    protected final void doCreation(final boolean skipCheck, final AbstractConfiguredObjectExceptionHandler exceptionHandler)
    {
        if(skipCheck || _dynamicState.get().getDynamicState() != DynamicState.OPENED)
        {
            onCreate();
            applyToChildren(new Action<ConfiguredObject<?>>()
            {
                @Override
                public void performAction(final ConfiguredObject<?> child)
                {
                        try
                        {
                            if (child instanceof AbstractConfiguredObject)
                            {
                                AbstractConfiguredObject configuredObject = (AbstractConfiguredObject) child;
                                configuredObject.doCreation(false, exceptionHandler);
                            }
                            else if(child instanceof AbstractConfiguredObjectProxy)
                            {
                                AbstractConfiguredObjectProxy configuredObject = (AbstractConfiguredObjectProxy) child;
                                configuredObject.doCreation(false, exceptionHandler);
                            }
                        }
                        catch (RuntimeException e)
                        {
                            exceptionHandler.handleException(e, child);
                        }

                }
            });
        }
    }

    protected void applyToChildren(Action<ConfiguredObject<?>> action)
    {
        for (Class<? extends ConfiguredObject> childClass : getModel().getChildTypes(getCategoryClass()))
        {
            Collection<? extends ConfiguredObject> children = getChildren(childClass);
            if (children != null)
            {
                for (ConfiguredObject<?> child : children)
                {
                    action.performAction(child);
                }
            }
        }
    }

    /**
     * Validation performed for configured object creation and opening.
     *
     * @throws IllegalConfigurationException indicates invalid configuration
     */
    public void onValidate()
    {
        for(ConfiguredObjectAttribute<?,?> attr : _attributeTypes.values())
        {
            if (!attr.isDerived())
            {
                ConfiguredSettableAttribute autoAttr = (ConfiguredSettableAttribute) attr;
                if (autoAttr.hasValidValues())
                {
                    Object desiredValueOrDefault = autoAttr.getValue(this);

                    if (desiredValueOrDefault != null && !checkValidValues(autoAttr, desiredValueOrDefault))
                    {
                        throw new IllegalConfigurationException("Attribute '" + autoAttr.getName()
                                                                + "' instance of "+ getClass().getName()
                                                                + " named '" + getName() + "'"
                                                                + " cannot have value '" + desiredValueOrDefault + "'"
                                                                + ". Valid values are: "
                                                                + autoAttr.validValues());
                    }
                }
                else if(!"".equals(autoAttr.validValuePattern()))
                {
                    Object desiredValueOrDefault = autoAttr.getValue(this);

                    if (desiredValueOrDefault != null && !checkValidValuePattern(autoAttr, desiredValueOrDefault))
                    {
                        throw new IllegalConfigurationException("Attribute '" + autoAttr.getName()
                                                                + "' instance of "+ getClass().getName()
                                                                + " named '" + getName() + "'"
                                                                + " cannot have value '" + desiredValueOrDefault + "'"
                                                                + ". Valid values pattern is: "
                                                                + autoAttr.validValuePattern());
                    }
                }
                if(autoAttr.isMandatory() && autoAttr.getValue(this) == null)
                {
                    throw new IllegalConfigurationException("Attribute '" + autoAttr.getName()
                                                            + "' instance of "+ getClass().getName()
                                                            + " named '" + getName() + "'"
                                                            + " cannot be null, as it is mandatory");
                }

            }
        }
    }

    protected final void setEncrypter(final ConfigurationSecretEncrypter encrypter)
    {
        _encrypter = encrypter;
        applyToChildren(new Action<ConfiguredObject<?>>()
        {
            @Override
            public void performAction(final ConfiguredObject<?> object)
            {
                if(object instanceof AbstractConfiguredObject)
                {
                    ((AbstractConfiguredObject)object).setEncrypter(encrypter);
                }
            }
        });
    }

    protected void onResolve()
    {
        Set<ConfiguredObjectAttribute<?,?>> unresolved = new HashSet<>();
        Set<ConfiguredObjectAttribute<?,?>> derived = new HashSet<>();


        for (ConfiguredObjectAttribute<?, ?> attr : _attributeTypes.values())
        {
            if(attr.isDerived())
            {
                derived.add(attr);
            }
            else
            {
                unresolved.add(attr);
            }
        }

        // If there is a context attribute, resolve it first, so that other attribute values
        // may support values containing references to context keys.
        ConfiguredObjectAttribute<?, ?> contextAttribute = _attributeTypes.get("context");
        if (contextAttribute != null && !contextAttribute.isDerived())
        {
            if(contextAttribute.isAutomated())
            {
                resolveAutomatedAttribute((ConfiguredSettableAttribute<?, ?>) contextAttribute);
            }
            unresolved.remove(contextAttribute);
        }

        boolean changed = true;
        while(!unresolved.isEmpty() || !changed)
        {
            changed = false;
            Iterator<ConfiguredObjectAttribute<?,?>> attrIter = unresolved.iterator();

            while (attrIter.hasNext())
            {
                ConfiguredObjectAttribute<?, ?> attr = attrIter.next();

                if(!(dependsOn(attr, unresolved) || (!derived.isEmpty() && dependsOn(attr, derived))))
                {
                    if(attr.isAutomated())
                    {
                        resolveAutomatedAttribute((ConfiguredSettableAttribute<?, ?>) attr);
                    }
                    attrIter.remove();
                    changed = true;
                }
            }
            // TODO - really we should define with meta data which attributes any given derived attr is dependent upon
            //        and only remove the derived attr as an obstacle when those fields are themselves resolved
            if(!changed && !derived.isEmpty())
            {
                changed = true;
                derived.clear();
            }
        }
    }

    private boolean dependsOn(final ConfiguredObjectAttribute<?, ?> attr,
                              final Set<ConfiguredObjectAttribute<?, ?>> unresolved)
    {
        Object value = _attributes.get(attr.getName());
        if(value == null && !"".equals(((ConfiguredSettableAttribute)attr).defaultValue()))
        {
            value = ((ConfiguredSettableAttribute)attr).defaultValue();
        }
        if(value instanceof String)
        {
            String interpolated = interpolate(this, (String)value);
            if(interpolated.contains("${this:"))
            {
                for(ConfiguredObjectAttribute<?,?> unresolvedAttr : unresolved)
                {
                    if(interpolated.contains("${this:"+unresolvedAttr.getName()))
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void resolveAutomatedAttribute(final ConfiguredSettableAttribute<?, ?> autoAttr)
    {
        String attrName = autoAttr.getName();
        if (_attributes.containsKey(attrName))
        {
            automatedSetValue(attrName, _attributes.get(attrName));
        }
        else if (!"".equals(autoAttr.defaultValue()))
        {
            automatedSetValue(attrName, autoAttr.defaultValue());
        }
    }

    private ListenableFuture<Void> attainStateIfOpenedOrReopenFailed()
    {
        if (_openComplete || getDesiredState() == State.DELETED)
        {
            return attainState();
        }
        else if (_openFailed)
        {
            return openAsync();
        }
        return Futures.immediateFuture(null);
    }

    protected void onOpen()
    {

    }

    protected ListenableFuture<Void> attainState()
    {
        return attainState(getDesiredState());
    }

    private ListenableFuture<Void> attainState(State desiredState)
    {
        final State currentState = getState();
        ListenableFuture<Void> returnVal;

        if (_attainStateFuture.isDone())
        {
            _attainStateFuture = SettableFuture.create();
        }

        if(currentState != desiredState)
        {
            Method stateChangingMethod = getStateChangeMethod(currentState, desiredState);
            if(stateChangingMethod != null)
            {
                try
                {
                    final SettableFuture<Void> stateTransitionResult = SettableFuture.create();
                    ListenableFuture<Void> stateTransitionFuture = (ListenableFuture<Void>) stateChangingMethod.invoke(this);
                    addFutureCallback(stateTransitionFuture, new FutureCallback<Void>()
                    {
                        @Override
                        public void onSuccess(Void result)
                        {
                            try
                            {
                                if (getState() != currentState)
                                {
                                    notifyStateChanged(currentState, getState());
                                }
                                stateTransitionResult.set(null);
                            }
                            catch (Throwable e)
                            {
                                stateTransitionResult.setException(e);
                            }
                            finally
                            {
                                _attainStateFuture.set(AbstractConfiguredObject.this);
                            }
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            // state transition failed to attain desired state
                            // setting the _attainStateFuture, so, object relying on it could get the configured object
                            _attainStateFuture.set(AbstractConfiguredObject.this);
                            stateTransitionResult.setException(t);
                        }
                    }, getTaskExecutor());
                    returnVal = stateTransitionResult;
                }
                catch (IllegalAccessException e)
                {
                    throw new ServerScopedRuntimeException("Unexpected access exception when calling state transition", e);
                }
                catch (InvocationTargetException e)
                {
                    // state transition failed to attain desired state
                    // setting the _attainStateFuture, so, object relying on it could get the configured object
                    _attainStateFuture.set(this);

                    Throwable underlying = e.getTargetException();
                    if(underlying instanceof RuntimeException)
                    {
                        throw (RuntimeException)underlying;
                    }
                    if(underlying instanceof Error)
                    {
                        throw (Error) underlying;
                    }
                    throw new ServerScopedRuntimeException("Unexpected checked exception when calling state transition", underlying);
                }
            }
            else
            {
                returnVal = Futures.immediateFuture(null);
                _attainStateFuture.set(this);
            }
        }
        else
        {
            returnVal = Futures.immediateFuture(null);
            _attainStateFuture.set(this);
        }
        return returnVal;
    }

    private Method getStateChangeMethod(final State currentState, final State desiredState)
    {
        Map<State, Method> stateChangeMethodMap = _stateChangeMethods.get(currentState);
        Method method = null;
        if(stateChangeMethodMap != null)
        {
            method = stateChangeMethodMap.get(desiredState);
        }
        return method;
    }


    protected void onCreate()
    {
    }

    @Override
    public final UUID getId()
    {
        return _id;
    }

    @Override
    public final String getName()
    {
        return _name;
    }

    @Override
    public final boolean isDurable()
    {
        return _durable;
    }

    @Override
    public final ConfiguredObjectFactory getObjectFactory()
    {
        return getModel().getObjectFactory();
    }

    @Override
    public final Model getModel()
    {
        return _model;
    }

    @Override
    public Class<? extends ConfiguredObject> getCategoryClass()
    {
        return _category;
    }

    @Override
    public Class<? extends ConfiguredObject> getTypeClass()
    {
        return _typeClass;
    }

    @Override
    public boolean managesChildStorage()
    {
        return _managesChildStorage;
    }

    @Override
    public Map<String,String> getContext()
    {
        return _context == null ? Collections.<String,String>emptyMap() : Collections.unmodifiableMap(_context);
    }

    @Override
    public State getDesiredState()
    {
        return _desiredState;
    }


    private ListenableFuture<Void> setDesiredState(final State desiredState)
            throws IllegalStateTransitionException, AccessControlException
    {
        return doOnConfigThread(new Task<ListenableFuture<Void>, RuntimeException>()
        {
            @Override
            public ListenableFuture<Void> execute()
            {
                final State state = getState();
                final State currentDesiredState = getDesiredState();
                if(desiredState == currentDesiredState && desiredState != state)
                {
                    return doAfter(attainStateIfOpenedOrReopenFailed(), new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            final State currentState = getState();
                            if (currentState != state)
                            {
                                notifyStateChanged(state, currentState);
                            }

                        }
                    });
                }
                else
                {
                    if (desiredState == State.DELETED)
                    {
                        return deleteAsync();
                    }
                    else
                    {
                        Map<String, Object> attributes = Collections.singletonMap(ConfiguredObject.DESIRED_STATE, desiredState);
                        ConfiguredObject<?> proxyForValidation = createProxyForValidation(attributes);
                        authoriseSetAttributes(proxyForValidation, attributes);
                        validateChange(proxyForValidation, attributes.keySet());

                        if (changeAttribute(ConfiguredObject.DESIRED_STATE, desiredState))
                        {
                            attributeSet(ConfiguredObject.DESIRED_STATE, currentDesiredState, desiredState);
                            return attainStateIfOpenedOrReopenFailed();
                        }
                        else
                        {
                            return Futures.immediateFuture(null);
                        }
                    }
                }
            }

            @Override
            public String getObject()
            {
                return AbstractConfiguredObject.this.toString();
            }

            @Override
            public String getAction()
            {
                return "set desired state";
            }

            @Override
            public String getArguments()
            {
                return String.valueOf(desiredState);
            }
        });
    }

    protected void validateChildDelete(final ConfiguredObject<?> child)
    {

    }

    @Override
    public State getState()
    {
        return _state;
    }

    protected void setState(State state)
    {
        _state = state;
    }


    protected void notifyStateChanged(final State currentState, final State desiredState)
    {
        List<ConfigurationChangeListener> copy;
        synchronized (_changeListeners)
        {
            copy = new ArrayList<ConfigurationChangeListener>(_changeListeners);
        }
        for(ConfigurationChangeListener listener : copy)
        {
            listener.stateChanged(this, currentState, desiredState);
        }
    }

    @Override
    public void addChangeListener(final ConfigurationChangeListener listener)
    {
        if(listener == null)
        {
            throw new NullPointerException("Cannot add a null listener");
        }
        synchronized (_changeListeners)
        {
            if(!_changeListeners.contains(listener))
            {
                _changeListeners.add(listener);
            }
        }
    }

    @Override
    public boolean removeChangeListener(final ConfigurationChangeListener listener)
    {
        if(listener == null)
        {
            throw new NullPointerException("Cannot remove a null listener");
        }
        synchronized (_changeListeners)
        {
            return _changeListeners.remove(listener);
        }
    }

    protected final void childAdded(ConfiguredObject<?> child)
    {
        synchronized (_changeListeners)
        {
            List<ConfigurationChangeListener> copy = new ArrayList<>(_changeListeners);
            for(ConfigurationChangeListener listener : copy)
            {
                listener.childAdded(this, child);
            }
        }
    }

    protected final void childRemoved(ConfiguredObject<?> child)
    {
        synchronized (_changeListeners)
        {
            List<ConfigurationChangeListener> copy = new ArrayList<>(_changeListeners);
            for(ConfigurationChangeListener listener : copy)
            {
                listener.childRemoved(this, child);
            }
        }
    }

    protected void attributeSet(String attributeName, Object oldAttributeValue, Object newAttributeValue)
    {

        final AuthenticatedPrincipal currentUser = AuthenticatedPrincipal.getCurrentUser();
        if(currentUser != null)
        {
            _attributes.put(LAST_UPDATED_BY, currentUser.getName());
            _lastUpdatedBy = currentUser.getName();
        }
        final Date currentTime = new Date();
        _attributes.put(LAST_UPDATED_TIME, currentTime);
        _lastUpdatedTime = currentTime;

        synchronized (_changeListeners)
        {
            List<ConfigurationChangeListener> copy = new ArrayList<ConfigurationChangeListener>(_changeListeners);
            for(ConfigurationChangeListener listener : copy)
            {
                listener.attributeSet(this, attributeName, oldAttributeValue, newAttributeValue);
            }
        }
    }

    @Override
    public final Object getAttribute(String name)
    {
        ConfiguredObjectAttribute<X,?> attr = (ConfiguredObjectAttribute<X, ?>) _attributeTypes.get(name);
        if(attr != null)
        {
            Object value = attr.getValue((X)this);
            if(value != null && !isSystemProcess() && attr.isSecureValue(value))
            {
                return SECURE_VALUES.get(value.getClass());
            }
            else
            {
                return value;
            }
        }
        else
        {
            throw new IllegalArgumentException("Unknown attribute: '" + name + "'");
        }
    }

    @Override
    public String getDescription()
    {
        return _description;
    }

    @Override
    public LifetimePolicy getLifetimePolicy()
    {
        return _lifetimePolicy;
    }

    @Override
    public final Map<String, Object> getActualAttributes()
    {
        synchronized (_attributes)
        {
            return new HashMap<String, Object>(_attributes);
        }
    }

    private Object getActualAttribute(final String name)
    {
        synchronized (_attributes)
        {
            return _attributes.get(name);
        }
    }

    private boolean changeAttribute(final String name, final Object desired)
    {
        synchronized (_attributes)
        {
            Object actualValue = _attributes.get(name);

            ConfiguredObjectAttribute<?,?> attr = _attributeTypes.get(name);

            if(attr.updateAttributeDespiteUnchangedValue() ||
               ((actualValue != null && !actualValue.equals(desired)) || (actualValue == null && desired != null)))
            {
                //TODO: don't put nulls
                _attributes.put(name, desired);
                if(attr != null && attr.isAutomated())
                {
                    automatedSetValue(name, desired);
                }
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    @Override
    public ConfiguredObject<?> getParent()
    {
        return _parent;
    }

    public final <T> T getAncestor(final Class<T> clazz)
    {
        return getModel().getAncestor(clazz, this);
    }

    @Override
    public final Collection<String> getAttributeNames()
    {
        return getTypeRegistry().getAttributeNames(getClass());
    }

    @Override
    public String toString()
    {
        return getCategoryClass().getSimpleName() + "[id=" + _id + ", name=" + getName() + ", type=" + getType() + "]";
    }

    @Override
    public final ConfiguredObjectRecord asObjectRecord()
    {
        return new ConfiguredObjectRecord()
        {
            @Override
            public UUID getId()
            {
                return AbstractConfiguredObject.this.getId();
            }

            @Override
            public String getType()
            {
                return getCategoryClass().getSimpleName();
            }

            @Override
            public Map<String, Object> getAttributes()
            {
                return Subject.doAs(getSubjectWithAddedSystemRights(), new PrivilegedAction<Map<String, Object>>()
                {
                    @Override
                    public Map<String, Object> run()
                    {
                        Map<String,Object> attributes = new LinkedHashMap<>();
                        Map<String,Object> actualAttributes = getActualAttributes();
                        for(ConfiguredObjectAttribute<?,?> attr : _attributeTypes.values())
                        {
                            if (attr.isPersisted() && !ID.equals(attr.getName()))
                            {
                                if(attr.isDerived())
                                {
                                    Object value = getAttribute(attr.getName());
                                    attributes.put(attr.getName(), toRecordedForm(attr, value));
                                }
                                else if(actualAttributes.containsKey(attr.getName()))
                                {
                                    Object value = actualAttributes.get(attr.getName());
                                    attributes.put(attr.getName(), toRecordedForm(attr, value));
                                }
                            }
                        }
                        return attributes;
                    }
                });
            }

            public Object toRecordedForm(final ConfiguredObjectAttribute<?, ?> attr, Object value)
            {
                if(value instanceof ConfiguredObject)
                {
                    value = ((ConfiguredObject)value).getId();
                }
                if(attr.isSecure() && _encrypter != null && value != null)
                {
                    if(value instanceof Collection || value instanceof Map)
                    {
                        ObjectMapper mapper = ConfiguredObjectJacksonModule.newObjectMapper(false);
                        try(StringWriter stringWriter = new StringWriter())
                        {
                            mapper.writeValue(stringWriter, value);
                            value = _encrypter.encrypt(stringWriter.toString());
                        }
                        catch (IOException e)
                        {
                            throw new IllegalConfigurationException("Failure when encrypting a secret value", e);
                        }
                    }
                    else
                    {
                        value = _encrypter.encrypt(value.toString());
                    }
                }
                return value;
            }

            @Override
            public Map<String, UUID> getParents()
            {
                Map<String, UUID> parents = new LinkedHashMap<>();
                Class<? extends ConfiguredObject> parentClass = getModel().getParentType(getCategoryClass());

                ConfiguredObject parent = (ConfiguredObject) getParent();
                if(parent != null)
                {
                    parents.put(parentClass.getSimpleName(), parent.getId());
                }

                return parents;
            }

            @Override
            public String toString()
            {
                return AbstractConfiguredObject.this.getClass().getSimpleName() + "[name=" + getName() + ", categoryClass=" + getCategoryClass() + ", type="
                        + getType() + ", id=" + getId() +  ", attributes=" + getAttributes() + "]";
            }
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    public <C extends ConfiguredObject> C createChild(final Class<C> childClass, final Map<String, Object> attributes)
    {
        return doSync(createChildAsync(childClass, attributes));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <C extends ConfiguredObject> ListenableFuture<C> createChildAsync(final Class<C> childClass, final Map<String, Object> attributes)
    {
        return doOnConfigThread(new Task<ListenableFuture<C>, RuntimeException>()
        {
            @Override
            public ListenableFuture<C> execute()
            {
                authoriseCreateChild(childClass, attributes);
                return doAfter(addChildAsync(childClass, attributes),
                                new CallableWithArgument<ListenableFuture<C>, C>()
                                {

                                    @Override
                                    public ListenableFuture<C> call(final C child) throws Exception
                                    {
                                        if (child != null)
                                        {
                                            childAdded(child);
                                        }
                                        return Futures.immediateFuture(child);
                                    }
                                });
            }

            @Override
            public String getObject()
            {
                return AbstractConfiguredObject.this.toString();
            }

            @Override
            public String getAction()
            {
                return "create child";
            }

            @Override
            public String getArguments()
            {
                if (attributes != null)
                {
                    return "childClass=" + childClass.getSimpleName() + ", name=" + attributes.get(NAME) + ", type=" + attributes.get(TYPE);
                }
                return "childClass=" + childClass.getSimpleName();
            }
        });
    }

    protected <C extends ConfiguredObject> ListenableFuture<C> addChildAsync(Class<C> childClass,
                                                                             Map<String, Object> attributes)
    {
        return getObjectFactory().createAsync(childClass, attributes, this);
    }

    private <C extends ConfiguredObject> void registerChild(final C child)
    {
        synchronized(_children)
        {
            Class categoryClass = child.getCategoryClass();
            UUID childId = child.getId();
            String name = child.getName();
            ConfiguredObject<?> existingWithSameId = _childrenById.get(categoryClass).get(childId);
            if(existingWithSameId != null)
            {
                throw new DuplicateIdException(existingWithSameId);
            }
            ConfiguredObject<?> existingWithSameName = _childrenByName.get(categoryClass).putIfAbsent(name, child);
            if (existingWithSameName != null)
            {
                throw new DuplicateNameException(existingWithSameName);
            }
            _childrenByName.get(categoryClass).put(name, child);
            _children.get(categoryClass).add(child);
            _childrenById.get(categoryClass).put(childId,child);
        }
    }

    public final void stop()
    {
        doSync(setDesiredState(State.STOPPED));
    }

    @Override
    public final void delete()
    {
        doSync(deleteAsync());
    }

    protected final <R>  R doSync(ListenableFuture<R> async)
    {
        try
        {
            return async.get();
        }
        catch (InterruptedException e)
        {
            throw new ServerScopedRuntimeException(e);
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            if(cause instanceof RuntimeException)
            {
                throw (RuntimeException) cause;
            }
            else if(cause instanceof Error)
            {
                throw (Error) cause;
            }
            else if(cause != null)
            {
                throw new ServerScopedRuntimeException(cause);
            }
            else
            {
                throw new ServerScopedRuntimeException(e);
            }

        }
    }

    protected final <R>  R doSync(ListenableFuture<R> async, long timeout, TimeUnit units) throws TimeoutException
    {
        try
        {
            return async.get(timeout, units);
        }
        catch (InterruptedException e)
        {
            throw new ServerScopedRuntimeException(e);
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            if(cause instanceof RuntimeException)
            {
                throw (RuntimeException) cause;
            }
            else if(cause instanceof Error)
            {
                throw (Error) cause;
            }
            else if(cause != null)
            {
                throw new ServerScopedRuntimeException(cause);
            }
            else
            {
                throw new ServerScopedRuntimeException(e);
            }

        }
    }

    @Override
    public final ListenableFuture<Void> deleteAsync()
    {
        final State currentDesiredState = getDesiredState();

        if (currentDesiredState == State.DELETED)
        {
            return Futures.immediateFuture(null);
        }

        Map<String, Object> attributes = Collections.singletonMap(ConfiguredObject.DESIRED_STATE, State.DELETED);
        ConfiguredObject<?> proxyForValidation = createProxyForValidation(attributes);
        authoriseSetAttributes(proxyForValidation, attributes);
        validateChange(proxyForValidation, attributes.keySet());
        checkReferencesOnDelete(getHierarchyRoot(this), this);

        // for DELETED state we should invoke transition method first to make sure that object can be deleted.
        // If method results in exception being thrown due to various integrity violations
        // then object cannot be deleted without prior resolving of integrity violations.
        // The state transition should be disallowed.

        if(_parent instanceof AbstractConfiguredObject<?>)
        {
            ((AbstractConfiguredObject<?>)_parent).validateChildDelete(AbstractConfiguredObject.this);
        }
        else if (_parent instanceof AbstractConfiguredObjectProxy)
        {
            ((AbstractConfiguredObjectProxy)_parent).validateChildDelete(AbstractConfiguredObject.this);
        }

        return deleteNoChecks();
    }

    private void checkReferencesOnDelete(final ConfiguredObject<?> referrer, final ConfiguredObject<?> referee)
    {
        if (!managesChildren(referee))
        {
            getModel().getChildTypes(referee.getCategoryClass())
                      .forEach(childClass -> referee.getChildren(childClass)
                                                    .forEach(child -> checkReferencesOnDelete(referrer, child)));
        }
        checkReferences(referrer, referee);
    }

    private ConfiguredObject<?> getHierarchyRoot(final AbstractConfiguredObject<X> o)
    {
        ConfiguredObject<?> object = o;
        do
        {
            ConfiguredObject<?> parent = object.getParent();
            if (parent == null || managesChildren(parent))
            {
                break;
            }
            object = parent;
        }
        while (true);
        return object;
    }

    private boolean managesChildren(final ConfiguredObject<?> object)
    {
        return managesChildren(object.getCategoryClass()) || managesChildren(object.getTypeClass());
    }

    private void checkReferences(final ConfiguredObject<?> referrer, final ConfiguredObject<?> referee)
    {
        if (hasReference(referrer, referee))
        {
            if (referee == this)
            {
                throw new IntegrityViolationException(String.format("%s '%s' is in use by %s '%s'.",
                                                                    referee.getCategoryClass().getSimpleName(),
                                                                    referee.getName(),
                                                                    referrer.getCategoryClass().getSimpleName(),
                                                                    referrer.getName()));
            }
            else
            {
                throw new IntegrityViolationException(String.format("Cannot delete %s '%s' as descendant %s '%s' is in use by %s '%s'.",
                                                                    this.getCategoryClass().getSimpleName(),
                                                                    this.getName(),
                                                                    referee.getCategoryClass().getSimpleName(),
                                                                    referee.getName(),
                                                                    referrer.getCategoryClass().getSimpleName(),
                                                                    referrer.getName()));
            }
        }

        if (!managesChildren(referrer))
        {
            getModel().getChildTypes(referrer.getCategoryClass())
                      .forEach(childClass -> referrer.getChildren(childClass)
                                                     .stream()
                                                     .filter(child -> child != this)
                                                     .forEach(child -> checkReferences(child, referee)));
        }
    }

    private boolean hasReference(final ConfiguredObject<?> referrer,
                                 final ConfiguredObject<?> referee)
    {
        if (referrer instanceof AbstractConfiguredObject)
        {
            return getModel().getTypeRegistry()
                             .getAttributes(referrer.getClass())
                             .stream()
                             .anyMatch(attribute -> {
                                 Class<?> type = attribute.getType();
                                 Type genericType = attribute.getGenericType();
                                 return isReferred(referee, type,
                                                   genericType,
                                                   () -> {
                                                       @SuppressWarnings("unchecked")
                                                       Object value =
                                                               ((ConfiguredObjectAttribute) attribute).getValue(referrer);
                                                       return value;
                                                   });
                             });
        }
        else
        {
            return referrer.getAttributeNames().stream().anyMatch(name -> {
                Object value = referrer.getAttribute(name);
                if (value != null)
                {
                    Class<?> type = value.getClass();
                    return isReferred(referee, type, type, () -> value);
                }
                return false;
            });
        }
    }

    private boolean isReferred(final ConfiguredObject<?> referee,
                               final Class<?> attributeValueType,
                               final Type attributeGenericType,
                               final Supplier<?> attributeValue)
    {
        final Class<? extends ConfiguredObject> referrerCategory = referee.getCategoryClass();
        if (referrerCategory.isAssignableFrom(attributeValueType))
        {
            return attributeValue.get() == referee;
        }
        else if (hasParameterOfType(attributeGenericType, referrerCategory))
        {
            Object value = attributeValue.get();
            if (value instanceof Collection)
            {
                return ((Collection<?>) value).stream().anyMatch(m -> m == referee);
            }
            else if (value instanceof Object[])
            {
                return Arrays.stream((Object[]) value).anyMatch(m -> m == referee);
            }
            else if (value instanceof Map)
            {
                return ((Map<?, ?>) value).entrySet()
                                          .stream()
                                          .anyMatch(e -> e.getKey() == referee
                                                         || e.getValue() == referee);
            }
        }
        return false;
    }

    private boolean hasParameterOfType(Type genericType, Class<?> parameterType)
    {
        if (genericType instanceof ParameterizedType)
        {
            Type[] types = ((ParameterizedType) genericType).getActualTypeArguments();
            return Arrays.stream(types).anyMatch(type -> {
                if (type instanceof Class && parameterType.isAssignableFrom((Class) type))
                {
                    return true;
                }
                else if (type instanceof ParameterizedType)
                {
                    Type rawType = ((ParameterizedType) type).getRawType();
                    return rawType instanceof Class && parameterType.isAssignableFrom((Class) rawType);
                }
                else if (type instanceof TypeVariable)
                {
                    Type[] bounds = ((TypeVariable) type).getBounds();
                    return Arrays.stream(bounds).anyMatch(boundType -> hasParameterOfType(boundType, parameterType));
                }
                return false;
            });
        }
        return false;
    }

    protected ListenableFuture<Void> deleteNoChecks()
    {
        final String simpleClassName = AbstractConfiguredObject.this.getClass().getSimpleName();
        final SettableFuture<Void> returnFuture = SettableFuture.create();
        final State currentDesiredState = getDesiredState();

        final ChainedListenableFuture<Void> future =
                doAfter(beforeDelete(), this::deleteChildren).then(this::onDelete)
                                                             .then(() -> {
                                                                 final State currentState = getState();
                                                                 setState(State.DELETED);
                                                                 notifyStateChanged(currentState, State.DELETED);
                                                                 changeAttribute(ConfiguredObject.DESIRED_STATE, State.DELETED);
                                                                 attributeSet(ConfiguredObject.DESIRED_STATE, currentDesiredState, State.DELETED);
                                                                 unregister(true);

                                                                 LOGGER.debug("Delete {} : {}",
                                                                              simpleClassName,
                                                                              getName());
                                                                 return Futures.immediateFuture(null);
                                                                       });
        addFutureCallback(future, new FutureCallback<Void>()
        {
            @Override
            public void onSuccess(final Void result)
            {
                returnFuture.set(null);
            }

            @Override
            public void onFailure(final Throwable t)
            {
                returnFuture.setException(t);
            }
        }, MoreExecutors.directExecutor());

        return returnFuture;
    }

    protected final ListenableFuture<Void> deleteChildren()
    {
        // If this object manages its own child-storage then don't propagate the delete.  The rationale
        // is that deleting the object will delete the storage that contains the children.  Telling each
        // child and their children to delete themselves would generate unnecessary 'delete' work in the
        // child-storage (which also might fail).
        if (managesChildStorage())
        {
            return Futures.immediateFuture(null);
        }

        final List<ListenableFuture<Void>> childDeleteFutures = new ArrayList<>();

        applyToChildren(child -> {

            final ListenableFuture<Void> childDeleteFuture;
            if (child instanceof AbstractConfiguredObject<?>)
            {
                 childDeleteFuture = ((AbstractConfiguredObject<?>) child).deleteNoChecks();
            }
            else if (child instanceof AbstractConfiguredObjectProxy)
            {
                childDeleteFuture = ((AbstractConfiguredObjectProxy) child).deleteNoChecks();
            }
            else
            {
                childDeleteFuture = Futures.immediateFuture(null);
            }

            addFutureCallback(childDeleteFuture, new FutureCallback<Void>()
            {
                @Override
                public void onSuccess(final Void result)
                {
                }

                @Override
                public void onFailure(final Throwable t)
                {
                    LOGGER.error("Exception occurred while deleting {} : {}",
                                 child.getClass().getSimpleName(), child.getName(), t);
                }
            }, getTaskExecutor());
            childDeleteFutures.add(childDeleteFuture);
        });

        ListenableFuture<List<Void>> combinedFuture = Futures.allAsList(childDeleteFutures);

        return Futures.transform(combinedFuture, input -> null, getTaskExecutor());
    }

    protected ListenableFuture<Void> beforeDelete()
    {
        return Futures.immediateFuture(null);
    }

    protected ListenableFuture<Void> onDelete()
    {
        return Futures.immediateFuture(null);
    }

    public final void start()
    {
        doSync(startAsync());
    }

    public ListenableFuture<Void> startAsync()
    {
        return setDesiredState(State.ACTIVE);
    }

    private void deleted()
    {
        unregister(true);
    }

    private void unregister(boolean removed)
    {
            if (_parent instanceof AbstractConfiguredObject<?>)
            {
                AbstractConfiguredObject<?> parentObj = (AbstractConfiguredObject<?>) _parent;
                parentObj.unregisterChild(this);
                if(removed)
                {
                    parentObj.childRemoved(this);
                }
            }
            else if (_parent instanceof AbstractConfiguredObjectProxy)
            {
                AbstractConfiguredObjectProxy parentObj = (AbstractConfiguredObjectProxy) _parent;
                parentObj.unregisterChild(this);
                if(removed)
                {
                    parentObj.childRemoved(this);
                }
            }

    }


    private <C extends ConfiguredObject> void unregisterChild(final C child)
    {
        Class categoryClass = child.getCategoryClass();
        synchronized(_children)
        {
            _children.get(categoryClass).remove(child);
            _childrenById.get(categoryClass).remove(child.getId(), child);
            _childrenByName.get(categoryClass).remove(child.getName(), child);
        }
    }

    @Override
    public final <C extends ConfiguredObject> C getChildById(final Class<C> clazz, final UUID id)
    {
        return (C) _childrenById.get(ConfiguredObjectTypeRegistry.getCategory(clazz)).get(id);
    }

    @Override
    public final <C extends ConfiguredObject> C getChildByName(final Class<C> clazz, final String name)
    {
        Class<? extends ConfiguredObject> categoryClass = ConfiguredObjectTypeRegistry.getCategory(clazz);
        return (C) _childrenByName.get(categoryClass).get(name);
    }

    @Override
    public <C extends ConfiguredObject> Collection<C> getChildren(final Class<C> clazz)
    {
        Collection<ConfiguredObject<?>> children = _children.get(clazz);
        if (children == null)
        {
            return Collections.EMPTY_LIST;
        }
        return Collections.unmodifiableList((List<? extends C>) children);
    }

    @Override
    public <C extends ConfiguredObject> ListenableFuture<C> getAttainedChildByName(final Class<C> childClass,
                                                                                   final String name)
    {
        C child = getChildByName(childClass, name);
        if (child instanceof AbstractConfiguredObject)
        {
            return ((AbstractConfiguredObject)child).getAttainStateFuture();
        }
        else if(child instanceof AbstractConfiguredObjectProxy)
        {
            return ((AbstractConfiguredObjectProxy)child).getAttainStateFuture();
        }
        else
        {
            return Futures.immediateFuture(child);
        }
    }

    @Override
    public <C extends ConfiguredObject> ListenableFuture<C> getAttainedChildById(final Class<C> childClass,
                                                                                   final UUID id)
    {
        C child = getChildById(childClass, id);
        if (child instanceof AbstractConfiguredObject)
        {
            return ((AbstractConfiguredObject)child).getAttainStateFuture();
        }
        else if(child instanceof AbstractConfiguredObjectProxy)
        {
            return ((AbstractConfiguredObjectProxy)child).getAttainStateFuture();
        }
        else
        {
            return Futures.immediateFuture(child);
        }
    }

    private <C extends ConfiguredObject> ListenableFuture<C> getAttainStateFuture()
    {
        return (ListenableFuture<C>) _attainStateFuture;
    }

    @Override
    public final TaskExecutor getTaskExecutor()
    {
        return _taskExecutor;
    }

    @Override
    public TaskExecutor getChildExecutor()
    {
        return getTaskExecutor();
    }

    protected final <T, E extends Exception> T runTask(Task<T,E> task) throws E
    {
        return _taskExecutor.run(task);
    }

    @Override
    public void setAttributes(Map<String, Object> attributes) throws IllegalStateException, AccessControlException, IllegalArgumentException
    {
        doSync(setAttributesAsync(attributes));
    }

    protected void postSetAttributes(final Set<String> actualUpdatedAttributes)
    {
    }

    protected final ChainedListenableFuture<Void> doAfter(ListenableFuture<?> first, final Runnable second)
    {
        return doAfter(getTaskExecutor(), first, second);
    }

    protected static <V> ChainedListenableFuture<Void>  doAfter(Executor executor, ListenableFuture<V> first, final Runnable second)
    {
        final ChainedSettableFuture<Void> returnVal = new ChainedSettableFuture<Void>(executor);
        addFutureCallback(first, new FutureCallback<V>()
        {
            @Override
            public void onSuccess(final V result)
            {
                try
                {
                    second.run();
                    returnVal.set(null);
                }
                catch(Throwable e)
                {
                    returnVal.setException(e);
                }
            }

            @Override
            public void onFailure(final Throwable t)
            {
                returnVal.setException(t);
            }
        }, executor);

        return returnVal;
    }

    public interface CallableWithArgument<V,A>
    {
        V call(A argument) throws Exception;
    }

    public static interface ChainedListenableFuture<V> extends ListenableFuture<V>
    {
        ChainedListenableFuture<Void> then(Runnable r);
        ChainedListenableFuture<V> then(Callable<ListenableFuture<V>> r);
        <A> ChainedListenableFuture<A> then(CallableWithArgument<ListenableFuture<A>,V> r);
    }

    public static class ChainedSettableFuture<V> extends AbstractFuture<V> implements ChainedListenableFuture<V>
    {
        private final Executor _exector;

        public ChainedSettableFuture(final Executor executor)
        {
            _exector = executor;
        }

        @Override
        public boolean set(V value)
        {
            return super.set(value);
        }

        @Override
        public boolean setException(Throwable throwable)
        {
            return super.setException(throwable);
        }

        @Override
        public ChainedListenableFuture<Void> then(final Runnable r)
        {
            return doAfter(_exector, this, r);
        }

        @Override
        public ChainedListenableFuture<V> then(final Callable<ListenableFuture<V>> r)
        {
            return doAfter(_exector, this,r);
        }

        @Override
        public <A> ChainedListenableFuture<A> then(final CallableWithArgument<ListenableFuture<A>,V> r)
        {
            return doAfter(_exector, this, r);
        }
    }

    protected final <V> ChainedListenableFuture<V> doAfter(ListenableFuture<V> first, final Callable<ListenableFuture<V>> second)
    {
        return doAfter(getTaskExecutor(), first, second);
    }

    protected final <V,A> ChainedListenableFuture<V> doAfter(ListenableFuture<A> first, final CallableWithArgument<ListenableFuture<V>,A> second)
    {
        return doAfter(getTaskExecutor(), first, second);
    }


    protected static <V> ChainedListenableFuture<V> doAfter(final Executor executor, ListenableFuture<V> first, final Callable<ListenableFuture<V>> second)
    {
        final ChainedSettableFuture<V> returnVal = new ChainedSettableFuture<V>(executor);
        addFutureCallback(first, new FutureCallback<V>()
        {
            @Override
            public void onSuccess(final V result)
            {
                try
                {
                    final ListenableFuture<V> future = second.call();
                    addFutureCallback(future, new FutureCallback<V>()
                    {
                        @Override
                        public void onSuccess(final V result)
                        {
                            returnVal.set(result);
                        }

                        @Override
                        public void onFailure(final Throwable t)
                        {
                            returnVal.setException(t);
                        }
                    }, executor);

                }
                catch(Throwable e)
                {
                    returnVal.setException(e);
                }
            }

            @Override
            public void onFailure(final Throwable t)
            {
                returnVal.setException(t);
            }
        }, executor);

        return returnVal;
    }


    protected static <V,A> ChainedListenableFuture<V> doAfter(final Executor executor, ListenableFuture<A> first, final CallableWithArgument<ListenableFuture<V>,A> second)
    {
        final ChainedSettableFuture<V> returnVal = new ChainedSettableFuture<>(executor);
        addFutureCallback(first, new FutureCallback<A>()
        {
            @Override
            public void onSuccess(final A result)
            {
                try
                {
                    final ListenableFuture<V> future = second.call(result);
                    addFutureCallback(future, new FutureCallback<V>()
                    {
                        @Override
                        public void onSuccess(final V result)
                        {
                            returnVal.set(result);
                        }

                        @Override
                        public void onFailure(final Throwable t)
                        {
                            returnVal.setException(t);
                        }
                    }, executor);

                }
                catch (Throwable e)
                {
                    returnVal.setException(e);
                }
            }

            @Override
            public void onFailure(final Throwable t)
            {
                returnVal.setException(t);
            }
        }, executor);

        return returnVal;
    }
    protected <V> ChainedListenableFuture<Void> doAfterAlways(ListenableFuture<V> future,
                                                              Runnable after)
    {
        return doAfterAlways(getTaskExecutor(), future, after);
    }

    protected static <V> ChainedListenableFuture<Void> doAfterAlways(Executor executor,
                                                                     ListenableFuture<V> future,
                                                                     final Runnable after)
    {
        final ChainedSettableFuture<Void> returnVal = new ChainedSettableFuture<Void>(executor);
        addFutureCallback(future, new FutureCallback<V>()
        {
            @Override
            public void onSuccess(final V result)
            {
                try
                {
                    after.run();
                    returnVal.set(null);
                }
                catch (Throwable e)
                {
                    returnVal.setException(e);
                }
            }

            @Override
            public void onFailure(final Throwable t)
            {
                try
                {
                    after.run();
                }
                finally
                {
                    returnVal.setException(t);
                }
            }
        }, executor);

        return returnVal;
    }

    public static <V> void addFutureCallback(ListenableFuture<V> future, final FutureCallback<V> callback,
                                             Executor taskExecutor)
    {
        final Subject subject = Subject.getSubject(AccessController.getContext());

        Futures.addCallback(future, new FutureCallback<V>()
        {
            @Override
            public void onSuccess(final V result)
            {
                Subject.doAs(subject, new PrivilegedAction<Void>()
                {
                    @Override
                    public Void run()
                    {
                        callback.onSuccess(result);
                        return null;
                    }
                });
            }

            @Override
            public void onFailure(final Throwable t)
            {
                Subject.doAs(subject, new PrivilegedAction<Void>()
                {
                    @Override
                    public Void run()
                    {
                        callback.onFailure(t);
                        return null;
                    }
                });
            }
        }, taskExecutor);
    }

    @Override
    public ListenableFuture<Void> setAttributesAsync(final Map<String, Object> attributes) throws IllegalStateException, AccessControlException, IllegalArgumentException
    {
        final Map<String,Object> updateAttributes = new HashMap<>(attributes);
        Object desiredState = updateAttributes.remove(ConfiguredObject.DESIRED_STATE);
        runTask(new Task<Void, RuntimeException>()
        {
            @Override
            public Void execute()
            {
                authoriseSetAttributes(createProxyForValidation(attributes), attributes);
                if (!isSystemProcess())
                {
                    validateChange(createProxyForValidation(attributes), attributes.keySet());
                }

                changeAttributes(updateAttributes);
                return null;
            }

            @Override
            public String getObject()
            {
                return AbstractConfiguredObject.this.toString();
            }

            @Override
            public String getAction()
            {
                return "set attributes";
            }

            @Override
            public String getArguments()
            {
                return "attributes number=" + attributes.size();
            }
        });
        if(desiredState != null)
        {
            State state;
            if(desiredState instanceof State)
            {
                state = (State)desiredState;
            }
            else if(desiredState instanceof String)
            {
                state = State.valueOf((String)desiredState);
            }
            else
            {
                throw new IllegalArgumentException("Cannot convert an object of type " + desiredState.getClass().getName() + " to a State");
            }
            return setDesiredState(state);
        }
        else
        {
            return Futures.immediateFuture(null);
        }
    }

    public void forceUpdateAllSecureAttributes()
    {
        applyToChildren(new Action<ConfiguredObject<?>>()
        {
            @Override
            public void performAction(final ConfiguredObject<?> object)
            {
                if (object instanceof AbstractConfiguredObject)
                {
                    ((AbstractConfiguredObject) object).forceUpdateAllSecureAttributes();
                }
                else if(object instanceof AbstractConfiguredObjectProxy)
                {
                    ((AbstractConfiguredObjectProxy) object).forceUpdateAllSecureAttributes();
                }
            }
        });
        doUpdateSecureAttributes();
    }

    private void doUpdateSecureAttributes()
    {
        Map<String,Object> secureAttributeValues = getSecureAttributeValues();
        if(!secureAttributeValues.isEmpty())
        {
            bulkChangeStart();
            for (Map.Entry<String, Object> attribute : secureAttributeValues.entrySet())
            {
                synchronized (_changeListeners)
                {
                    List<ConfigurationChangeListener> copy =
                            new ArrayList<>(_changeListeners);
                    for (ConfigurationChangeListener listener : copy)
                    {
                        listener.attributeSet(this, attribute.getKey(), attribute.getValue(), attribute.getValue());
                    }
                }

            }
            bulkChangeEnd();
        }
    }

    private Map<String,Object> getSecureAttributeValues()
    {
        Map<String,Object> secureAttributeValues = new HashMap<>();
        for (Map.Entry<String, ConfiguredObjectAttribute<?, ?>> attribute : _attributeTypes.entrySet())
        {
            if (attribute.getValue().isSecure() && _attributes.containsKey(attribute.getKey()))
            {
                secureAttributeValues.put(attribute.getKey(), _attributes.get(attribute.getKey()));
            }
        }
        return secureAttributeValues;
    }


    private void bulkChangeStart()
    {
        synchronized (_changeListeners)
        {
            List<ConfigurationChangeListener> copy = new ArrayList<ConfigurationChangeListener>(_changeListeners);
            for(ConfigurationChangeListener listener : copy)
            {
                listener.bulkChangeStart(this);
            }
        }
    }

    private void bulkChangeEnd()
    {
        synchronized (_changeListeners)
        {
            List<ConfigurationChangeListener> copy = new ArrayList<ConfigurationChangeListener>(_changeListeners);
            for(ConfigurationChangeListener listener : copy)
            {
                listener.bulkChangeEnd(this);
            }
        }
    }

    protected void changeAttributes(final Map<String, Object> attributes)
    {
        Collection<String> names = getAttributeNames();
        Set<String> updatedAttributes = new HashSet<>(attributes.size());
        try
        {
            bulkChangeStart();
            for (Map.Entry<String, Object> entry : attributes.entrySet())
            {
                String attributeName = entry.getKey();
                if (names.contains(attributeName))
                {
                    Object desired = entry.getValue();
                    Object expected = getAttribute(attributeName);
                    if (changeAttribute(attributeName, desired))
                    {
                        attributeSet(attributeName, expected, desired);
                        updatedAttributes.add(attributeName);
                    }
                }
            }
        }
        finally
        {
            try
            {
                postSetAttributes(updatedAttributes);
            }
            finally
            {
                bulkChangeEnd();
            }
        }
    }

    protected void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        for(ConfiguredObjectAttribute<?,?> attr : _attributeTypes.values())
        {
            if (!attr.isDerived() && changedAttributes.contains(attr.getName()))
            {
                ConfiguredSettableAttribute autoAttr = (ConfiguredSettableAttribute) attr;

                if (autoAttr.isImmutable() && !Objects.equals(autoAttr.getValue(this), autoAttr.getValue(proxyForValidation)))
                {
                    throw new IllegalConfigurationException("Attribute '" + autoAttr.getName() + "' cannot be changed.");
                }

                if (autoAttr.hasValidValues())
                {
                    Object desiredValue = autoAttr.getValue(proxyForValidation);
                    if ((autoAttr.isMandatory() || desiredValue != null)
                        && !checkValidValues(autoAttr, desiredValue))
                    {
                        throw new IllegalConfigurationException("Attribute '" + autoAttr.getName()
                                                                + "' instance of "+ getClass().getName()
                                                                + " named '" + getName() + "'"
                                                                + " cannot have value '" + desiredValue + "'"
                                                                + ". Valid values are: "
                                                                + autoAttr.validValues());
                    }
                }
                else if(!"".equals(autoAttr.validValuePattern()))
                {
                    Object desiredValueOrDefault = autoAttr.getValue(proxyForValidation);

                    if (desiredValueOrDefault != null && !checkValidValuePattern(autoAttr, desiredValueOrDefault))
                    {
                        throw new IllegalConfigurationException("Attribute '" + autoAttr.getName()
                                                                + "' instance of "+ getClass().getName()
                                                                + " named '" + getName() + "'"
                                                                + " cannot have value '" + desiredValueOrDefault + "'"
                                                                + ". Valid values pattern is: "
                                                                + autoAttr.validValuePattern());
                    }
                }


                if(autoAttr.isMandatory() && autoAttr.getValue(proxyForValidation) == null)
                {
                    throw new IllegalConfigurationException("Attribute '" + autoAttr.getName()
                                                            + "' instance of "+ getClass().getName()
                                                            + " named '" + getName() + "'"
                                                            + " cannot be null, as it is mandatory");
                }

            }

        }

    }

    private ConfiguredObject<?> createProxyForValidation(final Map<String, Object> attributes)
    {
        return (ConfiguredObject<?>) Proxy.newProxyInstance(getClass().getClassLoader(),
                                                            new Class<?>[]{_bestFitInterface},
                                                            new AttributeGettingHandler(attributes, _attributeTypes, this));
    }

    private ConfiguredObject<?> createProxyForInitialization(final Map<String, Object> attributes)
    {
        return (ConfiguredObject<?>) Proxy.newProxyInstance(getClass().getClassLoader(),
                                                            new Class<?>[]{_bestFitInterface},
                                                            new AttributeInitializationInvocationHandler(attributes, _attributeTypes, this));
    }

    private ConfiguredObject<?> createProxyForAuthorisation(final Class<? extends ConfiguredObject> category,
                                                            final Map<String, Object> attributes,
                                                            final ConfiguredObject<?> parent)
    {
        return (ConfiguredObject<?>) Proxy.newProxyInstance(getClass().getClassLoader(),
                                                            new Class<?>[]{category},
                                                            new AuthorisationProxyInvocationHandler(attributes,
                                                                                                    getTypeRegistry().getAttributeTypes(category),
                                                                                                    category, parent));
    }

    protected final <C extends ConfiguredObject<?>> void authoriseCreateChild(Class<C> childClass, Map<String, Object> attributes) throws AccessControlException
    {
        ConfiguredObject<?> configuredObject = createProxyForAuthorisation(childClass, attributes, this);
        authorise(configuredObject, null, Operation.CREATE, Collections.emptyMap());
    }

    @Override
    public final void authorise(Operation operation) throws AccessControlException
    {
        authorise(this, null, operation, Collections.emptyMap());
    }

    @Override
    public final void authorise(Operation operation, Map<String, Object> arguments) throws AccessControlException
    {
        authorise(this, null, operation, arguments);
    }

    @Override
    public final void authorise(SecurityToken token, Operation operation, Map<String, Object> arguments) throws AccessControlException
    {
        authorise(this, token, operation, arguments);
    }

    @Override
    public final SecurityToken newToken(final Subject subject)
    {
        AccessControl accessControl = getAccessControl();
        return accessControl == null ? null : accessControl.newToken(subject);
    }

    private void authorise(final ConfiguredObject<?> configuredObject,
                           SecurityToken token,
                           final Operation operation,
                           Map<String, Object> arguments)
    {

        AccessControl accessControl = getAccessControl();
        if(accessControl != null)
        {
            Result result = accessControl.authorise(token, operation, configuredObject, arguments);
            LOGGER.debug("authorise returned {}", result);
            if (result == Result.DEFER)
            {
                result = accessControl.getDefault();
                LOGGER.debug("authorise returned DEFER, returing default: {}", result);
            }

            if (result == Result.DENIED)
            {
                Class<? extends ConfiguredObject> categoryClass = configuredObject.getCategoryClass();
                String objectName = (String) configuredObject.getAttribute(ConfiguredObject.NAME);
                String operationName = operation.getName().equals(operation.getType().name())
                        ? operation.getName()
                        : (operation.getType().name() + "(" + operation.getName() + ")");
                StringBuilder exceptionMessage =
                        new StringBuilder(String.format("Permission %s is denied for : %s '%s'",
                                                        operationName, categoryClass.getSimpleName(), objectName));
                Model model = configuredObject.getModel();

                Class<? extends ConfiguredObject> parentClass = model.getParentType(categoryClass);
                if (parentClass != null)
                {
                    exceptionMessage.append(" on");
                    String objectCategory = parentClass.getSimpleName();
                    ConfiguredObject<?> parent = configuredObject.getParent();
                    exceptionMessage.append(" ").append(objectCategory);
                    if (parent != null)
                    {
                        exceptionMessage.append(" '")
                                .append(parent.getAttribute(ConfiguredObject.NAME))
                                .append("'");
                    }

                }
                throw new AccessControlException(exceptionMessage.toString());
            }
        }
    }


    private final void authoriseSetAttributes(final ConfiguredObject<?> proxyForValidation,
                                                               final Map<String, Object> modifiedAttributes)
    {
        if (modifiedAttributes.containsKey(DESIRED_STATE) && State.DELETED.equals(proxyForValidation.getDesiredState()))
        {
            authorise(Operation.DELETE);
            if (modifiedAttributes.size() == 1)
            {
                // nothing left to authorize
                return;
            }
        }

        authorise(this, null, Operation.UPDATE, modifiedAttributes);
    }

    protected Principal getSystemPrincipal()
    {
        return _systemPrincipal;
    }

    protected final Subject getSubjectWithAddedSystemRights()
    {
        Subject subject = Subject.getSubject(AccessController.getContext());
        if(subject == null)
        {
            subject = new Subject();
        }
        else
        {
            subject = new Subject(false, subject.getPrincipals(), subject.getPublicCredentials(), subject.getPrivateCredentials());
        }
        subject.getPrincipals().add(getSystemPrincipal());
        subject.setReadOnly();
        return subject;
    }

    protected final AccessControlContext getSystemTaskControllerContext(String taskName, Principal principal)
    {
        final Subject subject = getSystemTaskSubject(taskName, principal);
        return AccessController.doPrivileged
                (new PrivilegedAction<AccessControlContext>()
                {
                    @Override
                    public AccessControlContext run()
                    {
                        return new AccessControlContext
                                (AccessController.getContext(),
                                 new SubjectDomainCombiner(subject));
                    }
                }, null);
    }

    protected Subject getSystemTaskSubject(String taskName)
    {
        return getSystemSubject(new TaskPrincipal(taskName));
    }

    protected final Subject getSystemTaskSubject(String taskName, Principal principal)
    {
        return getSystemSubject(new TaskPrincipal(taskName), principal);
    }

    protected final boolean isSystemProcess()
    {
        Subject subject = Subject.getSubject(AccessController.getContext());
        return isSystemSubject(subject);
    }

    protected boolean isSystemSubject(final Subject subject)
    {
        return subject != null  && subject.getPrincipals().contains(getSystemPrincipal());
    }

    private Subject getSystemSubject(Principal... principals)
    {
        Set<Principal> principalSet = new HashSet<>(Arrays.asList(principals));
        principalSet.add(getSystemPrincipal());
        return new Subject(true,
                           principalSet,
                           Collections.emptySet(),
                           Collections.emptySet());
    }

    private int getAwaitAttainmentTimeout()
    {
        if (_awaitAttainmentTimeout == 0)
        {
            try
            {
                _awaitAttainmentTimeout = getContextValue(Integer.class, AWAIT_ATTAINMENT_TIMEOUT);
            }
            catch (IllegalArgumentException e)
            {
                _awaitAttainmentTimeout = DEFAULT_AWAIT_ATTAINMENT_TIMEOUT;
            }
        }
        return _awaitAttainmentTimeout;
    }

    protected final <C extends ConfiguredObject> C awaitChildClassToAttainState(final Class<C> childClass, final String name)
    {
        ListenableFuture<C> attainedChildByName = getAttainedChildByName(childClass, name);
        try
        {
            return (C) doSync(attainedChildByName, getAwaitAttainmentTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e)
        {
            LOGGER.warn("Gave up waiting for {} '{}' to attain state. Check object's state via Management.", childClass.getSimpleName(), name);
            return null;
        }
    }

    protected final <C extends ConfiguredObject> C awaitChildClassToAttainState(final Class<C> childClass, final UUID id)
    {
        ListenableFuture<C> attainedChildByName = getAttainedChildById(childClass, id);
        try
        {
            return (C) doSync(attainedChildByName, getAwaitAttainmentTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e)
        {
            LOGGER.warn("Gave up waiting for {} with ID {} to attain state. Check object's state via Management.", childClass.getSimpleName(), id);
            return null;
        }
    }

    protected AccessControl getAccessControl()
    {
        return _parentAccessControl;
    }

    @Override
    public final String getLastUpdatedBy()
    {
        return _lastUpdatedBy;
    }

    @Override
    public final Date getLastUpdatedTime()
    {
        return _lastUpdatedTime;
    }

    @Override
    public final String getCreatedBy()
    {
        return _createdBy;
    }

    @Override
    public final Date getCreatedTime()
    {
        return _createdTime;
    }

    @Override
    public final String getType()
    {
        return _type;
    }


    @Override
    public Map<String, Object> getStatistics()
    {
        return getStatistics(Collections.<String>emptyList());
    }

    @Override
    public Map<String, Object> getStatistics(List<String> statistics)
    {
        Collection<ConfiguredObjectStatistic<?, ?>> stats = getTypeRegistry().getStatistics(getClass());
        Map<String,Object> map = new HashMap<>();
        boolean allStats = statistics == null || statistics.isEmpty();
        for(ConfiguredObjectStatistic stat : stats)
        {
            if(allStats || statistics.contains(stat.getName()))
            {
                Object value = stat.getValue(this);
                if(value != null)
                {
                    map.put(stat.getName(), value);
                }
            }
        }
        return map;
    }

    @Override
    public String setContextVariable(final String name, final String value)
    {
        Map<String, String> context = new LinkedHashMap<>(getContext());
        String previousValue = context.put(name, value);
        setAttributes(Collections.singletonMap(CONTEXT, context));
        return previousValue;
    }

    @Override
    public String removeContextVariable(final String name)
    {
        Map<String, String> context = new LinkedHashMap<>(getContext());
        String previousValue = context.remove(name);
        setAttributes(Collections.singletonMap(CONTEXT, context));
        return previousValue;
    }

    @Override
    public <Y extends ConfiguredObject<Y>> Y findConfiguredObject(Class<Y> clazz, String name)
    {
        Collection<Y> reachable = getModel().getReachableObjects(this, clazz);
        for(Y candidate : reachable)
        {
            if(candidate.getName().equals(name))
            {
                return candidate;
            }
        }
        return null;
    }

    /**
     * Retrieve and interpolate a context variable of the given name and convert it to the given type.
     *
     * Note that this SHOULD not be called before the model has been resolved (e.g., not in the constructor).
     * @param <T> the type the interpolated context variable should be converted to
     * @param clazz the class object of the type the interpolated context variable should be converted to
     * @param propertyName the name of the context variable to retrieve
     * @return the interpolated context variable converted to an object of the given type
     * @throws IllegalArgumentException if the interpolated context variable cannot be converted to the given type
     */
    @Override
    public final <T> T getContextValue(Class<T> clazz, String propertyName)
    {
        return getContextValue(clazz, clazz, propertyName);
    }

    @Override
    public <T> T getContextValue(final Class<T> clazz, final Type type, final String propertyName)
    {
        AttributeValueConverter<T> converter = AttributeValueConverter.getConverter(clazz, type);
        return converter.convert("${" + propertyName + "}", this);
    }

    @Override
    public Set<String> getContextKeys(final boolean excludeSystem)
    {
        Map<String,String> inheritedContext = new HashMap<>(getTypeRegistry().getDefaultContext());
        if(!excludeSystem)
        {
            inheritedContext.putAll(System.getenv());
            //clone is synchronized and will avoid ConcurrentModificationException
            inheritedContext.putAll((Map) System.getProperties().clone());
        }
        generateInheritedContext(getModel(), this, inheritedContext);
        return Collections.unmodifiableSet(inheritedContext.keySet());
    }

    private ConfiguredObjectTypeRegistry getTypeRegistry()
    {
        return getModel().getTypeRegistry();
    }

    private OwnAttributeResolver getOwnAttributeResolver()
    {
        return _ownAttributeResolver;
    }

    private AncestorAttributeResolver getAncestorAttributeResolver()
    {
        return _ancestorAttributeResolver;
    }

    @Override
    public boolean hasEncrypter()
    {
        return _encrypter != null;
    }

    @Override
    public void decryptSecrets()
    {
        if(_encrypter != null)
        {
            for (Map.Entry<String, Object> entry : _attributes.entrySet())
            {
                ConfiguredObjectAttribute<X, ?> attr =
                        (ConfiguredObjectAttribute<X, ?>) _attributeTypes.get(entry.getKey());
                if (attr != null
                    && attr.isSecure()
                    && entry.getValue() instanceof String)
                {
                    String decrypt = _encrypter.decrypt((String) entry.getValue());
                    entry.setValue(decrypt);
                }

            }
        }
    }

    @Override
    public final Date getLastOpenedTime()
    {
        return _lastOpenedTime;
    }

    @Override
    public UserPreferences getUserPreferences()
    {
        return _userPreferences;
    }

    @Override
    public void setUserPreferences(final UserPreferences userPreferences)
    {
        _userPreferences = userPreferences;
    }

    private EventLogger getEventLogger()
    {
        if(_parent instanceof EventLoggerProvider)
        {
            return ((EventLoggerProvider)_parent).getEventLogger();
        }
        else if(_parent instanceof AbstractConfiguredObject)
        {
            final EventLogger eventLogger = ((AbstractConfiguredObject<?>) _parent).getEventLogger();
            if(eventLogger != null)
            {
                return eventLogger;
            }
        }
        return null;
    }

    protected void logOperation(String operation)
    {
        EventLogger eventLogger = getEventLogger();
        if(eventLogger != null)
        {
            eventLogger.message(new OperationLogMessage(this, operation));
        }
        else
        {
            LOGGER.info(getCategoryClass().getSimpleName()
                        + "("
                        + getName()
                        + ") : Operation "
                        + operation
                        + " invoked by user "
                        + AuthenticatedPrincipal.getCurrentUser().getName());
        }
    }

    //=========================================================================================

    static String interpolate(ConfiguredObject<?> object, String value)
    {
        if(object == null)
        {
            return value;
        }
        else
        {
            Map<String, String> inheritedContext = new HashMap<String, String>();
            generateInheritedContext(object.getModel(), object, inheritedContext);
            return Strings.expand(value, false,
                                  JSON_SUBSTITUTION_RESOLVER,
                                  getOwnAttributeResolver(object),
                                  getAncestorAttributeResolver(object),
                                  new Strings.MapResolver(inheritedContext),
                                  Strings.JAVA_SYS_PROPS_RESOLVER,
                                  Strings.ENV_VARS_RESOLVER,
                                  object.getModel().getTypeRegistry().getDefaultContextResolver());
        }
    }

    static String interpolate(Model model, String value)
    {
            return Strings.expand(value, false,
                                  JSON_SUBSTITUTION_RESOLVER,
                                  Strings.JAVA_SYS_PROPS_RESOLVER,
                                  Strings.ENV_VARS_RESOLVER,
                                  model.getTypeRegistry().getDefaultContextResolver());
    }


    private static OwnAttributeResolver getOwnAttributeResolver(final ConfiguredObject<?> object)
    {
        return object instanceof AbstractConfiguredObject
                ? ((AbstractConfiguredObject)object).getOwnAttributeResolver()
                : new OwnAttributeResolver(object);
    }

    private static AncestorAttributeResolver getAncestorAttributeResolver(final ConfiguredObject<?> object)
    {
        return object instanceof AbstractConfiguredObject
                ? ((AbstractConfiguredObject)object).getAncestorAttributeResolver()
                : new AncestorAttributeResolver(object);
    }



    static void generateInheritedContext(final Model model, final ConfiguredObject<?> object,
                                         final Map<String, String> inheritedContext)
    {
        Class<? extends ConfiguredObject> parentClass =
                model.getParentType(object.getCategoryClass());
        if(parentClass != null)
        {
            ConfiguredObject parent = object.getParent();
            if(parent != null)
            {
                generateInheritedContext(model, parent, inheritedContext);
            }
        }
        if(object.getContext() != null)
        {
            inheritedContext.putAll(object.getContext());
        }
    }


    private static final Strings.Resolver JSON_SUBSTITUTION_RESOLVER =
            Strings.createSubstitutionResolver("json:",
                                               new LinkedHashMap<String, String>()
                                               {
                                                   {
                                                       put("\\","\\\\");
                                                       put("\"","\\\"");
                                                   }
                                               });


    private static class AttributeGettingHandler implements InvocationHandler
    {
        private final Map<String,Object> _attributes;
        private final Map<String, ConfiguredObjectAttribute<?,?>> _attributeTypes;
        private final ConfiguredObject<?> _configuredObject;

        AttributeGettingHandler(final Map<String, Object> modifiedAttributes, Map<String, ConfiguredObjectAttribute<?,?>> attributeTypes, ConfiguredObject<?> configuredObject)
        {
            Map<String,Object> combinedAttributes = new HashMap<>();
            if (configuredObject != null)
            {
                combinedAttributes.putAll(configuredObject.getActualAttributes());
            }
            combinedAttributes.putAll(modifiedAttributes);
            _attributes = combinedAttributes;
            _attributeTypes = attributeTypes;
            _configuredObject = configuredObject;
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable
        {

            ConfiguredObjectAttribute attribute = getAttributeFromMethod(method);

            if(attribute != null && attribute.isAutomated())
            {
                return getValue(attribute);
            }
            else if(method.getName().equals("getAttribute") && args != null && args.length == 1 && args[0] instanceof String)
            {
                attribute = _attributeTypes.get((String)args[0]);
                if(attribute != null)
                {
                    return getValue(attribute);
                }
                else
                {
                    return null;
                }
            }
            else if(method.getName().equals("getActualAttributes") && (args == null || args.length == 0))
            {
                return Collections.unmodifiableMap(_attributes);
            }
            else if(method.getName().equals("toString") && (args == null || args.length == 0))
            {
                return "ValidationProxy{" + getCategoryClass().getSimpleName() + "/" + getType() + "}";
            }
            else if(method.getName().equals("getModel") && (args == null || args.length == 0))
            {
                return _configuredObject.getModel();
            }
            else
            {
                throw new UnsupportedOperationException(
                        "This class is only intended for value validation, and only getters on managed attributes are permitted.");
            }
        }

        protected Object getValue(final ConfiguredObjectAttribute attribute)
        {
            Object value;
            if(!attribute.isDerived())
            {
                ConfiguredSettableAttribute settableAttr = (ConfiguredSettableAttribute) attribute;
                value = _attributes.get(attribute.getName());
                if (value == null && !"".equals(settableAttr.defaultValue()))
                {
                    value = settableAttr.defaultValue();
                }
                return convert(settableAttr, value);
            }
            else
            {
                if(_attributes.containsKey(attribute.getName()))
                {
                    return _attributes.get(attribute.getName());
                }
                else if(_configuredObject != null)
                {
                    return _configuredObject.getAttribute(attribute.getName());
                }
                else
                {
                    return null;
                }
            }
        }

        protected Object convert(ConfiguredSettableAttribute attribute, Object value)
        {
            return attribute.convert(value, _configuredObject);
        }

        private ConfiguredObjectAttribute getAttributeFromMethod(final Method method)
        {
            if(!Modifier.isStatic(method.getModifiers()) && method.getParameterTypes().length==0)
            {
                for(ConfiguredObjectAttribute attribute : _attributeTypes.values())
                {
                    if((attribute instanceof ConfiguredObjectMethodAttribute) && ((ConfiguredObjectMethodAttribute)attribute).getGetter().getName().equals(method.getName()))
                    {
                        return attribute;
                    }
                }
            }
            return null;
        }

        protected String getType()
        {
            return _configuredObject.getType();
        }

        protected Class<? extends ConfiguredObject> getCategoryClass()
        {
            return _configuredObject.getCategoryClass();
        }

        ConfiguredObject<?> getConfiguredObject()
        {
            return _configuredObject;
        }
    }

    private static class AttributeInitializationInvocationHandler extends AttributeGettingHandler
    {

        AttributeInitializationInvocationHandler(final Map<String, Object> modifiedAttributes,
                                                 final Map<String, ConfiguredObjectAttribute<?, ?>> attributeTypes,
                                                 final ConfiguredObject<?> configuredObject)
        {
            super(modifiedAttributes, attributeTypes, configuredObject);
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable
        {
            if (Arrays.asList("getModel", "getCategoryClass", "getParent").contains(method.getName()))
            {
                return method.invoke(getConfiguredObject(), args);
            }
            else
            {
                return super.invoke(proxy, method, args);
            }
        }
    }

    private static class AuthorisationProxyInvocationHandler extends AttributeGettingHandler
    {
        private final Class<? extends ConfiguredObject> _category;
        private final ConfiguredObject<?> _parent   ;
        private Map<String, Object> _attributes;

        AuthorisationProxyInvocationHandler(Map<String, Object> attributes,
                                            Map<String, ConfiguredObjectAttribute<?, ?>> attributeTypes,
                                            Class<? extends ConfiguredObject> categoryClass,
                                            ConfiguredObject<?> parent)
        {
            super(attributes, attributeTypes, null);
            _parent = parent;
            _category = categoryClass;
            _attributes = attributes;
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable
        {
            if(method.getName().equals("getParent") && (args == null || args.length == 0))
            {
                return _parent;
            }
            else if(method.getName().equals("getCategoryClass"))
            {
                return _category;
            }
            else if(method.getName().equals("getModel") && (args == null || args.length == 0))
            {
                return _parent.getModel();
            }

            return super.invoke(proxy, method, args);
        }

        @Override
        protected Object convert(ConfiguredSettableAttribute attribute, Object value)
        {
            return attribute.convert(value, _parent);
        }

        @Override
        protected Class<? extends ConfiguredObject> getCategoryClass()
        {
            return _category;
        }

        @Override
        protected String getType()
        {
            return String.valueOf(_attributes.get(ConfiguredObject.TYPE));
        }
    }

    public final static class DuplicateIdException extends IllegalArgumentException
    {
        private DuplicateIdException(final ConfiguredObject<?> existing)
        {
            super("Child of type " + existing.getClass().getSimpleName() + " already exists with id of " + existing.getId());
        }
    }

    public static class DuplicateNameException extends IllegalArgumentException
    {
        private final ConfiguredObject<?> _existing;

        protected DuplicateNameException(final ConfiguredObject<?> existing)
        {
            this("Child of type " + existing.getClass().getSimpleName() + " already exists with name of " + existing.getName(), existing);
        }

        protected DuplicateNameException(String message, final ConfiguredObject<?> existing)
        {
            super(message);
            _existing = existing;
        }

        public String getName()
        {
            return _existing.getName();
        }

        public ConfiguredObject<?> getExisting()
        {
            return _existing;
        }
    }

    interface AbstractConfiguredObjectExceptionHandler
    {
        void handleException(RuntimeException exception, ConfiguredObject<?> source);
    }

    private static class OpenExceptionHandler implements AbstractConfiguredObjectExceptionHandler
    {
        @Override
        public void handleException(RuntimeException exception, ConfiguredObject<?> source)
        {
            if(source instanceof AbstractConfiguredObject<?>)
            {
                ((AbstractConfiguredObject)source).handleExceptionOnOpen(exception);
            }
            else if(source instanceof AbstractConfiguredObjectProxy)
            {
                ((AbstractConfiguredObjectProxy)source).handleExceptionOnOpen(exception);
            }
        }
    }

    private static class CreateExceptionHandler implements AbstractConfiguredObjectExceptionHandler
    {
        private final boolean _unregister;

        private CreateExceptionHandler()
        {
            this(false);
        }

        private CreateExceptionHandler(boolean unregister)
        {
            _unregister = unregister;
        }

        @Override
        public void handleException(RuntimeException exception, ConfiguredObject<?> source)
        {
            try
            {
                if (source.getState() != State.DELETED)
                {
                    // TODO - RG - This isn't right :-(
                    if (source instanceof AbstractConfiguredObject)
                    {
                        ((AbstractConfiguredObject) source).deleteNoChecks();
                    }
                    else if (source instanceof AbstractConfiguredObjectProxy)
                    {
                        ((AbstractConfiguredObjectProxy) source).deleteNoChecks();
                    }
                    else
                    {
                        source.deleteAsync();
                    }

                }
            }
            finally
            {
                if (_unregister)
                {
                    if(source instanceof AbstractConfiguredObject)
                    {
                        ((AbstractConfiguredObject)source).unregister(false);
                    }
                    else if (source instanceof AbstractConfiguredObjectProxy)
                    {
                        ((AbstractConfiguredObjectProxy)source).unregister(false);
                    }
                }
                throw exception;
            }
        }
    }

    private interface AbstractConfiguredObjectProxy
    {
        void registerChild(ConfiguredObject configuredObject);

        DynamicState getDynamicState();

        ListenableFuture<Void> doAttainState(AbstractConfiguredObjectExceptionHandler exceptionHandler);

        void doOpening(boolean skipCheck, AbstractConfiguredObjectExceptionHandler exceptionHandler);

        void handleExceptionOnOpen(RuntimeException exception);

        void unregister(boolean removed);

        void doValidation(boolean skipCheck, AbstractConfiguredObjectExceptionHandler exceptionHandler);

        void doResolution(boolean skipCheck, AbstractConfiguredObjectExceptionHandler exceptionHandler);

        void doCreation(boolean skipCheck, AbstractConfiguredObjectExceptionHandler exceptionHandler);

        void validateChildDelete(ConfiguredObject child);

        void unregisterChild(ConfiguredObject child);

        void childRemoved(ConfiguredObject child);

        ListenableFuture getAttainStateFuture();

        void forceUpdateAllSecureAttributes();

        ListenableFuture<Void> deleteNoChecks();
    }
}
