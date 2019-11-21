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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.plugin.ConfiguredObjectAttributeInjector;
import org.apache.qpid.server.plugin.ConfiguredObjectRegistration;
import org.apache.qpid.server.plugin.ConfiguredObjectTypeFactory;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.ServerScopedRuntimeException;
import org.apache.qpid.server.util.Strings;

public class ConfiguredObjectTypeRegistry
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredObjectTypeRegistry.class);

    private static Map<String, Integer> STANDARD_FIRST_FIELDS_ORDER = new HashMap<>();

    private static final ConcurrentMap<Class<?>, Class<? extends ConfiguredObject>> CATEGORY_CACHE =
            new ConcurrentHashMap<>();

    static
    {
        int i = 0;
        for (String name : Arrays.asList(ConfiguredObject.ID,
                                         ConfiguredObject.NAME,
                                         ConfiguredObject.DESCRIPTION,
                                         ConfiguredObject.TYPE,
                                         ConfiguredObject.DESIRED_STATE,
                                         ConfiguredObject.STATE,
                                         ConfiguredObject.DURABLE,
                                         ConfiguredObject.LIFETIME_POLICY,
                                         ConfiguredObject.CONTEXT))
        {
            STANDARD_FIRST_FIELDS_ORDER.put(name, i++);
        }

    }

    private static Map<String, Integer> STANDARD_LAST_FIELDS_ORDER = new HashMap<>();

    static
    {
        int i = 0;
        for (String name : Arrays.asList(ConfiguredObject.LAST_UPDATED_BY,
                                         ConfiguredObject.LAST_UPDATED_TIME,
                                         ConfiguredObject.CREATED_BY,
                                         ConfiguredObject.CREATED_TIME))
        {
            STANDARD_LAST_FIELDS_ORDER.put(name, i++);
        }

    }


    private static final Comparator<ConfiguredObjectAttributeOrStatistic<?, ?>> OBJECT_NAME_COMPARATOR =
            new Comparator<ConfiguredObjectAttributeOrStatistic<?, ?>>()
            {
                @Override
                public int compare(final ConfiguredObjectAttributeOrStatistic<?, ?> left,
                                   final ConfiguredObjectAttributeOrStatistic<?, ?> right)
                {
                    String leftName = left.getName();
                    String rightName = right.getName();
                    return compareAttributeNames(leftName, rightName);
                }
            };

    private static final Comparator<String> NAME_COMPARATOR = new Comparator<String>()
    {
        @Override
        public int compare(final String left, final String right)
        {
            return compareAttributeNames(left, right);
        }
    };

    private static int compareAttributeNames(final String leftName, final String rightName)
    {
        int result;
        if (leftName.equals(rightName))
        {
            result = 0;
        }
        else if (STANDARD_FIRST_FIELDS_ORDER.containsKey(leftName))
        {
            if (STANDARD_FIRST_FIELDS_ORDER.containsKey(rightName))
            {
                result = STANDARD_FIRST_FIELDS_ORDER.get(leftName) - STANDARD_FIRST_FIELDS_ORDER.get(rightName);
            }
            else
            {
                result = -1;
            }
        }
        else if (STANDARD_FIRST_FIELDS_ORDER.containsKey(rightName))
        {
            result = 1;
        }
        else if (STANDARD_LAST_FIELDS_ORDER.containsKey(rightName))
        {
            if (STANDARD_LAST_FIELDS_ORDER.containsKey(leftName))
            {
                result = STANDARD_LAST_FIELDS_ORDER.get(leftName) - STANDARD_LAST_FIELDS_ORDER.get(rightName);
            }
            else
            {
                result = -1;
            }
        }
        else if (STANDARD_LAST_FIELDS_ORDER.containsKey(leftName))
        {
            result = 1;
        }
        else
        {
            result = leftName.compareTo(rightName);
        }

        return result;
    }


    private final Map<Class<? extends ConfiguredObject>, Collection<ConfiguredObjectAttribute<?, ?>>> _allAttributes =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Collection<ConfiguredObjectAttribute<?, ?>>>());

    private final Map<Class<? extends ConfiguredObject>, Collection<ConfiguredObjectStatistic<?, ?>>> _allStatistics =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Collection<ConfiguredObjectStatistic<?, ?>>>());

    private final Map<Class<? extends ConfiguredObject>, Map<String, ConfiguredObjectAttribute<?, ?>>>
            _allAttributeTypes =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Map<String, ConfiguredObjectAttribute<?, ?>>>());

    private final Map<Class<? extends ConfiguredObject>, Map<String, AutomatedField>> _allAutomatedFields =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Map<String, AutomatedField>>());

    private final Map<String, String> _defaultContext =
            Collections.synchronizedMap(new HashMap<String, String>());

    private final Map<String, ManagedContextDefault> _contextDefinitions =
            Collections.synchronizedMap(new HashMap<String, ManagedContextDefault>());
    private final Map<Class<? extends ConfiguredObject>, Set<String>> _contextUses =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Set<String>>());

    private final Map<Class<? extends ConfiguredObject>, Set<Class<? extends ConfiguredObject>>> _knownTypes =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Set<Class<? extends ConfiguredObject>>>());

    private final Map<Class<? extends ConfiguredObject>, Collection<ConfiguredObjectAttribute<?, ?>>>
            _typeSpecificAttributes =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Collection<ConfiguredObjectAttribute<?, ?>>>());

    private final Map<Class<? extends ConfiguredObject>, Collection<ConfiguredObjectStatistic<?, ?>>>
            _typeSpecificStatistics =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Collection<ConfiguredObjectStatistic<?, ?>>>());

    private final Map<Class<? extends ConfiguredObject>, Map<State, Map<State, Method>>> _stateChangeMethods =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Map<State, Map<State, Method>>>());

    private final Map<Class<? extends ConfiguredObject>, Set<Class<? extends ManagedInterface>>> _allManagedInterfaces =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Set<Class<? extends ManagedInterface>>>());

    private final Map<Class<? extends ConfiguredObject>, Set<ConfiguredObjectOperation<?>>> _allOperations =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Set<ConfiguredObjectOperation<?>>>());


    private final Map<Class<? extends ConfiguredObject>, Map<String, Collection<String>>> _validChildTypes =
            Collections.synchronizedMap(new HashMap<Class<? extends ConfiguredObject>, Map<String, Collection<String>>>());

    private final ConfiguredObjectFactory _objectFactory;
    private final Iterable<ConfiguredObjectAttributeInjector> _attributeInjectors;

    public ConfiguredObjectTypeRegistry(Iterable<ConfiguredObjectRegistration> configuredObjectRegistrations,
                                        final Iterable<ConfiguredObjectAttributeInjector> attributeInjectors,
                                        Collection<Class<? extends ConfiguredObject>> categoriesRestriction,
                                        final ConfiguredObjectFactory objectFactory)
    {
        _objectFactory = objectFactory;
        _attributeInjectors = attributeInjectors;
        Set<Class<? extends ConfiguredObject>> categories = new HashSet<>();
        Set<Class<? extends ConfiguredObject>> types = new HashSet<>();


        for (ConfiguredObjectRegistration registration : configuredObjectRegistrations)
        {
            for (Class<? extends ConfiguredObject> configuredObjectClass : registration.getConfiguredObjectClasses())
            {
                if (categoriesRestriction.isEmpty()
                    || categoriesRestriction.contains(getCategory(configuredObjectClass)))
                {
                    try
                    {
                        process(configuredObjectClass);
                        ManagedObject annotation = configuredObjectClass.getAnnotation(ManagedObject.class);
                        if (annotation.category())
                        {
                            categories.add(configuredObjectClass);
                        }
                        else
                        {
                            Class<? extends ConfiguredObject> category = getCategory(configuredObjectClass);
                            if (category != null)
                            {
                                categories.add(category);
                            }
                        }
                        if (!"".equals(annotation.type()))
                        {
                            types.add(configuredObjectClass);
                        }
                    }
                    catch (NoClassDefFoundError ncdfe)
                    {
                        LOGGER.warn("A class definition could not be found while processing the model for '"
                                    + configuredObjectClass.getName()
                                    + "': "
                                    + ncdfe.getMessage());
                    }
                }
            }
        }
        for (Class<? extends ConfiguredObject> categoryClass : categories)
        {
            _knownTypes.put(categoryClass, new HashSet<Class<? extends ConfiguredObject>>());
        }

        for (Class<? extends ConfiguredObject> typeClass : types)
        {
            for (Class<? extends ConfiguredObject> categoryClass : categories)
            {
                if (categoryClass.isAssignableFrom(typeClass))
                {
                    ManagedObject annotation = typeClass.getAnnotation(ManagedObject.class);
                    String annotationType = annotation.type();
                    if (ModelRoot.class.isAssignableFrom(categoryClass) || factoryExists(categoryClass, annotationType))
                    {
                        _knownTypes.get(categoryClass).add(typeClass);
                    }
                }
            }
        }

        for (Class<? extends ConfiguredObject> categoryClass : categories)
        {
            Set<Class<? extends ConfiguredObject>> typesForCategory = _knownTypes.get(categoryClass);
            if (typesForCategory.isEmpty())
            {
                typesForCategory.add(categoryClass);
                _typeSpecificAttributes.put(categoryClass, Collections.emptySet());
                _typeSpecificStatistics.put(categoryClass, Collections.emptySet());
            }
            else
            {
                Set<String> commonAttributes = new HashSet<>();
                for (ConfiguredObjectAttribute<?, ?> attribute : _allAttributes.get(categoryClass))
                {
                    commonAttributes.add(attribute.getName());
                }
                Set<String> commonStatistics = new HashSet<>();
                for (ConfiguredObjectStatistic<?, ?> statistic : _allStatistics.get(categoryClass))
                {
                    commonStatistics.add(statistic.getName());
                }
                for (Class<? extends ConfiguredObject> typeClass : typesForCategory)
                {
                    Set<ConfiguredObjectAttribute<?, ?>> attributes = new HashSet<>();
                    for (ConfiguredObjectAttribute<?, ?> attr : _allAttributes.get(typeClass))
                    {
                        if (!commonAttributes.contains(attr.getName()))
                        {
                            attributes.add(attr);
                        }
                    }
                    _typeSpecificAttributes.put(typeClass, attributes);

                    Set<ConfiguredObjectStatistic<?, ?>> statistics = new HashSet<>();
                    for (ConfiguredObjectStatistic<?, ?> statistic : _allStatistics.get(typeClass))
                    {
                        if (!commonStatistics.contains(statistic.getName()))
                        {
                            statistics.add(statistic);
                        }
                    }
                    _typeSpecificStatistics.put(typeClass, statistics);
                }
            }
        }

        for (Class<? extends ConfiguredObject> type : types)
        {
            final ManagedObject annotation = type.getAnnotation(ManagedObject.class);
            String validChildren = annotation.validChildTypes();
            if (!"".equals(validChildren))
            {
                Method validChildTypesMethod = getValidChildTypesFunction(validChildren, type);
                if (validChildTypesMethod != null)
                {
                    try
                    {
                        _validChildTypes.put(type,
                                             (Map<String, Collection<String>>) validChildTypesMethod.invoke(null));
                    }
                    catch (IllegalAccessException | InvocationTargetException e)
                    {
                        throw new IllegalArgumentException("Exception while evaluating valid child types for "
                                                           + type.getName(), e);
                    }
                }

            }
        }

        validateContextDependencies();

    }

    public static boolean returnsCollectionOfConfiguredObjects(ConfiguredObjectOperation operation)
    {
        return Collection.class.isAssignableFrom(operation.getReturnType())
               && operation.getGenericReturnType() instanceof ParameterizedType
               && ConfiguredObject.class.isAssignableFrom(getCollectionMemberType((ParameterizedType) operation.getGenericReturnType()));
    }

    public static Class getCollectionMemberType(ParameterizedType collectionType)
    {
        return getRawType((collectionType).getActualTypeArguments()[0]);
    }

    public static Class getRawType(Type t)
    {
        if(t instanceof Class)
        {
            return (Class)t;
        }
        else if(t instanceof ParameterizedType)
        {
            return (Class)((ParameterizedType)t).getRawType();
        }
        else if(t instanceof TypeVariable)
        {
            Type[] bounds = ((TypeVariable)t).getBounds();
            if(bounds.length == 1)
            {
                return getRawType(bounds[0]);
            }
        }
        else if(t instanceof WildcardType)
        {
            Type[] upperBounds = ((WildcardType)t).getUpperBounds();
            if(upperBounds.length == 1)
            {
                return getRawType(upperBounds[0]);
            }
        }
        throw new ServerScopedRuntimeException("Unable to process type when constructing configuration model: " + t);
    }

    private void validateContextDependencies()
    {
        for(Map.Entry<Class<? extends ConfiguredObject>, Set<String>> entry : _contextUses.entrySet())
        {
            for (String dependency : entry.getValue())
            {
                if(!_contextDefinitions.containsKey(dependency))
                {
                    throw new IllegalArgumentException("Class "
                                                       + entry.getKey().getSimpleName()
                                                       + " defines a context dependency on a context variable '"
                                                       + dependency
                                                       + "' which is never defined");
                }
            }
        }
    }



    private boolean factoryExists(final Class<? extends ConfiguredObject> categoryClass, final String type)
    {
        try
        {
            final ConfiguredObjectTypeFactory factory =
                    _objectFactory.getConfiguredObjectTypeFactory(categoryClass.getSimpleName(), type);

            return factory != null && factory.getType().equals(type);
        }
        catch (NoFactoryForTypeException | NoFactoryForCategoryException e)
        {
            return false;
        }
    }

    private static Method getValidChildTypesFunction(final String validValue,
                                                     final Class<? extends ConfiguredObject> clazz)
    {
        if (validValue.matches("([\\w][\\w\\d_]+\\.)+[\\w][\\w\\d_\\$]*#[\\w\\d_]+\\s*\\(\\s*\\)"))
        {
            String function = validValue;
            try
            {
                String className = function.split("#")[0].trim();
                String methodName = function.split("#")[1].split("\\(")[0].trim();
                Class<?> validValueCalculatingClass = Class.forName(className);
                Method method = validValueCalculatingClass.getMethod(methodName);
                if (Modifier.isStatic(method.getModifiers()) && Modifier.isPublic(method.getModifiers()))
                {
                    if (Map.class.isAssignableFrom(method.getReturnType()))
                    {
                        if (method.getGenericReturnType() instanceof ParameterizedType)
                        {
                            Type keyType =
                                    ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[0];
                            if (keyType == String.class)
                            {
                                Type valueType =
                                        ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[1];
                                if (valueType instanceof ParameterizedType)
                                {
                                    ParameterizedType paramType = (ParameterizedType) valueType;
                                    final Type rawType = paramType.getRawType();
                                    final Type[] args = paramType.getActualTypeArguments();
                                    if (Collection.class.isAssignableFrom((Class<?>) rawType)
                                        && args.length == 1
                                        && args[0] == String.class)
                                    {
                                        return method;
                                    }
                                }
                            }
                        }
                    }
                }


                throw new IllegalArgumentException("The validChildTypes of the class "
                                                   + clazz.getSimpleName()
                                                   + " has value '"
                                                   + validValue
                                                   + "' but the method does not meet the requirements - is it public and static");

            }
            catch (ClassNotFoundException | NoSuchMethodException e)
            {
                throw new IllegalArgumentException("The validChildTypes of the class "
                                                   + clazz.getSimpleName()
                                                   + " has value '"
                                                   + validValue
                                                   + "' which looks like it should be a method,"
                                                   + " but no such method could be used.", e);
            }
        }
        else
        {
            throw new IllegalArgumentException("The validChildTypes of the class "
                                               + clazz.getSimpleName()
                                               + " has value '"
                                               + validValue
                                               + "' which does not match the required <package>.<class>#<method>() format.");
        }
    }

    public static Class<? extends ConfiguredObject> getCategory(final Class<?> clazz)
    {
        Class<? extends ConfiguredObject> category = CATEGORY_CACHE.get(clazz);
        if (category == null)
        {
            category = calculateCategory(clazz);
            if (category != null)
            {
                CATEGORY_CACHE.putIfAbsent(clazz, category);
            }
        }
        return category;
    }

    private static Class<? extends ConfiguredObject> calculateCategory(final Class<?> clazz)
    {
        ManagedObject annotation = clazz.getAnnotation(ManagedObject.class);
        if (annotation != null && annotation.category())
        {
            return (Class<? extends ConfiguredObject>) clazz;
        }
        for (Class<?> iface : clazz.getInterfaces())
        {
            Class<? extends ConfiguredObject> cat = getCategory(iface);
            if (cat != null)
            {
                return cat;
            }
        }
        if (clazz.getSuperclass() != null)
        {
            return getCategory(clazz.getSuperclass());
        }
        return null;
    }

    public Class<? extends ConfiguredObject> getTypeClass(final Class<? extends ConfiguredObject> clazz)
    {
        String typeName = getType(clazz);
        Class<? extends ConfiguredObject> typeClass = null;
        if (typeName != null)
        {
            Class<? extends ConfiguredObject> category = getCategory(clazz);
            Set<Class<? extends ConfiguredObject>> types = _knownTypes.get(category);
            if (types != null)
            {
                for (Class<? extends ConfiguredObject> type : types)
                {
                    ManagedObject annotation = type.getAnnotation(ManagedObject.class);
                    if (typeName.equals(annotation.type()))
                    {
                        typeClass = type;
                        break;
                    }
                }
            }
            if (typeClass == null)
            {
                if (typeName.equals(category.getSimpleName()))
                {
                    typeClass = category;
                }
            }
        }

        return typeClass;

    }

    public Collection<Class<? extends ConfiguredObject>> getTypeSpecialisations(Class<? extends ConfiguredObject> clazz)
    {
        Class<? extends ConfiguredObject> categoryClass = getCategory(clazz);
        if (categoryClass == null)
        {
            throw new IllegalArgumentException("Cannot locate ManagedObject information for " + clazz.getName());
        }
        Set<Class<? extends ConfiguredObject>> classes = _knownTypes.get(categoryClass);
        if (classes == null)
        {
            classes = (Set<Class<? extends ConfiguredObject>>) ((Set) Collections.singleton(clazz));
        }
        return Collections.unmodifiableCollection(classes);

    }

    public Collection<ConfiguredObjectAttribute<?, ?>> getTypeSpecificAttributes(Class<? extends ConfiguredObject> clazz)
    {
        Class<? extends ConfiguredObject> typeClass = getTypeClass(clazz);
        if (typeClass == null)
        {
            throw new IllegalArgumentException("Cannot locate ManagedObject information for " + clazz.getName());
        }
        Collection<ConfiguredObjectAttribute<?, ?>> typeAttrs = _typeSpecificAttributes.get(typeClass);
        return Collections.unmodifiableCollection(typeAttrs == null
                                                          ? Collections.<ConfiguredObjectAttribute<?, ?>>emptySet()
                                                          : typeAttrs);
    }

    public Collection<ConfiguredObjectStatistic<?, ?>> getTypeSpecificStatistics(final Class<? extends ConfiguredObject> clazz)
    {
        Class<? extends ConfiguredObject> typeClass = getTypeClass(clazz);
        if (typeClass == null)
        {
            throw new IllegalArgumentException("Cannot locate ManagedObject information for " + clazz.getName());
        }
        Collection<ConfiguredObjectStatistic<?, ?>> typeAttrs = _typeSpecificStatistics.get(typeClass);
        return Collections.unmodifiableCollection(typeAttrs == null
                                                          ? Collections.emptySet()
                                                          : typeAttrs);
    }

    public static String getType(final Class<? extends ConfiguredObject> clazz)
    {
        String type = getActualType(clazz);

        if ("".equals(type))
        {
            Class<? extends ConfiguredObject> category = getCategory(clazz);
            if (category == null)
            {
                throw new IllegalArgumentException("No category for " + clazz.getSimpleName());
            }
            ManagedObject annotation = category.getAnnotation(ManagedObject.class);
            if (annotation == null)
            {
                throw new NullPointerException("No definition found for category " + category.getSimpleName());
            }
            if (!"".equals(annotation.defaultType()))
            {
                type = annotation.defaultType();
            }
            else
            {
                type = category.getSimpleName();
            }
        }
        return type;
    }

    private static String getActualType(final Class<? extends ConfiguredObject> clazz)
    {
        ManagedObject annotation = clazz.getAnnotation(ManagedObject.class);
        if (annotation != null)
        {
            if (!"".equals(annotation.type()))
            {
                return annotation.type();
            }
        }


        for (Class<?> iface : clazz.getInterfaces())
        {
            if (ConfiguredObject.class.isAssignableFrom(iface))
            {
                String type = getActualType((Class<? extends ConfiguredObject>) iface);
                if (!"".equals(type))
                {
                    return type;
                }
            }
        }

        if (clazz.getSuperclass() != null && ConfiguredObject.class.isAssignableFrom(clazz.getSuperclass()))
        {
            String type = getActualType((Class<? extends ConfiguredObject>) clazz.getSuperclass());
            if (!"".equals(type))
            {
                return type;
            }
        }

        return "";
    }

    public Strings.Resolver getDefaultContextResolver()
    {
        return new Strings.MapResolver(_defaultContext);
    }

    static class AutomatedField
    {
        private final Field _field;
        private final Method _preSettingAction;
        private final Method _postSettingAction;

        private AutomatedField(final Field field, final Method preSettingAction, final Method postSettingAction)
        {
            _field = field;
            _preSettingAction = preSettingAction;
            _postSettingAction = postSettingAction;
        }

        public Field getField()
        {
            return _field;
        }

        public Method getPreSettingAction()
        {
            return _preSettingAction;
        }

        public Method getPostSettingAction()
        {
            return _postSettingAction;
        }
    }


    private <X extends ConfiguredObject> void process(final Class<X> clazz)
    {
        synchronized (_allAttributes)
        {
            if (_allAttributes.containsKey(clazz))
            {
                return;
            }
            doWithAllParents(clazz, new Action<Class<? extends ConfiguredObject>>()
            {
                @Override
                public void performAction(final Class<? extends ConfiguredObject> parent)
                {
                    process(parent);
                }
            });

            final SortedSet<ConfiguredObjectAttribute<?, ?>> attributeSet = new TreeSet<>(OBJECT_NAME_COMPARATOR);
            final SortedSet<ConfiguredObjectStatistic<?, ?>> statisticSet = new TreeSet<>(OBJECT_NAME_COMPARATOR);
            final Set<Class<? extends ManagedInterface>> managedInterfaces = new HashSet<>();
            final Set<ConfiguredObjectOperation<?>> operationsSet = new HashSet<>();
            final Set<String> contextSet = new HashSet<>();

            _allAttributes.put(clazz, attributeSet);
            _allStatistics.put(clazz, statisticSet);
            _allManagedInterfaces.put(clazz, managedInterfaces);
            _allOperations.put(clazz, operationsSet);
            _contextUses.put(clazz, contextSet);

            doWithAllParents(clazz, new Action<Class<? extends ConfiguredObject>>()
            {
                @Override
                public void performAction(final Class<? extends ConfiguredObject> parent)
                {
                    initialiseWithParentAttributes(attributeSet,
                                                   statisticSet,
                                                   managedInterfaces,
                                                   operationsSet,
                                                   contextSet,
                                                   parent);

                }
            });

            processMethods(clazz, attributeSet, statisticSet, operationsSet);

            processAttributesTypesAndFields(clazz);

            processDefaultContext(clazz, contextSet);

            processStateChangeMethods(clazz);

            processManagedInterfaces(clazz);

            processContextUages(clazz, contextSet);
        }
    }

    private <X extends ConfiguredObject> void processContextUages(final Class<X> clazz, final Set<String> contextSet)
    {
        if (clazz.isAnnotationPresent(ManagedContextDependency.class))
        {
            ManagedContextDependency dependencies = clazz.getAnnotation(ManagedContextDependency.class);
            for (String dependency : dependencies.value())
            {
                contextSet.add(dependency);
            }
        }
    }

    private static void doWithAllParents(Class<?> clazz, Action<Class<? extends ConfiguredObject>> action)
    {
        for (Class<?> parent : clazz.getInterfaces())
        {
            if (ConfiguredObject.class.isAssignableFrom(parent))
            {
                action.performAction((Class<? extends ConfiguredObject>) parent);
            }
        }
        final Class<?> superclass = clazz.getSuperclass();
        if (superclass != null && ConfiguredObject.class.isAssignableFrom(superclass))
        {
            action.performAction((Class<? extends ConfiguredObject>) superclass);
        }
    }

    private <X extends ConfiguredObject> void processMethods(final Class<X> clazz,
                                                             final SortedSet<ConfiguredObjectAttribute<?, ?>> attributeSet,
                                                             final SortedSet<ConfiguredObjectStatistic<?, ?>> statisticSet,
                                                             final Set<ConfiguredObjectOperation<?>> operationsSet)
    {
        for (Method method : clazz.getDeclaredMethods())
        {
            processMethod(clazz, attributeSet, statisticSet, operationsSet, method);
        }

        for (ConfiguredObjectAttributeInjector injector : _attributeInjectors)
        {
            if (injector.getTypeValidator().appliesToType((Class<? extends ConfiguredObject<?>>) clazz))
            {
                for (ConfiguredObjectInjectedAttribute<?, ?> attr : injector.getInjectedAttributes())
                {
                    if (attr.appliesToConfiguredObjectType((Class<? extends ConfiguredObject<?>>) clazz))
                    {
                        attributeSet.add(attr);
                    }
                }

                for (ConfiguredObjectInjectedStatistic<?, ?> attr : injector.getInjectedStatistics())
                {
                    if (attr.appliesToConfiguredObjectType((Class<? extends ConfiguredObject<?>>) clazz))
                    {
                        statisticSet.add(attr);
                    }
                }

                for (ConfiguredObjectInjectedOperation<?> operation : injector.getInjectedOperations())
                {
                    if (operation.appliesToConfiguredObjectType((Class<? extends ConfiguredObject<?>>) clazz))
                    {
                        operationsSet.add(operation);
                    }
                }
            }
        }
    }

    private <X extends ConfiguredObject> void processMethod(final Class<X> clazz,
                                                            final SortedSet<ConfiguredObjectAttribute<?, ?>> attributeSet,
                                                            final SortedSet<ConfiguredObjectStatistic<?, ?>> statisticSet,
                                                            final Set<ConfiguredObjectOperation<?>> operationsSet,
                                                            final Method m)
    {
        if (m.isAnnotationPresent(ManagedAttribute.class))
        {
            processManagedAttribute(clazz, attributeSet, m);
        }
        else if (m.isAnnotationPresent(DerivedAttribute.class))
        {
            processDerivedAttribute(clazz, attributeSet, m);

        }
        else if (m.isAnnotationPresent(ManagedStatistic.class))
        {
            processManagedStatistic(clazz, statisticSet, m);
        }
        else if (m.isAnnotationPresent(ManagedOperation.class))
        {
            processManagedOperation(clazz, operationsSet, m);
        }
    }

    private <X extends ConfiguredObject> void processManagedStatistic(final Class<X> clazz,
                                                                      final SortedSet<ConfiguredObjectStatistic<?, ?>> statisticSet,
                                                                      final Method m)
    {
        ManagedStatistic statAnnotation = m.getAnnotation(ManagedStatistic.class);
        if (!clazz.isInterface() || !ConfiguredObject.class.isAssignableFrom(clazz))
        {
            throw new ServerScopedRuntimeException("Can only define ManagedStatistics on interfaces which extend "
                                                   + ConfiguredObject.class.getSimpleName()
                                                   + ". "
                                                   + clazz.getSimpleName()
                                                   + " does not meet these criteria.");
        }
        ConfiguredObjectStatistic statistic = new ConfiguredObjectMethodStatistic(clazz, m, statAnnotation);
        if (statisticSet.contains(statistic))
        {
            statisticSet.remove(statistic);
        }
        statisticSet.add(statistic);
    }

    private <X extends ConfiguredObject> void processDerivedAttribute(final Class<X> clazz,
                                                                      final SortedSet<ConfiguredObjectAttribute<?, ?>> attributeSet,
                                                                      final Method m)
    {
        DerivedAttribute annotation = m.getAnnotation(DerivedAttribute.class);

        if (!clazz.isInterface() || !ConfiguredObject.class.isAssignableFrom(clazz))
        {
            throw new ServerScopedRuntimeException("Can only define DerivedAttributes on interfaces which extend "
                                                   + ConfiguredObject.class.getSimpleName()
                                                   + ". "
                                                   + clazz.getSimpleName()
                                                   + " does not meet these criteria.");
        }

        ConfiguredObjectAttribute<?, ?> attribute = new ConfiguredDerivedMethodAttribute<>(clazz, m, annotation);
        if (attributeSet.contains(attribute))
        {
            attributeSet.remove(attribute);
        }
        attributeSet.add(attribute);
    }

    private <X extends ConfiguredObject> void processManagedAttribute(final Class<X> clazz,
                                                                      final SortedSet<ConfiguredObjectAttribute<?, ?>> attributeSet,
                                                                      final Method m)
    {
        ManagedAttribute annotation = m.getAnnotation(ManagedAttribute.class);

        if (!clazz.isInterface() || !ConfiguredObject.class.isAssignableFrom(clazz))
        {
            throw new ServerScopedRuntimeException("Can only define ManagedAttributes on interfaces which extend "
                                                   + ConfiguredObject.class.getSimpleName()
                                                   + ". "
                                                   + clazz.getSimpleName()
                                                   + " does not meet these criteria.");
        }

        ConfiguredObjectAttribute<?, ?> attribute = new ConfiguredAutomatedAttribute<>(clazz, m, annotation);
        if (attributeSet.contains(attribute))
        {
            attributeSet.remove(attribute);
        }
        attributeSet.add(attribute);
    }

    private <X extends ConfiguredObject> void processManagedOperation(final Class<X> clazz,
                                                                      final Set<ConfiguredObjectOperation<?>> operationSet,
                                                                      final Method m)
    {
        ManagedOperation annotation = m.getAnnotation(ManagedOperation.class);

        if (!clazz.isInterface() || !ConfiguredObject.class.isAssignableFrom(clazz))
        {
            throw new ServerScopedRuntimeException("Can only define ManagedOperations on interfaces which extend "
                                                   + ConfiguredObject.class.getSimpleName()
                                                   + ". "
                                                   + clazz.getSimpleName()
                                                   + " does not meet these criteria.");
        }

        ConfiguredObjectOperation<?> operation = new ConfiguredObjectMethodOperation<>(clazz, m, this);
        Iterator<ConfiguredObjectOperation<?>> iter = operationSet.iterator();
        while (iter.hasNext())
        {
            final ConfiguredObjectOperation<?> existingOperation = iter.next();
            if (operation.getName().equals(existingOperation.getName()))
            {
                if (!operation.hasSameParameters(existingOperation))
                {
                    throw new IllegalArgumentException("Cannot redefine the operation "
                                                       + operation.getName()
                                                       + " with different parameters in "
                                                       + clazz.getSimpleName());
                }
                iter.remove();
                break;
            }
        }
        operationSet.add(operation);
    }


    private void initialiseWithParentAttributes(final SortedSet<ConfiguredObjectAttribute<?, ?>> attributeSet,
                                                final SortedSet<ConfiguredObjectStatistic<?, ?>> statisticSet,
                                                final Set<Class<? extends ManagedInterface>> managedInterfaces,
                                                final Set<ConfiguredObjectOperation<?>> operationsSet,
                                                final Set<String> contextSet,
                                                final Class<? extends ConfiguredObject> parent)
    {
        attributeSet.addAll(_allAttributes.get(parent));
        statisticSet.addAll(_allStatistics.get(parent));
        managedInterfaces.addAll(_allManagedInterfaces.get(parent));
        operationsSet.addAll(_allOperations.get(parent));
        contextSet.addAll(_contextUses.get(parent));
    }

    private <X extends ConfiguredObject> void processAttributesTypesAndFields(final Class<X> clazz)
    {
        Map<String, ConfiguredObjectAttribute<?, ?>> attrMap = new TreeMap<>(NAME_COMPARATOR);
        Map<String, AutomatedField> fieldMap = new HashMap<String, AutomatedField>();


        Collection<ConfiguredObjectAttribute<?, ?>> attrCol = _allAttributes.get(clazz);
        for (ConfiguredObjectAttribute<?, ?> attr : attrCol)
        {
            attrMap.put(attr.getName(), attr);
            if (attr.isAutomated())
            {
                fieldMap.put(attr.getName(), findField(attr, clazz));
            }

        }
        for (ConfiguredObjectAttributeInjector injector : _attributeInjectors)
        {
            for (ConfiguredObjectInjectedAttribute<?, ?> attr : injector.getInjectedAttributes())
            {
                if (!attrMap.containsKey(attr.getName())
                    && attr.appliesToConfiguredObjectType((Class<? extends ConfiguredObject<?>>) clazz))
                {
                    attrMap.put(attr.getName(), attr);
                }
            }
        }
        _allAttributeTypes.put(clazz, attrMap);
        _allAutomatedFields.put(clazz, fieldMap);
    }

    private <X extends ConfiguredObject> void processDefaultContext(final Class<X> clazz, final Set<String> contextSet)
    {
        for (Field field : clazz.getDeclaredFields())
        {
            if (Modifier.isStatic(field.getModifiers())
                && Modifier.isFinal(field.getModifiers())
                && field.isAnnotationPresent(ManagedContextDefault.class))
            {
                try
                {
                    ManagedContextDefault annotation = field.getAnnotation(ManagedContextDefault.class);
                    String name = annotation.name();
                    Object value = field.get(null);
                    if (!_defaultContext.containsKey(name))
                    {
                        final String stringValue;
                        if (value instanceof Collection || value instanceof Map)
                        {
                            try
                            {
                                stringValue = ConfiguredObjectJacksonModule.newObjectMapper(false).writeValueAsString(value);
                            }
                            catch (JsonProcessingException e)
                            {
                                throw new ServerScopedRuntimeException("Unable to convert value of type '"
                                                                       + value.getClass()
                                                                       + "' to a JSON string for context variable ${"
                                                                       + name
                                                                       + "}");
                            }
                        }
                        else
                        {
                            stringValue = String.valueOf(value);
                        }
                        _defaultContext.put(name, stringValue);
                        _contextDefinitions.put(name, annotation);
                        contextSet.add(name);
                    }
                    else
                    {
                        throw new IllegalArgumentException("Multiple definitions of the default context variable ${"
                                                           + name
                                                           + "}");
                    }
                }
                catch (IllegalAccessException e)
                {
                    throw new ServerScopedRuntimeException(
                            "Unexpected illegal access exception (only inspecting public static fields)",
                            e);
                }
            }
        }
    }

    private void processStateChangeMethods(Class<? extends ConfiguredObject> clazz)
    {
        final Map<State, Map<State, Method>> map = new HashMap<>();

        _stateChangeMethods.put(clazz, map);

        addStateTransitions(clazz, map);

        doWithAllParents(clazz, new Action<Class<? extends ConfiguredObject>>()
        {
            @Override
            public void performAction(final Class<? extends ConfiguredObject> parent)
            {
                inheritTransitions(parent, map);
            }
        });
    }

    private void inheritTransitions(final Class<? extends ConfiguredObject> parent,
                                    final Map<State, Map<State, Method>> map)
    {
        Map<State, Map<State, Method>> parentMap = _stateChangeMethods.get(parent);
        for (Map.Entry<State, Map<State, Method>> parentEntry : parentMap.entrySet())
        {
            if (map.containsKey(parentEntry.getKey()))
            {
                Map<State, Method> methodMap = map.get(parentEntry.getKey());
                for (Map.Entry<State, Method> methodEntry : parentEntry.getValue().entrySet())
                {
                    if (!methodMap.containsKey(methodEntry.getKey()))
                    {
                        methodMap.put(methodEntry.getKey(), methodEntry.getValue());
                    }
                }
            }
            else
            {
                map.put(parentEntry.getKey(), new HashMap<State, Method>(parentEntry.getValue()));
            }
        }
    }

    private void addStateTransitions(final Class<? extends ConfiguredObject> clazz,
                                     final Map<State, Map<State, Method>> map)
    {
        for (Method m : clazz.getDeclaredMethods())
        {
            if (m.isAnnotationPresent(StateTransition.class))
            {
                if (ListenableFuture.class.isAssignableFrom(m.getReturnType()))
                {
                    if (m.getParameterTypes().length == 0)
                    {
                        m.setAccessible(true);
                        StateTransition annotation = m.getAnnotation(StateTransition.class);

                        for (State state : annotation.currentState())
                        {
                            addStateTransition(state, annotation.desiredState(), m, map);
                        }

                    }
                    else
                    {
                        throw new ServerScopedRuntimeException(
                                "A state transition method must have no arguments. Method "
                                + m.getName()
                                + " on "
                                + clazz.getName()
                                + " does not meet this criteria.");
                    }
                }
                else
                {
                    throw new ServerScopedRuntimeException(
                            "A state transition method must return a ListenableFuture. Method "
                            + m.getName()
                            + " on "
                            + clazz.getName()
                            + " does not meet this criteria.");
                }
            }
        }
    }

    private void addStateTransition(final State fromState,
                                    final State toState,
                                    final Method method,
                                    final Map<State, Map<State, Method>> map)
    {
        if (map.containsKey(fromState))
        {
            Map<State, Method> toMap = map.get(fromState);
            if (!toMap.containsKey(toState))
            {
                toMap.put(toState, method);
            }
        }
        else
        {
            HashMap<State, Method> toMap = new HashMap<>();
            toMap.put(toState, method);
            map.put(fromState, toMap);
        }
    }

    private AutomatedField findField(final ConfiguredObjectAttribute<?, ?> attr, Class<?> objClass)
    {
        Class<?> clazz = objClass;
        while (clazz != null)
        {
            for (Field field : clazz.getDeclaredFields())
            {
                if (field.isAnnotationPresent(ManagedAttributeField.class) && field.getName()
                        .equals("_" + attr.getName().replace('.', '_')))
                {
                    try
                    {
                        ManagedAttributeField annotation = field.getAnnotation(ManagedAttributeField.class);
                        field.setAccessible(true);
                        Method beforeSet;
                        if (!"".equals(annotation.beforeSet()))
                        {
                            beforeSet = clazz.getDeclaredMethod(annotation.beforeSet());
                            beforeSet.setAccessible(true);
                        }
                        else
                        {
                            beforeSet = null;
                        }
                        Method afterSet;
                        if (!"".equals(annotation.afterSet()))
                        {
                            afterSet = clazz.getDeclaredMethod(annotation.afterSet());
                            afterSet.setAccessible(true);
                        }
                        else
                        {
                            afterSet = null;
                        }
                        return new AutomatedField(field, beforeSet, afterSet);
                    }
                    catch (NoSuchMethodException e)
                    {
                        throw new ServerScopedRuntimeException(
                                "Cannot find method referenced by annotation for pre/post setting action",
                                e);
                    }

                }
            }
            clazz = clazz.getSuperclass();
        }
        if (objClass.isInterface() || Modifier.isAbstract(objClass.getModifiers()))
        {
            return null;
        }
        throw new ServerScopedRuntimeException("Unable to find field definition for automated field "
                                               + attr.getName()
                                               + " in class "
                                               + objClass.getName());
    }

    public <X extends ConfiguredObject> Collection<String> getAttributeNames(Class<X> clazz)
    {
        final Collection<ConfiguredObjectAttribute<? super X, ?>> attrs = getAttributes(clazz);

        return new AbstractCollection<String>()
        {
            @Override
            public Iterator<String> iterator()
            {
                final Iterator<ConfiguredObjectAttribute<? super X, ?>> underlyingIterator = attrs.iterator();
                return new Iterator<String>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return underlyingIterator.hasNext();
                    }

                    @Override
                    public String next()
                    {
                        return underlyingIterator.next().getName();
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public int size()
            {
                return attrs.size();
            }
        };

    }

    protected <X extends ConfiguredObject> Collection<ConfiguredObjectAttribute<? super X, ?>> getAttributes(final Class<X> clazz)
    {
        processClassIfNecessary(clazz);
        final Collection<ConfiguredObjectAttribute<? super X, ?>> attributes = (Collection) _allAttributes.get(clazz);
        return attributes;
    }

    private <X extends ConfiguredObject> void processClassIfNecessary(final Class<X> clazz)
    {
        if (!_allAttributes.containsKey(clazz))
        {
            process(clazz);
        }
    }

    public Collection<ConfiguredObjectStatistic<?, ?>> getStatistics(final Class<? extends ConfiguredObject> clazz)
    {
        processClassIfNecessary(clazz);
        final Collection<ConfiguredObjectStatistic<?, ?>> statistics = _allStatistics.get(clazz);
        return statistics;
    }

    public Map<String, ConfiguredObjectOperation<?>> getOperations(final Class<? extends ConfiguredObject> clazz)
    {
        return getOperations(clazz, null);
    }

    public Map<String, ConfiguredObjectOperation<?>> getOperations(final Class<? extends ConfiguredObject> clazz, Predicate<ConfiguredObjectOperation<?>> predicate)
    {
        processClassIfNecessary(clazz);
        final Set<ConfiguredObjectOperation<?>> operations = _allOperations.get(clazz);
        if (operations == null)
        {
            return Collections.emptyMap();
        }
        else
        {
            Map<String, ConfiguredObjectOperation<?>> returnVal = new HashMap<>();
            for (ConfiguredObjectOperation<? extends ConfiguredObject<?>> operation : operations)
            {
                if (predicate == null || predicate.test(operation))
                {
                    returnVal.put(operation.getName(), operation);
                }
            }
            return returnVal;
        }
    }

    public Map<String, ConfiguredObjectAttribute<?, ?>> getAttributeTypes(final Class<? extends ConfiguredObject> clazz)
    {
        processClassIfNecessary(clazz);
        return _allAttributeTypes.get(clazz);
    }

    Map<String, AutomatedField> getAutomatedFields(Class<? extends ConfiguredObject> clazz)
    {
        processClassIfNecessary(clazz);
        return _allAutomatedFields.get(clazz);
    }

    Map<State, Map<State, Method>> getStateChangeMethods(final Class<? extends ConfiguredObject> objectClass)
    {
        processClassIfNecessary(objectClass);
        Map<State, Map<State, Method>> map = _stateChangeMethods.get(objectClass);

        return map != null ? Collections.unmodifiableMap(map) : Collections.<State, Map<State, Method>>emptyMap();
    }

    public Map<String, String> getDefaultContext()
    {
        return Collections.unmodifiableMap(_defaultContext);
    }


    public Set<Class<? extends ManagedInterface>> getManagedInterfaces(final Class<? extends ConfiguredObject> classObject)
    {
        processClassIfNecessary(classObject);
        Set<Class<? extends ManagedInterface>> interfaces = _allManagedInterfaces.get(classObject);
        return interfaces == null ? Collections.<Class<? extends ManagedInterface>>emptySet() : interfaces;
    }


    private <X extends ConfiguredObject> void processManagedInterfaces(Class<X> clazz)
    {
        final Set<Class<? extends ManagedInterface>> managedInterfaces = _allManagedInterfaces.get(clazz);
        for (Class<?> iface : clazz.getInterfaces())
        {
            if (iface.isAnnotationPresent(ManagedAnnotation.class) && ManagedInterface.class.isAssignableFrom(iface))
            {
                managedInterfaces.add((Class<? extends ManagedInterface>) iface);
            }
        }
    }

    public Collection<String> getValidChildTypes(Class<? extends ConfiguredObject> type,
                                                 Class<? extends ConfiguredObject> childType)
    {
        return getValidChildTypes(type, getCategory(childType).getSimpleName());
    }

    public Collection<String> getValidChildTypes(Class<? extends ConfiguredObject> type,
                                                 String childCategory)
    {
        final Map<String, Collection<String>> allValidChildTypes = _validChildTypes.get(getTypeClass(type));
        if (allValidChildTypes != null)
        {
            final Collection<String> validTypesForSpecificChild =
                    allValidChildTypes.get(childCategory);
            return validTypesForSpecificChild == null
                    ? null
                    : Collections.unmodifiableCollection(validTypesForSpecificChild);
        }
        else
        {
            return null;
        }
    }

    public Collection<ManagedContextDefault> getContextDependencies(Class<? extends ConfiguredObject> type)
    {
        Collection<String> dependencyNames = _contextUses.get(type);
        if (dependencyNames != null)
        {
            List<ManagedContextDefault> dependencies = new ArrayList<>(dependencyNames.size());
            for (String dependencyName : dependencyNames)
            {
                dependencies.add(_contextDefinitions.get(dependencyName));
            }
            return dependencies;
        }
        else
        {
            return Collections.emptySet();
        }
    }

    public Collection<ManagedContextDefault> getTypeSpecificContextDependencies(Class<? extends ConfiguredObject> type)
    {
        final Collection<ManagedContextDefault> contextDependencies = getContextDependencies(type);
        contextDependencies.removeAll(getContextDependencies(getCategory(type)));

        return contextDependencies;
    }
}
