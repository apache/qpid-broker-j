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

package org.apache.qpid.server.model.validator;

import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.util.ArrayUtils;
import org.apache.qpid.server.util.CollectionUtils;
import org.apache.qpid.server.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.SourceVersion;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class ValidatorResolver implements Resolver
{
    private static final class ValidValuesMethodSupplier implements Supplier<Collection<String>>
    {
        private final Method _method;

        ValidValuesMethodSupplier(Method method)
        {
            super();
            _method = Objects.requireNonNull(method);
        }

        @Override
        public Collection<String> get()
        {
            try
            {
                return (Collection<String>) _method.invoke(null);
            }
            catch (InvocationTargetException e)
            {
                LOGGER.warn("Could not invoke the validValues generation method " + _method.getName());
                LOGGER.debug("ValidValues '" + _method.getName() + "' method invocation error", e);
            }
            catch (IllegalAccessException e)
            {
                LOGGER.warn("Could not access the validValues generation method " + _method.getName());
                LOGGER.debug("ValidValues '" + _method.getName() + "' method access error", e);
            }
            return Collections.emptySet();
        }
    }

    private static final class AnnotationValidatorMethodSupplier implements Supplier<ValueValidator>
    {
        private final Method _method;

        AnnotationValidatorMethodSupplier(Method method)
        {
            _method = Objects.requireNonNull(method);
        }

        @Override
        public ValueValidator get()
        {
            try
            {
                return (ValueValidator) _method.invoke(null);
            }
            catch (InvocationTargetException e)
            {
                LOGGER.warn("Could not invoke the validator generation method " + _method.getName());
                LOGGER.debug("Validator factory method '" + _method.getName() + "' invocation error", e);
            }
            catch (IllegalAccessException e)
            {
                LOGGER.warn("Could not access the validator generation method " + _method.getName());
                LOGGER.debug("Validator factory method '" + _method.getName() + "' access error", e);
            }
            return null;
        }
    }

    private static final class AnnotationValidatorSupplier implements Supplier<ValueValidator>
    {
        private final Class<?> _clazz;

        AnnotationValidatorSupplier(Class<?> clazz)
        {
            _clazz = Objects.requireNonNull(clazz);
        }

        @Override
        public ValueValidator get()
        {
            try
            {
                return (ValueValidator) _clazz.newInstance();
            }
            catch (InstantiationException e)
            {
                LOGGER.warn("Could not instantiate the validator " + _clazz.getName());
                LOGGER.debug("Validator '" + _clazz.getName() + "' instantiation failed", e);
            }
            catch (IllegalAccessException e)
            {
                LOGGER.warn("Could not access the validator constructor " + _clazz.getName());
                LOGGER.debug("Validator '" + _clazz.getName() + "' constructor access error", e);
            }
            catch (RuntimeException e)
            {
                LOGGER.warn("Could not construct the validator " + _clazz.getName());
                LOGGER.debug("Validator '" + _clazz.getName() + "' construction error", e);
            }
            return null;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidatorResolver.class);

    private final String _attributeName;

    private final Class<?> _attributeType;

    private final String _configuredObjectName;

    private String _validValuePattern = "";

    private Supplier<Collection<String>> _validValuesSupplier = Collections::emptySet;

    private String _validator = "";

    private Supplier<ValueValidator> _annotationValidatorSupplier = () -> null;

    private ValueValidator _isMandatoryValidator = Optional.validator();

    private boolean _mandatoryAttribute = false;

    private boolean _immutableAttribute = false;

    public static Resolver newInstance(ManagedAttribute annotation, Class<?> configuredObject, Class<?> attributeType, String attributeName)
    {
        return new ValidatorResolver(configuredObject.getName(), attributeType, attributeName)
                .initValidValuePattern(annotation)
                .initChainValidator(annotation)
                .initValidValues(annotation)
                .initAnnotationValidator(annotation)
                .initImmutable(annotation);
    }

    public static Resolver newInstance(String validValuePattern, String[] validValues, boolean immutable, Class<?> attributeType, String attributeName)
    {
        return new ValidatorResolver(null, attributeType, attributeName)
                .initValidValuePattern(validValuePattern)
                .initValidValues(validValues)
                .initImmutable(immutable);
    }

    private ValidatorResolver(String configuredObjectName, Class<?> attributeType, String attributeName)
    {
        this._configuredObjectName = configuredObjectName;
        this._attributeName = Objects.requireNonNull(attributeName, "Attribute name is required");
        this._attributeType = Objects.requireNonNull(attributeType, "Attribute type is required");
    }

    @Override
    public Collection<String> validValues()
    {
        return _validValuesSupplier.get();
    }

    @Override
    public String validValuePattern()
    {
        return _validValuePattern;
    }

    @Override
    public String validator()
    {
        return _validator;
    }

    @Override
    public ValueValidator annotationValidator()
    {
        return _annotationValidatorSupplier.get();
    }

    @Override
    public boolean isMandatory()
    {
        return _mandatoryAttribute;
    }

    @Override
    public boolean isImmutable()
    {
        return _immutableAttribute;
    }

    @Override
    public ValueValidator validator(Function<Object, ?> valueConverter)
    {
        final Collection<?> validValues = validValues();
        final String validValuePattern = validValuePattern();

        if (!CollectionUtils.isEmpty(validValues))
        {
            return _isMandatoryValidator.andThen(ValidValues.validator(validValues, valueConverter));
        }
        if (!StringUtil.isEmpty(validValuePattern))
        {
            if (Collection.class.isAssignableFrom(_attributeType))
            {
                return _isMandatoryValidator.andThen(
                        org.apache.qpid.server.model.validator.Collection.validator(
                                Regex.validator(validValuePattern)));
            }
            else
            {
                return _isMandatoryValidator.andThen(Regex.validator(validValuePattern));
            }
        }
        return _isMandatoryValidator.andThen(annotationValidator());
    }

    private ValidatorResolver initAnnotationValidator(ManagedAttribute annotation)
    {
        return initAnnotationValidator(annotation.validator());
    }

    private ValidatorResolver initAnnotationValidator(String validator)
    {
        this._validator = StringUtil.trimToEmpty(validator);
        this._annotationValidatorSupplier = buildAnnotationValidatorSupplier(validator);
        return this;
    }

    private ValidatorResolver initValidValues(ManagedAttribute annotation)
    {
        return initValidValues(annotation.validValues());
    }

    private ValidatorResolver initValidValues(String[] validValues)
    {
        this._validValuesSupplier = buildValidValuesSupplier(validValues);
        return this;
    }

    private ValidatorResolver initChainValidator(ManagedAttribute annotation)
    {
        return initChainValidator(annotation.mandatory());
    }

    private ValidatorResolver initChainValidator(boolean isMandatory)
    {
        this._mandatoryAttribute = isMandatory;
        this._isMandatoryValidator = isMandatory ? Mandatory.validator() : Optional.validator();
        return this;
    }

    private ValidatorResolver initValidValuePattern(ManagedAttribute annotation)
    {
        return initValidValuePattern(annotation.validValuePattern());
    }

    private ValidatorResolver initValidValuePattern(String pattern)
    {
        this._validValuePattern = pattern;
        return this;
    }

    private ValidatorResolver initImmutable(ManagedAttribute annotation)
    {
        return initImmutable(annotation.immutable());
    }

    private ValidatorResolver initImmutable(boolean isImmutable)
    {
        this._immutableAttribute = isImmutable;
        return this;
    }

    private Supplier<Collection<String>> buildValidValuesSupplier(String[] validValues)
    {
        if (ArrayUtils.isEmpty(validValues))
        {
            return Collections::emptySet;
        }

        if (ArrayUtils.isSingle(validValues))
        {
            final Method method = findValidValuesFactoryMethod(validValues[0]);
            if (method != null)
            {
                return new ValidValuesMethodSupplier(method);
            }
        }
        final List<String> values = Arrays.asList(validValues);
        return () -> values;
    }

    private Method findValidValuesFactoryMethod(String validValue)
    {
        try
        {
            final String[] parts = validValue.split("#");
            final String className = parts[0].trim();
            if (validValue.contains("#") && SourceVersion.isName(className))
            {
                final String methodName = parts[1].split("\\(", 2)[0].trim();
                final Method method = Class.forName(className).getMethod(methodName);
                if (isValidValuesFactoryMethod(method))
                {
                    return method;
                }
            }
        }
        catch (ClassNotFoundException | NoSuchMethodException e)
        {
            if (_configuredObjectName != null)
            {
                LOGGER.warn("The validValues of the " + _attributeName + " attribute in class " + _configuredObjectName
                        + " has value '" + validValue + "' which looks like it should be a method,"
                        + " but no such method could be used");
            }
            else
            {
                LOGGER.warn("The validValues of the " + _attributeName
                        + " has value '" + validValue + "' which looks like it should be a method,"
                        + " but no such method could be used");
            }
            LOGGER.debug("The validValues '" + validValue + "' should be a method", e);
        }
        return null;
    }

    private Supplier<ValueValidator> buildAnnotationValidatorSupplier(String annotationValidator)
    {
        if (StringUtil.isEmpty(annotationValidator))
        {
            return () -> null;
        }
        final String validator = annotationValidator.trim();
        try
        {
            String className = validator;
            String methodName = null;

            if (validator.contains("#"))
            {
                final String[] parts = validator.split("#");

                className = parts[0].trim();
                methodName = parts[1].split("\\(", 2)[0].trim();
            }

            if (SourceVersion.isName(className))
            {
                final Class<?> validatorClass = Class.forName(className);
                final Method method = findValidatorFactoryMethod(methodName, validatorClass);

                if (method != null)
                {
                    return new AnnotationValidatorMethodSupplier(method);
                }

                if (ValueValidator.class.isAssignableFrom(validatorClass))
                {
                    return new AnnotationValidatorSupplier(validatorClass);
                }
            }
            LOGGER.warn("The validator of the " + _attributeName + " attribute in class " + _configuredObjectName
                    + " has value '" + validator + "' which should be a method or class,"
                    + " but no appropriate method or class can be found");
        }
        catch (ClassNotFoundException | NoSuchMethodException e)
        {
            LOGGER.warn("The validator of the " + _attributeName + " attribute in class " + _configuredObjectName
                    + " has value '" + validator + "' which should be a method or class,"
                    + " but no such method or class could be used");
            LOGGER.debug("The validator '" + validator + "' should be a method or class", e);
        }
        return () -> null;
    }

    private Method findValidatorFactoryMethod(String methodName, Class<?> validatorClass) throws NoSuchMethodException
    {
        if (StringUtil.isEmpty(methodName))
        {
            final List<Method> suggestedMethods = Arrays.stream(validatorClass.getDeclaredMethods())
                    .filter(this::isValidatorFactoryMethod)
                    .collect(Collectors.toList());
            if (CollectionUtils.isSingle(suggestedMethods))
            {
                return suggestedMethods.get(0);
            }
        }
        else
        {
            final Method method = validatorClass.getMethod(methodName);
            if (isValidatorFactoryMethod(method))
            {
                return method;
            }
        }
        return null;
    }

    private static boolean isValidValuesFactoryMethod(Method method)
    {
        if (isFactoryMethod(method) &&
                Collection.class.isAssignableFrom(method.getReturnType()) &&
                (method.getGenericReturnType() instanceof ParameterizedType))
        {
            final Type[] types = ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments();
            return (ArrayUtils.isSingle(types) && String.class.equals(types[0]));
        }
        return false;
    }

    private boolean isValidatorFactoryMethod(Method method)
    {
        return isFactoryMethod(method) &&
                ValueValidator.class.isAssignableFrom(method.getReturnType());
    }

    private static boolean isFactoryMethod(Method method)
    {
        return Modifier.isStatic(method.getModifiers()) &&
                Modifier.isPublic(method.getModifiers()) &&
                method.getParameterCount() == 0;
    }
}
