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
package org.apache.qpid.server.model.validation;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;


@SupportedAnnotationTypes({AttributeAnnotationValidator.MANAGED_ATTRIBUTE_CLASS_NAME,
                           AttributeAnnotationValidator.DERIVED_ATTRIBUTE_CLASS_NAME,
                           AttributeAnnotationValidator.MANAGED_STATISTIC_CLASS_NAME})
public class AttributeAnnotationValidator extends AbstractProcessor
{

    public static final String MANAGED_ATTRIBUTE_CLASS_NAME = "org.apache.qpid.server.model.ManagedAttribute";
    public static final String DERIVED_ATTRIBUTE_CLASS_NAME = "org.apache.qpid.server.model.DerivedAttribute";

    public static final String MANAGED_STATISTIC_CLASS_NAME = "org.apache.qpid.server.model.ManagedStatistic";





    private static final Set<TypeKind> VALID_PRIMITIVE_TYPES = new HashSet<>(Arrays.asList(TypeKind.BOOLEAN,
                                                                                           TypeKind.BYTE,
                                                                                           TypeKind.CHAR,
                                                                                           TypeKind.DOUBLE,
                                                                                           TypeKind.FLOAT,
                                                                                           TypeKind.INT,
                                                                                           TypeKind.LONG,
                                                                                           TypeKind.SHORT));
    private Elements elementUtils;
    private Types typeUtils;
    private Messager messager;

    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latest();
    }

    @Override
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv)
    {
        elementUtils = processingEnv.getElementUtils();
        typeUtils = processingEnv.getTypeUtils();
        messager = processingEnv.getMessager();

        processAttributes(roundEnv, MANAGED_ATTRIBUTE_CLASS_NAME, false, false);
        processAttributes(roundEnv, DERIVED_ATTRIBUTE_CLASS_NAME, true, true);

        processStatistics(roundEnv, MANAGED_STATISTIC_CLASS_NAME);

        return false;
    }

    public void processAttributes(final RoundEnvironment roundEnv,
                                  String elementName,
                                  final boolean allowedNamed,
                                  final boolean allowAbstractManagedTypes)
    {

        TypeElement annotationElement = elementUtils.getTypeElement(elementName);

        for (Element e : roundEnv.getElementsAnnotatedWith(annotationElement))
        {
            checkAnnotationIsOnMethodInInterface(annotationElement, e);

            ExecutableElement methodElement = (ExecutableElement) e;

            checkInterfaceExtendsConfiguredObject(annotationElement, methodElement);
            checkMethodTakesNoArgs(annotationElement, methodElement);
            checkMethodName(annotationElement, methodElement);
            checkMethodReturnType(annotationElement, methodElement, allowedNamed, allowAbstractManagedTypes);

            checkTypeAgreesWithName(annotationElement, methodElement);

            if(MANAGED_ATTRIBUTE_CLASS_NAME.equals(elementName))
            {
                checkValidValuesPatternOnAppropriateTypes(methodElement);
            }
        }
    }

    void checkValidValuesPatternOnAppropriateTypes(final ExecutableElement methodElement)
    {
        AnnotationMirror annotationMirror = getAnnotationMirror(methodElement, MANAGED_ATTRIBUTE_CLASS_NAME);

        if (hasValidValuePattern(annotationMirror)
            && !isStringOrCollectionOfStrings(methodElement.getReturnType()))
        {
            messager.printMessage(Diagnostic.Kind.ERROR,
                                  "@ManagedAttribute return type does not not support validValuePattern: "
                                    + methodElement.getReturnType().toString(),
                                  methodElement);
        }
    }

    private AnnotationMirror getAnnotationMirror(final ExecutableElement methodElement, final String annotationClassName)
    {
        for (AnnotationMirror annotationMirror : methodElement.getAnnotationMirrors())
        {
            Element annotationAsElement = annotationMirror.getAnnotationType().asElement();
            if (annotationClassName.equals(getFullyQualifiedName(annotationAsElement)))
            {
                return annotationMirror;
            }
        }
        return null;
    }


    private boolean hasValidValuePattern(final AnnotationMirror annotationMirror)
    {
        for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry : annotationMirror.getElementValues()
                .entrySet())
        {
            if ("validValuePattern".equals(entry.getKey().getSimpleName().toString()))
            {
                return true;
            }
        }
        return false;
    }

    private String getFullyQualifiedName(final Element element)
    {
        return elementUtils.getPackageOf(element).getQualifiedName().toString() + "." + element.getSimpleName().toString();
    }

    private boolean isStringOrCollectionOfStrings(TypeMirror type)
    {

        TypeMirror stringType = elementUtils.getTypeElement("java.lang.String").asType();
        TypeMirror collectionType = elementUtils.getTypeElement("java.util.Collection").asType();

        TypeElement typeAsElement = (TypeElement) typeUtils.asElement(type);

        return typeUtils.isAssignable(type, stringType)
                || (typeAsElement != null
                    && typeAsElement.getTypeParameters().size() == 1
                    && "java.lang.String".equals(getFullyQualifiedName(getErasedParameterType((DeclaredType) type, 0)))
                    && typeUtils.isAssignable(typeUtils.erasure(typeAsElement.asType()), collectionType));
    }

    private Element getErasedParameterType(final DeclaredType returnType, final int parameterIndex)
    {
        return typeUtils.asElement(typeUtils.erasure(returnType.getTypeArguments().get(parameterIndex)));
    }

    public void processStatistics(final RoundEnvironment roundEnv,
                                  String elementName)
    {
        TypeElement annotationElement = elementUtils.getTypeElement(elementName);

        for (Element e : roundEnv.getElementsAnnotatedWith(annotationElement))
        {
            checkAnnotationIsOnMethodInInterface(annotationElement, e);

            ExecutableElement methodElement = (ExecutableElement) e;

            checkInterfaceExtendsConfiguredObject(annotationElement, methodElement);
            checkMethodTakesNoArgs(annotationElement, methodElement);
            checkMethodName(annotationElement, methodElement);
            checkTypeAgreesWithName(annotationElement, methodElement);
            checkMethodReturnTypeIsNumberOrDate(annotationElement, methodElement);

        }
    }

    private void checkMethodReturnTypeIsNumberOrDate(final TypeElement annotationElement,
                                                     final ExecutableElement methodElement)
    {
        TypeMirror numberType = elementUtils.getTypeElement("java.lang.Number").asType();
        TypeMirror dateType = elementUtils.getTypeElement("java.util.Date").asType();
        if(!typeUtils.isAssignable(methodElement.getReturnType(),numberType)
                && !typeUtils.isSameType(methodElement.getReturnType(), dateType))
        {
            messager.printMessage(Diagnostic.Kind.ERROR,
                                  "@"
                                  + annotationElement.getSimpleName()
                                  + " return type does not extend java.lang.Number"
                                  + " and is not java.util.Date : "
                                  + methodElement.getReturnType().toString(),
                                  methodElement
                                 );
        }
    }

    public void checkTypeAgreesWithName(final TypeElement annotationElement, final ExecutableElement methodElement)
    {
        String methodName = methodElement.getSimpleName().toString();

        if((methodName.startsWith("is") || methodName.startsWith("has"))
            && !(methodElement.getReturnType().getKind() == TypeKind.BOOLEAN
                 || typeUtils.isSameType(typeUtils.boxedClass(typeUtils.getPrimitiveType(TypeKind.BOOLEAN)).asType(), methodElement.getReturnType())))
        {
            messager.printMessage(Diagnostic.Kind.ERROR,
                                  "@"
                                  + annotationElement.getSimpleName()
                                  + " return type is not boolean or Boolean: "
                                  + methodElement.getReturnType().toString(),
                                  methodElement
                                 );
        }
    }

    public void checkMethodReturnType(final TypeElement annotationElement,
                                      final ExecutableElement methodElement,
                                      final boolean allowNamed,
                                      final boolean allowAbstractManagedTypes)
    {
        if (!(isValidType(processingEnv, methodElement.getReturnType(), allowAbstractManagedTypes)
              || (allowNamed && isNamed(methodElement.getReturnType()))))
        {
            messager.printMessage(Diagnostic.Kind.ERROR,
                                  "@"
                                  + annotationElement.getSimpleName()
                                  + " cannot be applied to methods with return type "
                                  + methodElement.getReturnType().toString(),
                                  methodElement
                                 );
        }
    }

    public void checkMethodName(final TypeElement annotationElement, final ExecutableElement methodElement)
    {
        String methodName = methodElement.getSimpleName().toString();

        if (methodName.length() < 3
            || (methodName.length() < 4 && !methodName.startsWith("is"))
            || !(methodName.startsWith("is") || methodName.startsWith("get") || methodName.startsWith("has")))
        {
            messager.printMessage(Diagnostic.Kind.ERROR,
                                  "@"
                                  + annotationElement.getSimpleName()
                                  + " can only be applied to methods which of the form getXXX(), isXXX() or hasXXX()",
                                  methodElement
                                 );
        }
    }

    public void checkMethodTakesNoArgs(final TypeElement annotationElement, final ExecutableElement methodElement)
    {
        if (!methodElement.getParameters().isEmpty())
        {
            messager.printMessage(Diagnostic.Kind.ERROR,
                                  "@"
                                  + annotationElement.getSimpleName()
                                  + " can only be applied to methods which take no parameters",
                                  methodElement
                                 );
        }
    }

    public void checkInterfaceExtendsConfiguredObject(final TypeElement annotationElement, final Element e)
    {
        TypeMirror configuredObjectType = getErasure("org.apache.qpid.server.model.ConfiguredObject");
        TypeElement parent = (TypeElement) e.getEnclosingElement();


        if (!typeUtils.isAssignable(typeUtils.erasure(parent.asType()), configuredObjectType))
        {
            messager.printMessage(Diagnostic.Kind.ERROR,
                                  "@"
                                  + annotationElement.getSimpleName()
                                  + " can only be applied to methods within an interface which extends "
                                  + configuredObjectType.toString()
                                  + " which does not apply to "
                                  + parent.asType().toString(),
                                  e);
        }
    }

    public void checkAnnotationIsOnMethodInInterface(final TypeElement annotationElement, final Element e)
    {
        if (e.getKind() != ElementKind.METHOD || e.getEnclosingElement().getKind() != ElementKind.INTERFACE)
        {
            messager.printMessage(Diagnostic.Kind.ERROR,
                                  "@"
                                  + annotationElement.getSimpleName()
                                  + " can only be applied to methods within an interface",
                                  e
                                 );
        }
    }

    static boolean isValidType(ProcessingEnvironment processingEnv,
                               final TypeMirror type, final boolean allowAbstractManagedTypes)
    {
        Types typeUtils = processingEnv.getTypeUtils();
        Elements elementUtils = processingEnv.getElementUtils();
        Element typeElement = typeUtils.asElement(type);

        if (VALID_PRIMITIVE_TYPES.contains(type.getKind()))
        {
            return true;
        }
        for(TypeKind primitive : VALID_PRIMITIVE_TYPES)
        {
            if(typeUtils.isSameType(type, typeUtils.boxedClass(typeUtils.getPrimitiveType(primitive)).asType()))
            {
                return true;
            }
        }
        if(typeElement != null && typeElement.getKind()==ElementKind.ENUM)
        {
            return true;
        }

        String className = "org.apache.qpid.server.model.ConfiguredObject";
        TypeMirror configuredObjectType = getErasure(processingEnv, className);

        if(typeUtils.isAssignable(typeUtils.erasure(type), configuredObjectType))
        {
            return true;
        }

        final TypeElement managedAttributeTypeValueElement =
                elementUtils.getTypeElement(ManagedAttributeValueTypeValidator.MANAGED_ATTRIBUTE_VALUE_TYPE_CLASS_NAME);
        if(typeElement != null)
        {
            for (AnnotationMirror annotation : typeElement.getAnnotationMirrors())
            {
                if (annotation.getAnnotationType().asElement().equals(managedAttributeTypeValueElement))
                {
                    if(allowAbstractManagedTypes)
                    {
                        return true;
                    }
                    else
                    {
                        final Map<? extends ExecutableElement, ? extends AnnotationValue> annotationValues =
                                elementUtils.getElementValuesWithDefaults(annotation);
                        for(Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> element : annotationValues.entrySet())
                        {
                            if("isAbstract".contentEquals(element.getKey().getSimpleName()))
                            {
                                return element.getValue().getValue().equals(Boolean.FALSE);
                            }
                        }
                        return false;
                    }
                }
            }
        }
        if(typeUtils.isSameType(type,elementUtils.getTypeElement("java.lang.Object").asType()))
        {
            return true;
        }


        if(typeUtils.isSameType(type, elementUtils.getTypeElement("java.lang.String").asType()))
        {
            return true;
        }


        if(typeUtils.isSameType(type,elementUtils.getTypeElement("java.util.UUID").asType()))
        {
            return true;
        }

        if(typeUtils.isSameType(type,elementUtils.getTypeElement("java.util.Date").asType()))
        {
            return true;
        }

        if(typeUtils.isSameType(type,elementUtils.getTypeElement("java.net.URI").asType()))
        {
            return true;
        }

        if(typeUtils.isSameType(type,elementUtils.getTypeElement("java.security.cert.Certificate").asType()))
        {
            return true;
        }

        if(typeUtils.isSameType(type,elementUtils.getTypeElement("java.security.Principal").asType()))
        {
            return true;
        }

        TypeMirror erasedType = typeUtils.erasure(type);
        if(typeUtils.isSameType(erasedType, getErasure(processingEnv, "java.util.List"))
                || typeUtils.isSameType(erasedType, getErasure(processingEnv, "java.util.Set"))
                || typeUtils.isSameType(erasedType, getErasure(processingEnv, "java.util.Collection")))
        {
            for(TypeMirror paramType : ((DeclaredType)type).getTypeArguments())
            {
                if(!isValidType(processingEnv, paramType, allowAbstractManagedTypes))
                {
                    return false;
                }
            }
            return true;
        }

        if(typeUtils.isSameType(erasedType, getErasure(processingEnv, "java.util.Map")))
        {
            List<? extends TypeMirror> args = ((DeclaredType) type).getTypeArguments();
            if (args.size() != 2)
            {
                throw new IllegalArgumentException("Map types " + type + " must have exactly two type arguments");
            }
            return isValidType(processingEnv, args.get(0), false)
                   && (isValidType(processingEnv, args.get(1), false)
                       || typeUtils.isSameType(args.get(1), getErasure(processingEnv, "java.lang.Object")));
        }

        return false;
    }

    private boolean isNamed(final TypeMirror type)
    {
        return isNamed(processingEnv, type);
    }

    static boolean isNamed(ProcessingEnvironment processingEnv,
                               final TypeMirror type)
    {
        Types typeUtils = processingEnv.getTypeUtils();

        String className = "org.apache.qpid.server.model.Named";
        TypeMirror namedType = getErasure(processingEnv, className);

        return typeUtils.isAssignable(typeUtils.erasure(type), namedType);

    }


    private TypeMirror getErasure(final String className)
    {
        return getErasure(processingEnv, className);
    }

    private static TypeMirror getErasure(ProcessingEnvironment processingEnv, final String className)
    {
        final Types typeUtils = processingEnv.getTypeUtils();
        final Elements elementUtils = processingEnv.getElementUtils();
        return typeUtils.erasure(elementUtils.getTypeElement(className).asType());
    }

}
