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
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;


@SupportedAnnotationTypes({OperationAnnotationValidator.MANAGED_OPERATION_CLASS_NAME,
                           OperationAnnotationValidator.OPERATION_PARAM_CLASS_NAME})
public class OperationAnnotationValidator extends AbstractProcessor
{

    public static final String MANAGED_OPERATION_CLASS_NAME = "org.apache.qpid.server.model.ManagedOperation";
    public static final String OPERATION_PARAM_CLASS_NAME = "org.apache.qpid.server.model.Param";


    private static final Set<TypeKind> VALID_PRIMITIVE_TYPES = new HashSet<>(Arrays.asList(TypeKind.BOOLEAN,
                                                                                           TypeKind.BYTE,
                                                                                           TypeKind.CHAR,
                                                                                           TypeKind.DOUBLE,
                                                                                           TypeKind.FLOAT,
                                                                                           TypeKind.INT,
                                                                                           TypeKind.LONG,
                                                                                           TypeKind.SHORT));

    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latest();
    }

    @Override
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv)
    {

        Elements elementUtils = processingEnv.getElementUtils();
        TypeElement annotationElement = elementUtils.getTypeElement(MANAGED_OPERATION_CLASS_NAME);

        for (Element e : roundEnv.getElementsAnnotatedWith(annotationElement))
        {
            checkAnnotationIsOnMethodInInterface(annotationElement, e);

            ExecutableElement methodElement = (ExecutableElement) e;

            checkInterfaceExtendsConfiguredObject(annotationElement, methodElement);
            checkMethodArgsAreValid(annotationElement, methodElement);
            checkMethodReturnType(annotationElement, methodElement);

        }
        return false;
    }

    public void checkMethodReturnType(final TypeElement annotationElement, final ExecutableElement methodElement)
    {
        final TypeMirror returnType = methodElement.getReturnType();
        if (!(returnType.getKind() == TypeKind.VOID || isValidType(returnType, true)))
        {
            processingEnv.getMessager()
                    .printMessage(Diagnostic.Kind.ERROR,
                                  "@"
                                  + annotationElement.getSimpleName()
                                  + " cannot be applied to methods with return type "
                                  + returnType.toString(),
                                  methodElement
                                 );
        }
    }

    public void checkMethodArgsAreValid(final TypeElement annotationElement, final ExecutableElement methodElement)
    {
        Elements elementUtils = processingEnv.getElementUtils();
        TypeElement paramElement = elementUtils.getTypeElement(OPERATION_PARAM_CLASS_NAME);

        for (VariableElement varElem : methodElement.getParameters())
        {
            if(!isValidType(varElem.asType(), false))
            {
                processingEnv.getMessager()
                             .printMessage(Diagnostic.Kind.ERROR,
                                           "@"
                                           + paramElement.getSimpleName()
                                           + " cannot be applied to variables of type "
                                           + varElem.asType().toString(),
                                           methodElement
                                          );
            }
            String name = varElem.getSimpleName().toString();
            final List<? extends AnnotationMirror> annotationMirrors = varElem.getAnnotationMirrors();
            AnnotationMirror paramAnnotation = null;
            for(AnnotationMirror annotationMirror : annotationMirrors)
            {
                if(annotationMirror.getAnnotationType().asElement().equals(paramElement))
                {
                    paramAnnotation = annotationMirror;
                    break;
                }
            }
            if(paramAnnotation == null)
            {
                processingEnv.getMessager()
                        .printMessage(Diagnostic.Kind.ERROR,
                                      "Argument " + name + " of " + methodElement.getSimpleName()
                                      + " must be annotated with @"
                                      + paramElement.getSimpleName()
                                      + " or the method should not be annotated with @"
                                      + annotationElement.getSimpleName()
                                     );
            }
            else
            {
                String paramName = null;

                for(Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry : paramAnnotation.getElementValues().entrySet())
                {
                    if(entry.getKey().getSimpleName().toString().equals("name"))
                    {
                        paramName = String.valueOf(entry.getValue().getValue());
                    }
                }

                if(!name.equals(paramName))
                {
                    processingEnv.getMessager()
                            .printMessage(Diagnostic.Kind.ERROR,
                                          "Argument " + name + " of " + methodElement.getSimpleName()
                                          + " is annotated with @"
                                          + paramElement.getSimpleName()
                                          + "( name = \""
                                          + paramName
                                          + "\") the name must match the actual parameter name, i.e. it should read @"

                                          + paramElement.getSimpleName()
                                          + "( name = \""
                                          + name
                                          + "\")"
                                         );
                }

            }
        }
    }

    public void checkInterfaceExtendsConfiguredObject(final TypeElement annotationElement, final Element e)
    {
        Types typeUtils = processingEnv.getTypeUtils();
        TypeMirror configuredObjectType = getErasure("org.apache.qpid.server.model.ConfiguredObject");
        TypeElement parent = (TypeElement) e.getEnclosingElement();


        if (!typeUtils.isAssignable(typeUtils.erasure(parent.asType()), configuredObjectType))
        {
            processingEnv.getMessager()
                    .printMessage(Diagnostic.Kind.ERROR,
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
            processingEnv.getMessager()
                    .printMessage(Diagnostic.Kind.ERROR,
                                  "@"
                                  + annotationElement.getSimpleName()
                                  + " can only be applied to methods within an interface",
                                  e
                                 );
        }
    }

    boolean isValidType(final TypeMirror type, final boolean allowAbstractManagedTypes)
    {
        return AttributeAnnotationValidator.isValidType(processingEnv, type, allowAbstractManagedTypes);
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
