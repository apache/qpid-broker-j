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

package org.apache.qpid.server.model.validation;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@SupportedAnnotationTypes(ContentHeaderAnnotationValidator.CONTENT_HEADER_CLASS_NAME)
public class ContentHeaderAnnotationValidator extends AbstractProcessor
{
    public static final String CONTENT_HEADER_CLASS_NAME = "org.apache.qpid.server.model.RestContentHeader";
    public static final String CONTENT_CLASS_NAME = "org.apache.qpid.server.model.CustomRestHeaders";
    private static final Set<TypeKind> VALID_PRIMITIVE_TYPES = new HashSet<>(Arrays.asList(
            TypeKind.BOOLEAN,
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
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv)
    {
        Elements elementUtils = processingEnv.getElementUtils();
        TypeElement annotationElement = elementUtils.getTypeElement(CONTENT_HEADER_CLASS_NAME);

        for (Element e : roundEnv.getElementsAnnotatedWith(annotationElement))
        {
            ExecutableElement methodElement = (ExecutableElement) e;

            checkClassExtendsContent(annotationElement, methodElement);
            checkMethodHasNoArgs(annotationElement, methodElement);
            checkMethodReturnsString(annotationElement, methodElement);
        }

        return false;
    }

    private void checkMethodReturnsString(TypeElement annotationElement, ExecutableElement methodElement)
    {
        final TypeMirror returnType = methodElement.getReturnType();

        if (!isValidType(returnType))
        {
            processingEnv.getMessager()
                    .printMessage(Diagnostic.Kind.ERROR,
                            "@"
                                    + annotationElement.getSimpleName()
                                    + " can only be applied to methods with primitive, boxed primitive, enum, or String type return type but annotated Method "
                                    + methodElement.getSimpleName() + " returns "
                                    + returnType.toString(),
                            methodElement
                    );
        }
    }

    private boolean isValidType(TypeMirror type)
    {
        Types typeUtils = processingEnv.getTypeUtils();
        Elements elementUtils = processingEnv.getElementUtils();
        Element typeElement = typeUtils.asElement(type);

        if (VALID_PRIMITIVE_TYPES.contains(type.getKind()))
        {
            return true;
        }
        for (TypeKind primitive : VALID_PRIMITIVE_TYPES)
        {
            if (typeUtils.isSameType(type, typeUtils.boxedClass(typeUtils.getPrimitiveType(primitive)).asType()))
            {
                return true;
            }
        }
        if (typeElement != null && typeElement.getKind() == ElementKind.ENUM)
        {
            return true;
        }
        if (typeUtils.isSameType(type, elementUtils.getTypeElement("java.lang.String").asType()))
        {
            return true;
        }
        return false;
    }

    private void checkMethodHasNoArgs(TypeElement annotationElement, ExecutableElement methodElement)
    {
        if (!methodElement.getParameters().isEmpty())
        {
            processingEnv.getMessager()
                    .printMessage(Diagnostic.Kind.ERROR,
                            "@"
                                    + annotationElement.getSimpleName()
                                    + " can only be applied to methods without arguments"
                                    + " which does not apply to "
                                    + methodElement.getSimpleName(),
                            methodElement);
        }
    }

    private void checkClassExtendsContent(TypeElement annotationElement, Element e)
    {
        Types typeUtils = processingEnv.getTypeUtils();
        TypeMirror contentType = getErasure(CONTENT_CLASS_NAME);
        TypeElement parent = (TypeElement) e.getEnclosingElement();

        if (!typeUtils.isAssignable(typeUtils.erasure(parent.asType()), contentType))
        {
            processingEnv.getMessager()
                    .printMessage(Diagnostic.Kind.ERROR,
                            "@"
                                    + annotationElement.getSimpleName()
                                    + " can only be applied to methods within a class implementing "
                                    + contentType.toString()
                                    + " which does not apply to "
                                    + parent.asType().toString(),
                            e);
        }
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
