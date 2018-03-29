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


import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;

@SupportedAnnotationTypes(ManagedAttributeValueTypeValidator.MANAGED_ATTRIBUTE_VALUE_TYPE_CLASS_NAME)
public class ManagedAttributeValueTypeValidator extends AbstractProcessor
{

    public static final String MANAGED_ATTRIBUTE_VALUE_TYPE_CLASS_NAME =
            "org.apache.qpid.server.model.ManagedAttributeValueType";

    public static final String MANAGED_ATTRIBUTE_VALUE_CLASS_NAME =
            "org.apache.qpid.server.model.ManagedAttributeValue";

    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latest();
    }

    @Override
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv)
    {
        Elements elementUtils = processingEnv.getElementUtils();
        TypeElement annotationElement = elementUtils.getTypeElement(MANAGED_ATTRIBUTE_VALUE_TYPE_CLASS_NAME);

        for (Element e : roundEnv.getElementsAnnotatedWith(annotationElement))
        {
            boolean isAbstract = isAbstract(annotationElement, e);
            if(!isAbstract)
            {
                checkAnnotationIsOnInterface(annotationElement, e);
            }
            if(!isContent(e))
            {
                checkAllMethodsAreAccessors(e, isAbstract);
            }
        }
        return false;
    }

    private boolean isContent(final Element e)
    {
        return e.equals(processingEnv.getElementUtils().getTypeElement("org.apache.qpid.server.model.Content"));
    }

    private void checkAllMethodsAreAccessors(final Element e, final boolean isAbstract)
    {
        checkAllMethodsAreAccessors(e, new HashSet<Element>(), isAbstract);
    }

    private void checkAllMethodsAreAccessors(final Element e, Set<Element> checked, final boolean isAbstract)
    {

        if(!checked.add(e))
        {
            return;
        }

        final Name annotationName = processingEnv.getElementUtils()
                .getTypeElement(MANAGED_ATTRIBUTE_VALUE_TYPE_CLASS_NAME)
                .getSimpleName();

        for(Element memberElement : e.getEnclosedElements())
        {
            if(memberElement instanceof ExecutableElement)
            {
                final ExecutableElement methodElement = (ExecutableElement) memberElement;
                AttributeAnnotationValidator.isValidType(processingEnv, methodElement.getReturnType(), false);
                if(isNotAccessorMethod(methodElement)
                   && !isValidFactoryMethod(methodElement, e, isAbstract)
                   && methodElement.getKind() != ElementKind.CONSTRUCTOR)
                {
                    processingEnv.getMessager()
                            .printMessage(Diagnostic.Kind.ERROR,
                                          "Methods in an @" + annotationName
                                          + " interface can only be applied to methods which of the form getXXX(), isXXX() or hasXXX()",
                                          methodElement
                                         );
                }
            }
        }
        final List<? extends TypeMirror> interfaces = ((TypeElement) e).getInterfaces();
        for(TypeMirror mirror : interfaces)
        {
            checkAllMethodsAreAccessors(processingEnv.getTypeUtils().asElement(mirror), checked, isAbstract);
        }
    }

    private boolean isNotAccessorMethod(final ExecutableElement methodElement)
    {
        String methodName = methodElement.getSimpleName().toString();
        return methodName.length() < 3
                || (methodName.length() < 4 && !methodName.startsWith("is"))
                || !(methodName.startsWith("is") || methodName.startsWith("get") || methodName.startsWith("has"))
                || !methodElement.getTypeParameters().isEmpty();
    }

    private boolean isValidFactoryMethod(final ExecutableElement methodElement,
                                         final Element typeElement,
                                         final boolean isAbstract)
    {

        if (!isAbstract
            && methodElement.getSimpleName().toString().equals("newInstance")
            && methodElement.getModifiers().contains(Modifier.STATIC)
            && processingEnv.getTypeUtils().asElement(methodElement.getReturnType()) != null
            && processingEnv.getTypeUtils().asElement(methodElement.getReturnType()).equals(typeElement)
            && methodElement.getParameters().size() == 1
            && processingEnv.getTypeUtils()
                            .asElement(methodElement.getParameters().iterator().next().asType())
                            .equals(typeElement))
        {
            TypeElement annotationElement = processingEnv.getElementUtils()
                                                         .getTypeElement("org.apache.qpid.server.model.ManagedAttributeValueTypeFactoryMethod");

            return methodElement.getAnnotationMirrors()
                                .stream()
                                .anyMatch(a -> processingEnv.getTypeUtils().isSameType(a.getAnnotationType(),
                                                                                       annotationElement.asType()));
        }
        return false;
    }

    private void checkAnnotationIsOnInterface(final TypeElement annotationElement, final Element e)
    {
        if (e.getKind() != ElementKind.INTERFACE)
        {
            processingEnv.getMessager()
                         .printMessage(Diagnostic.Kind.ERROR,
                                       "@"
                                       + annotationElement.getSimpleName()
                                       + " can only be applied to an interface",
                                       e
                                      );
        }
        if (!processingEnv.getTypeUtils()
                          .isAssignable(e.asType(),
                                        processingEnv.getElementUtils()
                                                     .getTypeElement(MANAGED_ATTRIBUTE_VALUE_CLASS_NAME)
                                                     .asType()))
        {
            processingEnv.getMessager()
                         .printMessage(Diagnostic.Kind.ERROR,
                                       "@"
                                       + annotationElement.getSimpleName()
                                       + " can only be applied to an interface which extends " + MANAGED_ATTRIBUTE_VALUE_CLASS_NAME,
                                       e
                                      );
        }

    }

    private boolean isAbstract(final TypeElement annotationElement, final Element typeElement)
    {
        for (AnnotationMirror annotation : typeElement.getAnnotationMirrors())
        {
            if (annotation.getAnnotationType().asElement().equals(annotationElement))
            {

                Map<? extends ExecutableElement, ? extends AnnotationValue> annotationValues =
                        processingEnv.getElementUtils().getElementValuesWithDefaults(annotation);
                for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> element : annotationValues.entrySet())
                {
                    if ("isAbstract".contentEquals(element.getKey().getSimpleName()))
                    {
                        return element.getValue().getValue().equals(Boolean.TRUE);
                    }
                }
                break;
            }
        }
        return false;
    }
}
