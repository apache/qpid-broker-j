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
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
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
            checkAnnotationIsOnInterface(annotationElement, e);
            checkAllMethodsAreAccessors(e);
        }
        return false;
    }

    private void checkAllMethodsAreAccessors(final Element e)
    {
        checkAllMethodsAreAccessors(e, new HashSet<Element>());
    }

    private void checkAllMethodsAreAccessors(final Element e, Set<Element> checked)
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
                String methodName = methodElement.getSimpleName().toString();

                if (methodName.length() < 3
                    || (methodName.length() < 4 && !methodName.startsWith("is"))
                    || !(methodName.startsWith("is") || methodName.startsWith("get") || methodName.startsWith("has"))
                    || !methodElement.getTypeParameters().isEmpty() )
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
            checkAllMethodsAreAccessors(processingEnv.getTypeUtils().asElement(mirror), checked);
        }
    }

    public void checkAnnotationIsOnInterface(final TypeElement annotationElement, final Element e)
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
        if(!processingEnv.getTypeUtils().isAssignable(e.asType(), processingEnv.getElementUtils().getTypeElement(MANAGED_ATTRIBUTE_VALUE_CLASS_NAME).asType()))
        {
            processingEnv.getMessager()
                    .printMessage(Diagnostic.Kind.ERROR,
                                  "@"
                                  + annotationElement.getSimpleName()
                                  + " can only be applied to an interface",
                                  e
                                 );
        }
    }

}
