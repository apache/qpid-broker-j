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

package org.apache.qpid.server.protocol.v1_0;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;

public final class SymbolMapper
{
    private SymbolMapper()
    {

    }

    private static final String SYMBOL = "org.apache.qpid.server.protocol.v1_0.type.Symbol";
    private static final String SYMBOLS = "org.apache.qpid.server.protocol.v1_0.constants.Symbols";
    private static final String SYMBOL_TEXTS = "org.apache.qpid.server.protocol.v1_0.constants.SymbolTexts";
    private static final Map<String, String> NAME_MAPPING = new HashMap<>();

    static void initialize(final ProcessingEnvironment processingEnv)
    {
        if (!NAME_MAPPING.isEmpty())
        {
            return;
        }

        final List<VariableElement> symbolTextFields = getAllFields(processingEnv, SYMBOL_TEXTS);
        final List<VariableElement> symbolFields = getAllFields(processingEnv, SYMBOLS);
        final Map<String, VariableElement> symbolTextFieldsByName = new HashMap<>();

        for (final VariableElement field : symbolTextFields)
        {
            final String name = field.getSimpleName().toString();
            final VariableElement previous = symbolTextFieldsByName.putIfAbsent(name, field);
            if (previous != null)
            {
                throw new RuntimeException("Duplicate field name " + name + " found in SymbolTexts");
            }
        }

        for (final VariableElement field : symbolFields)
        {
            if (field.getModifiers().contains(Modifier.STATIC) && SYMBOL.equals(field.asType().toString()))
            {
                final String name = field.getSimpleName().toString();
                final VariableElement symbolTextField = symbolTextFieldsByName.get(name);
                if (symbolTextField != null && symbolTextField.getConstantValue() != null)
                {
                    NAME_MAPPING.put(String.valueOf(symbolTextField.getConstantValue()), "Symbols.%s".formatted(name));
                }
                else
                {
                    throw new RuntimeException("Field name " + name + " exists in Symbols but not in SymbolTexts. " +
                            "All Symbols field names must have a matching field name in SymbolTexts");
                }

            }
        }
    }

    static String getConstantName(String symbolText)
    {
        return NAME_MAPPING.getOrDefault(symbolText, "Symbol.valueOf(\"%s\")".formatted(symbolText));
    }

    private static List<VariableElement> getAllFields(final ProcessingEnvironment processingEnv, final String typeName)
    {
        final TypeElement typeElement = processingEnv.getElementUtils().getTypeElement(typeName);
        if (typeElement == null)
        {
            throw new RuntimeException("Unable to resolve type " + typeName + " during SymbolMapper initialization");
        }

        final List<VariableElement> fields = new ArrayList<>();
        collectFields(typeElement, fields, new HashSet<>());
        return fields;
    }

    private static void collectFields(final TypeElement typeElement,
                                      final List<VariableElement> fields,
                                      final Set<String> visited)
    {
        final String typeName = typeElement.getQualifiedName().toString();
        if (!visited.add(typeName))
        {
            return;
        }

        for (final Element element : typeElement.getEnclosedElements())
        {
            if (element.getKind() == ElementKind.FIELD)
            {
                fields.add((VariableElement) element);
            }
        }

        for (final TypeMirror iface : typeElement.getInterfaces())
        {
            if (iface instanceof DeclaredType)
            {
                final Element element = ((DeclaredType) iface).asElement();
                if (element instanceof TypeElement)
                {
                    collectFields((TypeElement) element, fields, visited);
                }
            }
        }
    }
}
