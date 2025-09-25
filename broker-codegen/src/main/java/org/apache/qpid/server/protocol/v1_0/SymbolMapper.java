/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"; you may not use this file except in compliance
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;

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

        final List<VariableElement> symbolTextFields = processingEnv.getElementUtils().getTypeElement(SYMBOL_TEXTS)
                .getEnclosedElements()
                .stream()
                .filter(element -> element.getKind() == ElementKind.FIELD)
                .map(VariableElement.class::cast)
                .toList();

        final List<VariableElement> symbolFields = processingEnv.getElementUtils().getTypeElement(SYMBOLS)
                .getEnclosedElements()
                .stream()
                .filter(element -> element.getKind() == ElementKind.FIELD)
                .map(VariableElement.class::cast)
                .toList();

        for (final VariableElement field : symbolFields)
        {
            if (field.getModifiers().contains(Modifier.STATIC) && SYMBOL.equals(field.asType().toString()))
            {
                final String name = field.getSimpleName().toString();
                final Optional<VariableElement> optSymbolTextField = symbolTextFields.stream()
                        .filter(element -> element.getSimpleName().toString().equals(name))
                        .findFirst();
                if (optSymbolTextField.isPresent() && optSymbolTextField.get().getConstantValue() != null)
                {
                    NAME_MAPPING.put(String.valueOf(optSymbolTextField.get().getConstantValue()), "Symbols.%s".formatted(name));
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
}
