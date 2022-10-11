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
package org.apache.qpid.server.instrumentation.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.objectweb.asm.Type;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodNode;

/**
 * Bean holding method metadata
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class MethodDescription implements MemberDescription
{
    /**
     * Method declaring class, e.g. java/lang/Object
     */
    private final String _declaringClass;

    /**
     * Method name, e.g. equals
     */
    private final String _name;

    /**
     * Method return type, e.g. java/lang/Object or Z / B / I for primitives
     */
    private final String _returnType;

    /**
     * Method parameters
     */
    private final List<String> _parameters;

    private static final Map<String, Supplier<String>> TYPES = Map.of(
            "Z", () -> "boolean",
            "B", () -> "byte",
            "C", () -> "char",
            "D", () -> "double",
            "F", () -> "float",
            "I", () -> "int",
            "J", () -> "long",
            "S", () -> "short");

    private static final List<String> PRIMITIVES = List.of("B", "Z", "C", "D", "F", "I", "J", "S", "V");

    public MethodDescription(final String declaringClass,
                             final String name,
                             final String returnType,
                             final List<String> parameters)
    {
        _declaringClass = declaringClass;
        _name = name;
        _returnType = returnType;
        _parameters = new ArrayList<>(parameters);
    }

    public static MethodDescription of(final ClassNode classNode,
                                       final MethodNode methodNode)
    {
        final String declaringClass = classNode.name;
        final String methodName = methodNode.name;
        final String returnType = Type.getReturnType(methodNode.desc).getInternalName();
        final List<String> parameters = Arrays.stream(Type.getArgumentTypes(methodNode.desc))
                .map(Type::getInternalName).collect(Collectors.toList());
        return new MethodDescription(declaringClass, methodName, returnType, parameters);
    }

    public String getDeclaringClass()
    {
        return _declaringClass;
    }

    public String getName()
    {
        return _name;
    }

    public String getReturnType()
    {
        return _returnType;
    }

    public List<String> getParameters()
    {
        return new ArrayList<>(_parameters);
    }

    public String getParameter(int i)
    {
        return _parameters.get(i);
    }

    public int getParameterCount()
    {
        return _parameters.size();
    }

    public boolean returnsVoid()
    {
        return "V".equalsIgnoreCase(_returnType);
    }

    public boolean returnsPrimitive()
    {
        return PRIMITIVES.contains(_returnType);
    }

    public String getSignature()
    {
        return _declaringClass.replace('/', '.') + "#" + _name +
               (_parameters.isEmpty() ? "" : _parameters.stream()
                    .map(param -> param.lastIndexOf('/') == -1 ?
                            param : param.substring(param.lastIndexOf('/') + 1))
                    .map(param -> TYPES.getOrDefault(param, () -> param).get())
                    .collect(Collectors.joining()));
    }

    @Override
    public String toString()
    {
        return "MethodDescription{" +
               "declaringClass='" + _declaringClass + '\'' +
               ", name='" + _name + '\'' +
               ", returnType='" + _returnType + '\'' +
               '}';
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        final MethodDescription that = (MethodDescription) o;
        return Objects.equals(_declaringClass, that._declaringClass) &&
               Objects.equals(_name, that._name) &&
               Objects.equals(_parameters, that._parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(_declaringClass, _name, _parameters);
    }
}
