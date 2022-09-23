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

import java.util.Objects;

import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;

/**
 * Bean holding automated field metadata
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class AutomatedFieldDescription implements MemberDescription
{
    private final String _declaringClass;
    private final String _name;
    private final MethodDescription _beforeSet;
    private final MethodDescription _afterSet;

    public AutomatedFieldDescription(final String declaringClass,
                                     final String name,
                                     final MethodDescription beforeSet,
                                     final MethodDescription afterSet)
    {
        _declaringClass = declaringClass;
        _name = name;
        _beforeSet = beforeSet;
        _afterSet = afterSet;
    }

    public static AutomatedFieldDescription of(final ClassNode classNode,
                                               final FieldNode fieldNode,
                                               final AnnotationNode annotationNode)
    {
        final String declaringClass = classNode.name;
        final String fieldName = fieldNode.name;
        MethodDescription beforeSet = null;
        MethodDescription afterSet = null;
        if (annotationNode.values != null)
        {
            for (int i = 0; i < annotationNode.values.size(); i++)
            {
                final Object name = annotationNode.values.get(i);
                if ("beforeSet".equals(name))
                {
                    beforeSet = createMethodDescription(classNode, annotationNode, i);
                }
                if ("afterSet".equals(name))
                {
                    afterSet = createMethodDescription(classNode, annotationNode, i);
                }
            }
        }
        return new AutomatedFieldDescription(declaringClass, fieldName, beforeSet, afterSet);
    }

    private static MethodDescription createMethodDescription(final ClassNode classNode,
                                                             final AnnotationNode annotationNode,
                                                             final int i)
    {
        final String beforeSetName = (String) annotationNode.values.get(i + 1);
        return classNode.methods.stream()
                .filter(methodNode -> Objects.equals(beforeSetName, methodNode.name))
                .findFirst().map(node -> MethodDescription.of(classNode, node)).orElse(null);
    }

    public String getDeclaringClass()
    {
        return _declaringClass;
    }

    public String getName()
    {
        return _name;
    }

    public String getSignature()
    {
        return _declaringClass.replace('/', '.') + "#" + _name;
    }

    public MethodDescription getBeforeSet()
    {
        return _beforeSet;
    }

    public MethodDescription getAfterSet()
    {
        return _afterSet;
    }

    @Override
    public String toString()
    {
        return "AutomatedFieldDescription{" +
               "_declaringClass='" + _declaringClass + '\'' +
               ", _name='" + _name + '\'' +
               ", _beforeSet=" + _beforeSet +
               ", _afterSet=" + _afterSet +
               '}';
    }
}
