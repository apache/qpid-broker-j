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
package org.apache.qpid.server.instrumentation.transformer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.MethodNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.instrumentation.metadata.AutomatedFieldDescription;
import org.apache.qpid.server.instrumentation.metadata.MethodDescription;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

/**
 * Class transformer entry point picking up which classes to transform and passing the to delegate transformers
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class QpidClassFileTransformer implements ClassFileTransformer
{
    /** Qualified class names */
    private static final String CO_METHOD_ATTRIBUTE_OR_STATISTICS =
            "org/apache/qpid/server/model/ConfiguredObjectMethodAttributeOrStatistic";
    private static final String CO_METHOD_OPERATION = "org/apache/qpid/server/model/ConfiguredObjectMethodOperation";
    private static final String CO_TYPE_REGISTRY =
            "org/apache/qpid/server/model/ConfiguredObjectTypeRegistry$AutomatedField";

    /** Mapping between argument names and qualified class names */
    private static final Map<String, String> TYPES = Map.of(
            "ConfiguredObjectMethodAttributeOrStatistic", CO_METHOD_ATTRIBUTE_OR_STATISTICS,
            "ConfiguredObjectMethodOperation", CO_METHOD_OPERATION,
            "AutomatedField", CO_TYPE_REGISTRY);

    /** System classpath property */
    private static final String CLASSPATH_PROPERTY = "java.class.path";

    /** Apache Qpid root package */
    private static final String QPID_ROOT_PACKAGE = "org/apache/qpid";

    /** Class file extension */
    private static final String CLASS_EXTENSION = ".class";

    /**
     * Annotations responsible for automated fields:
     * <p>
     * org.apache.qpid.server.model.ManagedAttributeField
     */
    private static final List<String> AUTOMATED_FIELD_ANNOTATIONS =
            List.of("Lorg/apache/qpid/server/model/ManagedAttributeField;");

    /**
     * Annotations responsible for attribute getters:
     * <p>
     * org.apache.qpid.server.model.DerivedAttribute,
     * org.apache.qpid.server.model.ManagedAttribute,
     * org.apache.qpid.server.model.ManagedStatistic
     */
    private static final List<String> GETTER_ANNOTATIONS = List.of(
            "Lorg/apache/qpid/server/model/DerivedAttribute;",
            "Lorg/apache/qpid/server/model/ManagedAttribute;",
            "Lorg/apache/qpid/server/model/ManagedStatistic;");

    /**
     * Annotations responsible for managed operations:
     * <p>
     * org.apache.qpid.server.model.ManagedOperation
     */
    private static final List<String> OPERATION_ANNOTATIONS =
            List.of("Lorg/apache/qpid/server/model/ManagedOperation;");

    /**
     * Annotations responsible for state transitions:
     * <p>
     * org.apache.qpid.server.model.StateTransition
     */
    private static final List<String> STATE_TRANSITION_ANNOTATIONS =
            List.of("Lorg/apache/qpid/server/model/StateTransition;");

    /** All annotations involved in class transformation */
    private static final List<String> ANNOTATIONS = Stream.of(
            AUTOMATED_FIELD_ANNOTATIONS,
            GETTER_ANNOTATIONS,
            OPERATION_ANNOTATIONS,
            STATE_TRANSITION_ANNOTATIONS).flatMap(List::stream).collect(Collectors.toUnmodifiableList());

    /** Logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(QpidClassFileTransformer.class);

    /** Class transformers */
    private final Map<String, QpidTransformer<?>> _transformers = new HashMap<>();

    /** Classes allowed to instrument (configured via agent arguments) */
    private final List<String> _allowedTypes;

    /**
     * Scans classpath finding methods and fields annotated by annotations of interest. Gathers information about
     * affected methods and fields, passing them to appropriate class transformers.
     *
     * @param instrumentation Instrumentation instance
     */
    public QpidClassFileTransformer(final String arg, final Instrumentation instrumentation)
    {
        LOGGER.info("Initializing QPID instrumentation agent");

        _allowedTypes = parseArgs(arg);

        // get classpath entries
        final String classPath = System.getProperty(CLASSPATH_PROPERTY);
        final List<String> classPathEntries =
                Arrays.stream(classPath.split(File.pathSeparator)).collect(Collectors.toList());

        // prepare collections to store metadata
        final List<AutomatedFieldDescription> automatedFields = new ArrayList<>();
        final List<MethodDescription> getters = new ArrayList<>();
        final List<MethodDescription> operations = new ArrayList<>();
        final List<MethodDescription> stateTransitions = new ArrayList<>();

        // iterate over classpath entries and parse class files
        for (final String classPathEntry : classPathEntries)
        {
            try (final JarFile jarFile = jarFile(classPathEntry))
            {
                if (jarFile != null)
                {
                    jarFile.stream()
                           .filter(jarEntry ->
                                jarEntry.getName().startsWith(QPID_ROOT_PACKAGE) &&
                                jarEntry.getName().endsWith(CLASS_EXTENSION))
                           .forEach(jarEntry ->
                                parse(jarFile, jarEntry, automatedFields, getters, operations, stateTransitions));
                }
            }
            catch (IOException e)
            {
                LOGGER.error("Error when accessing class files", e);
            }
        }

        LOGGER.info("Identified {} automated fields", automatedFields.size());
        LOGGER.info("Identified {} managed attribute methods", getters.size());
        LOGGER.info("Identified {} managed operation methods", operations.size());
        LOGGER.info("Identified {} state transition methods", stateTransitions.size());
        LOGGER.info("Loaded {} classes", instrumentation.getAllLoadedClasses().length);

        // create class transformers
        _transformers.put(CO_METHOD_ATTRIBUTE_OR_STATISTICS,
                          new ConfiguredObjectMethodAttributeOrStatisticTransformer<>(getters));
        _transformers.put(CO_METHOD_OPERATION, new ConfiguredObjectMethodOperationTransformer<>(operations));
        _transformers.put(CO_TYPE_REGISTRY, new ConfiguredObjectTypeRegistryTransformer<>(automatedFields));

        LOGGER.info("QPID instrumentation agent initialized");
    }

    /**
     * Parses agent arguments
     *
     * @param args Agent string argument
     *
     * @return List of classes to instrument
     */
    private List<String> parseArgs(String args)
    {
        if (args == null || args.isEmpty())
        {
            return new ArrayList<>(TYPES.values());
        }
        return Arrays
                .stream(args.split(","))
                .map(String::trim).filter(TYPES::containsKey)
                .map(TYPES::get).distinct().collect(Collectors.toList());
    }

    /**
     * Reads JAR file
     *
     * @param path File path
     * @return JarFile instance
     */
    private JarFile jarFile(final String path)
    {
        try
        {
            return new JarFile(path);
        }
        catch (IOException e)
        {
            LOGGER.debug("Error when parsing jar file", e);
            return null;
        }
    }

    /**
     * Parses JAR entry iterating over classes and saving information about methods and field of interest into the
     * lists provided.
     * <p>
     * To avoid eager class loading (classes being parsed shouldn't be loaded in the instrumentation agent, otherwise
     * they couldn't be transformed) ASM parser is used instead of reflection.
     *
     * @param jar              JAR file
     * @param jarEntry         JAR file entry
     * @param automatedFields  List of automated fields
     * @param getters          List of getter method
     * @param operations       List of managed operation methods
     * @param stateTransitions List of state transition methods
     */
    private void parse(final JarFile jar,
                       final JarEntry jarEntry,
                       final List<AutomatedFieldDescription> automatedFields,
                       final List<MethodDescription> getters,
                       final List<MethodDescription> operations,
                       final List<MethodDescription> stateTransitions)
    {
        try (final InputStream inputStream = jar.getInputStream(jarEntry))
        {
            final ClassReader classReader = new ClassReader(inputStream);
            final ClassNode classNode = new ClassNode();
            classReader.accept(classNode, ClassReader.EXPAND_FRAMES);
            final List<MethodNode> methods = classNode.methods;
            final List<FieldNode> fields = classNode.fields;

            methods.stream()
                   .filter(methodNode -> methodNode.visibleAnnotations != null)
                   .forEach(methodNode -> parse(classNode, methodNode, getters, operations, stateTransitions));

            fields.stream()
                  .filter(field -> field.visibleAnnotations != null)
                  .forEach(field -> parse(classNode, field, automatedFields));
        }
        catch (IOException e)
        {
            throw new ServerScopedRuntimeException(e);
        }
    }

    private void parse(final ClassNode classNode,
                       final MethodNode methodNode,
                       final List<MethodDescription> getters,
                       final List<MethodDescription> operations,
                       final List<MethodDescription> stateTransitions)
    {
        final List<AnnotationNode> annotations = methodNode.visibleAnnotations;
        annotations.stream().filter(node -> ANNOTATIONS.contains(node.desc))
                   .forEach(node -> parse(classNode, methodNode, node, getters, operations, stateTransitions));
    }

    private void parse(final ClassNode classNode,
                       final MethodNode methodNode,
                       final AnnotationNode node,
                       final List<MethodDescription> getters,
                       final List<MethodDescription> operations,
                       final List<MethodDescription> stateTransitions)
    {
        if (GETTER_ANNOTATIONS.contains(node.desc))
        {
            getters.add(MethodDescription.of(classNode, methodNode));
        }
        if (OPERATION_ANNOTATIONS.contains(node.desc))
        {
            operations.add(MethodDescription.of(classNode, methodNode));
        }
        if (STATE_TRANSITION_ANNOTATIONS.contains(node.desc))
        {
            stateTransitions.add(MethodDescription.of(classNode, methodNode));
        }
    }

    private void parse(final ClassNode classNode,
                       final FieldNode fieldNode,
                       final List<AutomatedFieldDescription> automatedFields)
    {
        final List<AnnotationNode> annotations = fieldNode.visibleAnnotations;
        annotations.stream()
                   .filter(node -> AUTOMATED_FIELD_ANNOTATIONS.contains(node.desc))
                   .forEach(node -> automatedFields.add(AutomatedFieldDescription.of(classNode, fieldNode, node)));
    }

    /**
     * Transforms original class
     *
     * @param loader              the defining loader of the class to be transformed,
     *                            may be {@code null} if the bootstrap loader
     * @param className           the name of the class in the internal form of fully
     *                            qualified class and interface names as defined in
     *                            <i>The Java Virtual Machine Specification</i>.
     *                            For example, <code>"java/util/List"</code>.
     * @param classBeingRedefined if this is triggered by a redefine or re-transform,
     *                            the class being redefined or re-transformed;
     *                            if this is a class load, {@code null}
     * @param protectionDomain    the protection domain of the class being defined or redefined
     * @param classfileBuffer     the input byte buffer in class file format - must not be modified
     * @return Transformed class in form of a byte array
     */
    public byte[] transform(final ClassLoader loader,
                            final String className,
                            final Class<?> classBeingRedefined,
                            final ProtectionDomain protectionDomain,
                            final byte[] classfileBuffer)
    {
        if (_allowedTypes.contains(className) && _transformers.containsKey(className))
        {
            LOGGER.info("Transforming {}", className);
            final byte[] bytes = _transformers.get(className).generate(classfileBuffer);
            LOGGER.info("{} transformed", className);
            return bytes;
        }
        return classfileBuffer;
    }
}
