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

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import org.apache.qpid.server.instrumentation.metadata.MemberDescription;
import org.apache.qpid.server.instrumentation.metadata.MethodDescription;
import org.apache.qpid.server.util.ServerScopedRuntimeException;

/**
 * Class contains constants and utility methods for instrumentation
 *
 * @param <T> Class member
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public abstract class AbstractQpidTransformer<T extends MemberDescription> implements QpidTransformer<T>
{
    protected static final Boolean FALSE = Boolean.FALSE;
    protected static final Boolean TRUE = Boolean.TRUE;

    /**
     * Type names
     */
    protected static final String ARRAYS = Type.getInternalName(Arrays.class);
    protected static final String BOOLEAN = Type.getInternalName(Boolean.class);
    protected static final String BYTE = Type.getInternalName(Byte.class);
    protected static final String CALLSITE = Type.getInternalName(CallSite.class);
    protected static final String CHARACTER = Type.getInternalName(Character.class);
    protected static final String CLASS = Type.getInternalName(Class.class);
    protected static final String CO = "org/apache/qpid/server/model/ConfiguredObject";
    protected static final String CO_TYPE_REGISTRY = "org/apache/qpid/server/model/ConfiguredObjectTypeRegistry";
    protected static final String COLLECTOR = Type.getInternalName(Collector.class);
    protected static final String COLLECTORS = Type.getInternalName(Collectors.class);
    protected static final String DOUBLE = Type.getInternalName(Double.class);
    protected static final String FIELD = Type.getInternalName(Field.class);
    protected static final String FLOAT = Type.getInternalName(Float.class);
    protected static final String FUNCTION = Type.getInternalName(Function.class);
    protected static final String INTEGER = Type.getInternalName(Integer.class);
    protected static final String LAMBDAMETAFACTORY = Type.getInternalName(LambdaMetafactory.class);
    protected static final String LONG = Type.getInternalName(Long.class);
    protected static final String LOOKUP = Type.getInternalName(MethodHandles.Lookup.class);
    protected static final String MAP = Type.getInternalName(Map.class);
    protected static final String METHOD = Type.getInternalName(Method.class);
    protected static final String METHOD_HANDLE = Type.getInternalName(MethodHandle.class);
    protected static final String METHOD_HANDLES = Type.getInternalName(MethodHandles.class);
    protected static final String METHODTYPE = Type.getInternalName(MethodType.class);
    protected static final String OBJECT = Type.getInternalName(Object.class);
    protected static final String SHORT = Type.getInternalName(Short.class);
    protected static final String SSRE = "org/apache/qpid/server/util/ServerScopedRuntimeException";
    protected static final String STREAM = Type.getInternalName(Stream.class);
    protected static final String STRING = Type.getInternalName(String.class);
    protected static final String STRING_BUILDER = Type.getInternalName(StringBuilder.class);
    protected static final String THROWABLE = Type.getInternalName(Throwable.class);
    protected static final String VOID = Type.getInternalName(Void.class);

    /**
     * Primitive types
     */
    protected static final String TYPE_BOOLEAN = "Z";
    protected static final String TYPE_BYTE = "B";
    protected static final String TYPE_CHARACTER = "C";
    protected static final String TYPE_DOUBLE = "D";
    protected static final String TYPE_FLOAT = "F";
    protected static final String TYPE_INT = "I";
    protected static final String TYPE_LONG = "J";
    protected static final String TYPE_SHORT = "S";
    protected static final String TYPE_VOID = "V";

    private static final Map<String, Supplier<String>> TYPES = Map.of(
            TYPE_BOOLEAN, () -> BOOLEAN,
            TYPE_BYTE, () -> BYTE,
            TYPE_CHARACTER, () -> CHARACTER,
            TYPE_DOUBLE, () -> DOUBLE,
            TYPE_FLOAT, () -> FLOAT,
            TYPE_INT, () -> INTEGER,
            TYPE_LONG, () -> LONG,
            TYPE_SHORT, () -> SHORT,
            TYPE_VOID, () -> VOID);

    /**
     * Field names
     */
    protected static final String FIELD_FIELD = "_field";
    protected static final String FIELD_HASHCODE = "_hashcode";
    protected static final String FIELD_SIGNATURE = "_signature";
    protected static final String TYPE = "TYPE";

    /**
     * Method names
     */
    protected static final String APPEND = "append";
    protected static final String APPLY = "apply";
    protected static final String CLINIT = "<clinit>";
    protected static final String COLLECT = "collect";
    protected static final String FORMAT = "format";
    protected static final String GET_CANONICAL_NAME = "getCanonicalName";
    protected static final String GET_DECLARING_CLASS = "getDeclaringClass";
    protected static final String GET_NAME = "getName";
    protected static final String GET_PARAM_TYPES = "getParameterTypes";
    protected static final String GET_SIMPLE_NAME = "getSimpleName";
    protected static final String HASHCODE = "hashCode";
    protected static final String INIT = "<init>";
    protected static final String INVOKE = "invoke";
    protected static final String INVOKE_EXACT = "invokeExact";
    protected static final String JOINING = "joining";
    protected static final String METAFACTORY = "metafactory";
    protected static final String METHOD_LOOKUP = "lookup";
    protected static final String METHOD_MAP = "map";
    protected static final String SET = "set";
    protected static final String SET_ACCESSIBLE = "setAccessible";
    protected static final String METHOD_STREAM = "stream";
    protected static final String TO_STRING = "toString";
    protected static final String VALUE_OF = "valueOf";

    /**
     * Method descriptors
     */
    protected static final String NO_ARGS = "()%s";
    protected static final String ONE_ARG = "(%s)%s";
    protected static final String DESC_APPEND = String.format(ONE_ARG, referenceName(STRING), referenceName(STRING_BUILDER));
    protected static final String DESC_APPLY = String.format("()%s", referenceName(FUNCTION));
    protected static final String DESC_COLLECT = String.format(ONE_ARG, referenceName(COLLECTOR), referenceName(OBJECT));
    protected static final String DESC_GET_CLASS = String.format(NO_ARGS, referenceName(CLASS));
    protected static final String DESC_GET_STRING = String.format(NO_ARGS, referenceName(STRING));
    protected static final String DESC_GET_PARAM_TYPES = String.format("()[%s", referenceName(CLASS));
    protected static final String DESC_HASHCODE = "()I";
    protected static final String DESC_JOINING = String.format("()%s", referenceName(COLLECTOR));
    protected static final String DESC_METAFACTORY = String.format("(%s%s%s%s%s%s)%s",
                                                                   referenceName(LOOKUP),
                                                                   referenceName(STRING),
                                                                   referenceName(METHODTYPE),
                                                                   referenceName(METHODTYPE),
                                                                   referenceName(METHOD_HANDLE),
                                                                   referenceName(METHODTYPE),
                                                                   referenceName(CALLSITE));
    protected static final String DESC_LOOKUP = String.format(NO_ARGS, referenceName(LOOKUP));
    protected static final String DESC_MAP = String.format(ONE_ARG, referenceName(FUNCTION), referenceName(STREAM));
    protected static final String DESC_EXCEPTION1 = String.format("(%s)V", referenceName(STRING));
    protected static final String DESC_EXCEPTION2 = String.format("(%s%s)V", referenceName(STRING), referenceName(THROWABLE));
    protected static final String DESC_FORMAT = String.format("(%s[%s)%s", referenceName(STRING), referenceName(OBJECT), referenceName(STRING));
    protected static final String DESC_FORNAME = String.format(ONE_ARG, referenceName(STRING), referenceName(CLASS));
    protected static final String DESC_GET_DECLARED_METHOD =
            String.format("(%s[%s)%s", referenceName(STRING), referenceName(CLASS), referenceName(METHOD));
    protected static final String DESC_PRIVATE_LOOKUP = String.format("(%s%s)%s", referenceName(CLASS), referenceName(LOOKUP), referenceName(LOOKUP));
    protected static final String DESC_SET = String.format("(%s%s)V", referenceName(OBJECT), referenceName(OBJECT));
    protected static final String DESC_STREAM = String.format("([%s)%s", referenceName(OBJECT), referenceName(STREAM));
    protected static final String DESC_UNREFLECT = String.format(ONE_ARG, referenceName(METHOD), referenceName(METHOD_HANDLE));

    /**
     * Types
     */
    protected static final Type TYPE_LAMBDAMETAFACTORY =
            Type.getType(String.format("(%s)%s", referenceName(OBJECT), referenceName(OBJECT)));
    protected static final Type TYPE_GET_SIMPLE_NAME =
            Type.getType(String.format("(%s)%s", referenceName(CLASS), referenceName(STRING)));

    /**
     * Handles
     */
    protected static final Handle HANDLE_LAMBDAMETAFACTORY =
            new Handle(Opcodes.H_INVOKESTATIC, LAMBDAMETAFACTORY, METAFACTORY, DESC_METAFACTORY, FALSE);
    protected static final Handle HANDLE_GET_SIMPLE_NAME =
            new Handle(Opcodes.H_INVOKEVIRTUAL, CLASS, GET_SIMPLE_NAME, DESC_GET_STRING, FALSE);

    @SuppressWarnings("java:S1181")
    public byte[] generate(byte[] bytes)
    {
        try
        {
            final ClassReader classReader = new ClassReader(bytes);
            final ClassWriter cw = new ClassWriter(classReader, ClassWriter.COMPUTE_FRAMES);
            classReader.accept(getTransformer(cw), ClassReader.EXPAND_FRAMES);
            return cw.toByteArray();
        }
        catch (Throwable t)
        {
            getLogger().error("Error during code instrumentation", t);
            throw new ServerScopedRuntimeException(t);
        }
    }

    protected static String referenceName(final String type)
    {
        if (type.charAt(0) == '[' || (type.charAt(0) == 'L' && type.endsWith(";")))
        {
            return type;
        }
        return TYPES.containsKey(type) ? type : String.format("L%s;", type.replace('.', '/'));
    }

    protected static String sig(final MethodDescription methodDescription)
    {
        return "(" + referenceName(methodDescription.getDeclaringClass()) +
               methodDescription.getParameters()
                                .stream()
                                .map(AbstractQpidTransformer::referenceName)
                                .collect(Collectors.joining()) + ")" +
               referenceName(methodDescription.getReturnType());
    }

    protected void visitMethodHandleFields(int size, final ClassWriter cw)
    {
        final int access = Opcodes.ACC_FINAL + Opcodes.ACC_STATIC;
        for (int i = 0; i < size; i++)
        {
            cw.visitField(access, "MH_" + i, referenceName(METHOD_HANDLE), null, null).visitEnd();
        }
    }

    protected void visitStaticInitializer(final List<MethodDescription> methods,
                                          final String internalClassName,
                                          final String className,
                                          final ClassWriter cw)
    {
        final MethodVisitor mv = cw.visitMethod(Opcodes.ACC_STATIC, CLINIT, "()V", null, null);
        mv.visitCode();

        int varIndex = 0;

        final Label tryStart = new Label();
        final Label tryEnd = new Label();
        final Label catchStart = new Label();
        mv.visitTryCatchBlock(tryStart, tryEnd, catchStart, THROWABLE);
        mv.visitLabel(tryStart);

        // MethodHandles.Lookup lookup = MethodHandles.lookup();
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, METHOD_HANDLES, METHOD_LOOKUP, DESC_LOOKUP, FALSE);
        mv.visitVarInsn(Opcodes.ASTORE, varIndex);
        final int lookupIndex = varIndex;

        String previousClassName = "";
        int declaringClassIndex = varIndex;
        for (int i = 0; i < methods.size(); i++)
        {
            final MethodDescription method = methods.get(i);
            final String declaringClass = method.getDeclaringClass().replace('/', '.');
            final String methodName = method.getName();

            if (!Objects.equals(previousClassName, declaringClass))
            {
                // Class type = Class.forName(...)
                mv.visitLdcInsn(declaringClass);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, CLASS, "forName", DESC_FORNAME, FALSE);
                varIndex++;
                mv.visitVarInsn(Opcodes.ASTORE, varIndex);
                declaringClassIndex = varIndex;

                // lookup = MethodHandles.privateLookupIn(declaringClass, lookup);
                mv.visitVarInsn(Opcodes.ALOAD, declaringClassIndex);
                mv.visitVarInsn(Opcodes.ALOAD, lookupIndex);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, METHOD_HANDLES, "privateLookupIn", DESC_PRIVATE_LOOKUP, FALSE);
                mv.visitVarInsn(Opcodes.ASTORE, lookupIndex);
            }

            mv.visitIntInsn(Opcodes.BIPUSH, method.getParameterCount());
            mv.visitTypeInsn(Opcodes.ANEWARRAY, CLASS);
            varIndex++;
            mv.visitVarInsn(Opcodes.ASTORE, varIndex);
            final int paramsIndex = varIndex;

            for (int j = 0; j < method.getParameterCount(); j++)
            {
                mv.visitVarInsn(Opcodes.ALOAD, paramsIndex);
                mv.visitIntInsn(Opcodes.BIPUSH, j);
                autoboxFieldIfNeeded(method.getParameter(j), mv);
                mv.visitVarInsn(Opcodes.ASTORE, varIndex + 1 + j);
                mv.visitVarInsn(Opcodes.ALOAD, varIndex + 1 + j);
                mv.visitInsn(Opcodes.AASTORE);
            }
            varIndex = varIndex + method.getParameterCount() + 1;

            mv.visitVarInsn(Opcodes.ALOAD, declaringClassIndex);
            mv.visitLdcInsn(methodName);
            mv.visitVarInsn(Opcodes.ALOAD, paramsIndex);

            // Method getter = type.getDeclaredMethod(...)
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, CLASS, "getDeclaredMethod", DESC_GET_DECLARED_METHOD, FALSE);
            varIndex++;
            mv.visitVarInsn(Opcodes.ASTORE, varIndex);

            // getter.setAccessible(true)
            mv.visitVarInsn(Opcodes.ALOAD, varIndex);
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, METHOD, SET_ACCESSIBLE, "(Z)V", FALSE);

            // MH_xx = methodHandle.unreflect(method)
            mv.visitVarInsn(Opcodes.ALOAD, lookupIndex);
            mv.visitVarInsn(Opcodes.ALOAD, varIndex);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, LOOKUP, "unreflect", DESC_UNREFLECT, FALSE);
            mv.visitFieldInsn(Opcodes.PUTSTATIC, internalClassName, "MH_" + i, referenceName(METHOD_HANDLE));

            previousClassName = declaringClass;
        }

        mv.visitLabel(tryEnd);
        mv.visitInsn(Opcodes.RETURN);

        visitCatchBlock("Failed to initialize getters for " + className, catchStart, varIndex, mv);

        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    protected void visitCatchBlock(final String errorMessage,
                                   final Label catchStart,
                                   int varIndex,
                                   final MethodVisitor mv)
    {
        mv.visitLabel(catchStart);
        mv.visitFrame(Opcodes.F_SAME1, 0, null, 1, new Object[]{THROWABLE});
        mv.visitVarInsn(Opcodes.ASTORE, varIndex);
        mv.visitTypeInsn(Opcodes.NEW, SSRE);
        mv.visitInsn(Opcodes.DUP);
        mv.visitLdcInsn(errorMessage);
        mv.visitVarInsn(Opcodes.ALOAD, varIndex);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, SSRE, INIT, DESC_EXCEPTION2, FALSE);
        mv.visitInsn(Opcodes.ATHROW);

        final Label catchEnd = new Label();
        mv.visitLabel(catchEnd);

        mv.visitLocalVariable("throwable", referenceName(THROWABLE), null, catchStart, catchEnd, varIndex);
    }

    protected void autoboxFieldIfNeeded(final String type, final MethodVisitor visitor)
    {
        if (TYPES.containsKey(type) && !TYPE_VOID.equals(type))
        {
            visitor.visitFieldInsn(Opcodes.GETSTATIC, TYPES.get(type).get(), TYPE, referenceName(CLASS));
        }
        else
        {
            visitor.visitLdcInsn(Type.getType(autoboxType(type)));
        }
    }

    protected void autoboxIfNeeded(final String in, final String out, final MethodVisitor visitor)
    {
        unboxOrAutobox(in, out, TYPE_BOOLEAN,"booleanValue", visitor);
        unboxOrAutobox(in, out, TYPE_BYTE, "byteValue", visitor);
        unboxOrAutobox(in, out, TYPE_CHARACTER, "charValue", visitor);
        unboxOrAutobox(in, out, TYPE_DOUBLE, "doubleValue", visitor);
        unboxOrAutobox(in, out, TYPE_FLOAT, "floatValue", visitor);
        unboxOrAutobox(in, out, TYPE_INT, "intValue", visitor);
        unboxOrAutobox(in, out, TYPE_LONG, "longValue", visitor);
        unboxOrAutobox(in, out, TYPE_SHORT, "shortValue", visitor);
    }

    private void unboxOrAutobox(final String in,
                                final String out,
                                final String type,
                                final String name,
                                final MethodVisitor methodVisitor)
    {
        final String wrapper = TYPES.get(type).get();
        if (wrapper.equals(in) && type.equals(out))
        {
            methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, wrapper, name, "()" + type, FALSE);
        }
        else if (type.equals(in) && wrapper.equals(out))
        {
            final String desc = "(" + type +")L" + wrapper + ";";
            methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, wrapper, VALUE_OF, desc, FALSE);
        }
    }

    /**
     * Returns the appropriate autoboxing type.
     */
    protected String autoboxType(final String unboxed)
    {
        return TYPES.getOrDefault(unboxed, () -> referenceName(unboxed)).get();
    }

    protected Map<String, AttributeStackAddress> createAttributesStackMap(final List<? extends MemberDescription> methods)
    {
        return methods.stream()
                .collect(Collectors.toMap(
                        MemberDescription::getSignature,
                        method -> new AttributeStackAddress(method.getSignature().hashCode()),
                        (key1, key2) -> key2));
    }

    /**
     * Stack map address for a particular attribute
     */
    protected static class AttributeStackAddress implements Comparable<AttributeStackAddress>
    {
        private final Label label;
        private final int hash;

        private AttributeStackAddress(final int hash)
        {
            this.label = new Label();
            this.hash = hash;
        }

        protected Label getLabel()
        {
            return label;
        }

        private int getHash()
        {
            return hash;
        }

        @Override
        public int compareTo(final AttributeStackAddress attributeStackAddress)
        {
            return Integer.compare(hash, attributeStackAddress.hash);
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
            final AttributeStackAddress that = (AttributeStackAddress) o;
            return hash == that.hash;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(hash);
        }
    }

    protected void fillHashesAndLabels(final Map<String, AttributeStackAddress> stackMap,
                                       final int[] hashes,
                                       final Label[] switchJumpLabels)
    {
        final List<AttributeStackAddress> stackAddresses = new ArrayList<>(stackMap.values());
        Collections.sort(stackAddresses);
        for (int i = 0; i < stackAddresses.size(); i++)
        {
            final AttributeStackAddress propertyStackAddress = stackAddresses.get(i);
            hashes[i] = propertyStackAddress.getHash();
            switchJumpLabels[i] = propertyStackAddress.getLabel();
        }
    }

    /**
     * Copies constructor code and adds at the end initialization of variables:
     * <p>
     * _signature = getter.getDeclaringClass().getName() + "#" + getter.getName();
     * _hashcode = _signature.hashcode();
     */
    protected static class ConstructorTransformer extends MethodVisitor
    {
        private final String _internalClassName;
        private final int _methodIndex;

        protected ConstructorTransformer(MethodVisitor delegate, String internalClassName, int methodIndex)
        {
            super(Opcodes.ASM9, delegate);
            _internalClassName = internalClassName;
            _methodIndex = methodIndex;
        }

        @Override()
        public void visitInsn(int opcode)
        {
            if (opcode == Opcodes.RETURN)
            {
                generateSignature(super.mv, _methodIndex, _internalClassName, FIELD_SIGNATURE);
                generateHashCode(super.mv, _internalClassName, FIELD_SIGNATURE, FIELD_HASHCODE);
            }
            super.visitInsn(opcode);
        }
    }

    protected static void generateSignature(final MethodVisitor mv,
                                            final int methodIndex,
                                            final String internalClassName,
                                            final String signatureField)
    {
        // _signature = method.getDeclaringClass().getName() +
        // "#" + method.getName() +
        // Arrays.stream(method.getParameterTypes()).map(Class::getSimpleName).collect(Collectors.joining());
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitTypeInsn(Opcodes.NEW, STRING_BUILDER);
        mv.visitInsn(Opcodes.DUP);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, STRING_BUILDER, INIT, "()V", FALSE);
        mv.visitVarInsn(Opcodes.ALOAD, methodIndex);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, METHOD, GET_DECLARING_CLASS, DESC_GET_CLASS, FALSE);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, CLASS, GET_CANONICAL_NAME, DESC_GET_STRING, FALSE);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, STRING_BUILDER, APPEND, DESC_APPEND, FALSE);
        mv.visitLdcInsn("#");
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, STRING_BUILDER, APPEND, DESC_APPEND, FALSE);
        mv.visitVarInsn(Opcodes.ALOAD, methodIndex);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, METHOD, GET_NAME, DESC_GET_STRING, FALSE);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, STRING_BUILDER, APPEND, DESC_APPEND, FALSE);
        mv.visitVarInsn(Opcodes.ALOAD, methodIndex);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, METHOD, GET_PARAM_TYPES, DESC_GET_PARAM_TYPES, FALSE);
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, ARRAYS, METHOD_STREAM, DESC_STREAM, FALSE);
        mv.visitInvokeDynamicInsn(APPLY, DESC_APPLY, HANDLE_LAMBDAMETAFACTORY,
                                     TYPE_LAMBDAMETAFACTORY, HANDLE_GET_SIMPLE_NAME, TYPE_GET_SIMPLE_NAME);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, STREAM, METHOD_MAP, DESC_MAP, TRUE);
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, COLLECTORS, JOINING, DESC_JOINING, FALSE);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, STREAM, COLLECT, DESC_COLLECT, TRUE);
        mv.visitTypeInsn(Opcodes.CHECKCAST, STRING);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, STRING_BUILDER, APPEND, DESC_APPEND, FALSE);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, STRING_BUILDER, TO_STRING, DESC_GET_STRING, FALSE);
        mv.visitFieldInsn(Opcodes.PUTFIELD, internalClassName, signatureField, referenceName(STRING));
    }

    protected static void generateHashCode(final MethodVisitor mv,
                                           final String internalClassName,
                                           final String signatureField,
                                           final String hashCodeField)
    {
        // _hashcode = _signature.hashcode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, signatureField, referenceName(STRING));
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, STRING, HASHCODE, DESC_HASHCODE, FALSE);
        mv.visitFieldInsn(Opcodes.PUTFIELD, internalClassName, hashCodeField, TYPE_INT);
    }
}
