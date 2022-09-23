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

import org.objectweb.asm.Opcodes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.instrumentation.metadata.MethodDescription;

/**
 * Transforms org.apache.qpid.server.model.ConfiguredObjectMethodOperation class.
 * <p>
 * Class structure after transformation:
 * <pre>
 * abstract class ConfiguredObjectMethodOperation
 * {
 *     static final MethodHandle MH_1;
 *     static final MethodHandle MH_2;
 *     static final MethodHandle MH_3;
 *     private final String _signature;
 *     private final int _hashcode;
 *
 *     static
 *     {
 *         MethodHandles.Lookup lookup = MethodHandles.lookup();
 *         Class declaringClass = Class.forName("org.apache.qpid.server.model.ConfiguredObject");
 *         Method method = declaringClass.getDeclaredMethod("getId");
 *         method.setAccessible(true);
 *         MH_1 = lookup.unreflect(method);
 *         method = declaringClass.getDeclaredMethod("getName");
 *         method.setAccessible(true);
 *         MH_2 = lookup.unreflect(method);
 *         method = declaringClass.getDeclaredMethod("getState");
 *         method.setAccessible(true);
 *         MH_3 = lookup.unreflect(method);
 *     }
 *
 *     ConfiguredObjectMethodOperation(final Class<C> clazz,
 *                                     final Method operation,
 *                                     final ConfiguredObjectTypeRegistry typeRegistry)
 *     {
 *         // original code
 *         // original code
 *         // original code
 *
 *         _signature = method.getDeclaringClass() + "#"  + method.getName() +
 *                Arrays.stream(getter.getParameterClasses()).map(Class::getSimpleName()).collect(Collectors.joining());
 *         _hashcode = _signature.hashCode();
 *     }
 *
 *     public Object perform(C subject, Map<String, Object> parameters)
 *     {
 *         //
 *         // original code
 *         //
 *
 *         // line "return _operation.invoke(subject, paramValues);" is replaced with following call:
 *
 *         switch(_hashcode)
 *         {
 *             case 1:
 *                 return MH_1.invokeExact(subject, paramValues[0]);
 *             case 2:
 *                 MH_2.invokeExact(subject, paramValues[0], paramValues[1], paramValues[2]);
 *                 return null;
 *             case 3:
 *                 return MH_2.invokeExact(subject, paramValues[0]);
 *             default:
 *                 throw new UnsupportedOperationException("No operation found");
 *         }
 *
 *         //
 *         // original code
 *         //
 *     }
 * }
 * </pre>
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ConfiguredObjectMethodOperationTransformer<T extends MethodDescription> extends AbstractQpidTransformer<T>
        implements QpidTransformer<T>
{
    /** Target class name */
    private static final String CLASS_NAME = "org.apache.qpid.server.model.ConfiguredObjectMethodOperation";

    /** Internal class name */
    private static final String INTERNAL_CLASS_NAME = "org/apache/qpid/server/model/ConfiguredObjectMethodOperation";

    /** Constructor descriptor */
    private static final String DESC_CONSTRUCTOR =
            String.format("(%s%s%s)V", referenceName(CLASS), referenceName(METHOD), referenceName(CO_TYPE_REGISTRY));

    /** Name of method ConfiguredObjectMethodOperation#perform() */
    private static final String PERFORM = "perform";

    /** Descriptor of method ConfiguredObjectMethodOperation#perform() */
    private static final String DESC_PERFORM = String.format("(%s%s)%s", referenceName(CO), referenceName(MAP), referenceName(OBJECT));

    /** Logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredObjectMethodOperationTransformer.class);

    /** List of methods @ManagedOperation */
    private final List<T> _methods;

    /**
     * Constructor stores method list
     *
     * @param methods Methods to be handled
     */
    public ConfiguredObjectMethodOperationTransformer(final List<T> methods)
    {
        super();
        _methods = new ArrayList<>(methods);
    }

    @Override
    public Logger getLogger()
    {
        return LOGGER;
    }

    @Override
    public List<T> getMemberDescriptions()
    {
        return new ArrayList<>(_methods);
    }

    @Override
    public ClassVisitor getTransformer(final ClassVisitor cv)
    {
        return new Transformer(cv, getMemberDescriptions());
    }

    /**
     * Copies class code adding custom logic
     */
    private class Transformer extends ClassVisitor
    {
        private final List<MethodDescription> _methods;

        @SuppressWarnings("unchecked")
        private Transformer(final ClassVisitor cv, final List<T> methods)
        {
            super(Opcodes.ASM9, cv);
            _methods = (List<MethodDescription>) methods;
        }

        /**
         * When reaching end of the class, static final MethodHandle variables are generated as well as additional
         * fields _signature and _hashcode.
         */
        public void visitEnd()
        {
            visitMethodHandleFields(_methods.size(), (ClassWriter) cv);
            final int access = Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL;
            visitField(access, FIELD_SIGNATURE, referenceName(STRING), null, null).visitEnd();
            visitField(access, FIELD_HASHCODE, TYPE_INT, null, null).visitEnd();
            visitStaticInitializer(_methods, INTERNAL_CLASS_NAME, CLASS_NAME, (ClassWriter) cv);
        }

        /**
         * Creates MethodVisitor which copies all class methods but constructor and
         * ConfiguredObjectMethodOperation#perform() method
         *
         * @param access     the method's access flags. This parameter also indicates if
         *                   the method is synthetic and/or deprecated.
         * @param name       the method's name.
         * @param desc       the method's descriptor
         * @param signature  the method's signature. May be {@literal null} if the method parameters,
         *                   return type and exceptions do not use generic types.
         * @param exceptions the internal names of the method's exception classes
         * @return MethodVisitor instance
         */
        @Override
        public MethodVisitor visitMethod(final int access,
                                         final String name,
                                         final String desc,
                                         final String signature,
                                         final String[] exceptions)
        {
            MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);

            // add custom logic for constructor
            if (name.equals(INIT) && desc.equals(DESC_CONSTRUCTOR))
            {
                final int methodIndex = 2; // index of method variable in constructor
                mv = new ConstructorTransformer(mv, INTERNAL_CLASS_NAME, methodIndex);
            }

            // add custom logic for perform()
            if (name.equals(PERFORM) && desc.equals(DESC_PERFORM))
            {
                mv = new PerformTransformer(mv, _methods);
            }

            return mv;
        }
    }

    /**
     * Copies ConfiguredObjectMethodOperation#perform() code
     * replacing "method.invoke()" with switch / case routine calling static final MethodHandle based on method hashcode.
     */
    private class PerformTransformer extends MethodVisitor
    {
        private final List<MethodDescription> _methods;

        private boolean skipOperations = false;

        private PerformTransformer(final MethodVisitor delegate, final List<MethodDescription> methods)
        {
            super(Opcodes.ASM9, delegate);
            _methods = methods;
        }

        @Override
        public void visitVarInsn(final int opcode, final int index)
        {
            if (!skipOperations)
            {
                super.visitVarInsn(opcode, index);
            }
        }

        @Override
        public void visitFieldInsn(final int opcode, final String owner, final String name, final String descriptor)
        {
            if (!skipOperations)
            {
                super.visitFieldInsn(opcode, owner, name, descriptor);
            }
        }

        @Override
        public void visitMethodInsn(final int opcode,
                                    final String owner,
                                    final String name,
                                    final String descriptor,
                                    final boolean isInterface)
        {
            if (opcode == Opcodes.INVOKEVIRTUAL && owner.equals(INTERNAL_CLASS_NAME) && "getParameterValue".equals(name))
            {
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
                skipOperations = true;
                return;
            }
            if (opcode == Opcodes.INVOKEVIRTUAL && owner.equals(METHOD) && INVOKE.equals(name))
            {
                skipOperations = false;

                final int configuredObjectIndex = 1;
                final int paramsIndex = 5;

                super.visitVarInsn(Opcodes.ALOAD, 0);
                super.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_CLASS_NAME, FIELD_HASHCODE, TYPE_INT);
                super.visitVarInsn(Opcodes.ISTORE, 7);
                final int signatureIndex = 7;

                final Map<String, AttributeStackAddress> stackMap = createAttributesStackMap(_methods);
                final int[] hashes = new int[stackMap.size()];
                final Label[] switchJumpLabels = new Label[stackMap.size()];
                fillHashesAndLabels(stackMap, hashes, switchJumpLabels);

                final Label label = new Label();

                super.visitVarInsn(Opcodes.ILOAD, signatureIndex);
                super.visitLookupSwitchInsn(label, hashes, switchJumpLabels);

                for (int i = 0; i < _methods.size(); i++)
                {
                    final MethodDescription method = _methods.get(i);
                    final String returnType = method.getReturnType();
                    final String signature = sig(method);
                    super.visitLabel(stackMap.get(method.getSignature()).getLabel());
                    super.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                    super.visitFieldInsn(Opcodes.GETSTATIC, INTERNAL_CLASS_NAME, "MH_" + i, referenceName(METHOD_HANDLE));
                    super.visitVarInsn(Opcodes.ALOAD, configuredObjectIndex);

                    for (int j = 0; j < method.getParameterCount(); j++)
                    {
                        String type = autoboxType(method.getParameter(j));
                        if (type.charAt(0) == 'L' && type.endsWith(";"))
                        {
                            type = type.substring(1, type.length() - 1);
                        }
                        super.visitVarInsn(Opcodes.ALOAD, paramsIndex);
                        super.visitIntInsn(Opcodes.BIPUSH, j);
                        super.visitInsn(Opcodes.AALOAD);
                        super.visitTypeInsn(Opcodes.CHECKCAST, type);
                        autoboxIfNeeded(autoboxType(method.getParameter(j)), method.getParameter(j), mv);
                    }

                    super.visitMethodInsn(Opcodes.INVOKEVIRTUAL, METHOD_HANDLE, INVOKE_EXACT, signature, FALSE);

                    if (method.returnsPrimitive() && !method.returnsVoid())
                    {
                        autoboxIfNeeded(returnType, autoboxType(returnType), mv);
                    }

                    if (method.returnsVoid())
                    {
                        super.visitInsn(Opcodes.ACONST_NULL);
                    }

                    super.visitInsn(Opcodes.ARETURN);
                }

                super.visitLabel(label);
                super.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

                /* create code: throw new UnsupportedOperationException(msg) */
                super.visitVarInsn(Opcodes.ALOAD, 0);
                super.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_CLASS_NAME, FIELD_SIGNATURE, referenceName(STRING));
                super.visitVarInsn(Opcodes.ASTORE, 8);
                super.visitTypeInsn(Opcodes.NEW, SSRE);
                super.visitInsn(Opcodes.DUP);
                super.visitLdcInsn("Failed to call operation %s on object %s");
                super.visitInsn(Opcodes.ICONST_2);
                super.visitTypeInsn(Opcodes.ANEWARRAY, OBJECT);
                super.visitInsn(Opcodes.DUP);
                super.visitInsn(Opcodes.ICONST_0);
                super.visitVarInsn(Opcodes.ALOAD, 8);
                super.visitInsn(Opcodes.AASTORE);
                super.visitInsn(Opcodes.DUP);
                super.visitInsn(Opcodes.ICONST_1);
                super.visitVarInsn(Opcodes.ALOAD, 1);
                super.visitInsn(Opcodes.AASTORE);
                super.visitMethodInsn(Opcodes.INVOKESTATIC, STRING, FORMAT, DESC_FORMAT, FALSE);
                super.visitMethodInsn(Opcodes.INVOKESPECIAL, SSRE, INIT, DESC_EXCEPTION1, FALSE);
                super.visitInsn(Opcodes.ATHROW);
            }
            else
            {
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
            }
        }
    }
}
