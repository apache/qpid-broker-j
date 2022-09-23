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
 * Transforms org.apache.qpid.server.model.ConfiguredObjectMethodAttributeOrStatistic class.
 * <p>
 * Class structure after transformation:
 * <pre>
 * abstract class ConfiguredObjectMethodAttributeOrStatistic
 * {
 *     static final MethodHandle MH_1;
 *     static final MethodHandle MH_2;
 *     static final MethodHandle MH_3;
 *     private final String _signature;
 *     private final int _hashcode;
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
 *     ConfiguredObjectMethodAttributeOrStatistic(final Method method)
 *     {
 *         _signature = method.getDeclaringClass() + "#"  + method.getName() +
 *             Arrays.stream(getter.getParameterClasses()).map(Class::getSimpleName()).collect(Collectors.joining());
 *         _hashcode = _signature.hashCode();
 *     }
 *     public T getValue(C configuredObject)
 *     {
 *         switch(_hashcode)
 *         {
 *             case 1:
 *                 return MH_1.invokeExact(configuredObject);
 *             case 2:
 *                 return MH_2.invokeExact(configuredObject);
 *             case 3:
 *                 return MH_2.invokeExact(configuredObject);
 *             default:
 *                 throw new UnsupportedOperationException("No accessor found");
 *         }
 *     }
 * }
 * </pre>
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ConfiguredObjectMethodAttributeOrStatisticTransformer<T extends MethodDescription> extends AbstractQpidTransformer<T>
        implements QpidTransformer<T>
{
    /** Target class name */
    private static final String CLASS_NAME = "org.apache.qpid.server.model.ConfiguredObjectMethodAttributeOrStatistic";

    /** Internal class name */
    private static final String INTERNAL_CLASS_NAME = "org/apache/qpid/server/model/ConfiguredObjectMethodAttributeOrStatistic";

    /** Name of the getValue() method */
    private static final String GET_VALUE = "getValue";

    /** Constructor descriptor */
    private static final String DESC_CONSTRUCTOR = String.format("(%s)V", referenceName(METHOD));

    /** Descriptor of getValue() method */
    private static final String DESC_GET_VALUE = String.format(ONE_ARG, referenceName(CO), referenceName(OBJECT));

    /** Logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredObjectMethodAttributeOrStatisticTransformer.class);

    /** List of methods @DerivedAttribute, @ManagedAttribute, @ManagedStatistic */
    final List<T> _methods;

    public ConfiguredObjectMethodAttributeOrStatisticTransformer(final List<T> methods)
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
         * Creates MethodVisitor which copies all class methods but constructor and getValue() method
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
                final int methodIndex = 1; // index of method variable in constructor
                mv = new ConstructorTransformer(mv, INTERNAL_CLASS_NAME, methodIndex);
            }

            // add custom logic for getValue()
            if (name.equals(GET_VALUE) && desc.equals(DESC_GET_VALUE))
            {
                mv = new GetValueTransformer(mv, _methods);
            }

            return mv;
        }
    }

    class GetValueTransformer extends MethodVisitor
    {
        private final MethodVisitor _target;
        private final List<MethodDescription> _methods;

        public GetValueTransformer(final MethodVisitor delegate, final List<MethodDescription> methods)
        {
            super(Opcodes.ASM9, null);
            _target = delegate;
            _methods = methods;
        }

        @Override()
        public void visitCode()
        {
            _target.visitCode();

            _target.visitVarInsn(Opcodes.ALOAD, 0);
            _target.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_CLASS_NAME, FIELD_HASHCODE, TYPE_INT);
            _target.visitVarInsn(Opcodes.ISTORE, 2);

            final Map<String, AttributeStackAddress> stackMap = createAttributesStackMap(_methods);
            final int[] hashes = new int[stackMap.size()];
            final Label[] switchJumpLabels = new Label[stackMap.size()];
            fillHashesAndLabels(stackMap, hashes, switchJumpLabels);

            final Label label = new Label();

            _target.visitVarInsn(Opcodes.ILOAD, 2);
            _target.visitLookupSwitchInsn(label, hashes, switchJumpLabels);

            for (int i = 0; i < _methods.size(); i++)
            {
                final MethodDescription method = _methods.get(i);
                final String declaringClass = method.getDeclaringClass();
                final String returnType = method.getReturnType();
                final String signature = String.format(ONE_ARG, referenceName(declaringClass), referenceName(returnType));
                _target.visitLabel(stackMap.get(method.getSignature()).getLabel());
                _target.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                _target.visitFieldInsn(Opcodes.GETSTATIC, INTERNAL_CLASS_NAME, "MH_" + i, referenceName(METHOD_HANDLE));
                _target.visitVarInsn(Opcodes.ALOAD, 1);
                _target.visitMethodInsn(Opcodes.INVOKEVIRTUAL, METHOD_HANDLE, INVOKE_EXACT, signature, FALSE);
                if (method.returnsPrimitive())
                {
                    autoboxIfNeeded(returnType, autoboxType(returnType), _target);
                }
                _target.visitInsn(Opcodes.ARETURN);
            }

            _target.visitLabel(label);
            _target.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

            _target.visitVarInsn(Opcodes.ALOAD, 0);
            _target.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_CLASS_NAME, FIELD_SIGNATURE, referenceName(STRING));
            _target.visitVarInsn(Opcodes.ASTORE, 3);
            _target.visitTypeInsn(Opcodes.NEW, SSRE);
            _target.visitInsn(Opcodes.DUP);
            _target.visitLdcInsn("No accessor to get attribute %s from object %s");
            _target.visitInsn(Opcodes.ICONST_2);
            _target.visitTypeInsn(Opcodes.ANEWARRAY, OBJECT);
            _target.visitInsn(Opcodes.DUP);
            _target.visitInsn(Opcodes.ICONST_0);
            _target.visitVarInsn(Opcodes.ALOAD, 3);
            _target.visitInsn(Opcodes.AASTORE);
            _target.visitInsn(Opcodes.DUP);
            _target.visitInsn(Opcodes.ICONST_1);
            _target.visitVarInsn(Opcodes.ALOAD, 1);
            _target.visitInsn(Opcodes.AASTORE);
            _target.visitMethodInsn(Opcodes.INVOKESTATIC, STRING, FORMAT, DESC_FORMAT, FALSE);
            _target.visitMethodInsn(Opcodes.INVOKESPECIAL, SSRE, INIT, DESC_EXCEPTION1, FALSE);
            _target.visitInsn(Opcodes.ATHROW);

            _target.visitInsn(Opcodes.RETURN);

            _target.visitMaxs(0, 0);
            _target.visitEnd();
        }
    }
}
