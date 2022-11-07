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
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.instrumentation.metadata.AutomatedFieldDescription;
import org.apache.qpid.server.instrumentation.metadata.MethodDescription;

/**
 * Transforms org.apache.qpid.server.model.ConfiguredObjectTypeRegistry$AutomatedField class.
 * <br>
 * Class structure after transformation:
 * <pre>
 * static class AutomatedField
 * {
 *     private final Field _field;
 *     private final Method _preSettingAction;
 *     private final Method _postSettingAction;
 *
 *     private final String _preSettingSignature;
 *     private final String _postSettingSignature;
 *     private final int _preSettingHashCode;
 *     private final int _postSettingHashCode;
 *
 *     static final MethodHandle MH_1;
 *     static final MethodHandle MH_2;
 *     static final MethodHandle MH_3;
 *     static final MethodHandle MH_4;
 *     static final MethodHandle MH_5;
 *     static final MethodHandle MH_6;
 *
 *     private ConfiguredObjectTypeRegistry$AutomatedField(Field field,
 *                                                         Method preSettingAction,
 *                                                         Method postSettingAction)
 *     {
 *         // original code
 *
 *         if (this._preSettingAction != null)
 *         {
 *             this._preSettingSignature = preSettingAction.getDeclaringClass().getCanonicalName() + "#" +
 *                 preSettingAction.getName() +
 *                 (String)Arrays.stream(preSettingAction.getParameterTypes()).map(Class::getSimpleName).collect(Collectors.joining());
 *             this._preSettingHashcode = this._preSettingSignature.hashCode();
 *         }
 *
 *         if (this._postSettingAction != null)
 *         {
 *             this._postSettingSignature = postSettingAction.getDeclaringClass().getCanonicalName() + "#" +
 *                 postSettingAction.getName() +
 *                 (String)Arrays.stream(postSettingAction.getParameterTypes()).map(Class::getSimpleName).collect(Collectors.joining());
 *             this._postSettingHashcode = this._postSettingSignature.hashCode();
 *         }
 *     }
 *
 *     public void set(ConfiguredObject configuredObject, Object value) throws IllegalAccessException, InvocationTargetException
 *     {
 *         if (this._preSettingAction != null)
 *         {
 *             switch (_preSettingHashcode)
 *             {
 *                 case 1:
 *                    MH_1.invokeExact(configuredObject);
 *                    break;
 *                 case 2:
 *                    MH_2.invokeExact(configuredObject);
 *                    break;
 *                 case 3:
 *                    MH_3.invokeExact(configuredObject);
 *                    break;
 *             }
 *
 *             this._field.set(configuredObject, value);
 *
 *             if (this._postSettingAction != null)
 *             {
 *                switch (_postSettingHashcode)
 *                {
 *                    case 4:
 *                       MH_4.invokeExact(configuredObject);
 *                       break;
 *                    case 5:
 *                       MH_5.invokeExact(configuredObject);
 *                       break;
 *                    case 6:
 *                       MH_6.invokeExact(configuredObject);
 *                       break;
 *                }
 *         }
 *     }
 * }
 * </pre>
 *
 * @param <T> Class member
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class ConfiguredObjectTypeRegistryTransformer<T extends AutomatedFieldDescription>
        extends AbstractQpidTransformer<T>
        implements QpidTransformer<T>
{
    /** Target class name */
    private static final String CLASS_NAME = "org.apache.qpid.server.model.ConfiguredObjectTypeRegistry$AutomatedField";

    /** Internal class name */
    private static final String INTERNAL_CLASS_NAME =
            "org/apache/qpid/server/model/ConfiguredObjectTypeRegistry$AutomatedField";

    /** Method descriptors */
    private static final String DESC_CONSTRUCTOR =
            String.format("(%s%s%s)V", referenceName(FIELD), referenceName(METHOD), referenceName(METHOD));

    /** Field names */
    private static final String PRESETTING_ACTION = "_preSettingAction";
    private static final String POSTSETTING_ACTION = "_postSettingAction";
    private static final String PRESETTING_SIGNATURE = "_preSettingSignature";
    private static final String POSTSETTING_SIGNATURE = "_postSettingSignature";
    private static final String PRESETTING_HASHCODE = "_preSettingHashcode";
    private static final String POSTSETTING_HASHCODE = "_postSettingHashcode";

    /** Logger */
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredObjectTypeRegistryTransformer.class);

    /** List of fields @AutomatedField */
    private final List<T> _fields;

    /**
     * Constructor stores method list
     *
     * @param fields Automated fields to be handled
     */
    public ConfiguredObjectTypeRegistryTransformer(final List<T> fields)
    {
        super();
        _fields = new ArrayList<>(fields);
    }

    @Override
    public Logger getLogger()
    {
        return LOGGER;
    }

    @Override
    public List<T> getMemberDescriptions()
    {
        return new ArrayList<>(_fields);
    }

    @Override
    public ClassVisitor getTransformer(final ClassVisitor cv)
    {
        return new Transformer(cv, getMemberDescriptions());
    }

    private class Transformer extends ClassVisitor
    {
        private final List<MethodDescription> _beforeSetMethods;
        private final List<MethodDescription> _afterSetMethods;
        private final List<MethodDescription> _allMethods;

        private Transformer(final ClassVisitor cv, final List<T> fields)
        {
            super(Opcodes.ASM9, cv);
            _beforeSetMethods = fields.stream().map(AutomatedFieldDescription::getBeforeSet)
                                      .filter(Objects::nonNull).distinct()
                                      .collect(Collectors.toList());
            _afterSetMethods = fields.stream().map(AutomatedFieldDescription::getAfterSet)
                                     .filter(Objects::nonNull).distinct()
                                     .collect(Collectors.toList());
            _allMethods = Stream.concat(_beforeSetMethods.stream(), _afterSetMethods.stream())
                                .collect(Collectors.toList());
        }

        @Override()
        public void visitEnd()
        {
            final int size = _allMethods.size();
            visitMethodHandleFields(size, (ClassWriter) cv);
            final int access = Opcodes.ACC_PRIVATE;
            visitField(access, PRESETTING_SIGNATURE, referenceName(STRING), null, null).visitEnd();
            visitField(access, POSTSETTING_SIGNATURE, referenceName(STRING), null, null).visitEnd();
            visitField(access, PRESETTING_HASHCODE, TYPE_INT, null, null).visitEnd();
            visitField(access, POSTSETTING_HASHCODE, TYPE_INT, null, null).visitEnd();
            visitStaticInitializer(_allMethods, INTERNAL_CLASS_NAME, CLASS_NAME, (ClassWriter) cv);
        }

        @Override
        public MethodVisitor visitMethod(final int access,
                                         final String name,
                                         final String desc,
                                         final String signature,
                                         final String[] exceptions)
        {
            MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);

            // add custom logic to constructor
            if (name.equals(INIT) && desc.equals(DESC_CONSTRUCTOR))
            {
                mv = new ConstructorTransformer(mv);
            }

            // add custom logic to set
            if (SET.equals(name) &&
                    "(Lorg/apache/qpid/server/model/ConfiguredObject;Ljava/lang/Object;)V".equals(desc))
            {
                mv = new SetTransformer(mv, _beforeSetMethods, _afterSetMethods, _allMethods);
            }

            return mv;
        }
    }

    private static class ConstructorTransformer extends MethodVisitor
    {
        private ConstructorTransformer(final MethodVisitor delegate)
        {
            super(Opcodes.ASM9, delegate);
        }

        @Override()
        public void visitInsn(final int opcode)
        {
            if (opcode == Opcodes.RETURN)
            {
                int argIndex = 2;
                initSettingAssociatedAction(argIndex, PRESETTING_ACTION, PRESETTING_SIGNATURE, PRESETTING_HASHCODE);
                mv.visitFrame(Opcodes.F_FULL,
                              4,
                              new Object[]{INTERNAL_CLASS_NAME, FIELD, referenceName(METHOD), referenceName(METHOD)},
                              0,
                              new Object[]{});

                argIndex = 3;
                initSettingAssociatedAction(argIndex, POSTSETTING_ACTION, POSTSETTING_SIGNATURE, POSTSETTING_HASHCODE);
                mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
            }
            super.visitInsn(opcode);
        }

        private void initSettingAssociatedAction(final int argIndex,
                                                 final String action,
                                                 final String signature,
                                                 final String hashcode)
        {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_CLASS_NAME, action, referenceName(METHOD));
            final Label outerPostSetting = new Label();
            mv.visitJumpInsn(Opcodes.IFNULL, outerPostSetting);
            final Label innerPostSetting = new Label();
            mv.visitLabel(innerPostSetting);

            generateSignature(mv, argIndex, INTERNAL_CLASS_NAME, signature);

            final Label postSetting = new Label();
            mv.visitLabel(postSetting);

            generateHashCode(mv, INTERNAL_CLASS_NAME, signature, hashcode);

            mv.visitLabel(outerPostSetting);
        }
    }

    private class SetTransformer extends MethodVisitor
    {
        private final MethodVisitor _target;
        private final List<MethodDescription> _beforeSetMethods;
        private final List<MethodDescription> _afterSetMethods;
        private final List<MethodDescription> _allMethods;

        private SetTransformer(final MethodVisitor delegate,
                               final List<MethodDescription> beforeSetMethods,
                               final List<MethodDescription> afterSetMethods,
                               final List<MethodDescription> allMethods)
        {
            super(Opcodes.ASM9, null);
            _target = delegate;
            _afterSetMethods = afterSetMethods;
            _beforeSetMethods = beforeSetMethods;
            _allMethods = allMethods;
        }

        @Override
        public void visitCode()
        {
            _target.visitCode();

            visitSettingAssociatedAction(PRESETTING_ACTION,
                                         _beforeSetMethods,
                                         PRESETTING_HASHCODE,
                                         PRESETTING_SIGNATURE);

            _target.visitVarInsn(Opcodes.ALOAD, 0);
            _target.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_CLASS_NAME, FIELD_FIELD, referenceName(FIELD));
            _target.visitVarInsn(Opcodes.ALOAD, 1);
            _target.visitVarInsn(Opcodes.ALOAD, 2);
            _target.visitMethodInsn(Opcodes.INVOKEVIRTUAL, FIELD, SET, DESC_SET, FALSE);

            visitSettingAssociatedAction(POSTSETTING_ACTION,
                                         _afterSetMethods,
                                         POSTSETTING_HASHCODE,
                                         POSTSETTING_SIGNATURE);
            _target.visitInsn(Opcodes.RETURN);

            _target.visitMaxs(0, 0);
            _target.visitEnd();
        }

        private void visitSettingAssociatedAction(final String action,
                                                  final List<MethodDescription> methods,
                                                  final String hashcode,
                                                  final String signature)
        {
            final Label outer = new Label();
            _target.visitLabel(outer);
            _target.visitVarInsn(Opcodes.ALOAD, 0);
            _target.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_CLASS_NAME, action, referenceName(METHOD));
            final Label outerPostSetting = new Label();
            _target.visitJumpInsn(Opcodes.IFNULL, outerPostSetting);
            final Label innerPostSetting = new Label();
            _target.visitLabel(innerPostSetting);

            call(_target,
                 _allMethods,
                 methods,
                 hashcode,
                 signature,
                 outerPostSetting);
            _target.visitInsn(Opcodes.POP);

            _target.visitLabel(outerPostSetting);
            _target.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
        }

        private void call(final MethodVisitor mv,
                          final List<MethodDescription> allMethods,
                          final List<MethodDescription> methods,
                          final String hashcodeVariable,
                          final String signatureVariable,
                          final Label label)
        {
            final int configuredObjectIndex = 1;

            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_CLASS_NAME, hashcodeVariable, TYPE_INT);
            mv.visitVarInsn(Opcodes.ISTORE, 7);
            final int signatureIndex = 7;

            final Map<String, AttributeStackAddress> stackMap = createAttributesStackMap(methods);
            final int[] hashes = new int[stackMap.size()];
            final Label[] switchJumpLabels = new Label[stackMap.size()];
            fillHashesAndLabels(stackMap, hashes, switchJumpLabels);

            final Label switchLabel = new Label();

            mv.visitVarInsn(Opcodes.ILOAD, signatureIndex);
            mv.visitLookupSwitchInsn(switchLabel, hashes, switchJumpLabels);

            for (int i = 0; i < allMethods.size(); i++)
            {
                final MethodDescription method = allMethods.get(i);
                if (!methods.contains(method))
                {
                    continue;
                }
                mv.visitLabel(stackMap.get(method.getSignature()).getLabel());
                mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                mv.visitFieldInsn(Opcodes.GETSTATIC, INTERNAL_CLASS_NAME, "MH_" + i, referenceName(METHOD_HANDLE));
                mv.visitVarInsn(Opcodes.ALOAD, configuredObjectIndex);
                mv.visitTypeInsn(Opcodes.CHECKCAST, method.getDeclaringClass());
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, METHOD_HANDLE, INVOKE_EXACT, sig(method), FALSE);
                mv.visitJumpInsn(Opcodes.GOTO, label);
            }

            mv.visitLabel(switchLabel);
            mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

            /* create code: throw new UnsupportedOperationException(msg) */
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_CLASS_NAME, signatureVariable, referenceName(STRING));
            mv.visitVarInsn(Opcodes.ASTORE, 3);
            mv.visitTypeInsn(Opcodes.NEW, SSRE);
            mv.visitInsn(Opcodes.DUP);
            mv.visitLdcInsn("Failed to call automated field callback %s");
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitTypeInsn(Opcodes.ANEWARRAY, OBJECT);
            mv.visitInsn(Opcodes.DUP);
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitInsn(Opcodes.AASTORE);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, STRING, FORMAT, DESC_FORMAT, FALSE);
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, SSRE, INIT, DESC_EXCEPTION1, FALSE);
            mv.visitInsn(Opcodes.ATHROW);
        }
    }
}
