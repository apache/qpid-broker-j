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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

import org.apache.qpid.server.License;


public class CompositeTypeConstructorGenerator  extends AbstractProcessor
{
    private static final List<String> RESTRICTED_TYPES = Arrays.asList(
            "org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError",
            "org.apache.qpid.server.protocol.v1_0.type.transport.ConnectionError",
            "org.apache.qpid.server.protocol.v1_0.type.transport.SessionError",
            "org.apache.qpid.server.protocol.v1_0.type.transport.LinkError",
            "org.apache.qpid.server.protocol.v1_0.type.transaction.TransactionErrors",
            "org.apache.qpid.server.protocol.v1_0.type.transport.ReceiverSettleMode",
            "org.apache.qpid.server.protocol.v1_0.type.transport.SenderSettleMode",
            "org.apache.qpid.server.protocol.v1_0.type.transport.Role",
            "org.apache.qpid.server.protocol.v1_0.type.security.SaslCode",
            "org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionDetectionPolicy",
            "org.apache.qpid.server.protocol.v1_0.type.extensions.soleconn.SoleConnectionEnforcementPolicy",
            "org.apache.qpid.server.protocol.v1_0.type.messaging.StdDistMode",
            "org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability",
            "org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusExpiryPolicy",
            "org.apache.qpid.server.protocol.v1_0.type.transaction.TxnCapability");


    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latest();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes()
    {
        return Collections.singleton(CompositeType.class.getName());
    }

    @Override
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnvironment)
    {
        if(roundEnvironment.processingOver())
        {
            return true;
        }

        Filer filer = processingEnv.getFiler();
        try
        {
            for (Element e : roundEnvironment.getElementsAnnotatedWith(CompositeType.class))
            {
                if(e.getKind() == ElementKind.CLASS)
                {
                    generateCompositeTypeConstructor(filer, (TypeElement) e);
                }
            }
        }
        catch (Exception e)
        {
            try(StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw))
            {
                e.printStackTrace(pw);
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Unexpected Error: " + sw.toString());
            }
            catch (IOException ioe)
            {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Error: " + ioe.getLocalizedMessage());
            }
        }
        return true;
    }


    private void generateCompositeTypeConstructor(final Filer filer, final TypeElement typeElement)
    {
        String objectQualifiedClassName = typeElement.getQualifiedName().toString();
        String objectSimpleName = typeElement.getSimpleName().toString();
        String compositeTypeConstructorNameSimpleName = objectSimpleName + "Constructor";
        PackageElement packageElement = (PackageElement) typeElement.getEnclosingElement();
        final String compositeTypeConstructorPackage = packageElement.getQualifiedName() + ".codec";
        String compositeTypeConstructorName = compositeTypeConstructorPackage + "." + compositeTypeConstructorNameSimpleName;
        final CompositeType annotation = typeElement.getAnnotation(CompositeType.class);

        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Generating composite constructor file for " + objectQualifiedClassName);

        try
        {
            JavaFileObject factoryFile = filer.createSourceFile(compositeTypeConstructorName);
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(factoryFile.openOutputStream(), "UTF-8"));
            pw.println("/*");
            for(String headerLine : License.LICENSE)
            {
                pw.println(" *" + headerLine);
            }
            pw.println(" */");
            pw.println();
            pw.print("package ");
            pw.print(compositeTypeConstructorPackage);
            pw.println(";");
            pw.println();

            pw.println("import java.util.List;");
            pw.println();
            pw.println("import org.apache.qpid.server.protocol.v1_0.codec.AbstractCompositeTypeConstructor;");
            pw.println("import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructorRegistry;");
            pw.println("import org.apache.qpid.server.protocol.v1_0.type.AmqpErrorException;");
            pw.println("import org.apache.qpid.server.protocol.v1_0.type.Symbol;");
            pw.println("import org.apache.qpid.server.protocol.v1_0.type.UnsignedLong;");
            pw.println("import org.apache.qpid.server.protocol.v1_0.type.transport.AmqpError;");
            pw.println("import org.apache.qpid.server.protocol.v1_0.type.transport.Error;");
            pw.println("import " + objectQualifiedClassName + ";");
            pw.println();

            pw.println("public final class " + compositeTypeConstructorNameSimpleName + " extends AbstractCompositeTypeConstructor<"+ objectSimpleName +">");
            pw.println("{");
            pw.println("    private static final " + compositeTypeConstructorNameSimpleName + " INSTANCE = new " + compositeTypeConstructorNameSimpleName + "();");
            pw.println();

            pw.println("    public static void register(DescribedTypeConstructorRegistry registry)");
            pw.println("    {");
            pw.println("        registry.register(Symbol.valueOf(\"" + annotation.symbolicDescriptor() + "\"), INSTANCE);");
            pw.println(String.format("        registry.register(UnsignedLong.valueOf(%#016x), INSTANCE);", annotation.numericDescriptor()));
            pw.println("    }");
            pw.println();

            pw.println("    @Override");
            pw.println("    protected String getTypeName()");
            pw.println("    {");
            pw.println("        return " + objectSimpleName + ".class.getSimpleName();");
            pw.println("    }");
            pw.println();

            pw.println("    @Override");
            pw.println("    protected " + objectSimpleName + " construct(final FieldValueReader fieldValueReader) throws AmqpErrorException");
            pw.println("    {");
            pw.println("        " + objectSimpleName + " obj = new " + objectSimpleName + "();");
            pw.println();
            generateAssigners(pw, typeElement);
            pw.println("        return obj;");
            pw.println("    }");
            pw.println("}");
            pw.close();
        }
        catch (IOException e)
        {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                                                     "Failed to write composite constructor file: "
                                                     + compositeTypeConstructorName
                                                     + " - "
                                                     + e.getLocalizedMessage());
        }
    }

    private void generateAssigners(final PrintWriter pw, final TypeElement typeElement)
    {
        Types typeUtils = processingEnv.getTypeUtils();

        final List<AnnotatedField> annotatedFields = new ArrayList<>();
        for (Element element : typeElement.getEnclosedElements())
        {
            if (element instanceof VariableElement && element.getKind() == ElementKind.FIELD)
            {
                boolean annotationFound = false;
                for(AnnotationMirror annotationMirror : element.getAnnotationMirrors())
                {
                    if(annotationMirror.getAnnotationType().toString().equals("org.apache.qpid.server.protocol.v1_0.CompositeTypeField"))
                    {
                        if (annotationFound)
                        {
                            processingEnv.getMessager()
                                         .printMessage(Diagnostic.Kind.ERROR,
                                                       String.format(
                                                               "More than one CompositeTypeField annotations on field '%s.%s'",
                                                               typeElement.getSimpleName(),
                                                               element.getSimpleName()));
                        }
                        annotationFound = true;
                        annotatedFields.add(new AnnotatedField((VariableElement) element, annotationMirror));
                    }
                }
            }
        }

        annotatedFields.sort(Comparator.comparingInt(AnnotatedField::getIndex));

        for (int index = 0; index < annotatedFields.size(); ++index)
        {
            AnnotatedField annotatedField = annotatedFields.get(index);
            final VariableElement variableElement = annotatedField.getVariableElement();
            final String fieldName = stripUnderscore(variableElement.getSimpleName().toString());
            if (annotatedField.getIndex() != index)
            {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,String.format(
                        "Unexpected CompositeTypeField index '%d' is specified on field '%s' of '%s'. Expected %d.",
                        annotatedField.getIndex(),
                        fieldName,
                        typeElement.getSimpleName(),
                        index));
            }

            final String baseIndent = "        ";
            if (variableElement.asType().getKind() == TypeKind.ARRAY)
            {
                final TypeMirror componentType = ((ArrayType) variableElement.asType()).getComponentType();
                final String functionString;
                if (annotatedField.getFactory() != null)
                {
                    functionString = "x -> " + annotatedField.getFactory() + "(x)";
                }
                else if (RESTRICTED_TYPES.contains(componentType))
                {
                    functionString = variableElement.asType().toString() + "::valueOf";
                }
                else
                {
                    functionString = "x -> (" + componentType + ") x";
                }
                pw.println(String.format("        %s %s = fieldValueReader.readArrayValue(%d, \"%s\", %s, %s.class, %s);",
                                             annotatedField.getVariableElement().asType(),
                                             fieldName,
                                             index,
                                             fieldName,
                                             annotatedField.isMandatory(),
                                             componentType,
                                             functionString));
                optionallyWrapInNullCheck(!annotatedField.isMandatory(), pw, baseIndent, fieldName, indent -> {
                    pw.println(indent + "obj." + getSetterName(variableElement) + "(" + fieldName + ");");
                });
            }
            else if (annotatedField.getFactory() != null || RESTRICTED_TYPES.contains(variableElement.asType().toString()))
            {
                String functionName = annotatedField.getFactory() != null ? annotatedField.getFactory() : variableElement.asType().toString() + ".valueOf";
                pw.println(String.format("        Object %s = fieldValueReader.readValue(%d, \"%s\", %s, Object.class);",
                                         fieldName,
                                         index,
                                         fieldName,
                                         annotatedField.isMandatory()));
                optionallyWrapInNullCheck(!annotatedField.isMandatory(), pw, baseIndent, fieldName, indent -> {
                    pw.println(indent + "try");
                    pw.println(indent + "{");
                    pw.println(indent + "    obj." + getSetterName(variableElement) + "(" + functionName + "(" + fieldName + "));");
                    pw.println(indent + "}");
                    pw.println(indent + "catch (RuntimeException e)");
                    pw.println(indent + "{");
                    pw.println(indent + "    Error error = new Error(AmqpError.DECODE_ERROR, \"Could not decode value field '" + fieldName + "' of '" + typeElement.getSimpleName() + "'\");");
                    pw.println(indent + "    throw new AmqpErrorException(error, e);");
                    pw.println(indent + "}");
                });
            }
            else if (typeUtils.isSameType(typeUtils.erasure(variableElement.asType()),
                                          getErasure(processingEnv, "java.util.Map")))
            {
                List<? extends TypeMirror> args = ((DeclaredType) variableElement.asType()).getTypeArguments();
                if (args.size() != 2)
                {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                                                             "Map types must have exactly two type arguments");
                }
                pw.println(String.format("        %s %s = fieldValueReader.readMapValue(%d, \"%s\", %s, %s.class, %s.class);",
                                         annotatedField.getVariableElement().asType(),
                                         fieldName,
                                         index,
                                         fieldName,
                                         annotatedField.isMandatory(),
                                         args.get(0),
                                         args.get(1)));
                optionallyWrapInNullCheck(!annotatedField.isMandatory(), pw, baseIndent, fieldName, indent -> {
                    pw.println(indent + "obj." + getSetterName(variableElement) + "(" + fieldName + ");");
                });
            }
            else
            {
                pw.println(String.format("        %s %s = fieldValueReader.readValue(%d, \"%s\", %s, %s.class);",
                                         annotatedField.getVariableElement().asType(),
                                         fieldName,
                                         index,
                                         fieldName,
                                         annotatedField.isMandatory(),
                                         annotatedField.getVariableElement().asType()));
                optionallyWrapInNullCheck(!annotatedField.isMandatory(), pw, baseIndent, fieldName, indent -> {
                    pw.println(indent + "obj." + getSetterName(variableElement) + "(" + fieldName + ");");
                });
            }

            pw.println();
        }
    }

    private void optionallyWrapInNullCheck(boolean wrap, PrintWriter pw, String indent, String fieldName, Consumer<String> f)
    {
        if (wrap)
        {
            pw.println(indent + "if (" + fieldName + " != null)");
            pw.println(indent + "{");
            indent = "    " + indent;
        }
        f.accept(indent);
        if (wrap)
        {
            indent = indent.substring(4);
            pw.println(indent + "}");
        }
    }

    private String getSetterName(final VariableElement variableElement)
    {
        final String fieldName = stripUnderscore(variableElement.getSimpleName().toString());
        return "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
    }

    private String stripUnderscore(final String fieldName)
    {
        if (fieldName.startsWith("_"))
        {
            return fieldName.substring(1);
        }
        return fieldName;
    }

    private static class AnnotatedField
    {
        private final VariableElement _variableElement;
        private final AnnotationMirror _annotationMirror;
        private final int _index;
        private final String _factory;
        private final boolean _mandatory;

        public AnnotatedField(final VariableElement variableElement, final AnnotationMirror annotationMirror)
        {
            _variableElement = variableElement;
            _annotationMirror = annotationMirror;
            String factory = null;
            boolean mandatory = false;
            int index = -1;
            for (Map.Entry<? extends ExecutableElement,? extends AnnotationValue> entry : annotationMirror.getElementValues().entrySet())
            {
                if ("index".contentEquals(entry.getKey().getSimpleName()))
                {
                    index = (int) entry.getValue().getValue();
                }
                else if ("deserializationConverter".contentEquals(entry.getKey().getSimpleName()))
                {
                    factory = (String) entry.getValue().getValue();
                }
                else if ("mandatory".contentEquals(entry.getKey().getSimpleName()))
                {
                    mandatory = (boolean) entry.getValue().getValue();
                }
            }
            _index = index;
            _mandatory = mandatory;
            _factory = factory;
        }

        public VariableElement getVariableElement()
        {
            return _variableElement;
        }

        public AnnotationMirror getAnnotationMirror()
        {
            return _annotationMirror;
        }

        public int getIndex()
        {
            return _index;
        }

        public String getFactory()
        {
            return _factory;
        }

        public boolean isMandatory()
        {
            return _mandatory;
        }
    }

    private static TypeMirror getErasure(ProcessingEnvironment processingEnv, final String className)
    {
        final Types typeUtils = processingEnv.getTypeUtils();
        final Elements elementUtils = processingEnv.getElementUtils();
        return typeUtils.erasure(elementUtils.getTypeElement(className).asType());
    }

}