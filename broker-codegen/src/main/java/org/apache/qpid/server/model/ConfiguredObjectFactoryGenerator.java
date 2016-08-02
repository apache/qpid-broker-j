/*
 *
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
package org.apache.qpid.server.model;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
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
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

import org.apache.qpid.server.License;

public class ConfiguredObjectFactoryGenerator extends AbstractProcessor
{
    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latest();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes()
    {
        return Collections.singleton(ManagedObjectFactoryConstructor.class.getName());
    }

    @Override
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv)
    {

        if(roundEnv.processingOver())
        {

            return true;
        }

        Filer filer = processingEnv.getFiler();

        try
        {

            for (Element e : roundEnv.getElementsAnnotatedWith(ManagedObjectFactoryConstructor.class))
            {
                if(e.getKind() == ElementKind.CONSTRUCTOR)
                {
                    ExecutableElement constructorElement = (ExecutableElement) e;
                    generateAccessControlEnforcingSubclass(filer, constructorElement);
                    generateObjectFactory(filer, constructorElement);
                }
            }

        }
        catch (Exception e)
        {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, "Error: " + e.getLocalizedMessage());
        }

        return true;
    }

    private void generateAccessControlEnforcingSubclass(final Filer filer, final ExecutableElement constructorElement)
    {
        TypeElement classElement = (TypeElement) constructorElement.getEnclosingElement();
        String childClassName = classElement.getQualifiedName().toString() + "WithAccessChecking";
        String childClassSimpleName = classElement.getSimpleName().toString() + "WithAccessChecking";
        String objectSimpleName = classElement.getSimpleName().toString();
        PackageElement packageElement = (PackageElement) classElement.getEnclosingElement();
        try
        {
            JavaFileObject factoryFile = filer.createSourceFile(childClassName);
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(factoryFile.openOutputStream(), "UTF-8"));
            pw.println("/*");
            for (String headerLine : License.LICENSE)
            {
                pw.println(" *" + headerLine);
            }
            pw.println(" */");
            pw.println();
            pw.print("package ");
            pw.print(packageElement.getQualifiedName());
            pw.println(";");
            pw.println();
            pw.println("import static org.apache.qpid.server.security.access.Operation.METHOD;");
            pw.println();
            pw.println("import java.util.Map;");
            pw.println();
            pw.println("import org.apache.qpid.server.util.FixedKeyMapCreator;");
            pw.println();
            pw.println("final class " + childClassSimpleName + " extends "+ objectSimpleName);
            pw.println("{");
            pw.print("    " + childClassSimpleName + "(final Map<String, Object> attributes");
            boolean first = true;
            for(VariableElement param : constructorElement.getParameters())
            {
                if(first)
                {
                    first = false;
                }
                else
                {
                    pw.print(", final " + param.asType() + " " + createParamNameFromType(param));
                }

            }
            pw.println(")");

            pw.println("    {");
            pw.print("        super(attributes");
            first = true;
            for(VariableElement param : constructorElement.getParameters())
            {
                if(first)
                {
                    first = false;
                }
                else
                {
                    pw.print(", " + createParamNameFromType(param));
                }
            }
            pw.println(");");

            pw.println("    }");
            pw.println();

            generateAccessCheckedMethods(classElement, pw, new HashSet<TypeElement>(), new HashSet<String>());

            pw.println("}");

            pw.close();
        }
        catch (ClassNotFoundException | IOException e)
        {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                                                     "Failed to write file: "
                                                     + childClassName
                                                     + " - "
                                                     + e.getLocalizedMessage());
        }

    }

    private void generateAccessCheckedMethods(final TypeElement typeElement,
                                              final PrintWriter pw,
                                              final HashSet<TypeElement> processedClasses,
                                              final HashSet<String> processedMethods) throws ClassNotFoundException
    {
        if(processedClasses.add(typeElement))
        {
            Element superClassElement = processingEnv.getTypeUtils().asElement(typeElement.getSuperclass());
            if(superClassElement instanceof TypeElement)
            {
                generateAccessCheckedMethods((TypeElement) superClassElement, pw, processedClasses, processedMethods);
            }

            for(TypeMirror ifMirror : typeElement.getInterfaces())
            {
                Element ifElement = processingEnv.getTypeUtils().asElement(ifMirror);
                if(ifElement instanceof TypeElement)
                {
                    generateAccessCheckedMethods((TypeElement) ifElement, pw, processedClasses, processedMethods);
                }
            }

            for(Element element : typeElement.getEnclosedElements())
            {
                if(element instanceof ExecutableElement && element.getKind() == ElementKind.METHOD && processedMethods.add(element.getSimpleName().toString()))
                {
                    for(AnnotationMirror annotationMirror : element.getAnnotationMirrors())
                    {
                        if(annotationMirror.getAnnotationType().toString().equals("org.apache.qpid.server.model.ManagedOperation"))
                        {
                            processManagedOperation(pw, (ExecutableElement) element);
                            break;
                        }
                    }
                }
            }
        }

    }

    private void processManagedOperation(final PrintWriter pw, final ExecutableElement methodElement)
    {

        if(!methodElement.getParameters().isEmpty())
        {
            pw.print("    private static final FixedKeyMapCreator ");
            pw.print(methodElement.getSimpleName().toString().replaceAll("([A-Z])", "_$1").toUpperCase() + "_MAP_CREATOR");
            pw.print(" = new FixedKeyMapCreator(\"");
            boolean first = true;
            for(VariableElement param : methodElement.getParameters())
            {
                if(first)
                {
                    first = false;
                }
                else
                {
                    pw.print("\", \"");
                }
                pw.print(getParamName(param));
            }
            pw.println("\");");
            pw.println();
        }

        pw.print("    public " + methodElement.getReturnType() + " " + methodElement.getSimpleName().toString() + "(");
        boolean first = true;
        for(VariableElement param : methodElement.getParameters())
        {
            if(first)
            {
                first = false;
            }
            else
            {
                pw.print(", ");
            }
            pw.print("final ");
            pw.print(param.asType());
            pw.print(" ");

            pw.print(getParamName(param));
        }
        pw.println(")");
        pw.println("    {");
        pw.print("        authorise(METHOD(\"");
        pw.print(methodElement.getSimpleName().toString());
        pw.print("\")");
        if(!methodElement.getParameters().isEmpty())
        {
            pw.print(", ");
            pw.print(methodElement.getSimpleName().toString().replaceAll("([A-Z])", "_$1").toUpperCase() + "_MAP_CREATOR");
            pw.print(".createMap(");
            first = true;
            for(VariableElement param : methodElement.getParameters())
            {
                if(first)
                {
                    first = false;
                }
                else
                {
                    pw.print(", ");
                }
                pw.print(getParamName(param));
            }

            pw.print(")");
        }
        pw.println(");");
        pw.println();

        pw.print("        ");
        if(methodElement.getReturnType().getKind() != TypeKind.VOID)
        {
            pw.print("return ");
        }
        pw.print("super.");
        pw.print(methodElement.getSimpleName().toString());
        pw.print("(");
        first = true;
        for(VariableElement param : methodElement.getParameters())
        {
            if(first)
            {
                first = false;
            }
            else
            {
                pw.print(", ");
            }
            pw.print(getParamName(param));
        }
        pw.println(");");
        pw.println("    }");
        pw.println();
    }

    private String getParamName(final VariableElement param)
    {
        return getParamName(getAnnotation(param, "org.apache.qpid.server.model.Param"));
    }

    private String getParamName(final AnnotationMirror paramAnnotation)
    {
        String paramName = null;
        for(ExecutableElement paramElement : paramAnnotation.getElementValues().keySet())
        {
            if(paramElement.getSimpleName().toString().equals("name"))
            {
                AnnotationValue value = paramAnnotation.getElementValues().get(paramElement);
                paramName = value.getValue().toString();
                break;
            }
        }
        return paramName;
    }

    private AnnotationMirror getAnnotation(final Element param, final String name)
    {
        for(AnnotationMirror annotationMirror : param.getAnnotationMirrors())
        {
            if(annotationMirror.getAnnotationType().toString().equals(name))
            {
                return annotationMirror;
            }
        }
        return null;
    }

    private String createParamNameFromType(final VariableElement param)
    {
        TypeMirror erasureType = processingEnv.getTypeUtils().erasure(param.asType());
        String nameWithPackage = erasureType.toString().toLowerCase();
        return nameWithPackage.substring(nameWithPackage.lastIndexOf('.')+1);
    }

    private String generateObjectFactory(final Filer filer, final ExecutableElement constructorElement)
    {
        TypeElement classElement = (TypeElement) constructorElement.getEnclosingElement();
        String factoryName = classElement.getQualifiedName().toString() + "Factory";
        String factorySimpleName = classElement.getSimpleName().toString() + "Factory";
        String objectSimpleName = classElement.getSimpleName().toString();
        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Generating factory file for " + classElement.getQualifiedName().toString());
        final ManagedObjectFactoryConstructor annotation =
                constructorElement.getAnnotation(ManagedObjectFactoryConstructor.class);
        PackageElement packageElement = (PackageElement) classElement.getEnclosingElement();

        try
        {
            JavaFileObject factoryFile = filer.createSourceFile(factoryName);
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(factoryFile.openOutputStream(), "UTF-8"));
            pw.println("/*");
            for(String headerLine : License.LICENSE)
            {
                pw.println(" *" + headerLine);
            }
            pw.println(" */");
            pw.println();
            pw.print("package ");
            pw.print(packageElement.getQualifiedName());
            pw.println(";");
            pw.println();

            pw.println("import java.util.Map;");
            pw.println();
            pw.println("import org.apache.qpid.server.model.AbstractConfiguredObjectTypeFactory;");
            pw.println("import org.apache.qpid.server.model.ConfiguredObject;");
            pw.println("import org.apache.qpid.server.plugin.PluggableService;");
            if(annotation.conditionallyAvailable())
            {
                pw.println("import org.apache.qpid.server.plugin.ConditionallyAvailable;");
            }
            pw.println();
            pw.println("@PluggableService");
            pw.println("public final class " + factorySimpleName + " extends AbstractConfiguredObjectTypeFactory<"+ objectSimpleName +">");
            if(annotation.conditionallyAvailable())
            {
                pw.println("    implements ConditionallyAvailable");
            }
            pw.println("{");
            pw.println("    public " + factorySimpleName + "()");
            pw.println("    {");
            pw.println("        super(" + objectSimpleName + ".class);");
            pw.println("    }");
            pw.println();
            pw.println("    @Override");
            pw.println("    protected "+objectSimpleName+" createInstance(final Map<String, Object> attributes, final ConfiguredObject<?>... parents)");
            pw.println("    {");
            pw.print("        return new "+objectSimpleName+ "WithAccessChecking(attributes");
            boolean first = true;
            for(VariableElement param : constructorElement.getParameters())
            {
                if(first)
                {
                    first = false;
                }
                else
                {
                    TypeMirror erasureType = processingEnv.getTypeUtils().erasure(param.asType());
                    pw.print(", getParent("+erasureType.toString()+".class,parents)");
                }
            }
            pw.println(");");
            pw.println("    }");
            if(annotation.conditionallyAvailable())
            {
                pw.println();
                pw.println("    @Override");
                pw.println("    public boolean isAvailable()");
                pw.println("    {");
                pw.println("        return " + objectSimpleName + ".isAvailable();");
                pw.println("    }");

            }

            pw.println("}");

            pw.close();
        }
        catch (IOException e)
        {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                                                     "Failed to write factory file: "
                                                     + factoryName
                                                     + " - "
                                                     + e.getLocalizedMessage());
        }

        return factoryName;
    }

}
