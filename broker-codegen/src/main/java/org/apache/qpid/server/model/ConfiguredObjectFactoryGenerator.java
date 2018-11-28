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
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
import javax.lang.model.type.PrimitiveType;
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
            pw.println("import static org.apache.qpid.server.security.access.Operation.INVOKE_METHOD;");
            pw.println();
            pw.println("import java.util.Map;");
            pw.println("import java.util.concurrent.ExecutionException;");
            pw.println();
            pw.println("import com.google.common.util.concurrent.Futures;");
            pw.println("import com.google.common.util.concurrent.ListenableFuture;");
            pw.println();
            pw.println("import org.apache.qpid.server.configuration.updater.Task;");
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

            generateAccessCheckedMethods(childClassSimpleName, classElement, pw, new HashSet<TypeElement>(), new HashSet<String>());

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

    private void generateAccessCheckedMethods(final String className,
                                              final TypeElement typeElement,
                                              final PrintWriter pw,
                                              final HashSet<TypeElement> processedClasses,
                                              final HashSet<String> processedMethods) throws ClassNotFoundException
    {
        if(processedClasses.add(typeElement))
        {
            Element superClassElement = processingEnv.getTypeUtils().asElement(typeElement.getSuperclass());
            if(superClassElement instanceof TypeElement)
            {
                generateAccessCheckedMethods(className, (TypeElement) superClassElement, pw, processedClasses, processedMethods);
            }

            for(TypeMirror ifMirror : typeElement.getInterfaces())
            {
                Element ifElement = processingEnv.getTypeUtils().asElement(ifMirror);
                if(ifElement instanceof TypeElement)
                {
                    generateAccessCheckedMethods(className, (TypeElement) ifElement, pw, processedClasses, processedMethods);
                }
            }

            for(Element element : typeElement.getEnclosedElements())
            {
                if(element instanceof ExecutableElement && element.getKind() == ElementKind.METHOD && !processedMethods.contains(element.getSimpleName().toString()))
                {
                    for(AnnotationMirror annotationMirror : element.getAnnotationMirrors())
                    {
                        if(annotationMirror.getAnnotationType().toString().equals("org.apache.qpid.server.model.ManagedOperation"))
                        {
                            processedMethods.add(element.getSimpleName().toString());
                            processManagedOperation(pw, className, (ExecutableElement) element, annotationMirror);
                            break;
                        }
                        else if(annotationMirror.getAnnotationType().toString().equals("org.apache.qpid.server.model.DoOnConfigThread"))
                        {
                            processedMethods.add(element.getSimpleName().toString());
                            processDoOnConfigMethod(pw, className, (ExecutableElement) element, annotationMirror);
                            break;
                        }
                    }
                }
            }
        }

    }
    private void processDoOnConfigMethod(final PrintWriter pw, final String className, final ExecutableElement methodElement, final AnnotationMirror annotationMirror)
    {
        pw.println("    @Override");
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
        final List<? extends TypeMirror> thrownTypes = methodElement.getThrownTypes();
        if (!thrownTypes.isEmpty())
        {
            pw.println(thrownTypes.stream().map(TypeMirror::toString).collect(Collectors.joining(" , ", "    throws ", "")));
        }
        pw.println("    {");


        final String parameterList = getParameterList(methodElement);

        final String callToSuper = "super." + methodElement.getSimpleName().toString() + parameterList;

        pw.print("        ");
        if (methodElement.getReturnType().getKind() != TypeKind.VOID)
        {
            pw.print("return ");
        }
        pw.println(wrapByDoOnConfigThread(callToSuper, className, methodElement));


        pw.println("    }");
        pw.println();
    }

    private void processManagedOperation(final PrintWriter pw, final String className, final ExecutableElement methodElement, final AnnotationMirror annotationMirror)
    {
        final Map<? extends ExecutableElement, ? extends AnnotationValue> elementValues =
                processingEnv.getElementUtils().getElementValuesWithDefaults(annotationMirror);
        boolean wrapCallToSuper = false;
        boolean log = false;
        boolean  skipAclCheck = false;
        for (ExecutableElement executableElement : elementValues.keySet())
        {
            if ("changesConfiguredObjectState".contentEquals(executableElement.getSimpleName()))
            {
                wrapCallToSuper = (Boolean) elementValues.get(executableElement).getValue();
            }
            else if("log".contentEquals(executableElement.getSimpleName()))
            {
                log = (Boolean) elementValues.get(executableElement).getValue();
            }
            else if("skipAclCheck".contentEquals(executableElement.getSimpleName()))
            {
                skipAclCheck = (Boolean) elementValues.get(executableElement).getValue();
            }
        }

        if(!(methodElement.getParameters().isEmpty() || skipAclCheck))
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

        final String parameterList = getParameterList(methodElement);

        if (!skipAclCheck)
        {
            pw.print("        authorise(INVOKE_METHOD(\"");
            pw.print(methodElement.getSimpleName().toString());
            pw.print("\")");


            if (!methodElement.getParameters().isEmpty())
            {
                pw.print(", ");
                pw.print(methodElement.getSimpleName().toString().replaceAll("([A-Z])", "_$1").toUpperCase()
                         + "_MAP_CREATOR");
                pw.print(".createMap" + parameterList);
            }
            pw.println(");");
            pw.println();
        }

        if(log)
        {
            pw.print("        logOperation(\"");
            pw.print(methodElement.getSimpleName().toString());
            pw.println("\");");
        }

        final String callToSuper = "super." + methodElement.getSimpleName().toString() + parameterList;

        pw.print("        ");
        if (methodElement.getReturnType().getKind() != TypeKind.VOID)
        {
            pw.print("return ");
        }
        if (wrapCallToSuper)
        {
            pw.println(wrapByDoOnConfigThread(callToSuper, className, methodElement));
        }
        else
        {
            pw.println(callToSuper + ";");
        }


        pw.println("    }");
        pw.println();
    }

    private String wrapByDoOnConfigThread(final String callToWrap,
                                          final String className,
                                          final ExecutableElement methodElement)
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter pw = new PrintWriter(stringWriter);
        String boxedReturnTypeName = getBoxedReturnTypeAsString(methodElement);
        pw.println("doSync(doOnConfigThread(new Task<ListenableFuture<"
                   + boxedReturnTypeName
                   + ">, RuntimeException>()");
        pw.println("            {");
        pw.println("                private String _args;");
        pw.println("                @Override");
        pw.println("                public ListenableFuture<"
                   + boxedReturnTypeName
                   + "> execute()");
        pw.println("                {");
        final List<? extends TypeMirror> thrownTypes = methodElement.getThrownTypes();
        if (!thrownTypes.isEmpty())
        {
            pw.println("                    try");
            pw.println("                    {");
        }
        if (methodElement.getReturnType().getKind() != TypeKind.VOID)
        {
            pw.println("                    return Futures.<"
                       + boxedReturnTypeName
                       + ">immediateFuture("
                       + className
                       + "."
                       + callToWrap
                       + ");");
        }
        else
        {
            pw.println("                    " + className + "." + callToWrap + ";");
            pw.println("                    return Futures.<"
                       + boxedReturnTypeName
                       + ">immediateFuture(null);");
        }
        if (!thrownTypes.isEmpty())
        {
            pw.println("                    }");
            pw.println(thrownTypes.stream()
                                  .map(TypeMirror::toString)
                                  .collect(Collectors.joining(" | ", "                    catch (", " e)")));
            pw.println("                    {");
            pw.println("                        return Futures.immediateFailedFuture(e);");
            pw.println("                    }");
        }
        pw.println("                }");

        pw.println("                @Override");
        pw.println("                public String getObject()");
        pw.println("                {");
        pw.println("                    return " + className + ".this.toString();");
        pw.println("                }");

        pw.println("                @Override");
        pw.println("                public String getAction()");
        pw.println("                {");
        pw.println("                    return \"" + methodElement.getSimpleName() + "\";");
        pw.println("                }");

        pw.println("                @Override");
        pw.println("                public String getArguments()");
        pw.println("                {");
        if (!methodElement.getParameters().isEmpty())
        {
            pw.println("                    if (_args == null)");
            pw.println("                    {");
            boolean first = true;
            String args = "_args = ";
            for (VariableElement param : methodElement.getParameters())
            {
                if (!first)
                {
                    args += " + \",\" + ";
                }
                else
                {
                    first = false;
                }
                args += "\"" + getParamName(param) + "=\" + " + getParamName(param);
            }
            pw.println("                        " + args + ";");
            pw.println("                    }");
        }
        pw.println("                    return _args;");
        pw.println("                }");

        pw.println("            }));");
        return stringWriter.toString();
    }

    private String getBoxedReturnTypeAsString(final ExecutableElement methodElement)
    {
        TypeMirror returnType = methodElement.getReturnType();
        String returnTypeName;
        if (returnType.getKind().isPrimitive())
        {
            TypeElement returnTypeElement =
                    processingEnv.getTypeUtils().boxedClass((PrimitiveType) returnType);
            returnTypeName = returnTypeElement.asType().toString();
        }
        else if (returnType.getKind() == TypeKind.VOID)
        {
            returnTypeName = "Void";
        }
        else
        {
            returnTypeName = methodElement.getReturnType().toString();
        }
        return returnTypeName;
    }

    private String getParameterList(final ExecutableElement methodElement)
    {
        StringBuilder parametersStringBuilder = new StringBuilder("(");
        boolean first = true;
        for(VariableElement param : methodElement.getParameters())
        {
            if(first)
            {
                first = false;
            }
            else
            {
                parametersStringBuilder.append(", ");
            }
            parametersStringBuilder.append(getParamName(param));
        }
        parametersStringBuilder.append(")");
        return parametersStringBuilder.toString();
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
        String objectQualifiedClassName = classElement.getQualifiedName().toString();
        String factoryName = objectQualifiedClassName + "Factory";
        String factorySimpleName = classElement.getSimpleName().toString() + "Factory";
        String objectSimpleName = classElement.getSimpleName().toString();
        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Generating factory file for " + objectQualifiedClassName);
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
            pw.println("    protected "+objectSimpleName+" createInstance(final Map<String, Object> attributes, final ConfiguredObject<?> parent)");
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
                    pw.print(String.format(", (%s)parent", erasureType.toString()));
                }
            }
            pw.println(");");
            pw.println("    }");
            if(annotation.conditionallyAvailable())
            {
                final String condition = annotation.condition();
                pw.println();
                pw.println("    @Override");
                pw.println("    public boolean isAvailable()");
                pw.println("    {");
                if ("".equals(condition))
                {
                    pw.println("        return " + objectSimpleName + ".isAvailable();");
                }
                else
                {
                    if (condition.matches("([\\w][\\w\\d_]+\\.)+[\\w][\\w\\d_\\$]*#[\\w\\d_]+\\s*\\(\\s*\\)"))
                    {
                        pw.println("        return " + condition.replace('#', '.') + ";");

                    }
                    else
                    {
                        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                                                                 String.format(
                                                                         "Invalid condition expression for '%s' : %s",
                                                                         objectQualifiedClassName,
                                                                         condition));
                    }
                }
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
