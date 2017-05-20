/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.server.management.plugin.servlet.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectAttribute;
import org.apache.qpid.server.model.ConfiguredObjectAttributeOrStatistic;
import org.apache.qpid.server.model.ConfiguredObjectFinder;
import org.apache.qpid.server.model.ConfiguredObjectOperation;
import org.apache.qpid.server.model.ConfiguredObjectStatistic;
import org.apache.qpid.server.model.ConfiguredSettableAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.OperationParameter;

public class ApiDocsServlet extends AbstractServlet
{
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiDocsServlet.class);

    private static final Set<Character> VOWELS = new HashSet<>(Arrays.asList('a','e','i','o','u'));

    public static final Comparator<Class<? extends ConfiguredObject>> CLASS_COMPARATOR =
            new Comparator<Class<? extends ConfiguredObject>>()
            {
                @Override
                public int compare(final Class<? extends ConfiguredObject> o1,
                                   final Class<? extends ConfiguredObject> o2)
                {
                    return o1.getSimpleName().compareTo(o2.getSimpleName());
                }

            };

    private transient final ConcurrentMap<Class<? extends ConfiguredObject>, List<Class<? extends ConfiguredObject>>> _typeSpecialisations = new ConcurrentHashMap<>();

    public ApiDocsServlet()
    {
        super();
    }

    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response,
                         final ConfiguredObject<?> managedObject) throws ServletException, IOException
    {
        ConfiguredObjectFinder finder = getConfiguredObjectFinder(managedObject);

        final Class<? extends ConfiguredObject> configuredClass;

        final Class<? extends ConfiguredObject>[] hierarchy;

        final String[] servletPathParts = request.getServletPath().split("/");
        final Model model = managedObject.getModel();
        if(servletPathParts.length < 4)
        {
            configuredClass = null;
            hierarchy = null;
        }
        else
        {
            configuredClass = getConfiguredClass(request, managedObject);
            if (configuredClass == null)
            {
                sendError(response, HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            hierarchy = finder.getHierarchy(configuredClass);
            if (hierarchy == null)
            {
                sendError(response, HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            if(!_typeSpecialisations.containsKey(configuredClass))
            {
                List<Class<? extends ConfiguredObject>> types = new ArrayList<>(model
                                                                                        .getTypeRegistry().getTypeSpecialisations(configuredClass));
                _typeSpecialisations.putIfAbsent(configuredClass, types);
            }
        }
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);


        PrintWriter writer = response.getWriter();

        writePreamble(writer);
        writeHead(writer, hierarchy, configuredClass);

        if(hierarchy == null)
        {
            writer.println("<table class=\"api\">");
            writer.println("<thead>");
            writer.println("<tr>");
            writer.println("<th class=\"type\">Type</th>");
            writer.println("<th class=\"path\">Path</th>");
            writer.println("<th class=\"description\">Description</th>");
            writer.println("</tr>");
            writer.println("</thead>");
            writer.println("<tbody>");
            SortedSet<Class<? extends ConfiguredObject>> managedCategories = new TreeSet<>(CLASS_COMPARATOR);
            managedCategories.addAll(finder.getManagedCategories());
            String pathStem = "/" + servletPathParts[1] + "/" + (servletPathParts.length == 2 ? "latest" : servletPathParts[2]) + "/";
            for(Class<? extends ConfiguredObject> category : managedCategories)
            {
                Class<? extends ConfiguredObject> objClass = category;
                String path = pathStem + category.getSimpleName().toLowerCase();
                writer.println("<tr>");
                writer.println("<td class=\"type\"><a href=" + ((servletPathParts.length == 2) ? "\"latest/" : "")+ objClass.getSimpleName().toLowerCase()+"\">"+objClass.getSimpleName()+"</a></td>");
                writer.println("<td class=\"path\">" + path + "</td>");
                writer.println("<td class=\"description\">"+
                               objClass.getAnnotation(ManagedObject.class).description()+"</td>");
                writer.println("</tr>");
            }
            writer.println("</tbody>");
            writer.println("</table>");

        }
        else
        {
            final List<Class<? extends ConfiguredObject>> types = _typeSpecialisations.get(configuredClass);
            writeCategoryDescription(writer, configuredClass);
            writeUsage(writer, request, hierarchy, configuredClass);
            writeTypes(writer, model, types);
            writeAttributes(writer, configuredClass, model, types);
            writeStatistics(writer, configuredClass, model, types);
            writeOperations(writer, configuredClass, model, types);
            writeContext(writer, configuredClass, model, types);
        }

        writeFoot(writer);
    }

    private void writePreamble(final PrintWriter writer)
    {
        writer.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"");
        writer.println("\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
        writer.println("<html>");


    }

    private void writeHead(final PrintWriter writer,
                           final Class<? extends ConfiguredObject>[] hierarchy,
                           final Class<? extends ConfiguredObject> configuredClass)
    {
        writer.println("<head>");
        writer.println("<link rel=\"icon\" type=\"image/png\" href=\"/images/qpid-favicon.png\">");
        writer.println("<link rel=\"stylesheet\" type=\"text/css\" href=\"/css/apidocs.css\">");
        writeTitle(writer, hierarchy, configuredClass);

        writer.println("</head>");
        writer.println("<body>");
    }

    private void writeTitle(final PrintWriter writer,
                            final Class<? extends ConfiguredObject>[] hierarchy,
                            final Class<? extends ConfiguredObject> configuredClass)
    {
        writer.print("<title>");
        if(hierarchy == null)
        {
            writer.print("Qpid API");
        }
        else
        {
            writer.print("Qpid API: " + configuredClass.getSimpleName());
        }
        writer.println("</title>");
    }

    private void writeCategoryDescription(PrintWriter writer,
                                          final Class<? extends ConfiguredObject> configuredClass)
    {
        writer.println("<h1>"+configuredClass.getSimpleName()+"</h1>");
        writer.println(configuredClass.getAnnotation(ManagedObject.class).description());
    }

    private void writeUsage(final PrintWriter writer,
                            final HttpServletRequest request,
                            final Class<? extends ConfiguredObject>[] hierarchy,
                            final Class<? extends ConfiguredObject> configuredClass)
    {
        writer.println("<a name=\"usage\"><h2>Usage</h2></a>");
        writer.println("<table class=\"usage\">");
        writer.println("<tbody>");
        writer.print("<tr><th class=\"operation\">Read</th><td class=\"method\">GET</td><td class=\"path\">" + request.getServletPath()
                .replace("apidocs", "api"));

        for (final Class<? extends ConfiguredObject> category : hierarchy)
        {
            writer.print("[/&lt;" + category.getSimpleName().toLowerCase() + " name or id&gt;");
        }
        for(int i = 0; i < hierarchy.length; i++)
        {
            writer.print("] ");
        }
        writer.println("</td></tr>");

        writer.print("<tr><th class=\"operation\">Update</th><td class=\"method\">PUT or POST</td><td class=\"path\">"
                     + request.getServletPath().replace("apidocs", "api"));
        for (final Class<? extends ConfiguredObject> category : hierarchy)
        {
            writer.print("/&lt;" + category.getSimpleName().toLowerCase() + " name or id&gt;");
        }

        writer.print(
                "<tr><th class=\"operation\">Create</th><td class=\"method\">PUT or POST</td><td class=\"path\">"
                + request.getServletPath().replace("apidocs", "api"));
        for (int i = 0; i < hierarchy.length - 1; i++)
        {
            writer.print("/&lt;" + hierarchy[i].getSimpleName().toLowerCase() + " name or id&gt;");
        }

        writer.print("<tr><th class=\"operation\">Delete</th><td class=\"method\">DELETE</td><td class=\"path\">"
                     + request.getServletPath().replace("apidocs", "api"));
        for (final Class<? extends ConfiguredObject> category : hierarchy)
        {
            writer.print("/&lt;" + category.getSimpleName().toLowerCase() + " name or id&gt;");
        }

        writer.println("</tbody>");
        writer.println("</table>");

    }


    private void writeTypes(final PrintWriter writer,
                            final Model model,
                            final List<Class<? extends ConfiguredObject>> types)
    {
        if(!types.isEmpty() && !(types.size() == 1 && getTypeName(types.iterator().next(), model).trim().equals("")))
        {
            writer.println("<a name=\"types\"><h2>Types</h2></a>");
            writer.println("<table class=\"types\">");
            writer.println("<thead>");
            writer.println("<tr><th class=\"type\">Type</th><th class=\"description\">Description</th></tr>");
            writer.println("</thead>");

            writer.println("<tbody>");
            for (Class<? extends ConfiguredObject> type : types)
            {
                writer.print("<tr><td class=\"type\">");
                writer.print(getTypeName(type, model));
                writer.print("</td><td class=\"description\">");
                writer.print(type.getAnnotation(ManagedObject.class).description());
                writer.println("</td></tr>");

            }
            writer.println("</tbody>");
        }

        writer.println("</table>");
    }

    private String getTypeName(final Class<? extends ConfiguredObject> type, Model model)
    {
        return type.getAnnotation(ManagedObject.class).type() == null
                            ? model.getTypeRegistry().getTypeClass(type).getSimpleName()
                            : type.getAnnotation(ManagedObject.class).type();
    }

    private void writeAttributes(final PrintWriter writer,
                                 final Class<? extends ConfiguredObject> configuredClass, final Model model,
                                 final List<Class<? extends ConfiguredObject>> types)
    {
        writer.println("<a name=\"types\"><h2>Attributes</h2></a>");
        writer.println("<h2>Common Attributes</h2>");

        writeAttributesTable(writer, model.getTypeRegistry().getAttributeTypes(configuredClass).values());

        for(Class<? extends ConfiguredObject> type : types)
        {
            String typeName = getTypeName(type, model);
            Collection<ConfiguredObjectAttribute<?, ?>> typeSpecificAttributes =
                    model.getTypeRegistry().getTypeSpecificAttributes(type);
            if(!typeSpecificAttributes.isEmpty())
            {
                writer.println("<h2><span class=\"type\">"+typeName+"</span> Specific Attributes</h2>");
                writeAttributesTable(writer, typeSpecificAttributes);
            }


        }

    }

    private void writeAttributesTable(final PrintWriter writer,
                                      final Collection<ConfiguredObjectAttribute<?, ?>> attributeTypes)
    {
        writer.println("<table class=\"attributes\">");
        writer.println("<thead>");
        writer.println("<tr><th class=\"name\">Attribute Name</th><th class=\"type\">Type</th><th class=\"properties\">Properties</th><th class=\"description\">Description</th></tr>");
        writer.println("</thead>");
        writer.println("<tbody>");

        for(ConfiguredObjectAttribute attribute : attributeTypes)
        {
            String properties;
            if (attribute instanceof ConfiguredSettableAttribute)
            {
                ConfiguredSettableAttribute settableAttribute = (ConfiguredSettableAttribute)attribute;
                if (settableAttribute.isImmutable())
                {
                    properties = "read/settable on create only";
                }
                else
                {
                    properties = "read/write";
                }
            }
            else
            {
                properties = "read only";
            }
            writer.println("<tr><td class=\"name\">"
                           + attribute.getName()
                           + "</td><td class=\"type\">"
                           + renderType(attribute)
                           + "</td><td class=\"properties\">"
                           + properties
                           + "</td><td class=\"description\">"
                           + attribute.getDescription()
                           + "</td></tr>");
        }
        writer.println("</tbody>");

        writer.println("</table>");

    }
    private void writeStatistics(final PrintWriter writer,
                                 final Class<? extends ConfiguredObject> configuredClass, final Model model,
                                 final List<Class<? extends ConfiguredObject>> types)
    {
        writer.println("<a name=\"types\"><h2>Statistics</h2></a>");
        writer.println("<h2>Common Statistics</h2>");

        writeStatisticsTable(writer, model.getTypeRegistry().getStatistics(configuredClass));

        for(Class<? extends ConfiguredObject> type : types)
        {
            String typeName = getTypeName(type, model);
            Collection<ConfiguredObjectStatistic<?, ?>> typeSpecificStatistics =
                    model.getTypeRegistry().getTypeSpecificStatistics(type);
            if(!typeSpecificStatistics.isEmpty())
            {
                writer.println("<h2><span class=\"type\">"+typeName+"</span> Specific Statistics</h2>");
                writeStatisticsTable(writer, typeSpecificStatistics);
            }
        }
    }

    private void writeStatisticsTable(final PrintWriter writer,
                                      final Collection<ConfiguredObjectStatistic<?,?>> statisticTypes)
    {
        writer.println("<table class=\"statistics\">");
        writer.println("<thead>");
        writer.println("<tr><th class=\"name\">Statistic Name</th><th class=\"type\">Type</th><th class=\"units\">Units</th><th class=\"statisticType\">Stat Type</th><th class=\"description\">Description</th></tr>");
        writer.println("</thead>");
        writer.println("<tbody>");

        for(ConfiguredObjectStatistic statistic : statisticTypes)
        {
            writer.println("<tr><td class=\"name\">"
                           + statistic.getName()
                           + "</td><td class=\"type\">"
                           + renderType(statistic)
                           + "</td><td class=\"units\">"
                           + statistic.getUnits()
                           + "</td><td class=\"stattype\">"
                           + statistic.getStatisticType()
                           + "</td><td class=\"description\">"
                           + statistic.getDescription()
                           + "</td></tr>");
        }
        writer.println("</tbody>");

        writer.println("</table>");

    }

    private void writeOperations(final PrintWriter writer,
                                 final Class<? extends ConfiguredObject> configuredClass, final Model model,
                                 final List<Class<? extends ConfiguredObject>> types)
    {
        writer.println("<a name=\"types\"><h2>Operations</h2></a>");
        writer.println("<h2>Common Operations</h2>");

        Collection<ConfiguredObjectOperation<?>> categoryOperations =
                model.getTypeRegistry().getOperations(configuredClass).values();
        writeOperationsTables(writer, categoryOperations);
        for(Class<? extends ConfiguredObject> type : types)
        {
            String typeName = getTypeName(type, model);
            Set<ConfiguredObjectOperation<?>> typeSpecificOperations = new HashSet<>(model.getTypeRegistry().getOperations(type).values());
            typeSpecificOperations.removeAll(categoryOperations);

            if(!typeSpecificOperations.isEmpty() && type != configuredClass)
            {
                writer.println("<h2><span class=\"type\">"+typeName+"</span> Specific Operations</h2>");
                writeOperationsTables(writer, typeSpecificOperations);
            }
        }

    }

    private void writeOperationsTables(PrintWriter writer,
                                       Collection<ConfiguredObjectOperation<?>> operations)
    {
        for(ConfiguredObjectOperation<?> operation : operations)
        {
            writer.println("<table class=\"operation\">");
            writer.println("<thead>");
            writer.println("<tr><th class=\"name\">Operation Name</th><th class=\"returnType\">Return Type</th><th class=\"description\">Description</th></tr>");
            writer.println("</thead>");
            writer.println("<tbody>");

            writer.println("<tr><td class=\"name\">"
                           + operation.getName()
                           + "</td><td class=\"type\">"
                           + renderType(operation)
                           + "</td><td class=\"description\">"
                           + operation.getDescription()
                           + "</td></tr>");
            if (!operation.getParameters().isEmpty())
            {
                writer.println("<tr><td class=\"allparameters\" colspan=\"3\">"
                               + renderParameters(operation.getParameters())
                               + "</td></tr>");
            }

            writer.println("</tbody>");
            writer.println("</table>");
        }
    }

    private String renderParameters(final List<OperationParameter> parameters)
    {
        StringBuilder writer = new StringBuilder();

        writer.append("<table class=\"parameters\">");
        writer.append("<thead>");
        writer.append(
                "<tr><th class=\"name\">Parameter Name</th><th>Type</th><th>Default</th><th>Mandatory?</th><th>Description</th></tr>");
        writer.append("</thead>");
        writer.append("<tbody>");

        for (OperationParameter param : parameters)
        {
            writer.append("<tr><td class=\"name\">"
                          + param.getName()
                          + "</td><td class=\"type\">"
                          + renderType(param)
                          + "</td><td class=\"default\">"
                          + param.getDefaultValue()
                          + "</td><td class=\"mandatory\">"
                          + param.isMandatory()
                          + "</td><td class=\"description\">"
                          + param.getDescription()
                          + "</td></tr>");
        }

        writer.append("</tbody>");
        writer.append("</table>");

        return writer.toString();
    }

    private String renderType(final ConfiguredObjectAttributeOrStatistic attributeOrStatistic)
    {
        final Class type = attributeOrStatistic.getType();

        Collection<String> validValues;
        String validValuePattern;

        if(attributeOrStatistic instanceof ConfiguredSettableAttribute)
        {
            ConfiguredSettableAttribute<?,?> settableAttribute = (ConfiguredSettableAttribute<?,?>) attributeOrStatistic;
            validValues = settableAttribute.hasValidValues() ? settableAttribute.validValues() : null;
            validValuePattern = settableAttribute.validValuePattern();
        }
        else
        {
            validValues = null;
            validValuePattern = "";
        }

        return renderType(type, validValues, validValuePattern);
    }

    private String renderType(final OperationParameter parameter)
    {
        final Class type = parameter.getType();
        List<String> validValues = parameter.getValidValues();
        return renderType(type, validValues, "");
    }

    private String renderType(Class type, Collection<String> validValues, final String validValuePattern) {
        if(Enum.class.isAssignableFrom(type))
        {
            return "<div class=\"restriction\" title=\"enum: " + EnumSet.allOf(type) + "\">string</div>";
        }
        else if(ConfiguredObject.class.isAssignableFrom(type))
        {
            return "<div class=\"restriction\" title=\"name or id of a" + (VOWELS.contains(type.getSimpleName().toLowerCase().charAt(0)) ? "n " : " ") + type.getSimpleName() + "\">string</div>";
        }
        else if(UUID.class == type)
        {
            return "<div class=\"restriction\" title=\"must be a UUID\">string</div>";
        }
        else
        {
            StringBuilder returnVal = new StringBuilder();
            final boolean hasValuesRestriction = validValues != null && !validValues.isEmpty();

            // TODO - valid values and patterns might contain characters which should be escaped
            if(hasValuesRestriction)
            {
                returnVal.append("<div class=\"restricted\" title=\"Valid values: " + validValues + "\">");
            }
            else if(validValuePattern != null && !"".equals(validValuePattern))
            {
                returnVal.append("<div class=\"restricted\" title=\"Valid value pattern: " + validValuePattern+ "\">");
            }

            if(Number.class.isAssignableFrom(type))
            {
                returnVal.append("number");
            }
            else if(Boolean.class == type)
            {
                returnVal.append("boolean");
            }
            else if(String.class == type)
            {
                returnVal.append("string");
            }
            else if(Collection.class.isAssignableFrom(type))
            {
                // TODO - generate a description of the type in the array
                returnVal.append("array");
            }
            else if(Map.class.isAssignableFrom(type))
            {
                // TODO - generate a description of the type in the object
                returnVal.append("object");
            }
            else
            {
                returnVal.append(type.getSimpleName());
            }
            if(hasValuesRestriction)
            {
                returnVal.append("</div>");
            }
            return returnVal.toString();
        }
    }

    private String renderType(final ConfiguredObjectOperation<?> operation)
    {
        return operation.getGenericReturnType() instanceof Class ?
                ((Class) operation.getGenericReturnType()).getName() :
                operation.getGenericReturnType().toString();
    }

    private void writeContext(final PrintWriter writer,
                              final Class<? extends ConfiguredObject> configuredClass,
                              final Model model, final List<Class<? extends ConfiguredObject>> types)
    {
        writer.println("<a name=\"types\"><h2>Context Variables</h2></a>");
        writer.println("<h2>Common Context Variables</h2>");

        Collection<ManagedContextDefault> defaultContexts =
                model.getTypeRegistry().getContextDependencies(configuredClass);
        writeContextDefaults(writer, defaultContexts);
        for(Class<? extends ConfiguredObject> type : types)
        {
            String typeName = getTypeName(type, model);
            Collection<ManagedContextDefault> typeSpecificDefaults = model.getTypeRegistry().getTypeSpecificContextDependencies(type);

            if(!typeSpecificDefaults.isEmpty() && type != configuredClass)
            {
                writer.println("<h2><span class=\"type\">"+typeName+"</span> Specific Context Variables</h2>");
                writeContextDefaults(writer, typeSpecificDefaults);
            }
        }

    }

    private void writeContextDefaults(PrintWriter writer,
                                       Collection<ManagedContextDefault> contextDefaults)
    {
        writer.println("<table class=\"contextDefault\">");
        writer.println("<thead>");
        writer.println("<tr><th class=\"name\">Name</th><th class=\"description\">Description</th></tr>");
        writer.println("</thead>");
        writer.println("<tbody>");

        for(ManagedContextDefault contextDefault : contextDefaults)
        {

            writer.println("<tr><td class=\"name\">"
                           + contextDefault.name()
                           + "</td><td class=\"description\">"
                           + contextDefault.description()
                           + "</td></tr>");
        }
        writer.println("</tbody>");
        writer.println("</table>");

    }


    private void writeFoot(final PrintWriter writer)
    {
        writer.println("</body>");
        writer.println("</html>");
    }

    private Class<? extends ConfiguredObject> getConfiguredClass(HttpServletRequest request, ConfiguredObject<?> managedObject)
    {
        final String[] servletPathElements = request.getServletPath().split("/");
        String categoryName = servletPathElements[servletPathElements.length-1];
        Model model = managedObject.getModel();
        for(Class<? extends ConfiguredObject> category : model.getSupportedCategories())
        {
            if(category.getSimpleName().toLowerCase().equals(categoryName))
            {
                return category;
            }
        }
        return null;
    }
}
