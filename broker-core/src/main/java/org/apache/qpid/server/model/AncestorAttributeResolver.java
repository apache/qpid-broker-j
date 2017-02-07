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

package org.apache.qpid.server.model;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.qpid.server.util.Strings;

public class AncestorAttributeResolver implements Strings.Resolver
{

    public static final String PREFIX = "ancestor:";
    private final ThreadLocal<Set<String>> _stack = new ThreadLocal<>();
    private final ConfiguredObject<?> _object;
    private final ObjectMapper _objectMapper;

    public AncestorAttributeResolver(final ConfiguredObject<?> object)
    {
        _object = object;
        _objectMapper = ConfiguredObjectJacksonModule.newObjectMapper(false);
    }

    @Override
    public String resolve(final String variable, final Strings.Resolver resolver)
    {
        boolean clearStack = false;
        Set<String> currentStack = _stack.get();
        if (currentStack == null)
        {
            currentStack = new HashSet<>();
            _stack.set(currentStack);
            clearStack = true;
        }

        try
        {
            if (variable.startsWith(PREFIX))
            {
                String classQualifiedAttrName = variable.substring(PREFIX.length());

                if (currentStack.contains(classQualifiedAttrName))
                {
                    throw new IllegalArgumentException("The value of attribute "
                                                       + classQualifiedAttrName
                                                       + " is defined recursively");
                }
                else
                {
                    currentStack.add(classQualifiedAttrName);

                    int colonIndex = classQualifiedAttrName.indexOf(":");
                    if (colonIndex == -1)
                    {
                        return null;
                    }

                    String categorySimpleClassName = classQualifiedAttrName.substring(0, colonIndex);
                    String attributeName = classQualifiedAttrName.substring(colonIndex + 1);

                    final Class<? extends ConfiguredObject> ancestorCategory = findAncestorCategoryBySimpleClassName(categorySimpleClassName, _object.getCategoryClass());
                    if (ancestorCategory == null)
                    {
                        return null;
                    }

                    final ConfiguredObject ancestorOrSelf = _object.getModel().getAncestor(ancestorCategory, _object);

                    if (ancestorOrSelf == null)
                    {
                        return null;
                    }


                    Object returnVal = ancestorOrSelf.getAttribute(attributeName);
                    String returnString;
                    if (returnVal == null)
                    {
                        returnString = null;
                    }
                    else if (returnVal instanceof Map || returnVal instanceof Collection)
                    {
                        try
                        {
                            StringWriter writer = new StringWriter();

                            _objectMapper.writeValue(writer, returnVal);

                            returnString = writer.toString();
                        }
                        catch (IOException e)
                        {
                            throw new IllegalArgumentException(e);
                        }
                    }
                    else if (returnVal instanceof ConfiguredObject)
                    {
                        returnString = ((ConfiguredObject) returnVal).getId().toString();
                    }
                    else
                    {
                        returnString = returnVal.toString();
                    }

                    return returnString;
                }
            }
            else
            {
                return null;
            }
        }
        finally
        {
            if (clearStack)
            {
                _stack.remove();
            }

        }
    }

    private Class<? extends ConfiguredObject> findAncestorCategoryBySimpleClassName(String targetCategorySimpleClassName, Class<? extends ConfiguredObject> objectCategory)
    {
        if (targetCategorySimpleClassName.equals(objectCategory.getSimpleName().toLowerCase()))
        {
            return objectCategory;
        }


        Class<? extends ConfiguredObject> parentCategory = _object.getModel().getParentType(objectCategory);
        if(parentCategory != null)
        {
            Class<? extends ConfiguredObject> targetCategoryClass =
                    findAncestorCategoryBySimpleClassName(targetCategorySimpleClassName, parentCategory);
            if (targetCategoryClass != null)
            {
                return targetCategoryClass;
            }
        }

        return null;
    }
}
