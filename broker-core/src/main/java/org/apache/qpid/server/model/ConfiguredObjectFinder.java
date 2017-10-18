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

import static org.apache.qpid.server.model.ConfiguredObjectTypeRegistry.getCollectionMemberType;
import static org.apache.qpid.server.model.ConfiguredObjectTypeRegistry.returnsCollectionOfConfiguredObjects;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.qpid.server.util.Strings;

public class ConfiguredObjectFinder
{
    private final ConfiguredObject<?> _root;
    private final Map<Class<? extends ConfiguredObject>, List<Class<? extends ConfiguredObject>>> _hierarchies =
            new HashMap<>();
    private final Model _model;
    private final Map<Class<? extends ConfiguredObject>, ConfiguredObjectOperation<ConfiguredObject<?>>>
            _associatedChildrenOperations = new HashMap<>();


    public ConfiguredObjectFinder(final ConfiguredObject<?> root)
    {
        _root = root;
        _model = root.getModel();
        final Class<? extends ConfiguredObject> managedCategory = root.getCategoryClass();
        addManagedHierarchies(managedCategory, managedCategory);

        for (ConfiguredObjectOperation operation : _model.getTypeRegistry().getOperations(managedCategory).values())
        {
            if (operation.isAssociateAsIfChildren() && returnsCollectionOfConfiguredObjects(operation))
            {
                Class<? extends ConfiguredObject> associatedChildCategory =
                        (getCollectionMemberType((ParameterizedType) operation.getGenericReturnType()));
                _associatedChildrenOperations.put(associatedChildCategory, operation);
                addManagedHierarchies(associatedChildCategory, associatedChildCategory);
            }
        }
    }

    private void addManagedHierarchies(Class<? extends ConfiguredObject> category,
                                       final Class<? extends ConfiguredObject> rootCategory)
    {
        if (!_hierarchies.containsKey(category))
        {
            _hierarchies.put(category, calculateHierarchy(category, rootCategory));

            for (Class<? extends ConfiguredObject> childClass : _model.getChildTypes(category))
            {
                addManagedHierarchies(childClass, rootCategory);
            }
        }
    }

    public Collection<Class<? extends ConfiguredObject>> getManagedCategories()
    {
        return Collections.unmodifiableCollection(_hierarchies.keySet());
    }

    private String[] getPathElements(final String path)
    {
        String[] pathElements = path.split("(?<!\\\\)" + Pattern.quote("/"));
        for(int i = 0; i<pathElements.length; i++)
        {
            pathElements[i] = pathElements[i].replaceAll("\\\\(.)","$1");
        }
        return pathElements;
    }

    public ConfiguredObject<?> findObjectFromPath(String path, Class<? extends ConfiguredObject> category)
    {
        return findObjectFromPath(Arrays.asList(getPathElements(path)), category);
    }

    public ConfiguredObject<?> findObjectFromPath(List<String> path, Class<? extends ConfiguredObject> category)
    {
        Collection<ConfiguredObject<?>> candidates = findObjectsFromPath(path, getHierarchy(category), false);
        if(candidates == null || candidates.isEmpty())
        {
            return null;
        }
        else if(candidates.size() == 1)
        {
            return candidates.iterator().next();
        }
        else
        {
            throw new IllegalArgumentException("More than one object matching path was found");
        }
    }

    public Class<? extends ConfiguredObject>[] getHierarchy(String categoryName)
    {
        for(Class<? extends ConfiguredObject> category : _model.getSupportedCategories())
        {
            if(category.getSimpleName().toLowerCase().equals(categoryName))
            {
                return getHierarchy(category);
            }
        }
        return null;
    }

    public Class<? extends ConfiguredObject>[] getHierarchy(final Class<? extends ConfiguredObject> category)
    {
        List<Class<? extends ConfiguredObject>> hierarchy = _hierarchies.get(ConfiguredObjectTypeRegistry.getCategory(category));
        return hierarchy == null ? null : hierarchy.toArray(new Class[hierarchy.size()]);
    }

    public Set<Class<? extends ConfiguredObject>> getAssociatedChildCategories()
    {
        return Collections.unmodifiableSet(_associatedChildrenOperations.keySet());
    }

    public Collection<ConfiguredObject<?>> findObjectsFromPath(List<String> path,
                                                               Class<? extends ConfiguredObject>[] hierarchy,
                                                               boolean allowWildcards)
    {
        Collection<ConfiguredObject<?>> parents = new ArrayList<>();
        if (hierarchy.length == 0)
        {
            return Collections.singletonList(_root);
        }

        Map<Class<? extends ConfiguredObject>, String> filters = new HashMap<>();
        Collection<ConfiguredObject<?>> children = new ArrayList<>();
        boolean wildcard = false;

        Class<? extends ConfiguredObject> parentType = _root.getCategoryClass();

        parents.add(_root);

        for (int i = 0; i < hierarchy.length; i++)
        {
            if (_model.getChildTypes(parentType).contains(hierarchy[i]))
            {
                parentType = hierarchy[i];
                for (ConfiguredObject<?> parent : parents)
                {
                    if (path.size() > i
                        && path.get(i) != null
                        && !path.get(i).equals("*")
                        && path.get(i).trim().length() != 0)
                    {
                        List<ConfiguredObject<?>> childrenOfParent = new ArrayList<>();
                        for (ConfiguredObject<?> child : parent.getChildren(hierarchy[i]))
                        {
                            if (child.getName().equals(path.get(i)))
                            {
                                childrenOfParent.add(child);
                            }
                        }
                        if (childrenOfParent.isEmpty())
                        {
                            return null;
                        }
                        else
                        {
                            children.addAll(childrenOfParent);
                        }
                    }
                    else
                    {
                        if (allowWildcards)
                        {
                            wildcard = true;
                            children.addAll((Collection<? extends ConfiguredObject<?>>) parent.getChildren(hierarchy[i]));
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }
            else if (i == 0)
            {
                final ConfiguredObjectOperation<ConfiguredObject<?>> op =
                        _associatedChildrenOperations.get(hierarchy[0]);
                if (op != null)
                {
                    parentType = hierarchy[i];

                    final Collection<? extends ConfiguredObject<?>> associated =
                            (Collection<? extends ConfiguredObject<?>>) op.perform(_root,
                                                                                   Collections.<String, Object>emptyMap());

                    if (path.size() > i
                        && path.get(i) != null
                        && !path.get(i).equals("*")
                        && path.get(i).trim().length() != 0)
                    {
                        for (ConfiguredObject<?> child : associated)
                        {
                            if (child.getName().equals(path.get(i)))
                            {
                                children.add(child);
                            }
                        }
                        if (children.isEmpty())
                        {
                            return null;
                        }
                    }
                    else
                    {
                        if (allowWildcards)
                        {
                            wildcard = true;
                            children.addAll(associated);
                        }
                        else
                        {
                            return null;
                        }
                    }
                }
            }
            else
            {
                children = parents;
                if (path.size() > i
                    && path.get(i) != null
                    && !path.get(i).equals("*")
                    && path.get(i).trim().length() != 0)
                {
                    filters.put(hierarchy[i], path.get(i));
                }
                else
                {
                    if (allowWildcards)
                    {
                        wildcard = true;
                    }
                    else
                    {
                        return null;
                    }
                }
            }

            parents = children;
            children = new ArrayList<>();
        }

        if (!filters.isEmpty() && !parents.isEmpty())
        {
            Collection<ConfiguredObject<?>> potentials = parents;
            parents = new ArrayList<>();

            for (ConfiguredObject o : potentials)
            {

                boolean match = true;

                for (Map.Entry<Class<? extends ConfiguredObject>, String> entry : filters.entrySet())
                {
                    final ConfiguredObject<?> ancestor = o.getModel().getAncestor(entry.getKey(), o);
                    match = ancestor != null && ancestor.getName().equals(entry.getValue());
                    if (!match)
                    {
                        break;
                    }
                }
                if (match)
                {
                    parents.add(o);
                }
            }
        }

        if (parents.isEmpty() && !wildcard)
        {
            parents = null;
        }
        return parents;
    }

    public ConfiguredObject findObjectParentsFromPath(final List<String> names,
                                                      final Class<? extends ConfiguredObject>[] hierarchy,
                                                      final Class<? extends ConfiguredObject> objClass)
    {
        Model model = _root.getModel();
        Collection<ConfiguredObject<?>>[] objects = new Collection[hierarchy.length];
        for (int i = 0; i < hierarchy.length - 1; i++)
        {
            objects[i] = new HashSet<>();
            if (i == 0)
            {
                for (ConfiguredObject object : _root.getChildren(hierarchy[0]))
                {
                    if (object.getName().equals(names.get(0)))
                    {
                        objects[0].add(object);
                        break;
                    }
                }
            }
            else
            {
                boolean foundAncestor = false;
                for (int j = i - 1; j >= 0; j--)
                {
                    if (model.getChildTypes(hierarchy[j]).contains(hierarchy[i]))
                    {
                        for (ConfiguredObject<?> parent : objects[j])
                        {
                            for (ConfiguredObject<?> object : parent.getChildren(hierarchy[i]))
                            {
                                if (object.getName().equals(names.get(i)))
                                {
                                    objects[i].add(object);
                                }
                            }
                        }
                        foundAncestor = true;
                        break;
                    }
                }
                if(!foundAncestor)
                {
                    if(model.getChildTypes(_root.getCategoryClass()).contains(hierarchy[i]))
                    {
                        for (ConfiguredObject<?> object : _root.getChildren(hierarchy[i]))
                        {
                            if (object.getName().equals(names.get(i)))
                            {
                                objects[i].add(object);
                            }
                        }
                    }
                }
            }
        }
        Class<? extends ConfiguredObject> parentClass = model.getParentType(objClass);
        for (int i = hierarchy.length - 2; i >= 0; i--)
        {
            if (parentClass.equals(hierarchy[i]))
            {
                if (objects[i].size() == 1)
                {
                    return (objects[i].iterator().next());
                }
                else
                {
                    throw new IllegalArgumentException("Cannot deduce parent of class "
                                                       + hierarchy[i].getSimpleName());
                }
            }
        }
        return null;
    }

    public String getPath(final ConfiguredObject<?> object)
    {
        final List<String> pathAsList = getPathAsList(object);
        ListIterator<String> iter = pathAsList.listIterator();
        while(iter.hasNext())
        {
            String element = iter.next();
            iter.set(element.replaceAll("([\\\\/])", "\\\\$1"));

        }
        return Strings.join("/", pathAsList);
    }

    public List<String> getPathAsList(final ConfiguredObject<?> object)
    {
        final List<Class<? extends ConfiguredObject>> hierarchy = _hierarchies.get(object.getCategoryClass());
        List<String> pathElements = new ArrayList<>();
        for (Class<? extends ConfiguredObject> ancestorClass : hierarchy)
        {
            pathElements.add(_model.getAncestor(ancestorClass, object).getName());
        }
        return pathElements;
    }


    private List<Class<? extends ConfiguredObject>> calculateHierarchy(Class<? extends ConfiguredObject> category,
                                                                       Class<? extends ConfiguredObject> rootClass)
    {
        Class<? extends ConfiguredObject> managedCategoryClass = _root.getCategoryClass();

        return calculateHierarchy(category, rootClass, managedCategoryClass, _model);
    }

    private static List<Class<? extends ConfiguredObject>> calculateHierarchy(Class<? extends ConfiguredObject> category,
                                                                              final Class<? extends ConfiguredObject> rootClass,
                                                                              final Class<? extends ConfiguredObject> managedCategoryClass,
                                                                              final Model model)
    {
        List<Class<? extends ConfiguredObject>> hierarchyList = new ArrayList<>();

        if (category != rootClass)
        {
            Class<? extends ConfiguredObject> parentCategory;

            hierarchyList.add(category);

            while (!rootClass.equals(parentCategory = model.getParentType(category)))
            {
                if (parentCategory != null)
                {
                    hierarchyList.add(parentCategory);
                    if (!model.getAncestorCategories(parentCategory).contains(rootClass))
                    {
                        break;
                    }
                    category = parentCategory;
                }
                else
                {
                    break;
                }
            }
        }
        if (rootClass != managedCategoryClass)
        {
            hierarchyList.add(rootClass);
        }
        Collections.reverse(hierarchyList);

        return hierarchyList;
    }

    public Collection<? extends ConfiguredObject> getAssociatedChildren(final Class<? extends ConfiguredObject> childClass)
    {
        final ConfiguredObjectOperation<ConfiguredObject<?>> op =
                _associatedChildrenOperations.get(childClass);
        if (op != null)
        {

            return Collections.unmodifiableCollection((Collection<? extends ConfiguredObject<?>>) op.perform(_root,
                                                                                                             Collections.<String, Object>emptyMap()));
        }
        else
        {
            return Collections.emptySet();
        }
    }
}
