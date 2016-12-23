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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class Model
{

    <X extends ConfiguredObject<X>> Collection<X> getReachableObjects(final ConfiguredObject<?> object,
                                                                      final Class<X> clazz)
    {
        Class<? extends ConfiguredObject> category = ConfiguredObjectTypeRegistry.getCategory(object.getClass());
        Class<? extends ConfiguredObject> ancestorClass = getAncestorClassWithGivenDescendant(category, clazz);
        if(ancestorClass != null)
        {
            ConfiguredObject ancestor = getAncestor(ancestorClass, category, object);
            if(ancestor != null)
            {
                return getAllDescendants(ancestor, ancestorClass, clazz);
            }
        }
        return null;
    }

    <X extends ConfiguredObject<X>> Collection<X> getAllDescendants(final ConfiguredObject ancestor,
                                                                    final Class<? extends ConfiguredObject> ancestorClass,
                                                                    final Class<X> clazz)
    {
        Set<X> descendants = new HashSet<X>();
        for(Class<? extends ConfiguredObject> childClass : getChildTypes(ancestorClass))
        {
            Collection<? extends ConfiguredObject> children = ancestor.getChildren(childClass);
            if(childClass == clazz)
            {

                if(children != null)
                {
                    descendants.addAll((Collection<X>)children);
                }
            }
            else
            {
                if(children != null)
                {
                    for(ConfiguredObject child : children)
                    {
                        descendants.addAll(getAllDescendants(child, childClass, clazz));
                    }
                }
            }
        }
        return descendants;
    }

    public <C> C getAncestor(final Class<C> ancestorClass, final ConfiguredObject<?> object)
    {
        return getAncestor(ancestorClass, object.getCategoryClass(), object);
    }

    public  <C> C getAncestor(final Class<C> ancestorClass,
                              final Class<? extends ConfiguredObject> category,
                              final ConfiguredObject<?> object)
    {
        if(ancestorClass.isInstance(object))
        {
            return (C) object;
        }
        else
        {
            Class<? extends ConfiguredObject> parentClass = getParentType(category);
            if(parentClass != null)
            {
                ConfiguredObject<?> parent = object.getParent();
                C ancestor = getAncestor(ancestorClass, parentClass, parent);
                if (ancestor != null)
                {
                    return ancestor;
                }
            }
        }
        return null;
    }

    public Class<? extends ConfiguredObject> getAncestorClassWithGivenDescendant(
            final Class<? extends ConfiguredObject> category,
            final Class<? extends ConfiguredObject> descendantClass)
    {
        Collection<Class<? extends ConfiguredObject>> candidateClasses =
                Collections.<Class<? extends ConfiguredObject>>singleton(category);
        while(!candidateClasses.isEmpty())
        {
            for(Class<? extends ConfiguredObject> candidate : candidateClasses)
            {
                if(hasDescendant(candidate, descendantClass))
                {
                    return candidate;
                }
            }
            Set<Class<? extends ConfiguredObject>> previous = new HashSet<>(candidateClasses);
            candidateClasses = new HashSet<>();
            for(Class<? extends ConfiguredObject> prev : previous)
            {
                final Class<? extends ConfiguredObject> parentType = getParentType(prev);
                if(parentType != null)
                {
                    candidateClasses.add(parentType);
                }
            }
        }
        return null;
    }

    private boolean hasDescendant(final Class<? extends ConfiguredObject> candidate,
                                  final Class<? extends ConfiguredObject> descendantClass)
    {
        int oldSize = 0;

        Set<Class<? extends ConfiguredObject>> allDescendants = new HashSet<>(getChildTypes(candidate));
        while(allDescendants.size() > oldSize)
        {
            oldSize = allDescendants.size();
            Set<Class<? extends ConfiguredObject>> prev = new HashSet<>(allDescendants);
            for(Class<? extends ConfiguredObject> clazz : prev)
            {
                allDescendants.addAll(getChildTypes(clazz));
            }
            if(allDescendants.contains(descendantClass))
            {
                break;
            }
        }
        return allDescendants.contains(descendantClass);
    }

    public final Collection<Class<? extends ConfiguredObject>> getDescendantCategories(Class<? extends ConfiguredObject> parent)
    {
        Set<Class<? extends ConfiguredObject>> allDescendants = new HashSet<>();
        for(Class<? extends ConfiguredObject> clazz : getChildTypes(parent))
        {
            if(allDescendants.add(clazz))
            {
                allDescendants.addAll(getDescendantCategories(clazz));
            }
        }

        return allDescendants;
    }

    public final Collection<Class<? extends ConfiguredObject>> getAncestorCategories(Class<? extends ConfiguredObject> category)
    {
        Set<Class<? extends ConfiguredObject>> allAncestors = new HashSet<>();
        Class<? extends ConfiguredObject> clazz = getParentType(category);
        if(clazz != null)
        {
            if(allAncestors.add(clazz))
            {
                allAncestors.addAll(getAncestorCategories(clazz));
            }
        }

        return allAncestors;
    }

    public abstract Collection<Class<? extends ConfiguredObject>> getSupportedCategories();
    public abstract Collection<Class<? extends ConfiguredObject>> getChildTypes(Class<? extends ConfiguredObject> parent);

    public abstract Class<? extends ConfiguredObject> getRootCategory();

    public abstract Class<? extends ConfiguredObject> getParentType(Class<? extends ConfiguredObject> child);

    public abstract int getMajorVersion();
    public abstract int getMinorVersion();

    public abstract ConfiguredObjectFactory getObjectFactory();

    public abstract ConfiguredObjectTypeRegistry getTypeRegistry();

    public static boolean isSpecialization(final Model model,
                                           final Model specialization,
                                           final Class<? extends ConfiguredObject> specializationPoint)
    {
        if(model.getSupportedCategories().contains(specializationPoint)
               && specialization.getSupportedCategories().containsAll(model.getSupportedCategories())
               && model.getChildTypes(specializationPoint).isEmpty())
        {
            final Collection<Class<? extends ConfiguredObject>> modelSupportedCategories = new ArrayList<>(model.getSupportedCategories());
            modelSupportedCategories.remove(specializationPoint);
            for(Class<? extends ConfiguredObject> category : modelSupportedCategories)
            {
                if(!model.getChildTypes(category).equals(specialization.getChildTypes(category)))
                {
                    return false;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }
}
