package org.apache.qpid.server.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.util.Action;

public class AbstractConfigurationStoreUpgraderAndRecoverer
{
    private final Map<String, StoreUpgraderPhase> _upgraders = new HashMap<>();

    List<ConfiguredObjectRecord> upgrade(final DurableConfigurationStore store,
                                         final List<ConfiguredObjectRecord> records,
                                         final String rootCategory,
                                         final String modelVersionAttributeName)
    {
        GenericStoreUpgrader upgrader = new GenericStoreUpgrader(rootCategory,
                                                                 modelVersionAttributeName, store, _upgraders);
        upgrader.upgrade(records);
        return upgrader.getRecords();
    }

    void register(StoreUpgraderPhase upgrader)
    {
        _upgraders.put(upgrader.getFromVersion(), upgrader);
    }

    void applyRecursively(final ConfiguredObject<?> object, final RecursiveAction<ConfiguredObject<?>> action)
    {
        applyRecursively(object, action, new HashSet<ConfiguredObject<?>>());
    }

    void applyRecursively(final ConfiguredObject<?> object,
                                  final RecursiveAction<ConfiguredObject<?>> action,
                                  final HashSet<ConfiguredObject<?>> visited)
    {
        if(!visited.contains(object))
        {
            visited.add(object);
            action.performAction(object);
            if (action.applyToChildren(object))
            {
                for (Class<? extends ConfiguredObject> childClass : object.getModel().getChildTypes(object.getCategoryClass()))
                {
                    Collection<? extends ConfiguredObject> children = object.getChildren(childClass);
                    if (children != null)
                    {
                        for (ConfiguredObject<?> child : children)
                        {
                            applyRecursively(child, action, visited);
                        }
                    }
                }
            }
        }
    }



    interface RecursiveAction<C> extends Action<C>
    {
        boolean applyToChildren(C object);
    }


}
