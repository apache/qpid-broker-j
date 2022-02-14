package org.apache.qpid.server.security.auth.manager;

import java.util.List;

import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedObject;

@ManagedObject( category = false, type = "Composite" )
public interface CompositeUsernamePasswordAuthenticationManager<T extends CompositeUsernamePasswordAuthenticationManager<T>>
        extends CachingAuthenticationProvider<T>, UsernamePasswordAuthenticationProvider<T>
{
    String PROVIDER_TYPE = "Composite";

    @ManagedAttribute(description = "delegate authentication providers", mandatory = true)
    List<String> getDelegates();

}
