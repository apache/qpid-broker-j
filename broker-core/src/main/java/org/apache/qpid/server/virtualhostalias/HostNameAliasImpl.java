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
package org.apache.qpid.server.virtualhostalias;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.HostNameAlias;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.Port;

public class HostNameAliasImpl
        extends AbstractFixedVirtualHostNodeAlias<HostNameAliasImpl>
        implements HostNameAlias<HostNameAliasImpl>
{

    private static final Logger LOG = LoggerFactory.getLogger(HostNameAliasImpl.class);

    private final Set<InetAddress> _localAddresses = new CopyOnWriteArraySet<>();
    private final Set<String> _localAddressNames = new CopyOnWriteArraySet<>();
    private final Lock _addressLock = new ReentrantLock();
    private final AtomicBoolean _addressesComputed = new AtomicBoolean();


    @ManagedObjectFactoryConstructor
    protected HostNameAliasImpl(final Map<String, Object> attributes, final Port port)
    {
        super(attributes, port);
    }

    @Override
    protected void onOpen()
    {
        super.onOpen();
        String bindingAddress = getPort().getBindingAddress();
        Thread thread = new Thread(new NetworkAddressResolver(),
                                   "Network Address Resolver (Port: "
                                   + (useAllAddresses(bindingAddress) ? "" : bindingAddress)
                                   + ":" + getPort().getPort() +")");
        thread.setDaemon(true);
        thread.start();
    }



    @Override
    protected boolean matches(final String host)
    {
        if(_localAddressNames.contains(host))
        {
            return true;
        }
        while(!_addressesComputed.get())
        {
            Lock lock = _addressLock;
            lock.lock();
            lock.unlock();
        }

        boolean isNetworkAddress = true;
        if (!_localAddressNames.contains(host))
        {
            try
            {
                InetAddress inetAddress = InetAddress.getByName(host);
                if (!_localAddresses.contains(inetAddress))
                {
                    isNetworkAddress = false;
                }
                else
                {
                    _localAddressNames.add(host);
                }
            }
            catch (UnknownHostException e)
            {
                // ignore
                isNetworkAddress = false;
            }
        }
        return isNetworkAddress;

    }

    private class NetworkAddressResolver implements Runnable
    {
        @Override
        public void run()
        {
            _addressesComputed.set(false);
            Lock lock = _addressLock;

            lock.lock();
            String bindingAddress = getPort().getBindingAddress();
            try
            {
                Collection<InetAddress> inetAddresses;
                if(useAllAddresses(bindingAddress))
                {
                    inetAddresses = getAllInetAddresses();
                }
                else
                {
                    inetAddresses = Collections.singleton(InetAddress.getByName(bindingAddress));
                }
                for (InetAddress address : inetAddresses)
                {
                    _localAddresses.add(address);
                    String hostAddress = address.getHostAddress();
                    if (hostAddress != null)
                    {
                        _localAddressNames.add(hostAddress);
                    }
                    String hostName = address.getHostName();
                    if (hostName != null)
                    {
                        _localAddressNames.add(hostName);
                    }
                    String canonicalHostName = address.getCanonicalHostName();
                    if (canonicalHostName != null)
                    {
                        _localAddressNames.add(canonicalHostName);
                    }
                }
            }
            catch (SocketException | UnknownHostException e)
            {
                LOG.error("Unable to correctly calculate host name aliases for port " + getPort().getName()
                         + ". This may lead to connection failures.", e);
            }
            finally
            {
                _addressesComputed.set(true);
                lock.unlock();
            }
        }

        private Collection<InetAddress> getAllInetAddresses() throws SocketException
        {
            Set<InetAddress> addresses = new TreeSet<>(HostNameAliasImpl::compareAddresses);
            for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces()))
            {
                for (InterfaceAddress inetAddress : networkInterface.getInterfaceAddresses())
                {
                    addresses.add(inetAddress.getAddress());
                }
            }
            return addresses;
        }
    }

    private boolean useAllAddresses(final String bindingAddress)
    {
        return bindingAddress == null || bindingAddress.trim().equals("") || bindingAddress.trim().equals("*");
    }

    private static int compareAddresses(final InetAddress left, final InetAddress right)
    {
        byte[] leftBytes;
        byte[] rightBytes;
        if(left.isLoopbackAddress() != right.isLoopbackAddress())
        {
            return left.isLoopbackAddress() ? -1 : 1;
        }
        else if(left.isSiteLocalAddress() != right.isSiteLocalAddress())
        {
            return left.isSiteLocalAddress() ? -1 : 1;
        }
        else if(left.isLinkLocalAddress() != right.isLinkLocalAddress())
        {
            return left.isLinkLocalAddress() ? 1 : -1;
        }
        else if(left.isMulticastAddress() != right.isMulticastAddress())
        {
            return left.isMulticastAddress() ? 1 : -1;
        }
        else if(left instanceof Inet4Address && !(right instanceof Inet4Address))
        {
            return -1;
        }
        else if(right instanceof Inet4Address && !(left instanceof Inet4Address))
        {
            return 1;
        }
        else if((leftBytes = left.getAddress()).length == (rightBytes = right.getAddress()).length)
        {
            for(int i = 0; i < left.getAddress().length; i++)
            {
                int compare = Byte.compare(leftBytes[i], rightBytes[i]);
                if(compare != 0)
                {
                    return compare;
                }
            }
            return 0;
        }
        else
        {
            return Integer.compare(left.getAddress().length, right.getAddress().length);
        }
    }
}
