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
package org.apache.qpid.server.store.berkeleydb.replication;

import static org.apache.qpid.systests.admin.SpawnBrokerAdmin.SYSTEST_PROPERTY_SPAWN_BROKER_STARTUP_TIME;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.sleepycat.je.rep.ReplicationConfig;

import org.apache.qpid.server.virtualhostnode.AbstractVirtualHostNode;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHARemoteReplicationNode;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHAVirtualHostNode;
import org.apache.qpid.server.virtualhostnode.berkeleydb.BDBHAVirtualHostNodeImpl;
import org.apache.qpid.systests.admin.BrokerAdminException;
import org.apache.qpid.systests.admin.SpawnBrokerAdmin;
import org.apache.qpid.test.utils.PortHelper;
import org.apache.qpid.tests.utils.BrokerAdmin;

public class GroupBrokerAdmin
{
    private static final int WAIT_LIMIT = Integer.getInteger("qpid.test.ha.await", 10000);
    private static final String AMQP_NODE_TYPE = "org.apache.qpid.VirtualHostNode";
    private static final String AMQP_REMOTE_NODE_TYPE = "org.apache.qpid.server.model.RemoteReplicationNode";
    private static final String ROLE_MASTER = "MASTER";
    private static final String ROLE_REPLICA = "REPLICA";
    private static final String NODE_TYPE = "BDB_HA";
    private static final String HOST = "127.0.0.1";

    private GroupMember[] _members;
    private ListeningExecutorService _executorService;
    private SpawnBrokerAdmin[] _brokers;
    private String _groupName;

    public void beforeTestClass(final Class testClass)
    {
        GroupConfig runBrokerAdmin = (GroupConfig) testClass.getAnnotation(GroupConfig.class);
        int numberOfNodes = runBrokerAdmin == null ? 2 : runBrokerAdmin.numberOfNodes();
        _groupName = runBrokerAdmin == null ? "test-ha" : runBrokerAdmin.groupName();
        _executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numberOfNodes));

        _brokers = Stream.generate(SpawnBrokerAdmin::new).limit(numberOfNodes).toArray(SpawnBrokerAdmin[]::new);

        boolean started = false;
        try
        {
            int startupTimeout = Integer.getInteger(SYSTEST_PROPERTY_SPAWN_BROKER_STARTUP_TIME, 30000);
            awaitFuture(startupTimeout, invokeParallel(Arrays.stream(_brokers).map(a -> (Callable<Void>) () -> {
                a.beforeTestClass(testClass);
                return null;
            }).collect(Collectors.toList())));

            started = true;
        }
        finally
        {
            if (!started)
            {
                for (SpawnBrokerAdmin a : _brokers)
                {
                    a.afterTestClass(testClass);
                }
                _executorService.shutdown();
                _brokers = null;
                _executorService = null;
            }
        }
    }

    public void beforeTestMethod(final Class testClass, final Method method)
    {
        _members = initializeGroupData(_groupName, _brokers);
        GroupMember first = _members[0];
        first.getAdmin().beforeTestMethod(_members[0].getName(), NODE_TYPE, _members[0].getNodeAttributes());
        awaitNodeRoleReplicaOrMaster(first);
        ListenableFuture<Void> f;
        if (_members.length > 2)
        {
            f = invokeParallel(Arrays.stream(_members).skip(1).map(m -> (Callable<Void>) () -> {
                m.getAdmin().beforeTestMethod(m.getName(), NODE_TYPE, m.getNodeAttributes());
                return null;
            }).collect(Collectors.toList()));
        }
        else
        {
            for (int i = 1; i < _members.length; i++)
            {
                _members[i].getAdmin()
                           .beforeTestMethod(_members[i].getName(), NODE_TYPE, _members[i].getNodeAttributes());
            }
            f = Futures.immediateFuture(null);
        }

        awaitFuture(WAIT_LIMIT, f);
        awaitAllTransitionIntoReplicaOrMaster();
    }

    public void afterTestMethod(final Class testClass, final Method method)
    {
        if (_members != null)
        {
            awaitFuture(WAIT_LIMIT, invokeParallel(Arrays.stream(_members).map(m -> (Callable<Void>) () -> {
                m.getAdmin().afterTestMethod(testClass, method);
                return null;
            }).collect(Collectors.toList())));
            _members = null;
        }
    }

    public void afterTestClass(final Class testClass)
    {
        try
        {
            if (_brokers != null)
            {
                awaitFuture(WAIT_LIMIT, invokeParallel(Arrays.stream(_brokers).map(m -> (Callable<Void>) () -> {
                    m.afterTestClass(testClass);
                    return null;
                }).collect(Collectors.toList())));
                _brokers = null;
            }
        }
        finally
        {
            if (_executorService != null)
            {
                _executorService.shutdown();
            }
        }
    }

    public ListenableFuture<Void> restart()
    {
        awaitFuture(WAIT_LIMIT, invokeParallel(Arrays.stream(_members).map(m -> (Callable<Void>) () -> {
            m.getAdmin().restart();
            return null;
        }).collect(Collectors.toList())));
        awaitAllTransitionIntoReplicaOrMaster();

        return Futures.immediateFuture(null);
    }

    public void stop()
    {
        awaitFuture(WAIT_LIMIT, invokeParallel(Arrays.stream(_members).map(m -> (Callable<Void>) () -> {
            m.getAdmin().stop();
            return null;
        }).collect(Collectors.toList())));
    }

    public void start()
    {
        awaitFuture(WAIT_LIMIT, invokeParallel(Arrays.stream(_members).map(m -> (Callable<Void>) () -> {
            m.getAdmin().start();
            return null;
        }).collect(Collectors.toList())));

        awaitAllTransitionIntoReplicaOrMaster();
    }

    public void startNode(final int amqpPort)
    {
        GroupMember member = getMemberByAmqpPort(amqpPort);
        member.getAdmin().start();
        awaitNodeRole(amqpPort, ROLE_MASTER, ROLE_REPLICA);
    }

    public void stopNode(final int amqpPort)
    {
        GroupMember member = getMemberByAmqpPort(amqpPort);
        member.getAdmin().stop();
    }

    public int[] getGroupAmqpPorts()
    {
        int[] ports = new int[_members.length];
        int i = 0;
        for (GroupMember m : _members)
        {
            ports[i++] = m.getAmqpPort();
        }
        return ports;
    }

    public int[] getBdbPorts()
    {
        int[] ports = new int[_members.length];
        int i = 0;
        for (GroupMember m : _members)
        {
            ports[i++] = m.getBdbPort();
        }
        return ports;
    }

    public int getAmqpPort(final int... exclude)
    {
        Set<Integer> excluded = Arrays.stream(exclude).boxed().collect(Collectors.toSet());
        return Arrays.stream(_members)
                     .map(GroupMember::getAmqpPort)
                     .filter(p -> !excluded.contains(p))
                     .findFirst()
                     .orElseThrow(() -> new BrokerAdminException("Amqp Port not found"));
    }

    public String getHost()
    {
        return HOST;
    }

    public Map<Integer, String> groupThreadDumps()
    {
        Map<Integer, String> threadDumps = new HashMap<>();
        for (GroupMember m : _members)
        {
            threadDumps.put(m._amqpPort, m.getAdmin().dumpThreads());
        }
        return threadDumps;
    }

    public String getHelperHostPort()
    {
        return HOST + ":" + _members[0].getBdbPort();
    }

    public Map<String, Object> getNodeAttributes(final int amqpPort)
    {
        GroupMember member = getMemberByAmqpPort(amqpPort);
        return member.getAdmin().getAttributes(true, member.getName(), AMQP_NODE_TYPE);
    }

    public void setNodeAttributes(final int amqpPort, final Map<String, Object> attributes)
    {
        GroupMember member = getMemberByAmqpPort(amqpPort);
        member.getAdmin().update(true, member.getName(), AMQP_NODE_TYPE, attributes);
    }

    public void setDesignatedPrimary(int brokerPort, boolean designatedPrimary)
    {
        setNodeAttributes(brokerPort, Collections.singletonMap(BDBHAVirtualHostNode.DESIGNATED_PRIMARY,
                                                               String.valueOf(designatedPrimary)));
    }

    public void awaitNodeRole(final int amqpPort, String... role)
    {
        awaitNodeToAttainAttributeValue(amqpPort, BDBHAVirtualHostNode.ROLE, (Object[]) role);
    }

    public Object awaitNodeToAttainAttributeValue(final int amqpPort,
                                                  final String attributeName,
                                                  final Object... attributeValue)
    {
        GroupMember member = getMemberByAmqpPort(amqpPort);
        return member.getAdmin().awaitAttributeValue(WAIT_LIMIT,
                                                     true,
                                                     member.getName(),
                                                     AMQP_NODE_TYPE,
                                                     attributeName,
                                                     attributeValue);
    }

    public Map<String, Object> getRemoteNodeAttributes(final int amqpPort, final int remoteAmqpPort)
    {
        GroupMember member = getMemberByAmqpPort(amqpPort);
        GroupMember member2 = getMemberByAmqpPort(remoteAmqpPort);
        return member.getAdmin()
                     .getAttributes(true,
                                    member.getName() + "/" + member2.getName(),
                                    AMQP_REMOTE_NODE_TYPE);
    }

    public void setRemoteNodeAttributes(final int amqpPort,
                                        final int remoteAmqpPort,
                                        final Map<String, Object> attributes)
    {

        GroupMember member = getMemberByAmqpPort(amqpPort);
        GroupMember member2 = getMemberByAmqpPort(remoteAmqpPort);
        member.getAdmin()
              .update(true,
                      member.getName() + "/" + member2.getName(),
                      AMQP_REMOTE_NODE_TYPE,
                      attributes);
    }

    public void awaitRemoteNodeRole(final int amqpPort, final int remoteAmqpPort, final String... role)
    {

        GroupMember member = getMemberByAmqpPort(amqpPort);
        GroupMember member2 = getMemberByAmqpPort(remoteAmqpPort);
        member.getAdmin().awaitAttributeValue(WAIT_LIMIT,
                                              true,
                                              member.getName() + "/" + member2.getName(),
                                              AMQP_REMOTE_NODE_TYPE,
                                              BDBHARemoteReplicationNode.ROLE,
                                              ROLE_REPLICA,
                                              ROLE_MASTER);
    }

    private SpawnBrokerAdmin getNodeAdmin(final int amqpPort)
    {
        GroupMember member = getMemberByAmqpPort(amqpPort);
        return member.getAdmin();
    }

    private GroupMember[] initializeGroupData(final String groupName, final SpawnBrokerAdmin[] admins)
    {
        PortHelper helper = new PortHelper();
        int[] ports = new int[admins.length];
        String[] addresses = new String[admins.length];
        int port = -1;
        for (int i = 0; i < admins.length; i++)
        {
            port = i == 0 ? helper.getNextAvailable() : helper.getNextAvailable(port + 1);
            addresses[i] = HOST + ":" + port;
            ports[i] = port;
        }

        Map<String, String> context = new HashMap<>();
        context.put(ReplicationConfig.INSUFFICIENT_REPLICAS_TIMEOUT, "2 s");
        context.put(ReplicationConfig.ELECTIONS_PRIMARY_RETRIES, "0");
        context.put(AbstractVirtualHostNode.VIRTUALHOST_BLUEPRINT_CONTEXT_VAR, "{\"type\":\"BDB_HA\"}");

        String permitted = objectToJson(addresses);
        String contextAsString = objectToJson(context);

        GroupMember[] members = new GroupMember[admins.length];
        for (int i = 0; i < admins.length; i++)
        {
            String nodeName = "node-" + ports[i];

            Map<String, Object> nodeAttributes = new HashMap<>();
            nodeAttributes.put(BDBHAVirtualHostNode.GROUP_NAME, groupName);
            nodeAttributes.put(BDBHAVirtualHostNode.NAME, nodeName);
            nodeAttributes.put(BDBHAVirtualHostNode.ADDRESS, addresses[i]);
            nodeAttributes.put(BDBHAVirtualHostNode.TYPE, BDBHAVirtualHostNodeImpl.VIRTUAL_HOST_NODE_TYPE);
            nodeAttributes.put(BDBHAVirtualHostNode.DEFAULT_VIRTUAL_HOST_NODE, true);
            nodeAttributes.put(BDBHAVirtualHostNode.HELPER_ADDRESS, addresses[0]);
            if (i > 0)
            {
                nodeAttributes.put(BDBHAVirtualHostNode.HELPER_NODE_NAME, "node-" + ports[0]);
            }
            nodeAttributes.put(BDBHAVirtualHostNode.PERMITTED_NODES, permitted);
            nodeAttributes.put(BDBHAVirtualHostNode.CONTEXT, contextAsString);
            members[i] = new GroupMember(nodeName,
                                         admins[i].getBrokerAddress(BrokerAdmin.PortType.AMQP).getPort(),
                                         port,
                                         admins[i],
                                         nodeAttributes);
        }
        return members;
    }

    private GroupMember getMemberByAmqpPort(final int amqpPort)
    {
        return Arrays.stream(_members)
                     .filter(m -> m.getAmqpPort() == amqpPort)
                     .findFirst()
                     .orElseThrow(() -> new BrokerAdminException(
                             String.format("Could not find node by amqp port %d", amqpPort)));
    }

    private <T> ListenableFuture<T> invokeParallel(Collection<Callable<T>> tasks)
    {
        try
        {
            @SuppressWarnings("unchecked")
            List<ListenableFuture<T>> futures = (List) _executorService.invokeAll(tasks);
            ListenableFuture<List<T>> combinedFuture = Futures.allAsList(futures);
            return Futures.transform(combinedFuture, input -> null, _executorService);
        }
        catch (InterruptedException e)
        {
            Thread.interrupted();
            return Futures.immediateFailedFuture(e);
        }
    }

    private <T> void awaitFuture(long waitLimit, ListenableFuture<T> future)
    {
        try
        {
            future.get(waitLimit, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.interrupted();
            throw new BrokerAdminException("Interrupted", e);
        }
        catch (ExecutionException e)
        {
            throw new BrokerAdminException("Operation failed", e.getCause());
        }
        catch (TimeoutException e)
        {
            throw new BrokerAdminException("Timeout");
        }
    }

    private void awaitAllTransitionIntoReplicaOrMaster()
    {
        Map<String, Object> roles = new ConcurrentHashMap<>();
        awaitFuture(WAIT_LIMIT, invokeParallel(Arrays.stream(_members).map(m -> (Callable<Void>) () -> {
            Object role = awaitNodeRoleReplicaOrMaster(m);
            roles.put(m.getName(), String.valueOf(role));
            return null;
        }).collect(Collectors.toList())));

        if (roles.values().stream().noneMatch(role -> ROLE_MASTER.equals(role) || ROLE_REPLICA.equals(role)))
        {
            throw new BrokerAdminException("Unexpected node roles " + Joiner.on(", ").withKeyValueSeparator(" -> ")
                                                                            .join(roles));
        }
    }

    private Object awaitNodeRoleReplicaOrMaster(final GroupMember m)
    {
        return m.getAdmin().awaitAttributeValue(WAIT_LIMIT,
                                                true,
                                                m.getName(),
                                                AMQP_NODE_TYPE,
                                                BDBHAVirtualHostNode.ROLE,
                                                ROLE_REPLICA,
                                                ROLE_MASTER);
    }

    private String objectToJson(final Object object)
    {
        try
        {
            return new ObjectMapper().writeValueAsString(object);
        }
        catch (JsonProcessingException e)
        {
            throw new BrokerAdminException("Cannot convert object to json", e);
        }
    }

    private class GroupMember
    {
        private final Map<String, Object> _nodeAttributes;
        private final SpawnBrokerAdmin _admin;
        private final int _bdbPort;
        private final int _amqpPort;
        private final String _name;

        private GroupMember(final String name,
                            final int amqpPort,
                            final int bdbPort,
                            final SpawnBrokerAdmin admin,
                            final Map<String, Object> nodeAttributes)
        {
            _name = name;
            _admin = admin;
            _bdbPort = bdbPort;
            _amqpPort = amqpPort;
            _nodeAttributes = nodeAttributes;
        }

        private String getName()
        {
            return _name;
        }

        private SpawnBrokerAdmin getAdmin()
        {
            return _admin;
        }

        private int getBdbPort()
        {
            return _bdbPort;
        }

        private int getAmqpPort()
        {
            return _amqpPort;
        }

        private Map<String, Object> getNodeAttributes()
        {
            return _nodeAttributes;
        }
    }
}
