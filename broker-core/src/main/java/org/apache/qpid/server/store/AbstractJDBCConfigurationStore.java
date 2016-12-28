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
package org.apache.qpid.server.store;

import static org.apache.qpid.server.store.AbstractJDBCConfigurationStore.State.CONFIGURED;
import static org.apache.qpid.server.store.AbstractJDBCConfigurationStore.State.OPEN;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.model.UUIDGenerator;
import org.apache.qpid.server.store.handler.ConfiguredObjectRecordHandler;

public abstract class AbstractJDBCConfigurationStore implements MessageStoreProvider, DurableConfigurationStore
{
    private final static String CONFIGURATION_VERSION_TABLE_NAME_SUFFIX = "QPID_CONFIG_VERSION";
    private final static String CONFIGURED_OBJECTS_TABLE_NAME_SUFFIX = "QPID_CONFIGURED_OBJECTS";
    private final static String CONFIGURED_OBJECT_HIERARCHY_TABLE_NAME_SUFFIX = "QPID_CONFIGURED_OBJECT_HIERARCHY";

    private static final int DEFAULT_CONFIG_VERSION = 0;


    public enum State { CLOSED, CONFIGURED, OPEN };
    private State _state = State.CLOSED;
    private final Object _lock = new Object();

    private String _tableNamePrefix = "";


    protected void setTableNamePrefix(final String tableNamePrefix)
    {
        _tableNamePrefix = tableNamePrefix;
    }

    @Override
    public boolean openConfigurationStore(ConfiguredObjectRecordHandler handler,
                                          final ConfiguredObjectRecord... initialRecords)
    {
        changeState(CONFIGURED, OPEN);
        try
        {
            Collection<? extends ConfiguredObjectRecord> records = doVisitAllConfiguredObjectRecords(handler);
            boolean isNew = records.isEmpty();

            if(isNew)
            {
                records = Arrays.asList(initialRecords);
                try
                {
                    try (Connection conn = newConnection())
                    {
                        for (ConfiguredObjectRecord record : records)
                        {
                            updateConfiguredObject(record, true, conn);
                        }
                        conn.commit();
                    }
                }
                catch (SQLException e)
                {
                    throw new StoreException("Error updating configured objects in database: " + e.getMessage(), e);
                }
            }

            for(ConfiguredObjectRecord record : records)
            {
                handler.handle(record);
            }
            return isNew;
        }
        catch (SQLException e)
        {
            throw new StoreException("Cannot visit configured object records", e);
        }
    }

    @Override
    public void reload(final ConfiguredObjectRecordHandler handler) throws StoreException
    {
        assertState(State.OPEN);
        try
        {
            Collection<? extends ConfiguredObjectRecord> records = doVisitAllConfiguredObjectRecords(handler);

            for(ConfiguredObjectRecord record : records)
            {
                handler.handle(record);
            }
        }
        catch (SQLException e)
        {
            throw new StoreException("Cannot visit configured object records", e);
        }
    }

    private String getConfigurationVersionTableName()
    {
        return _tableNamePrefix + CONFIGURATION_VERSION_TABLE_NAME_SUFFIX;
    }

    private String getConfiguredObjectsTableName()
    {
        return _tableNamePrefix + CONFIGURED_OBJECTS_TABLE_NAME_SUFFIX;
    }

    private String getConfiguredObjectHierarchyTableName()
    {
        return _tableNamePrefix + CONFIGURED_OBJECT_HIERARCHY_TABLE_NAME_SUFFIX;
    }

    private Collection<ConfiguredObjectRecordImpl> doVisitAllConfiguredObjectRecords(ConfiguredObjectRecordHandler handler) throws SQLException
    {
        Map<UUID, ConfiguredObjectRecordImpl> configuredObjects = new HashMap<UUID, ConfiguredObjectRecordImpl>();
        final ObjectMapper objectMapper = new ObjectMapper();
        try (Connection conn = newAutoCommitConnection())
        {
            PreparedStatement stmt = conn.prepareStatement("SELECT id, object_type, attributes FROM " + getConfiguredObjectsTableName());
            try
            {
                ResultSet rs = stmt.executeQuery();
                try
                {
                    while (rs.next())
                    {
                        String id = rs.getString(1);
                        String objectType = rs.getString(2);
                        String attributes = getBlobAsString(rs, 3);
                        final ConfiguredObjectRecordImpl configuredObjectRecord =
                                new ConfiguredObjectRecordImpl(UUID.fromString(id), objectType,
                                                               objectMapper.readValue(attributes, Map.class));
                        configuredObjects.put(configuredObjectRecord.getId(), configuredObjectRecord);

                    }
                }
                catch (IOException e)
                {
                    throw new StoreException("Error recovering persistent state: " + e.getMessage(), e);
                }
                finally
                {
                    rs.close();
                }
            }
            finally
            {
                stmt.close();
            }
            stmt = conn.prepareStatement("SELECT child_id, parent_type, parent_id FROM " + getConfiguredObjectHierarchyTableName());
            try
            {
                try (ResultSet rs = stmt.executeQuery())
                {
                    while (rs.next())
                    {
                        UUID childId = UUID.fromString(rs.getString(1));
                        String parentType = rs.getString(2);
                        UUID parentId = UUID.fromString(rs.getString(3));

                        ConfiguredObjectRecordImpl child = configuredObjects.get(childId);
                        ConfiguredObjectRecordImpl parent = configuredObjects.get(parentId);

                        if (child != null && parent != null)
                        {
                            child.addParent(parentType, parent);
                        }
                    }
                }
            }
            finally
            {
                stmt.close();
            }

        }
        return configuredObjects.values();

    }

    protected void upgradeIfNecessary(ConfiguredObject<?> parent) throws StoreException
    {
        Connection connection = null;
        try
        {
            connection = newAutoCommitConnection();

            boolean tableExists = tableExists(getConfigurationVersionTableName(), connection);
            if(tableExists)
            {
                int configVersion = getConfigVersion(connection);

                getLogger().debug("Upgrader read existing config version {}", configVersion);

                switch(configVersion)
                {

                    case 7:
                        upgradeFromV7(parent);
                        break;
                    default:
                        throw new UnsupportedOperationException("Cannot upgrade from configuration version : "
                                                                + configVersion);
                }
            }
        }
        catch (SQLException se)
        {
            throw new StoreException("Failed to upgrade database", se);
        }
        finally
        {
            JdbcUtils.closeConnection(connection, getLogger());
        }

    }

    private void upgradeFromV7(ConfiguredObject<?> parent) throws SQLException
    {
        @SuppressWarnings("serial")
        Map<String, String> defaultExchanges = new HashMap<String, String>()
        {{
            put("amq.direct", "direct");
            put("amq.topic", "topic");
            put("amq.fanout", "fanout");
            put("amq.match", "headers");
        }};

        Connection connection = newConnection();
        try
        {
            String virtualHostName = parent.getName();
            UUID virtualHostId = UUIDGenerator.generateVhostUUID(virtualHostName);

            String stringifiedConfigVersion = "0." + DEFAULT_CONFIG_VERSION;

            boolean tableExists = tableExists(getConfigurationVersionTableName(), connection);
            if(tableExists)
            {
                int configVersion = getConfigVersion(connection);

                getLogger().debug("Upgrader read existing config version {}", configVersion);

                stringifiedConfigVersion = "0." + configVersion;
            }

            Map<String, Object> virtualHostAttributes = new HashMap<String, Object>();
            virtualHostAttributes.put("modelVersion", stringifiedConfigVersion);
            virtualHostAttributes.put("name", virtualHostName);

            ConfiguredObjectRecord virtualHostRecord = new ConfiguredObjectRecordImpl(virtualHostId, "VirtualHost", virtualHostAttributes);
            insertConfiguredObject(virtualHostRecord, connection);

            getLogger().debug("Upgrader created VirtualHost configuration entry with config version {}", stringifiedConfigVersion);

            Map<UUID,Map<String,Object>> bindingsToUpdate = new HashMap<UUID, Map<String, Object>>();
            List<UUID> others = new ArrayList<UUID>();
            final ObjectMapper objectMapper = ConfiguredObjectJacksonModule.newObjectMapper(true);

            PreparedStatement stmt = connection.prepareStatement("SELECT id, object_type, attributes FROM " + getConfiguredObjectsTableName());
            try
            {
                try (ResultSet rs = stmt.executeQuery())
                {
                    while (rs.next())
                    {
                        UUID id = UUID.fromString(rs.getString(1));
                        String objectType = rs.getString(2);
                        if ("VirtualHost".equals(objectType))
                        {
                            continue;
                        }
                        Map<String, Object> attributes = objectMapper.readValue(getBlobAsString(rs, 3), Map.class);

                        if (objectType.endsWith("Binding"))
                        {
                            bindingsToUpdate.put(id, attributes);
                        }
                        else
                        {
                            if (objectType.equals("Exchange"))
                            {
                                defaultExchanges.remove((String) attributes.get("name"));
                            }
                            others.add(id);
                        }
                    }
                }
                catch (IOException e)
                {
                    throw new StoreException("Error recovering persistent state: " + e.getMessage(), e);
                }
            }
            finally
            {
                stmt.close();
            }

            stmt = connection.prepareStatement("INSERT INTO " + getConfiguredObjectHierarchyTableName()
                                                       + " ( child_id, parent_type, parent_id) VALUES (?,?,?)");
            try
            {
                for (UUID id : others)
                {
                    stmt.setString(1, id.toString());
                    stmt.setString(2, "VirtualHost");
                    stmt.setString(3, virtualHostId.toString());
                    stmt.execute();
                }
                for(Map.Entry<UUID, Map<String,Object>> bindingEntry : bindingsToUpdate.entrySet())
                {
                    stmt.setString(1, bindingEntry.getKey().toString());
                    stmt.setString(2,"Queue");
                    stmt.setString(3, bindingEntry.getValue().remove("queue").toString());
                    stmt.execute();

                    stmt.setString(1, bindingEntry.getKey().toString());
                    stmt.setString(2,"Exchange");
                    stmt.setString(3, bindingEntry.getValue().remove("exchange").toString());
                    stmt.execute();
                }
            }
            finally
            {
                stmt.close();
            }

            for (Map.Entry<String, String> defaultExchangeEntry : defaultExchanges.entrySet())
            {
                UUID id = UUIDGenerator.generateExchangeUUID(defaultExchangeEntry.getKey(), virtualHostName);
                Map<String, Object> exchangeAttributes = new HashMap<String, Object>();
                exchangeAttributes.put("name", defaultExchangeEntry.getKey());
                exchangeAttributes.put("type", defaultExchangeEntry.getValue());
                exchangeAttributes.put("lifetimePolicy", "PERMANENT");
                Map<String, UUID> parents = Collections.singletonMap("VirtualHost", virtualHostRecord.getId());
                ConfiguredObjectRecord exchangeRecord = new org.apache.qpid.server.store.ConfiguredObjectRecordImpl(id, "Exchange", exchangeAttributes, parents);
                insertConfiguredObject(exchangeRecord, connection);
            }

            stmt = connection.prepareStatement("UPDATE " + getConfiguredObjectsTableName()
                                                       + " set object_type =?, attributes = ? where id = ?");
            try
            {
                for(Map.Entry<UUID, Map<String,Object>> bindingEntry : bindingsToUpdate.entrySet())
                {
                    stmt.setString(1, "Binding");
                    byte[] attributesAsBytes = objectMapper.writeValueAsBytes(bindingEntry.getValue());

                    ByteArrayInputStream bis = new ByteArrayInputStream(attributesAsBytes);
                    stmt.setBinaryStream(2, bis, attributesAsBytes.length);
                    stmt.setString(3, bindingEntry.getKey().toString());
                    stmt.execute();
                }
            }
            catch (IOException e)
            {
                throw new StoreException("Error recovering persistent state: " + e.getMessage(), e);
            }
            finally
            {
                stmt.close();
            }

            if (tableExists)
            {
                dropConfigVersionTable(connection);
            }

            connection.commit();
        }
        catch(SQLException e)
        {
            try
            {
                connection.rollback();
            }
            catch(SQLException re)
            {
            }
            throw e;
        }
        finally
        {
            connection.close();
        }
    }

    protected abstract Logger getLogger();

    protected abstract String getSqlBlobType();

    protected abstract String getSqlVarBinaryType(int size);

    protected abstract String getSqlBigIntType();


    protected void createOrOpenConfigurationStoreDatabase() throws StoreException
    {
        Connection conn = null;
        try
        {
            conn = newAutoCommitConnection();

            createConfiguredObjectsTable(conn);
            createConfiguredObjectHierarchyTable(conn);
        }
        catch (SQLException e)
        {
            throw new StoreException("Unable to open configuration tables", e);
        }
        finally
        {
            JdbcUtils.closeConnection(conn, getLogger());
        }
    }

    private void dropConfigVersionTable(final Connection conn) throws SQLException
    {
        if(!tableExists(getConfigurationVersionTableName(), conn))
        {
            Statement stmt = conn.createStatement();
            try
            {
                stmt.execute("DROP TABLE " + getConfigurationVersionTableName());
            }
            finally
            {
                stmt.close();
            }
        }
    }

    private void createConfiguredObjectsTable(final Connection conn) throws SQLException
    {
        if(!tableExists(getConfiguredObjectsTableName(), conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute("CREATE TABLE "
                             + getConfiguredObjectsTableName()
                             + " ( id VARCHAR(36) not null, object_type varchar(255), attributes "
                             + getSqlBlobType()
                             + ",  PRIMARY KEY (id))");
            }
        }
    }

    private void createConfiguredObjectHierarchyTable(final Connection conn) throws SQLException
    {
        if(!tableExists(getConfiguredObjectHierarchyTableName(), conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute("CREATE TABLE " + getConfiguredObjectHierarchyTableName()
                             + " ( child_id VARCHAR(36) not null, parent_type varchar(255), parent_id VARCHAR(36),  PRIMARY KEY (child_id, parent_type))");
            }
        }
    }

    protected boolean tableExists(final String tableName, final Connection conn) throws SQLException
    {
        return JdbcUtils.tableExists(tableName, conn);
    }

    private int getConfigVersion(Connection conn) throws SQLException
    {
        Statement stmt = conn.createStatement();
        try
        {
            ResultSet rs = stmt.executeQuery("SELECT version FROM " + getConfigurationVersionTableName());
            try
            {

                if(rs.next())
                {
                    return rs.getInt(1);
                }
                return DEFAULT_CONFIG_VERSION;
            }
            finally
            {
                rs.close();
            }

        }
        finally
        {
            stmt.close();
        }

    }

    @Override
    public void create(ConfiguredObjectRecord object) throws StoreException
    {
        assertState(OPEN);
        try
        {
            Connection conn = newConnection();
            try
            {
                insertConfiguredObject(object, conn);
                conn.commit();
            }
            finally
            {
                conn.close();
            }
        }
        catch (SQLException e)
        {
            throw new StoreException("Error creating ConfiguredObject " + object, e);
        }
    }

    /**
     * Convenience method to create a new Connection configured for TRANSACTION_READ_COMMITED
     * isolation and with auto-commit transactions enabled.
     */
    protected Connection newAutoCommitConnection() throws SQLException
    {
        final Connection connection = newConnection();
        try
        {
            connection.setAutoCommit(true);
        }
        catch (SQLException sqlEx)
        {

            try
            {
                connection.close();
            }
            finally
            {
                throw sqlEx;
            }
        }

        return connection;
    }

    /**
     * Convenience method to create a new Connection configured for TRANSACTION_READ_COMMITED
     * isolation and with auto-commit transactions disabled.
     */
    protected Connection newConnection() throws SQLException
    {
        final Connection connection = getConnection();
        try
        {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
        catch (SQLException sqlEx)
        {
            try
            {
                connection.close();
            }
            finally
            {
                throw sqlEx;
            }
        }
        return connection;
    }

    protected abstract Connection getConnection() throws SQLException;

    private void insertConfiguredObject(ConfiguredObjectRecord configuredObject, final Connection conn) throws StoreException
    {
        try
        {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT object_type, attributes FROM " + getConfiguredObjectsTableName()
                                                                        + " where id = ?"))
            {
                stmt.setString(1, configuredObject.getId().toString());
                boolean exists;
                try (ResultSet rs = stmt.executeQuery())
                {
                    exists = rs.next();

                }
                // If we don't have any data in the result set then we can add this configured object
                if (!exists)
                {
                    try (PreparedStatement insertStmt = conn.prepareStatement("INSERT INTO " + getConfiguredObjectsTableName()
                                                                                              + " ( id, object_type, attributes) VALUES (?,?,?)"))
                    {
                        insertStmt.setString(1, configuredObject.getId().toString());
                        insertStmt.setString(2, configuredObject.getType());
                        if (configuredObject.getAttributes() == null)
                        {
                            insertStmt.setNull(3, Types.BLOB);
                        }
                        else
                        {
                            final Map<String, Object> attributes = configuredObject.getAttributes();
                            final ObjectMapper objectMapper = ConfiguredObjectJacksonModule.newObjectMapper(true);
                            byte[] attributesAsBytes = objectMapper.writeValueAsBytes(attributes);

                            ByteArrayInputStream bis = new ByteArrayInputStream(attributesAsBytes);
                            insertStmt.setBinaryStream(3, bis, attributesAsBytes.length);
                        }
                        insertStmt.execute();
                    }

                    writeHierarchy(configuredObject, conn);
                }

            }

        }
        catch (IOException | SQLException e)
        {
            throw new StoreException("Error inserting of configured object " + configuredObject + " into database: " + e.getMessage(), e);
        }
    }

    @Override
    public UUID[] remove(ConfiguredObjectRecord... objects) throws StoreException
    {
        assertState(OPEN);

        Collection<UUID> removed = new ArrayList<UUID>(objects.length);
        try
        {

            try (Connection conn = newAutoCommitConnection())
            {
                for (ConfiguredObjectRecord record : objects)
                {
                    if (removeConfiguredObject(record.getId(), conn) != 0)
                    {
                        removed.add(record.getId());
                    }
                }
            }
        }
        catch (SQLException e)
        {
            throw new StoreException("Error deleting of configured objects " + Arrays.asList(objects) + " from database: " + e.getMessage(), e);
        }
        return removed.toArray(new UUID[removed.size()]);
    }

    private int removeConfiguredObject(final UUID id, final Connection conn) throws SQLException
    {
        final int results;
        PreparedStatement stmt = conn.prepareStatement("DELETE FROM " + getConfiguredObjectsTableName()
                                                           + " where id = ?");
        try
        {
            stmt.setString(1, id.toString());
            results = stmt.executeUpdate();
        }
        finally
        {
            stmt.close();
        }
        stmt = conn.prepareStatement("DELETE FROM " + getConfiguredObjectHierarchyTableName()
                                         + " where child_id = ?");
        try
        {
            stmt.setString(1, id.toString());
            stmt.executeUpdate();
        }
        finally
        {
            stmt.close();
        }

        return results;
    }

    @Override
    public void update(boolean createIfNecessary, ConfiguredObjectRecord... records) throws StoreException
    {
        assertState(OPEN);
        try
        {
            try (Connection conn = newConnection())
            {
                for (ConfiguredObjectRecord record : records)
                {
                    updateConfiguredObject(record, createIfNecessary, conn);
                }
                conn.commit();
            }
        }
        catch (SQLException e)
        {
            throw new StoreException("Error updating configured objects in database: " + e.getMessage(), e);
        }
    }

    private void updateConfiguredObject(ConfiguredObjectRecord configuredObject,
                                        boolean createIfNecessary,
                                        Connection conn)
            throws SQLException, StoreException
    {
        try (PreparedStatement stmt = conn.prepareStatement("SELECT object_type, attributes FROM " + getConfiguredObjectsTableName()
                                                                + " where id = ?"))
        {
            stmt.setString(1, configuredObject.getId().toString());
            try (ResultSet rs = stmt.executeQuery())
            {

                final ObjectMapper objectMapper = ConfiguredObjectJacksonModule.newObjectMapper(true);
                if (rs.next())
                {
                    try (PreparedStatement stmt2 = conn.prepareStatement("UPDATE " + getConfiguredObjectsTableName()
                                                                                         + " set object_type =?, attributes = ? where id = ?"))
                    {
                        stmt2.setString(1, configuredObject.getType());
                        if (configuredObject.getAttributes() != null)
                        {
                            byte[] attributesAsBytes = objectMapper.writeValueAsBytes(
                                    configuredObject.getAttributes());
                            ByteArrayInputStream bis = new ByteArrayInputStream(attributesAsBytes);
                            stmt2.setBinaryStream(2, bis, attributesAsBytes.length);
                        }
                        else
                        {
                            stmt2.setNull(2, Types.BLOB);
                        }
                        stmt2.setString(3, configuredObject.getId().toString());
                        stmt2.execute();
                    }
                }
                else if (createIfNecessary)
                {
                    try (PreparedStatement insertStmt = conn.prepareStatement("INSERT INTO " + getConfiguredObjectsTableName()
                                                                                              + " ( id, object_type, attributes) VALUES (?,?,?)"))
                    {
                        insertStmt.setString(1, configuredObject.getId().toString());
                        insertStmt.setString(2, configuredObject.getType());
                        if (configuredObject.getAttributes() == null)
                        {
                            insertStmt.setNull(3, Types.BLOB);
                        }
                        else
                        {
                            final Map<String, Object> attributes = configuredObject.getAttributes();
                            byte[] attributesAsBytes = objectMapper.writeValueAsBytes(attributes);
                            ByteArrayInputStream bis = new ByteArrayInputStream(attributesAsBytes);
                            insertStmt.setBinaryStream(3, bis, attributesAsBytes.length);
                        }
                        insertStmt.execute();
                    }
                    writeHierarchy(configuredObject, conn);
                }
            }
        }
        catch (IOException e)
        {
            throw new StoreException("Error updating configured object "
                                     + configuredObject
                                     + " in database: "
                                     + e.getMessage(), e);
        }
    }

    private void writeHierarchy(final ConfiguredObjectRecord configuredObject, final Connection conn) throws SQLException, StoreException
    {
        try (PreparedStatement insertStmt = conn.prepareStatement("INSERT INTO " + getConfiguredObjectHierarchyTableName()
                                                                      + " ( child_id, parent_type, parent_id) VALUES (?,?,?)"))
        {
            for (Map.Entry<String, UUID> parentEntry : configuredObject.getParents().entrySet())
            {
                insertStmt.setString(1, configuredObject.getId().toString());
                insertStmt.setString(2, parentEntry.getKey());
                insertStmt.setString(3, parentEntry.getValue().toString());

                insertStmt.execute();
            }
        }
    }

    protected abstract String getBlobAsString(ResultSet rs, int col) throws SQLException;

    @Override
    public void onDelete(ConfiguredObject<?> parent)
    {
        // TODO should probably check we are closed
        try
        {
            Connection conn = newAutoCommitConnection();
            try
            {

                for (String tableName : Arrays.asList(
                        getConfiguredObjectsTableName(),
                        getConfiguredObjectHierarchyTableName()))
                {
                    Statement stmt = conn.createStatement();
                    try
                    {
                        stmt.execute("DROP TABLE " +  tableName);
                    }
                    catch(SQLException e)
                    {
                        getLogger().warn("Failed to drop table '" + tableName + "' :" + e);
                    }
                    finally
                    {
                        stmt.close();
                    }
                }
            }
            finally
            {
                conn.close();
            }
        }
        catch(SQLException e)
        {
            getLogger().error("Exception while deleting store tables", e);
        }
    }

    private static final class ConfiguredObjectRecordImpl implements ConfiguredObjectRecord
    {

        private final UUID _id;
        private final String _type;
        private final Map<String, Object> _attributes;
        private final Map<String, UUID> _parents = new HashMap<>();

        private ConfiguredObjectRecordImpl(final UUID id,
                                           final String type,
                                           final Map<String, Object> attributes)
        {
            _id = id;
            _type = type;
            _attributes = Collections.unmodifiableMap(attributes);
        }

        @Override
        public UUID getId()
        {
            return _id;
        }

        @Override
        public String getType()
        {
            return _type;
        }

        private void addParent(String parentType, ConfiguredObjectRecord parent)
        {
            _parents.put(parentType, parent.getId());
        }

        @Override
        public Map<String, Object> getAttributes()
        {
            return _attributes;
        }

        @Override
        public Map<String, UUID> getParents()
        {
            return Collections.unmodifiableMap(_parents);
        }

        @Override
        public String toString()
        {
            return "ConfiguredObjectRecordImpl [_id=" + _id + ", _type=" + _type + ", _attributes=" + _attributes + ", _parents="
                    + _parents + "]";
        }
    }

    private final void assertState(State state)
    {
        synchronized (_lock)
        {
            if(_state != state)
            {
                throw new IllegalStateException("The store must be in state " + state + " to perform this operation, but it is in state " + _state + " instead");
            }
        }
    }

    protected final void changeState(State oldState, State newState)
    {
        synchronized (_lock)
        {
            assertState(oldState);
            _state = newState;
        }
    }

    protected final void setState(State newState)
    {
        synchronized (_lock)
        {
            _state = newState;
        }
    }


    protected final void doIfNotState(State state, Runnable action)
    {
        synchronized (_lock)
        {
            if(_state != state)
            {
                action.run();
            }
        }
    }
}
