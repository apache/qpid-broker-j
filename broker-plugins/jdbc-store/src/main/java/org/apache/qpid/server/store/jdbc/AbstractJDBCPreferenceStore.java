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
 */

package org.apache.qpid.server.store.jdbc;

import static org.apache.qpid.server.store.jdbc.JdbcUtils.tableExists;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ConfiguredObjectJacksonModule;
import org.apache.qpid.server.model.ModelVersion;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.preferences.PreferenceRecord;
import org.apache.qpid.server.store.preferences.PreferenceRecordImpl;
import org.apache.qpid.server.store.preferences.PreferenceStore;
import org.apache.qpid.server.store.preferences.PreferenceStoreUpdater;
import org.apache.qpid.server.util.BaseAction;

public abstract class AbstractJDBCPreferenceStore implements PreferenceStore
{
    private static final String PREFERENCES_VERSION_TABLE_NAME = "PREFERENCES_VERSION";
    private static final String PREFERENCES_TABLE_NAME = "PREFERENCES";

    private static final String CREATE_PREFERENCES_VERSION_TABLE = "CREATE TABLE %s ( version VARCHAR(20) NOT NULL )";
    private static final String INSERT_INTO_PREFERENCES_VERSION = "INSERT INTO %s ( version ) VALUES ( ? )";
    private static final String SELECT_FROM_PREFERENCES_VERSION = "SELECT version FROM %s";

    private static final String INSERT_INTO_PREFERENCES = "INSERT INTO %s ( id, attributes ) VALUES ( ?, ? )";
    private static final String DELETE_FROM_PREFERENCES = "DELETE FROM %s where id = ?";
    private static final String SELECT_FROM_PREFERENCES = "SELECT id, attributes FROM %s";
    private static final String FIND_PREFERENCE = "SELECT attributes FROM %s WHERE id = ?";
    private static final String UPDATE_PREFERENCES = "UPDATE %s SET attributes = ? WHERE id = ?";

    private final AtomicReference<StoreState> _storeState = new AtomicReference<>(StoreState.CLOSED);
    private final ReentrantReadWriteLock _useOrCloseRWLock = new ReentrantReadWriteLock(true);

    private String _tableNamePrefix = "";

    protected void setTableNamePrefix(final String tableNamePrefix)
    {
        _tableNamePrefix = tableNamePrefix == null ? "" : tableNamePrefix;
    }

    @Override
    public Collection<PreferenceRecord> openAndLoad(final PreferenceStoreUpdater updater) throws StoreException
    {
        if (!_storeState.compareAndSet(StoreState.CLOSED, StoreState.OPENING))
        {
            throw new IllegalStateException(String.format("PreferenceStore cannot be opened when in state '%s'",
                                                          _storeState.get()));
        }

        try
        {
            _storeState.set(StoreState.OPENED);

            Collection<PreferenceRecord> records;

            try (Connection connection = getConnection())
            {
                createVersionTable(connection);
                createPreferencesTable(connection);
                ModelVersion preferencesVersion = getPreferencesVersion(connection);
                ModelVersion brokerModelVersion = ModelVersion.fromString(BrokerModel.MODEL_VERSION);
                if (brokerModelVersion.lessThan(preferencesVersion))
                {
                    throw new StoreException(String.format("Cannot downgrade preference store from '%s' to '%s'", preferencesVersion, brokerModelVersion));
                }

                records = getPreferenceRecords(connection);

                if (preferencesVersion.lessThan(brokerModelVersion))
                {
                    final Collection<UUID> ids = new HashSet<>();
                    for (PreferenceRecord record : records)
                    {
                        ids.add(record.getId());
                    }

                    records = updater.updatePreferences(preferencesVersion.toString(), records);

                    removeAndAdd(ids,
                                 records,
                                 transactedConnection -> updateVersion(transactedConnection,
                                                                       brokerModelVersion.toString()));
                }
            }

            return records;
        }
        catch (SQLException e)
        {
            _storeState.set(StoreState.ERRORED);
            close();
            throw new StoreException(e);
        }
    }

    @Override
    public void updateOrCreate(final Collection<PreferenceRecord> preferenceRecords)
    {
        _useOrCloseRWLock.readLock().lock();
        try
        {
            if (!getStoreState().equals(StoreState.OPENED))
            {
                throw new IllegalStateException("PreferenceStore is not opened");
            }

            performSafeTransaction(new BaseAction<Connection, Exception>()
            {
                @Override
                public void performAction(final Connection connection) throws Exception
                {
                    updateOrCreateInternal(connection, preferenceRecords);
                }
            });
        }
        finally
        {
            _useOrCloseRWLock.readLock().unlock();
        }
    }

    @Override
    public void replace(final Collection<UUID> preferenceRecordsToRemove,
                        final Collection<PreferenceRecord> preferenceRecordsToAdd)
    {
        removeAndAdd(preferenceRecordsToRemove, preferenceRecordsToAdd, null);
    }

    private void removeAndAdd(final Collection<UUID> preferenceRecordsToRemove,
                              final Collection<PreferenceRecord> preferenceRecordsToAdd,
                              final BaseAction<Connection, SQLException> preCommitAction)
    {
        _useOrCloseRWLock.readLock().lock();
        try
        {
            if (!getStoreState().equals(StoreState.OPENED))
            {
                throw new IllegalStateException("PreferenceStore is not opened");
            }

            performSafeTransaction(new BaseAction<Connection, Exception>()
            {
                @Override
                public void performAction(final Connection connection) throws Exception
                {
                    for (UUID id : preferenceRecordsToRemove)
                    {
                        try (PreparedStatement deleteStatement = connection.prepareStatement(String.format(
                                DELETE_FROM_PREFERENCES,
                                getPreferencesTableName())))
                        {
                            deleteStatement.setString(1, id.toString());
                            int deletedCount = deleteStatement.executeUpdate();
                            if (deletedCount == 1)
                            {
                                getLogger().debug(String.format(
                                        "Failed to delete preference with id %s : no such record",
                                        id));
                            }
                        }
                    }
                    updateOrCreateInternal(connection, preferenceRecordsToAdd);
                    if (preCommitAction != null)
                    {
                        preCommitAction.performAction(connection);
                    }
                }
            });
        }
        finally
        {
            _useOrCloseRWLock.readLock().unlock();
        }
    }

    @Override
    public void onDelete()
    {
        _useOrCloseRWLock.writeLock().lock();
        try
        {
            close();
            doDelete();
        }
        finally
        {
            _useOrCloseRWLock.writeLock().unlock();
        }
    }

    @Override
    public void close()
    {
        _useOrCloseRWLock.writeLock().lock();
        try
        {
            while (true)
            {
                StoreState storeState = getStoreState();
                if (storeState.equals(StoreState.OPENED) || storeState.equals(StoreState.ERRORED))
                {
                    if (_storeState.compareAndSet(storeState, StoreState.CLOSING))
                    {
                        break;
                    }
                }
                else if (storeState.equals(StoreState.CLOSED) || storeState.equals(StoreState.CLOSING))
                {
                    return;
                }
            }

            doClose();

            _storeState.set(StoreState.CLOSED);
        }
        finally
        {
            _useOrCloseRWLock.writeLock().unlock();
        }
    }

    protected void dropTables(final Connection connection) throws SQLException
    {
        try (Statement dropTableStatement = connection.createStatement())
        {
            dropTableStatement.execute(String.format("DROP TABLE %s", getPreferencesTableName()));
            dropTableStatement.execute(String.format("DROP TABLE %s", getPreferencesVersionTableName()));
        }
        catch (SQLException e)
        {
            getLogger().warn("Failed to drop preferences table", e);
        }
    }

    protected abstract void doDelete();

    protected abstract void doClose();

    protected abstract Logger getLogger();

    protected abstract Connection getConnection() throws SQLException;

    protected abstract String getSqlBlobType();

    protected abstract String getBlobAsString(ResultSet rs, int col) throws SQLException;

    StoreState getStoreState()
    {
        return _storeState.get();
    }

    private void updateOrCreateInternal(final Connection conn,
                                        final Collection<PreferenceRecord> preferenceRecords)
            throws SQLException, JsonProcessingException
    {
        for (PreferenceRecord record : preferenceRecords)
        {
            try (PreparedStatement stmt = conn.prepareStatement(String.format(FIND_PREFERENCE,
                                                                              getPreferencesTableName())))
            {
                stmt.setString(1, record.getId().toString());
                try (ResultSet rs = stmt.executeQuery())
                {
                    if (rs.next())
                    {
                        try (PreparedStatement updateStatement = conn.prepareStatement(String.format(UPDATE_PREFERENCES,
                                                                                                     getPreferencesTableName())))
                        {
                            setAttributesAsBlob(updateStatement, 1, record.getAttributes());
                            updateStatement.setString(2, record.getId().toString());
                            updateStatement.execute();
                        }
                    }
                    else
                    {
                        try (PreparedStatement insertStatement = conn.prepareStatement(String.format(
                                INSERT_INTO_PREFERENCES,
                                getPreferencesTableName())))
                        {
                            insertStatement.setString(1, record.getId().toString());
                            setAttributesAsBlob(insertStatement, 2, record.getAttributes());
                            insertStatement.execute();
                        }
                    }
                }
            }
        }
    }

    private void performSafeTransaction(final BaseAction<Connection, Exception> transactedAction)
    {
        Connection connection = null;
        try
        {
            connection = getTransactedConnection();
            transactedAction.performAction(connection);
            connection.commit();
        }
        catch (Exception e)
        {
            try
            {
                if (connection != null)
                {
                    connection.rollback();
                }
            }
            catch (SQLException e1)
            {
                getLogger().error("Failed to rollback transaction", e1);
            }
            throw new StoreException(e);
        }
        finally
        {
            try
            {
                if (connection != null)
                {
                    connection.close();
                }
            }
            catch (SQLException e)
            {
                getLogger().warn("Failed to close JDBC connection", e);
            }
        }
    }

    private Connection getTransactedConnection() throws SQLException
    {
        final Connection connection = getConnection();
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return connection;
    }

    private void setAttributesAsBlob(final PreparedStatement preparedSqlStatement,
                                     final int parameterIndex,
                                     final Map<String, Object> attributes)
            throws JsonProcessingException, SQLException
    {
        final ObjectMapper objectMapper = ConfiguredObjectJacksonModule.newObjectMapper(true);
        if (attributes != null)
        {
            byte[] attributesAsBytes = objectMapper.writeValueAsBytes(attributes);
            ByteArrayInputStream bis = new ByteArrayInputStream(attributesAsBytes);
            preparedSqlStatement.setBinaryStream(parameterIndex, bis, attributesAsBytes.length);
        }
        else
        {
            preparedSqlStatement.setNull(parameterIndex, Types.BLOB);
        }
    }

    private void createVersionTable(final Connection conn) throws SQLException
    {
        if (!tableExists(getPreferencesVersionTableName(), conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute(String.format(CREATE_PREFERENCES_VERSION_TABLE, getPreferencesVersionTableName()));
            }

            updateVersion(conn, BrokerModel.MODEL_VERSION);
        }
    }

    private void updateVersion(final Connection conn, final String currentVersion) throws SQLException
    {
        try (PreparedStatement pstmt = conn.prepareStatement(String.format(INSERT_INTO_PREFERENCES_VERSION,
                                                                           getPreferencesVersionTableName())))
        {
            pstmt.setString(1, currentVersion);
            pstmt.execute();
        }
    }

    private void createPreferencesTable(final Connection conn) throws SQLException
    {
        if (!tableExists(getPreferencesTableName(), conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute("CREATE TABLE "
                             + getPreferencesTableName()
                             + " ( id VARCHAR(36) not null, attributes "
                             + getSqlBlobType()
                             + ",  PRIMARY KEY (id))");
            }
        }
    }

    protected ModelVersion getPreferencesVersion(Connection conn) throws SQLException
    {
        ModelVersion storedVersion = null;
        try (Statement stmt = conn.createStatement())
        {
            try (ResultSet rs = stmt.executeQuery(String.format(SELECT_FROM_PREFERENCES_VERSION,
                                                                getPreferencesVersionTableName())))
            {
                while (rs.next())
                {
                    String versionString = rs.getString(1);
                    try
                    {
                        ModelVersion version = ModelVersion.fromString(versionString);
                        if (storedVersion == null || storedVersion.lessThan(version))
                        {
                            storedVersion = version;
                        }
                    }
                    catch (IllegalArgumentException e)
                    {
                        throw new StoreException("preference store version is malformed", e);
                    }
                }
            }
        }
        if (storedVersion == null)
        {
            throw new StoreException("No preferences version found");
        }
        return storedVersion;
    }

    private Collection<PreferenceRecord> getPreferenceRecords(final Connection connection) throws SQLException
    {
        Collection<PreferenceRecord> records = new LinkedHashSet<>();
        final ObjectMapper objectMapper = new ObjectMapper();
        try (PreparedStatement stmt = connection.prepareStatement(String.format(SELECT_FROM_PREFERENCES,
                                                                                getPreferencesTableName())))
        {
            try (ResultSet rs = stmt.executeQuery())
            {
                while (rs.next())
                {
                    String id = rs.getString(1);
                    String attributes = getBlobAsString(rs, 2);
                    final PreferenceRecord preferenceRecord = new PreferenceRecordImpl(UUID.fromString(id), objectMapper.readValue(attributes, Map.class));
                    records.add(preferenceRecord);
                }
            }
            catch (IOException e)
            {
                throw new StoreException("Error recovering persistent state: " + e.getMessage(), e);
            }
        }
        return records;
    }

    private String getPreferencesTableName()
    {
        return _tableNamePrefix + PREFERENCES_TABLE_NAME;
    }

    private String getPreferencesVersionTableName()
    {
        return _tableNamePrefix + PREFERENCES_VERSION_TABLE_NAME;
    }

    enum StoreState
    {
        CLOSED, OPENING, OPENED, CLOSING, ERRORED;
    }
}
