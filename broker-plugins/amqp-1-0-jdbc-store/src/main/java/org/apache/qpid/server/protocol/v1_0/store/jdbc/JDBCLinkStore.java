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

package org.apache.qpid.server.protocol.v1_0.store.jdbc;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.BrokerModel;
import org.apache.qpid.server.model.ModelVersion;
import org.apache.qpid.server.protocol.v1_0.LinkDefinition;
import org.apache.qpid.server.protocol.v1_0.LinkDefinitionImpl;
import org.apache.qpid.server.protocol.v1_0.LinkKey;
import org.apache.qpid.server.protocol.v1_0.store.AbstractLinkStore;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreUpdater;
import org.apache.qpid.server.protocol.v1_0.store.LinkStoreUtils;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Source;
import org.apache.qpid.server.protocol.v1_0.type.messaging.Target;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.transport.Role;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.jdbc.JDBCContainer;
import org.apache.qpid.server.store.jdbc.JDBCDetails;
import org.apache.qpid.server.store.jdbc.JdbcUtils;
import org.apache.qpid.server.util.Action;

public class JDBCLinkStore extends AbstractLinkStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCLinkStore.class);
    private static final String LINKS_TABLE_NAME_SUFFIX = "AMQP_1_0_LINKS";
    private static final String VERSION_TABLE_NAME_SUFFIX = "AMQP_1_0_LINKS_VERSION";
    private final JDBCContainer _jdbcContainer;
    private final String _tableNamePrefix;
    private final String _sqlBlobType;
    private final String _sqlTimestampType;
    private final boolean _isUseBytesMethodsForBlob;
    private final Action<Connection> _cleanUpAction;

    JDBCLinkStore(final JDBCContainer jdbcContainer)
    {
        _jdbcContainer = jdbcContainer;
        _tableNamePrefix = jdbcContainer.getTableNamePrefix();
        JDBCDetails jdbcDetails = jdbcContainer.getJDBCDetails();
        _sqlBlobType = jdbcDetails.getBlobType();
        _sqlTimestampType = jdbcDetails.getTimestampType();
        _isUseBytesMethodsForBlob = jdbcDetails.isUseBytesMethodsForBlob();
        _cleanUpAction = this::cleanUp;
        jdbcContainer.addDeleteAction(_cleanUpAction);
    }

    @Override
    protected Collection<LinkDefinition<Source, Target>> doOpenAndLoad(final LinkStoreUpdater updater) throws StoreException
    {
        Collection<LinkDefinition<Source, Target>> linkDefinitions;
        try
        {
            checkTransactionIsolationLevel();
            createOrOpenStoreDatabase();
            linkDefinitions = getLinks();
            ModelVersion storedVersion = getStoredVersion();
            ModelVersion currentVersion =
                    new ModelVersion(BrokerModel.MODEL_MAJOR_VERSION, BrokerModel.MODEL_MINOR_VERSION);
            if (storedVersion.lessThan(currentVersion))
            {
                linkDefinitions = performUpdate(updater, linkDefinitions, storedVersion, currentVersion);
            }
            else if (currentVersion.lessThan(storedVersion))
            {
                throw new StoreException(String.format("Cannot downgrade the store from %s to %s",
                                                       storedVersion,
                                                       currentVersion));
            }
        }
        catch (SQLException e)
        {
            throw new StoreException("Cannot open link store", e);
        }
        return linkDefinitions;
    }

    @Override
    protected void doClose() throws StoreException
    {

    }

    @Override
    protected void doSaveLink(final LinkDefinition<Source, Target> link) throws StoreException
    {
        String linkKey = generateLinkKey(link);
        Connection connection = getConnection();
        try
        {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            try (PreparedStatement preparedStatement = connection.prepareStatement(
                    String.format(
                            "SELECT remote_container_id, link_name, link_role, source, target FROM %s WHERE link_key = ?",
                            getLinksTableName())))
            {
                preparedStatement.setString(1, linkKey);
                try (ResultSet resultSet = preparedStatement.executeQuery())
                {
                    if (resultSet.next())
                    {
                        update(connection, linkKey, link);
                    }
                    else
                    {
                        insert(connection, linkKey, link);
                    }
                }
            }
            connection.commit();
        }
        catch (SQLException e)
        {
            try
            {
                connection.rollback();
            }
            catch (SQLException re)
            {
                LOGGER.debug("Rollback failed on rolling back saving link transaction", re);
            }
            throw new StoreException(String.format("Cannot save link %s", new LinkKey(link)), e);
        }
        finally
        {
            JdbcUtils.closeConnection(connection, LOGGER);
        }
    }

    @Override
    protected void doDeleteLink(final LinkDefinition<Source, Target> link) throws StoreException
    {

        try (Connection connection = getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(
                     String.format("DELETE FROM %s WHERE link_key = ?", getLinksTableName())))
        {
            preparedStatement.setString(1, generateLinkKey(link));
            preparedStatement.execute();
        }
        catch (SQLException e)
        {
            throw new StoreException(String.format("Cannot delete link %s", new LinkKey(link)), e);
        }
    }


    @Override
    protected void doDelete()
    {
        _jdbcContainer.removeDeleteAction(_cleanUpAction);
        try (Connection connection = getConnection())
        {
            cleanUp(connection);
        }
        catch (IllegalStateException e)
        {
            LOGGER.warn("Could not delete Link store: {}", e.getMessage());
        }
        catch (SQLException e)
        {
            throw new StoreException("Error deleting Link store", e);
        }
    }

    private void cleanUp(final Connection connection)
    {
        JdbcUtils.dropTables(connection, LOGGER, Arrays.asList(getLinksTableName(), getVersionTableName()));
    }

    @Override
    public TerminusDurability getHighestSupportedTerminusDurability()
    {
        return TerminusDurability.CONFIGURATION;
    }

    private void checkTransactionIsolationLevel() throws SQLException
    {
        try (Connection connection = getConnection())
        {
            DatabaseMetaData metaData = connection.getMetaData();
            if (!metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE))
            {
                throw new StoreException(String.format(
                        "The RDBMS '%s' does not support required transaction isolation level 'serializable'",
                        metaData.getDatabaseProductName()));
            }
        }
    }

    private Connection getConnection()
    {
        return _jdbcContainer.getConnection();
    }

    private void createOrOpenStoreDatabase() throws SQLException
    {
        try (Connection conn = getConnection())
        {
            conn.setAutoCommit(true);

            createLinksTable(conn);
            createVersionTable(conn);
        }
    }

    private void createVersionTable(final Connection conn) throws SQLException
    {
        String versionTableName = getVersionTableName();
        if (!JdbcUtils.tableExists(versionTableName, conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute(String.format("CREATE TABLE %s"
                                           + " (version varchar(10) PRIMARY KEY ,"
                                           + " version_time %s)", versionTableName, _sqlTimestampType));
            }
            updateVersion(conn, ModelVersion.fromString(BrokerModel.MODEL_VERSION));
        }
    }


    private void createLinksTable(final Connection conn) throws SQLException
    {
        if (!JdbcUtils.tableExists(getLinksTableName(), conn))
        {
            try (Statement stmt = conn.createStatement())
            {
                stmt.execute(String.format("CREATE TABLE %1$s"
                                           + " ( link_key varchar(44) PRIMARY KEY ,"
                                           + " remote_container_id %2$s, "
                                           + " link_name %2$s,"
                                           + " link_role INTEGER,"
                                           + " source %2$s,"
                                           + " target %2$s )", getLinksTableName(), _sqlBlobType));
            }
        }
    }

    private String getLinksTableName()
    {
        return _tableNamePrefix + LINKS_TABLE_NAME_SUFFIX;
    }

    private String getVersionTableName()
    {
        return _tableNamePrefix + VERSION_TABLE_NAME_SUFFIX;
    }

    private Collection<LinkDefinition<Source, Target>> performUpdate(final LinkStoreUpdater updater,
                                                                     Collection<LinkDefinition<Source, Target>> linkDefinitions,
                                                                     final ModelVersion storedVersion,
                                                                     final ModelVersion currentVersion)
            throws SQLException
    {
        linkDefinitions = updater.update(storedVersion.toString(), linkDefinitions);
        Connection connection = getConnection();
        try
        {
            connection.setAutoCommit(false);

            try (Statement statement = connection.createStatement())
            {
                statement.execute("DELETE FROM " + getLinksTableName());
            }

            for (LinkDefinition<? extends BaseSource, ? extends BaseTarget> linkDefinition : linkDefinitions)
            {
                insert(connection, generateLinkKey(linkDefinition), linkDefinition);
            }
            updateVersion(connection, currentVersion);
            connection.commit();
        }
        catch (SQLException e)
        {
            try
            {
                connection.rollback();
            }
            catch (SQLException re)
            {
                LOGGER.debug("Cannot rollback transaction", re);
            }
            throw e;
        }
        finally
        {
            JdbcUtils.closeConnection(connection, LOGGER);
        }
        return linkDefinitions;
    }

    private Collection<LinkDefinition<Source, Target>> getLinks() throws SQLException
    {
        Collection<LinkDefinition<Source, Target>> links = new ArrayList<>();
        try (Connection connection = getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(String.format(
                     "SELECT remote_container_id, link_name, link_role, source, target FROM %s",
                     getLinksTableName())))
        {
            while (resultSet.next())
            {
                String remoteContainerId = getBlobValueAsString(resultSet, 1);
                String linkName = getBlobValueAsString(resultSet, 2);
                Role role = Role.valueOf(resultSet.getBoolean(3));
                Source source = (Source) getBlobAsAmqpObject(resultSet, 4);
                Target target = (Target) getBlobAsAmqpObject(resultSet, 5);

                links.add(new LinkDefinitionImpl<>(remoteContainerId, linkName, role, source, target));
            }
        }
        catch (IllegalArgumentException e)
        {
            throw new StoreException("Cannot load links from store", e);
        }
        return links;
    }

    private Object getBlobAsAmqpObject(final ResultSet resultSet, final int index) throws SQLException
    {
        byte[] sourceBytes;
        if (_isUseBytesMethodsForBlob)
        {
            sourceBytes = resultSet.getBytes(index);
        }
        else
        {
            Blob blob = resultSet.getBlob(index);
            try (InputStream is = blob.getBinaryStream())
            {
                sourceBytes = ByteStreams.toByteArray(is);
            }
            catch (IOException e)
            {
                throw new StoreException("Cannot convert blob to string", e);
            }
            finally
            {
                blob.free();
            }
        }
        return LinkStoreUtils.amqpBytesToObject(sourceBytes);
    }

    private String getBlobValueAsString(final ResultSet resultSet, final int index) throws SQLException
    {
        if (_isUseBytesMethodsForBlob)
        {
            return new String(resultSet.getBytes(index), UTF_8);
        }

        Blob blob = resultSet.getBlob(index);
        try (InputStream is = blob.getBinaryStream();
             InputStreamReader isr = new InputStreamReader(is, UTF_8))
        {
            return CharStreams.toString(isr);
        }
        catch (IOException e)
        {
            throw new StoreException("Cannot convert blob to string", e);
        }
        finally
        {
            blob.free();
        }
    }

    private ModelVersion getStoredVersion() throws SQLException
    {
        ModelVersion version = null;
        try (Connection connection = getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(String.format("SELECT version FROM %s",
                                                                        getVersionTableName())))
        {
            while (resultSet.next())
            {
                ModelVersion storedVersion = ModelVersion.fromString(resultSet.getString(1));
                if (version == null || version.lessThan(storedVersion))
                {
                    version = storedVersion;
                }
            }
        }
        if (version == null)
        {
            throw new StoreException("Version of links is not found");
        }
        return version;
    }

    private void updateVersion(final Connection connection, final ModelVersion currentVersion) throws SQLException
    {
        String version = currentVersion.toString();
        try (PreparedStatement statement = connection.prepareStatement(String.format(
                "INSERT INTO %s (version, version_time) VALUES (?,?)",
                getVersionTableName())))
        {
            statement.setString(1, version);
            statement.setDate(2, new java.sql.Date(System.currentTimeMillis()));
            if (statement.executeUpdate() != 1)
            {
                throw new StoreException(String.format("Cannot insert version '%s' into version table", version));
            }
        }
    }


    private void insert(final Connection connection,
                        final String linkKey,
                        final LinkDefinition<? extends BaseSource, ? extends BaseTarget> linkDefinition)
            throws SQLException
    {
        try (PreparedStatement statement = connection.prepareStatement(String.format(
                "INSERT INTO %s (link_key, remote_container_id, link_name, link_role, source, target) VALUES (?,?,?,?,?,?)",
                getLinksTableName())))
        {
            statement.setString(1, linkKey);
            saveStringAsBlob(statement, 2, linkDefinition.getRemoteContainerId());
            saveStringAsBlob(statement, 3, linkDefinition.getName());
            statement.setInt(4, linkDefinition.getRole().getValue() ? 1 : 0);
            saveObjectAsBlob(statement, 5, linkDefinition.getSource());
            saveObjectAsBlob(statement, 6, linkDefinition.getTarget());
            if (statement.executeUpdate() != 1)
            {
                throw new StoreException(String.format("Cannot save link %s", new LinkKey(linkDefinition)));
            }
        }
    }

    private void update(final Connection connection,
                        final String linkKey,
                        final LinkDefinition<? extends BaseSource, ? extends BaseTarget> linkDefinition)
            throws SQLException
    {
        try (PreparedStatement statement = connection.prepareStatement(String.format(
                "UPDATE %s SET source = ?, target = ? WHERE link_key = ?",
                getLinksTableName())))
        {
            saveObjectAsBlob(statement, 1, linkDefinition.getSource());
            saveObjectAsBlob(statement, 2, linkDefinition.getTarget());
            statement.setString(3, linkKey);
            if (statement.executeUpdate() != 1)
            {
                throw new StoreException(String.format("Cannot save link %s", new LinkKey(linkDefinition)));
            }
        }
    }

    private void saveObjectAsBlob(final PreparedStatement statement, final int index, final Object object)
            throws SQLException
    {
        saveBytesAsBlob(statement, index, LinkStoreUtils.objectToAmqpBytes(object));
    }

    private void saveBytesAsBlob(final PreparedStatement statement, final int index, final byte[] bytes)
            throws SQLException
    {
        if (_isUseBytesMethodsForBlob)
        {
            statement.setBytes(index, bytes);
        }
        else
        {
            try (InputStream inputStream = new ByteArrayInputStream(bytes))
            {
                statement.setBlob(index, inputStream);
            }
            catch (IOException e)
            {
                throw new StoreException("Cannot save link", e);
            }
        }
    }

    private void saveStringAsBlob(final PreparedStatement statement, final int index, final String value)
            throws SQLException
    {
        saveBytesAsBlob(statement, index, value.getBytes(UTF_8));
    }

    private String generateLinkKey(final LinkDefinition<?, ?> linkDefinition)
    {
        MessageDigest md;
        try
        {
            md = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new StoreException("Cannot generate SHA-256 checksum", e);
        }

        md.update(linkDefinition.getRemoteContainerId().getBytes(UTF_8));
        md.update(linkDefinition.getName().getBytes(UTF_8));
        md.update(linkDefinition.getRole().getValue() ? (byte) 1 : (byte) 0);

        return Base64.getEncoder().encodeToString(md.digest());
    }
}
