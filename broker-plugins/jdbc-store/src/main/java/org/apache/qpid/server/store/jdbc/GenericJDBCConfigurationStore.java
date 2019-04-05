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


import static org.apache.qpid.server.store.jdbc.AbstractJDBCConfigurationStore.State.CLOSED;
import static org.apache.qpid.server.store.jdbc.AbstractJDBCConfigurationStore.State.CONFIGURED;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreProvider;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.preferences.PreferenceStore;

/**
 * Implementation of a DurableConfigurationStore backed by Generic JDBC Database
 * that also provides a MessageStore.
 */
public class GenericJDBCConfigurationStore extends AbstractJDBCConfigurationStore implements MessageStoreProvider
{
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericJDBCConfigurationStore.class);

    private final MessageStore _providedMessageStore = new ProvidedJDBCMessageStore();
    private final ProvidedPreferenceStore _providedPreferenceStore = new ProvidedPreferenceStore();
    private String _connectionURL;
    private ConnectionProvider _connectionProvider;

    private String _blobType;
    private String _blobStorage;
    private String _varBinaryType;
    private String _bigIntType;
    private boolean _useBytesMethodsForBlob;

    private ConfiguredObject<?> _parent;
    private final Class<? extends ConfiguredObject> _rootClass;

    public GenericJDBCConfigurationStore(final Class<? extends ConfiguredObject> rootClass)
    {
        _rootClass = rootClass;
    }

    @Override
    public void init(ConfiguredObject<?> parent)
            throws StoreException
    {
        changeState(CLOSED, CONFIGURED);
        _parent = parent;

        JDBCSettings settings = (JDBCSettings) parent;
        super.setTableNamePrefix(settings.getTableNamePrefix());
        _providedPreferenceStore.setTableNamePrefix(settings.getTableNamePrefix());
        _connectionURL = settings.getConnectionUrl();

        JDBCDetails details = JDBCDetails.getDetailsForJdbcUrl(_connectionURL, parent);

        if (!details.isKnownVendor() && getLogger().isInfoEnabled())
        {
            getLogger().info("Do not recognize vendor from connection URL: " + _connectionURL
                             + " Using fallback settings " + details);
        }
        if (details.isOverridden() && getLogger().isInfoEnabled())
        {
            getLogger().info("One or more JDBC details were overridden from context. "
                             + " Using settings : " + details);
        }

        _connectionProvider = JdbcUtils.createConnectionProvider(parent, LOGGER);
        _blobType = details.getBlobType();
        _blobStorage = details.getBlobStorage();
        _varBinaryType = details.getVarBinaryType();
        _useBytesMethodsForBlob = details.isUseBytesMethodsForBlob();
        _bigIntType = details.getBigintType();

        createOrOpenConfigurationStoreDatabase();

    }

    @Override
    public void upgradeStoreStructure() throws StoreException
    {
        upgradeIfNecessary(_parent);
    }

    @Override
    public Connection getConnection() throws SQLException
    {
        return _connectionProvider.getConnection();
    }

    @Override
    public void closeConfigurationStore() throws StoreException
    {
        try
        {
            _connectionProvider.close();
        }
        catch (SQLException e)
        {
            throw new StoreException("Unable to close connection provider ", e);
        }
        setState(State.CLOSED);
    }

    @Override
    public void onDelete(final ConfiguredObject<?> parent)
    {
        assertState(CLOSED);
        ConnectionProvider connectionProvider = JdbcUtils.createConnectionProvider(parent, LOGGER);
        try
        {
            try (Connection conn = connectionProvider.getConnection())
            {
                conn.setAutoCommit(true);
                onDelete(conn);
            }
            catch (SQLException e)
            {
                getLogger().error("Exception while deleting store tables", e);
            }
        }
        finally
        {
            try
            {
                connectionProvider.close();
            }
            catch (SQLException e)
            {
                LOGGER.warn("Unable to close connection provider ", e);
            }
        }
    }

    @Override
    protected String getSqlBlobType()
    {
        return _blobType;
    }

    @Override
    protected String getSqlBlobStorage(String columnName)
    {
        return String.format(_blobStorage, columnName);
    }

    @Override
    protected String getSqlVarBinaryType(int size)
    {
        return String.format(_varBinaryType, size);
    }

    @Override
    public String getSqlBigIntType()
    {
        return _bigIntType;
    }

    @Override
    protected String getBlobAsString(ResultSet rs, int col) throws SQLException
    {
        byte[] bytes;
        if(_useBytesMethodsForBlob)
        {
            bytes = rs.getBytes(col);
            return new String(bytes,UTF8_CHARSET);
        }
        else
        {
            Blob blob = rs.getBlob(col);
            if(blob == null)
            {
                return null;
            }
            bytes = blob.getBytes(1, (int)blob.length());
        }
        return new String(bytes, UTF8_CHARSET);

    }

    protected InputStream getBlobAsInputStream(ResultSet rs, int col) throws SQLException
    {
        if(_useBytesMethodsForBlob)
        {
            return new ByteArrayInputStream(rs.getBytes(col));
        }
        else
        {
            Blob dataAsBlob = rs.getBlob(col);
            return dataAsBlob.getBinaryStream();
        }
    }

    @Override
    protected Logger getLogger()
    {
        return LOGGER;
    }

    @Override
    public MessageStore getMessageStore()
    {
        return _providedMessageStore;
    }

    public PreferenceStore getPreferenceStore()
    {
        return _providedPreferenceStore;
    }

    private class ProvidedJDBCMessageStore extends GenericAbstractJDBCMessageStore
    {
        @Override
        protected String getTablePrefix(final ConfiguredObject<?> parent)
        {
            return ((JDBCSettings)_parent).getTableNamePrefix();
        }

        @Override
        protected void doOpen(final ConfiguredObject<?> parent)
        {
            // Nothing to do, store provided by DerbyConfigurationStore
        }

        @Override
        public Connection getConnection() throws SQLException
        {
            return GenericJDBCConfigurationStore.this.getConnection();
        }

        @Override
        protected void doClose()
        {
            // Nothing to do, store provided by DerbyConfigurationStore
        }

        @Override
        public String getStoreLocation()
        {
            return GenericJDBCConfigurationStore.this._connectionURL;
        }

        @Override
        public File getStoreLocationAsFile()
        {
            return null;
        }

        @Override
        public void onDelete(final ConfiguredObject<?> parent)
        {
            if (isMessageStoreOpen())
            {
                throw new IllegalStateException("Cannot delete the store as the provided store is still open");
            }

            try(Connection connection = GenericJDBCConfigurationStore.this.getConnection())
            {
                onDelete(connection);
            }
            catch (SQLException e)
            {
                throw new StoreException("Cannot get connection to perform deletion", e);
            }
        }

        @Override
        protected Logger getLogger()
        {
            return GenericJDBCConfigurationStore.this.getLogger();
        }

        @Override
        protected String getSqlBlobType()
        {
            return GenericJDBCConfigurationStore.this.getSqlBlobType();
        }

        @Override
        protected String getSqlBlobStorage(String columnName)
        {
            return GenericJDBCConfigurationStore.this.getSqlBlobStorage(columnName);
        }

        @Override
        protected String getSqlVarBinaryType(int size)
        {
            return GenericJDBCConfigurationStore.this.getSqlVarBinaryType(size);
        }

        @Override
        protected String getSqlBigIntType()
        {
            return GenericJDBCConfigurationStore.this.getSqlBigIntType();
        }

        @Override
        protected InputStream getBlobAsInputStream(final ResultSet rs, final int col) throws SQLException
        {
            return GenericJDBCConfigurationStore.this.getBlobAsInputStream(rs, col);
        }
    }

    private class ProvidedPreferenceStore extends AbstractJDBCPreferenceStore
    {
        private final Logger LOGGER = LoggerFactory.getLogger(ProvidedPreferenceStore.class);

        @Override
        protected Logger getLogger()
        {
            return LOGGER;
        }

        @Override
        protected Connection getConnection() throws SQLException
        {
            return GenericJDBCConfigurationStore.this.getConnection();
        }

        @Override
        protected String getSqlBlobType()
        {
            return GenericJDBCConfigurationStore.this.getSqlBlobType();
        }

        @Override
        protected String getBlobAsString(final ResultSet rs, final int col) throws SQLException
        {
            return GenericJDBCConfigurationStore.this.getBlobAsString(rs, col);
        }

        @Override
        protected void doDelete()
        {
            try
            {
                dropTables(GenericJDBCConfigurationStore.this.getConnection());
            }
            catch (SQLException e)
            {
                getLogger().warn("Could not drop preference database tables on deletion", e);
            }
        }

        @Override
        public void doClose()
        {
            // noop
        }
    }


}
