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
package org.apache.qpid.server.store.jdbc;


import static org.apache.qpid.server.store.jdbc.JdbcUtils.createConnectionProvider;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.StoreException;

/**
 * Implementation of a MessageStore backed by a Generic JDBC Database.
 */
public class GenericJDBCMessageStore extends GenericAbstractJDBCMessageStore
{

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericJDBCMessageStore.class);

    private String _connectionURL;
    private ConnectionProvider _connectionProvider;

    private String _blobType;
    private String _blobStorage;
    private String _varBinaryType;
    private String _bigIntType;
    private boolean _useBytesMethodsForBlob;

    @Override
    protected void doOpen(final ConfiguredObject<?> parent) throws StoreException
    {
        JDBCSettings settings = (JDBCSettings)parent;
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
                             +  " Using settings : " + details);
        }

        _blobType = details.getBlobType();
        _blobStorage = details.getBlobStorage();
        _varBinaryType = details.getVarBinaryType();
        _useBytesMethodsForBlob = details.isUseBytesMethodsForBlob();
        _bigIntType = details.getBigintType();
        _connectionProvider = createConnectionProvider(parent, LOGGER);
    }

    @Override
    protected String getTablePrefix(final ConfiguredObject<?> parent)
    {
        JDBCSettings settings = (JDBCSettings)parent;
        return settings.getTableNamePrefix();
    }

    @Override
    public Connection getConnection() throws SQLException
    {
        return _connectionProvider.getConnection();
    }

    @Override
    protected void doClose()
    {
        try
        {
            _connectionProvider.close();
        }
        catch (SQLException e)
        {
            throw new StoreException("Unable to close connection provider ", e);
        }
    }


    @Override
    protected Logger getLogger()
    {
        return LOGGER;
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
    public String getSqlBigIntType()
    {
        return _bigIntType;
    }

    @Override
    public String getStoreLocation()
    {
        return _connectionURL;
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
            throw new IllegalStateException("Cannot delete the store as the store is still open");
        }

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
}
