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
package org.apache.qpid.client;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.transport.ConnectionSettings;
import org.apache.qpid.url.URLHelper;
import org.apache.qpid.url.URLSyntaxException;

public class BrokerDetails implements Serializable
{
    /*
     * Known URL Options
     * @see ConnectionURL
    */
    public static final String OPTIONS_RETRY = "retries";
    public static final String OPTIONS_CONNECT_TIMEOUT = "connecttimeout";
    public static final String OPTIONS_CONNECT_DELAY = "connectdelay";
    public static final String OPTIONS_HEARTBEAT = "heartbeat";
    private static final String OPTIONS_IDLE_TIMEOUT = "idle_timeout";
    public static final String OPTIONS_SASL_MECHS = "sasl_mechs";
    public static final String OPTIONS_SASL_ENCRYPTION = "sasl_encryption";
    public static final String OPTIONS_SSL = "ssl";
    public static final String OPTIONS_TCP_NO_DELAY = "tcp_nodelay";
    public static final String OPTIONS_SASL_PROTOCOL_NAME = "sasl_protocol";
    public static final String OPTIONS_SASL_SERVER_NAME = "sasl_server";
    public static final String OPTIONS_TRUST_STORE = "trust_store";
    public static final String OPTIONS_TRUST_STORE_PASSWORD = "trust_store_password";
    public static final String OPTIONS_KEY_STORE = "key_store";
    public static final String OPTIONS_KEY_STORE_PASSWORD = "key_store_password";
    public static final String OPTIONS_SSL_VERIFY_HOSTNAME = "ssl_verify_hostname";
    public static final String OPTIONS_SSL_CERT_ALIAS = "ssl_cert_alias";
    public static final String OPTIONS_CLIENT_CERT_PRIV_KEY_PATH = "client_cert_priv_key_path";
    public static final String OPTIONS_CLIENT_CERT_PATH = "client_cert_path";
    public static final String OPTIONS_CLIENT_CERT_INTERMEDIARY_CERT_PATH = "client_cert_intermediary_cert_path" ;
    public static final String OPTIONS_TRUSTED_CERTIFICATES_PATH = "trusted_certs_path";

    public static final String OPTIONS_ENCRYPTION_TRUST_STORE = "encryption_trust_store";
    public static final String OPTIONS_ENCRYPTION_TRUST_STORE_PASSWORD = "encryption_trust_store_password";
    public static final String OPTIONS_ENCRYPTION_REMOTE_TRUST_STORE = "encryption_remote_trust_store";
    public static final String OPTIONS_ENCRYPTION_KEY_STORE = "encryption_key_store";
    public static final String OPTIONS_ENCRYPTION_KEY_STORE_PASSWORD = "encryption_key_store_password";


    public static final int DEFAULT_PORT = 5672;
    public static final String TCP = "tcp";
    public static final String SOCKET = "socket";
    public static final String DEFAULT_TRANSPORT = BrokerDetails.TCP;
    public static final String URL_FORMAT_EXAMPLE =
            "<transport>://<hostname>[:<port Default=\"" + BrokerDetails.DEFAULT_PORT + "\">][?<option>='<value>'[,<option>='<value>']]";
    public static final int DEFAULT_CONNECT_TIMEOUT = 30000;
    public static final boolean USE_SSL_DEFAULT = false;
    // pulled these properties from the new BrokerDetails class in the qpid package
    public static final String PROTOCOL_TCP = "tcp";
    public static final String PROTOCOL_TLS = "tls";
    public static final String VIRTUAL_HOST = "virtualhost";
    public static final String CLIENT_ID = "client_id";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    private static final long serialVersionUID = 8762219750300869355L;
    private String _host;
    private int _port;
    private String _transport;

    private Map<String, String> _options = new HashMap<String, String>();
    private AMQConnectionURL _connectionUrl;

    public BrokerDetails(BrokerDetails details)
    {
        _host = details.getHost();
        _port = details.getPort();
        _transport = details.getTransport();
        _options = new HashMap<>(details._options);
        _connectionUrl = details._connectionUrl;
    }

    public BrokerDetails(){}
    
    public BrokerDetails(String url) throws URLSyntaxException
    {        
      
        // URL should be of format tcp://host:port?option='value',option='value'
        try
        {
            URI connection = new URI(url);

            String transport = connection.getScheme();

            // Handles some defaults to minimise changes to existing broker URLS e.g. localhost
            if (transport != null)
            {
                //todo this list of valid transports should be enumerated somewhere
                if (!(transport.equalsIgnoreCase(BrokerDetails.TCP) || transport.equalsIgnoreCase(BrokerDetails.SOCKET)))
                {
                    if (transport.equalsIgnoreCase("localhost"))
                    {
                        connection = new URI(DEFAULT_TRANSPORT + "://" + url);
                        transport = connection.getScheme();
                    }
                    else
                    {
                        if (url.charAt(transport.length()) == ':' && url.charAt(transport.length() + 1) != '/')
                        {
                            //Then most likely we have a host:port value
                            connection = new URI(DEFAULT_TRANSPORT + "://" + url);
                            transport = connection.getScheme();
                        }
                        else
                        {
                            throw URLHelper.parseError(0, transport.length(), "Unknown transport", url);
                        }
                    }
                }
                else if (url.indexOf("//") == -1)
                {
                    throw new URLSyntaxException(url, "Missing '//' after the transport In broker URL",transport.length()+1,1);
                }
            }
            else
            {
                //Default the transport
                connection = new URI(DEFAULT_TRANSPORT + "://" + url);
                transport = connection.getScheme();
            }

            if (transport == null)
            {
                throw URLHelper.parseError(-1, "Unknown transport in broker URL:'"
                        + url + "' Format: " + URL_FORMAT_EXAMPLE, "");
            }

            setTransport(transport);

            String host = connection.getHost();

            // Fix for Java 1.5
            if (host == null)
            {
                host = "";
                
                String auth = connection.getAuthority();
                if (auth != null)
                {
                    // contains both host & port myhost:5672                
                    if (auth.contains(":"))
                    {
                        host = auth.substring(0,auth.indexOf(":"));
                    }
                    else
                    {
                        host = auth;
                    }
                }

            }

            setHost(host);

            int port = connection.getPort();

            if (port == -1)
            {
                // Fix for when there is port data but it is not automatically parseable by getPort().
                String auth = connection.getAuthority();

                if (auth != null && auth.contains(":"))
                {
                    int start = auth.indexOf(":") + 1;
                    int end = start;
                    boolean looking = true;
                    boolean found = false;
                    // Throw an URL exception if the port number is not specified
                    if (start == auth.length())
                    {
                        throw URLHelper.parseError(connection.toString().indexOf(auth) + end - 1,
                                connection.toString().indexOf(auth) + end, "Port number must be specified",
                                connection.toString());
                    }
                    //Walk the authority looking for a port value.
                    while (looking)
                    {
                        try
                        {
                            end++;
                            Integer.parseInt(auth.substring(start, end));

                            if (end >= auth.length())
                            {
                                looking = false;
                                found = true;
                            }
                        }
                        catch (NumberFormatException nfe)
                        {
                            looking = false;
                        }

                    }
                    if (found)
                    {
                        setPort(Integer.parseInt(auth.substring(start, end)));
                    }
                    else
                    {
                        throw URLHelper.parseError(connection.toString().indexOf(connection.getAuthority()) + end - 1,
                                             "Illegal character in port number", connection.toString());
                    }

                }
                else
                {
                    setPort(DEFAULT_PORT);
                }
            }
            else
            {
                setPort(port);
            }

            String queryString = connection.getQuery();

            URLHelper.parseOptions(_options, queryString);

            //Fragment is #string (not used)
        }
        catch (URISyntaxException uris)
        {
            if (uris instanceof URLSyntaxException)
            {
                throw(URLSyntaxException) uris;
            }

            throw URLHelper.parseError(uris.getIndex(), uris.getReason(), uris.getInput());
        }
    }

    public BrokerDetails(String host, int port)
    {
        _host = host;
        _port = port;
    }

    public String getHost()
    {
        return _host;
    }

    public void setHost(String _host)
    {
        this._host = _host;
    }

    public int getPort()
    {
        return _port;
    }

    public void setPort(int _port)
    {
        this._port = _port;
    }

    public String getTransport()
    {
        return _transport;
    }

    public void setTransport(String _transport)
    {
        this._transport = _transport;
    }


    public String getProperty(String key)
    {
        String value = _options.get(key);
        if(value == null && _connectionUrl != null)
        {
            value = _connectionUrl.getOption(key);
        }
        return value;
    }

    public void setProperty(String key, String value)
    {
        _options.put(key, value);
    }

    private int lookupConnectTimeout()
    {
        if (_options.containsKey(OPTIONS_CONNECT_TIMEOUT))
        {
            try
            {
                return Integer.parseInt(_options.get(OPTIONS_CONNECT_TIMEOUT));
            }
            catch (NumberFormatException nfe)
            {
                //Do nothing as we will use the default below.
            }
        }

        return BrokerDetails.DEFAULT_CONNECT_TIMEOUT;
    }
    
    public boolean getBooleanProperty(String propName)
    {
        return getBooleanProperty(propName, false);
    }
    
    public boolean getBooleanProperty(String propName, boolean defaultValue)
    {
    	if (_options.containsKey(propName))
    	{
            if (_options.get(propName).equalsIgnoreCase("false"))
            {
                return false;
            }
            else if (_options.get(propName).equalsIgnoreCase("true"))
            {
                return true;
            }
            else
            {
               return defaultValue;
            }
    	}
    	else
    	{
    		return defaultValue;
    	}
    }    

    private int getIntegerProperty(String key)
    {
        String stringValue = getProperty(key);
        try
        {
            return Integer.parseInt(stringValue);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("Cannot parse key " + key + " with value '" + stringValue + "' as integer.", e);
        }
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer();

        sb.append(_transport);
        sb.append("://");
        sb.append(_host);
        sb.append(':');
        sb.append(_port);

        sb.append(printOptionsURL());

        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        BrokerDetails bd = (BrokerDetails) o;

        return _host.toLowerCase().equals(bd.getHost() == null ? null : bd.getHost().toLowerCase()) &&
               (_port == bd.getPort()) &&
               _transport.toLowerCase().equals(bd.getTransport() == null ? null : bd.getTransport().toLowerCase());
        //TODO do we need to compare all the options as well?
    }

    @Override
    public int hashCode()
    {
        int result = _host != null ? _host.toLowerCase().hashCode() : 0;
        result = 31 * result + _port;
        result = 31 * result + (_transport != null ? _transport.toLowerCase().hashCode() : 0);
        return result;
    }

    private String printOptionsURL()
    {
        StringBuffer optionsURL = new StringBuffer();

        optionsURL.append('?');

        if (!(_options.isEmpty()))
        {

            for (String key : _options.keySet())
            {
                optionsURL.append(key);

                optionsURL.append("='");

                if (OPTIONS_TRUST_STORE_PASSWORD.equals(key) || OPTIONS_KEY_STORE_PASSWORD.equals(key))
                {
                    optionsURL.append("********");
                }
                else
                {
                    optionsURL.append(_options.get(key));
                }

                optionsURL.append("'");

                optionsURL.append(URLHelper.DEFAULT_OPTION_SEPERATOR);
            }
        }

        //removeKey the extra DEFAULT_OPTION_SEPERATOR or the '?' if there are no options
        optionsURL.deleteCharAt(optionsURL.length() - 1);

        return optionsURL.toString();
    }

    public static String checkTransport(String broker)
    {
        if ((!broker.contains("://")))
        {
            return "tcp://" + broker;
        }
        else
        {
            return broker;
        }
    }

    public ConnectionSettings buildConnectionSettings()
    {
        ConnectionSettings conSettings = new ConnectionSettings();

        conSettings.setHost(getHost());
        conSettings.setPort(getPort());
        conSettings.setTransport(getTransport());

        // ------------ sasl options ---------------
        if (getProperty(BrokerDetails.OPTIONS_SASL_MECHS) != null)
        {
            conSettings.setSaslMechs(
                    getProperty(BrokerDetails.OPTIONS_SASL_MECHS));
        }

        // Sun SASL Kerberos client uses the
        // protocol + servername as the service key.

        if (getProperty(BrokerDetails.OPTIONS_SASL_PROTOCOL_NAME) != null)
        {
            conSettings.setSaslProtocol(
                    getProperty(BrokerDetails.OPTIONS_SASL_PROTOCOL_NAME));
        }


        if (getProperty(BrokerDetails.OPTIONS_SASL_SERVER_NAME) != null)
        {
            conSettings.setSaslServerName(
                    getProperty(BrokerDetails.OPTIONS_SASL_SERVER_NAME));
        }

        conSettings.setUseSASLEncryption(
                getBooleanProperty(BrokerDetails.OPTIONS_SASL_ENCRYPTION));

        // ------------- ssl options ---------------------
        conSettings.setUseSSL(getBooleanProperty(BrokerDetails.OPTIONS_SSL));

        if (getProperty(BrokerDetails.OPTIONS_TRUST_STORE) != null)
        {
            conSettings.setTrustStorePath(
                    getProperty(BrokerDetails.OPTIONS_TRUST_STORE));
        }

        if (getProperty(BrokerDetails.OPTIONS_TRUST_STORE_PASSWORD) != null)
        {
            conSettings.setTrustStorePassword(
                    getProperty(BrokerDetails.OPTIONS_TRUST_STORE_PASSWORD));
        }

        if (getProperty(BrokerDetails.OPTIONS_KEY_STORE) != null)
        {
            conSettings.setKeyStorePath(
                    getProperty(BrokerDetails.OPTIONS_KEY_STORE));
        }

        if (getProperty(BrokerDetails.OPTIONS_KEY_STORE_PASSWORD) != null)
        {
            conSettings.setKeyStorePassword(
                    getProperty(BrokerDetails.OPTIONS_KEY_STORE_PASSWORD));
        }

        if (getProperty(BrokerDetails.OPTIONS_SSL_CERT_ALIAS) != null)
        {
            conSettings.setCertAlias(
                    getProperty(BrokerDetails.OPTIONS_SSL_CERT_ALIAS));
        }

        if (getProperty(BrokerDetails.OPTIONS_CLIENT_CERT_PRIV_KEY_PATH) != null)
        {
            conSettings.setClientCertificatePrivateKeyPath(
                    getProperty(BrokerDetails.OPTIONS_CLIENT_CERT_PRIV_KEY_PATH));
        }

        if (getProperty(BrokerDetails.OPTIONS_CLIENT_CERT_PATH) != null)
        {
            conSettings.setClientCertificatePath(
                    getProperty(BrokerDetails.OPTIONS_CLIENT_CERT_PATH));
        }

        if (getProperty(BrokerDetails.OPTIONS_CLIENT_CERT_INTERMEDIARY_CERT_PATH) != null)
        {
            conSettings.setClientCertificateIntermediateCertsPath(
                    getProperty(BrokerDetails.OPTIONS_CLIENT_CERT_INTERMEDIARY_CERT_PATH));
        }

        if (getProperty(BrokerDetails.OPTIONS_TRUSTED_CERTIFICATES_PATH) != null)
        {
            conSettings.setTrustedCertificatesFile(
                    getProperty(BrokerDetails.OPTIONS_TRUSTED_CERTIFICATES_PATH));
        }
        // ----------------------------

        boolean defaultSSLVerifyHostName = Boolean.parseBoolean(
                System.getProperty(ClientProperties.CONNECTION_OPTION_SSL_VERIFY_HOST_NAME,
                    String.valueOf(ClientProperties.DEFAULT_CONNECTION_OPTION_SSL_VERIFY_HOST_NAME)));
        conSettings.setVerifyHostname(getBooleanProperty(BrokerDetails.OPTIONS_SSL_VERIFY_HOSTNAME, defaultSSLVerifyHostName ));

        // ----------------------------

        if (getProperty(BrokerDetails.OPTIONS_ENCRYPTION_KEY_STORE) != null)
        {
            conSettings.setEncryptionKeyStorePath(
                    getProperty(BrokerDetails.OPTIONS_ENCRYPTION_KEY_STORE));
        }

        if (getProperty(BrokerDetails.OPTIONS_ENCRYPTION_KEY_STORE_PASSWORD) != null)
        {
            conSettings.setEncryptionKeyStorePassword(
                    getProperty(BrokerDetails.OPTIONS_ENCRYPTION_KEY_STORE_PASSWORD));
        }


        if (getProperty(BrokerDetails.OPTIONS_ENCRYPTION_TRUST_STORE) != null)
        {
            conSettings.setEncryptionTrustStorePath(
                    getProperty(BrokerDetails.OPTIONS_ENCRYPTION_TRUST_STORE));
        }

        if (getProperty(BrokerDetails.OPTIONS_ENCRYPTION_TRUST_STORE_PASSWORD) != null)
        {
            conSettings.setEncryptionKeyStorePassword(
                    getProperty(BrokerDetails.OPTIONS_ENCRYPTION_TRUST_STORE_PASSWORD));
        }


        if (getProperty(BrokerDetails.OPTIONS_ENCRYPTION_REMOTE_TRUST_STORE) != null)
        {
            conSettings.setEncryptionRemoteTrustStoreName(
                    getProperty(BrokerDetails.OPTIONS_ENCRYPTION_REMOTE_TRUST_STORE));
        }

        // ----------------------------

        if (getProperty(BrokerDetails.OPTIONS_TCP_NO_DELAY) != null)
        {
            conSettings.setTcpNodelay(
                    getBooleanProperty(BrokerDetails.OPTIONS_TCP_NO_DELAY,true));
        }

        conSettings.setConnectTimeout(lookupConnectTimeout());

        if (getProperty(BrokerDetails.OPTIONS_HEARTBEAT) != null)
        {
            conSettings.setHeartbeatInterval(getIntegerProperty(BrokerDetails.OPTIONS_HEARTBEAT));
        }
        else if (getProperty(BrokerDetails.OPTIONS_IDLE_TIMEOUT) != null)
        {
            conSettings.setHeartbeatInterval(getIntegerProperty(BrokerDetails.OPTIONS_IDLE_TIMEOUT) / 1000);
        }

        return conSettings;
    }

    public void setConnectionUrl(final AMQConnectionURL connectionUrl)
    {
        _connectionUrl = connectionUrl;
    }
}
