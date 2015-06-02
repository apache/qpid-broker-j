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
package org.apache.qpid.url;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.framing.AMQShortString;

public class AMQBindingURL implements BindingURL
{
    private static final Logger _logger = LoggerFactory.getLogger(AMQBindingURL.class);

    private String _url;
    private String _exchangeClass = ExchangeDefaults.DIRECT_EXCHANGE_CLASS;
    private String _exchangeName = "";
    private String _destinationName = "";
    private String _queueName = "";
    private String[] _bindingKeys = new String[0];
    private HashMap<String, String> _options;

    public AMQBindingURL(String url) throws URISyntaxException
    {
        // format:
        // <exch_class>://<exch_name>/[<destination>]/[<queue>]?<option>='<value>'[,<option>='<value>']*
        _logger.debug("Parsing URL: " + url);
        _url = url;
        _options = new HashMap<String, String>();

        parseBindingURL();
    }

    private void parseBindingURL() throws URISyntaxException
    {
        BindingURLParser parser = new BindingURLParser();
        parser.parse(_url,this);
        _logger.debug("URL Parsed: " + this);
    }


    public String getURL()
    {
        return _url;
    }

    public String getExchangeClass()
    {
        return _exchangeClass;
    }

    public void setExchangeClass(String exchangeClass)
    {

        _exchangeClass = exchangeClass;
        if (exchangeClass.equals(ExchangeDefaults.TOPIC_EXCHANGE_CLASS))
        {
            setOption(BindingURL.OPTION_EXCLUSIVE, "true");
        }

    }

    public String getExchangeName()
    {
        return _exchangeName;
    }

    public void setExchangeName(String name)
    {
        _exchangeName = name;
    }

    public String getDestinationName()
    {
        return _destinationName;
    }

    public void setDestinationName(String name)
    {
        _destinationName = name;
    }

    public String getQueueName()
    {
        return _queueName;
    }

    public void setQueueName(String name)
    {
        _queueName = name;
    }

    public String getOption(String key)
    {
        return _options.get(key);
    }

    @Override
    public Map<String,Object> getConsumerOptions()
    {
        Map<String,Object> options = new HashMap<>();
        for(Map.Entry<String,String> option : _options.entrySet())
        {
            if(!NON_CONSUMER_OPTIONS.contains(option.getKey()))
            {
                options.put(option.getKey(), option.getValue());
            }
        }
        return options;
    }

    public void setOption(String key, String value)
    {
        _options.put(key, value);
    }

    public boolean containsOption(String key)
    {
        return _options.containsKey(key);
    }

    public String getRoutingKey()
    {
        if (_exchangeClass.equals(ExchangeDefaults.DIRECT_EXCHANGE_CLASS))
        {
            if (containsOption(BindingURL.OPTION_ROUTING_KEY))
            {
                return getOption(OPTION_ROUTING_KEY);
            }
            else
            {
                return getQueueName();
            }
        }

        if (containsOption(BindingURL.OPTION_ROUTING_KEY))
        {
            return getOption(OPTION_ROUTING_KEY);
        }

        return getDestinationName();
    }

    public String[] getBindingKeys()
    {
        if (_bindingKeys != null && _bindingKeys.length>0)
        {
            return _bindingKeys;
        }
        else
        {
            return new String[]{getRoutingKey()};
        }
    }

    public void setBindingKeys(String[] keys)
    {
        _bindingKeys = keys;
    }

    public void setRoutingKey(String key)
    {
        setOption(OPTION_ROUTING_KEY, key);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append(_exchangeClass);
        sb.append("://");
        sb.append(_exchangeName);
        sb.append('/');
        sb.append(_destinationName);
        sb.append('/');
        sb.append(_queueName);

        sb.append(URLHelper.printOptions(_options));

        // temp hack
        if (getRoutingKey() == null || getRoutingKey().equals(""))
        {

            if (!sb.toString().contains("?"))
            {
                sb.append("?");
            }
            else
            {
                sb.append("&");
            }

            for (String key :_bindingKeys)
            {
                sb.append(BindingURL.OPTION_BINDING_KEY).append("='").append(key).append("'&");
            }

            return sb.toString().substring(0,sb.toString().length()-1);
        }
        else
        {
            return sb.toString();
        }
    }
}
