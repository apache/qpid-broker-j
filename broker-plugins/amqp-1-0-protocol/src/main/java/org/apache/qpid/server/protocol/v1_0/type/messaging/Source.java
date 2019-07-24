
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


package org.apache.qpid.server.protocol.v1_0.type.messaging;


import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.server.protocol.v1_0.CompositeType;
import org.apache.qpid.server.protocol.v1_0.CompositeTypeField;
import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.DistributionMode;
import org.apache.qpid.server.protocol.v1_0.type.Outcome;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.UnsignedInteger;

@CompositeType( symbolicDescriptor = "amqp:source:list", numericDescriptor = 0x0000000000000028L)
public class Source implements BaseSource, Terminus
{
    @CompositeTypeField(index = 0)
    private String _address;

    @CompositeTypeField(index = 1)
    private TerminusDurability _durable;

    @CompositeTypeField(index = 2)
    private TerminusExpiryPolicy _expiryPolicy;

    @CompositeTypeField(index = 3)
    private UnsignedInteger _timeout;

    @CompositeTypeField(index = 4)
    private Boolean _dynamic;

    @CompositeTypeField(index = 5, deserializationConverter = "org.apache.qpid.server.protocol.v1_0.DeserializationFactories.convertToNodeProperties")
    private Map<Symbol, Object> _dynamicNodeProperties;

    @CompositeTypeField(index = 6, deserializationConverter = "org.apache.qpid.server.protocol.v1_0.DeserializationFactories.convertToDistributionMode")
    private DistributionMode _distributionMode;

    @CompositeTypeField(index = 7)
    private Map<Symbol, Filter> _filter;

    @CompositeTypeField(index = 8)
    private Outcome _defaultOutcome;

    @CompositeTypeField(index = 9)
    private Symbol[] _outcomes;

    @CompositeTypeField(index = 10)
    private Symbol[] _capabilities;

    public Source()
    {
        super();
    }

    public Source(final Source source)
    {
        super();
        _address = source.getAddress();
        _durable = source.getDurable();
        _expiryPolicy = source.getExpiryPolicy();
        _timeout = source.getTimeout();
        _dynamic = source.getDynamic();
        Map<Symbol, Object> dynamicNodeProperties = source.getDynamicNodeProperties();
        _dynamicNodeProperties = dynamicNodeProperties == null ? null : new LinkedHashMap<>(dynamicNodeProperties);
        _distributionMode = source.getDistributionMode();
        Map<Symbol, Filter> filter = source.getFilter();
        _filter = filter == null ? null : new LinkedHashMap<>(filter);
        _defaultOutcome = source.getDefaultOutcome();
        Symbol[] outcomes = source.getOutcomes();
        _outcomes = outcomes == null ? null : Arrays.copyOf(outcomes, outcomes.length);;
        Symbol[] capabilities = source.getCapabilities();
        _capabilities = capabilities == null ? null : Arrays.copyOf(capabilities, capabilities.length);
    }

    public String getAddress()
    {
        return _address;
    }

    public void setAddress(String address)
    {
        _address = address;
    }

    public TerminusDurability getDurable()
    {
        return _durable;
    }

    public void setDurable(TerminusDurability durable)
    {
        _durable = durable;
    }

    public TerminusExpiryPolicy getExpiryPolicy()
    {
        return _expiryPolicy;
    }

    public void setExpiryPolicy(TerminusExpiryPolicy expiryPolicy)
    {
        _expiryPolicy = expiryPolicy;
    }

    public UnsignedInteger getTimeout()
    {
        return _timeout;
    }

    public void setTimeout(UnsignedInteger timeout)
    {
        _timeout = timeout;
    }

    public Boolean getDynamic()
    {
        return _dynamic;
    }

    public void setDynamic(Boolean dynamic)
    {
        _dynamic = dynamic;
    }

    public Map<Symbol, Object> getDynamicNodeProperties()
    {
        return _dynamicNodeProperties;
    }

    public void setDynamicNodeProperties(Map<Symbol, Object> dynamicNodeProperties)
    {
        _dynamicNodeProperties = dynamicNodeProperties;
    }

    public DistributionMode getDistributionMode()
    {
        return _distributionMode;
    }

    public void setDistributionMode(DistributionMode distributionMode)
    {
        _distributionMode = distributionMode;
    }

    public Map<Symbol, Filter> getFilter()
    {
        return _filter;
    }

    public void setFilter(Map<Symbol, Filter> filter)
    {
        _filter = filter;
    }

    public Outcome getDefaultOutcome()
    {
        return _defaultOutcome;
    }

    public void setDefaultOutcome(Outcome defaultOutcome)
    {
        _defaultOutcome = defaultOutcome;
    }

    public Symbol[] getOutcomes()
    {
        return _outcomes;
    }

    public void setOutcomes(Symbol... outcomes)
    {
        _outcomes = outcomes;
    }

    public Symbol[] getCapabilities()
    {
        return _capabilities;
    }

    public void setCapabilities(Symbol[] capabilities)
    {
        _capabilities = capabilities;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Source{");
        final int origLength = builder.length();

        if(_address != null)
        {
            if(builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("address=").append(_address);
        }

        if(_durable != null)
        {
            if(builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("durable=").append(_durable);
        }

        if(_expiryPolicy != null)
        {
            if(builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("expiryPolicy=").append(_expiryPolicy);
        }

        if(_timeout != null)
        {
            if(builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("timeout=").append(_timeout);
        }

        if(_dynamic != null)
        {
            if(builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("dynamic=").append(_dynamic);
        }

        if(_dynamicNodeProperties != null)
        {
            if(builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("dynamicNodeProperties=").append(_dynamicNodeProperties);
        }

        if(_distributionMode != null)
        {
            if(builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("distributionMode=").append(_distributionMode);
        }

        if(_filter != null)
        {
            if(builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("filter=").append(_filter);
        }

        if(_defaultOutcome != null)
        {
            if(builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("defaultOutcome=").append(_defaultOutcome);
        }

        if(_outcomes != null)
        {
            if(builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("outcomes=").append(Arrays.toString(_outcomes));
        }

        if(_capabilities != null)
        {
            if(builder.length() != origLength)
            {
                builder.append(',');
            }
            builder.append("capabilities=").append(Arrays.toString(_capabilities));
        }

        builder.append('}');
        return builder.toString();
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final Source source = (Source) o;

        if (_address != null ? !_address.equals(source._address) : source._address != null)
        {
            return false;
        }
        if (_durable != null ? !_durable.equals(source._durable) : source._durable != null)
        {
            return false;
        }
        if (_expiryPolicy != null ? !_expiryPolicy.equals(source._expiryPolicy) : source._expiryPolicy != null)
        {
            return false;
        }
        if (_timeout != null ? !_timeout.equals(source._timeout) : source._timeout != null)
        {
            return false;
        }
        if (_dynamic != null ? !_dynamic.equals(source._dynamic) : source._dynamic != null)
        {
            return false;
        }
        if (_dynamicNodeProperties != null
                ? !_dynamicNodeProperties.equals(source._dynamicNodeProperties)
                : source._dynamicNodeProperties != null)
        {
            return false;
        }
        if (_distributionMode != null
                ? !_distributionMode.equals(source._distributionMode)
                : source._distributionMode != null)
        {
            return false;
        }
        if (_filter != null ? !_filter.equals(source._filter) : source._filter != null)
        {
            return false;
        }
        if (_defaultOutcome != null)
        {
            if  (source._defaultOutcome == null)
            {
                return false;
            }

            if (_defaultOutcome.getSymbol() != null)
            {
                if (source._defaultOutcome.getSymbol() == null)
                {
                    return false;
                }

                if (!_defaultOutcome.getSymbol().equals(source._defaultOutcome.getSymbol()))
                {
                    return false;
                }
            }
            else if (source._defaultOutcome.getSymbol() != null)
            {
                return false;
            }
        }
        else if (source._defaultOutcome != null)
        {
            return false;
        }

        return Arrays.equals(_outcomes, source._outcomes) && Arrays.equals(_capabilities, source._capabilities);
    }

    @Override
    public int hashCode()
    {
        int result = _address != null ? _address.hashCode() : 0;
        result = 31 * result + (_durable != null ? _durable.hashCode() : 0);
        result = 31 * result + (_expiryPolicy != null ? _expiryPolicy.hashCode() : 0);
        result = 31 * result + (_timeout != null ? _timeout.hashCode() : 0);
        result = 31 * result + (_dynamic != null ? _dynamic.hashCode() : 0);
        result = 31 * result + (_dynamicNodeProperties != null ? _dynamicNodeProperties.hashCode() : 0);
        result = 31 * result + (_distributionMode != null ? _distributionMode.hashCode() : 0);
        result = 31 * result + (_filter != null ? _filter.hashCode() : 0);
        result = 31 * result + (_defaultOutcome != null ? _defaultOutcome.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(_outcomes);
        result = 31 * result + Arrays.hashCode(_capabilities);
        return result;
    }
}
