package org.apache.qpid.server.user.connection.limits.plugins;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import org.apache.qpid.server.user.connection.limits.config.Rule;

class ConnectionLimitRuleImpl implements ConnectionLimitRule
{
    private final Rule _rule;

    ConnectionLimitRuleImpl(Rule rule)
    {
        _rule = Objects.requireNonNull(rule);
    }

    @Override
    public String getPort()
    {
        return _rule.getPort();
    }

    @Override
    public String getIdentity()
    {
        return _rule.getIdentity();
    }

    @Override
    public Boolean getBlocked()
    {
        return _rule.isUserBlocked();
    }

    @Override
    public Integer getCountLimit()
    {
        if (!_rule.isUserBlocked())
        {
            return _rule.getCountLimit();
        }
        return null;
    }

    @Override
    public Integer getFrequencyLimit()
    {
        if (!_rule.isUserBlocked())
        {
            return _rule.getFrequencyLimit();
        }
        return null;
    }

    @Override
    public Long getFrequencyPeriod()
    {
        if (!_rule.isUserBlocked() && _rule.getFrequencyLimit() != null)
        {
            return Optional.ofNullable(_rule.getFrequencyPeriod()).map(Duration::toMillis).orElse(null);
        }
        return null;
    }
}
