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
package org.apache.qpid.server.query.engine.parsing.query;

import static org.apache.qpid.server.query.engine.evaluator.EvaluationContext.BROKER;
import static org.apache.qpid.server.query.engine.evaluator.EvaluationContext.STATISTICS;

import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Model;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.query.engine.exception.Errors;
import org.apache.qpid.server.query.engine.exception.QueryParsingException;
import org.apache.qpid.server.query.engine.model.Domain;
import org.apache.qpid.server.query.engine.parsing.expression.literal.ConstantExpression;
import org.apache.qpid.server.query.engine.parsing.factory.AccessorExpressionFactory;
import org.apache.qpid.server.query.engine.parsing.factory.ProjectionExpressionFactory;
import org.apache.qpid.server.query.engine.retriever.AclRuleRetriever;
import org.apache.qpid.server.query.engine.retriever.BindingRetriever;
import org.apache.qpid.server.query.engine.retriever.CertificateRetriever;
import org.apache.qpid.server.query.engine.retriever.ConfiguredObjectRetriever;
import org.apache.qpid.server.query.engine.retriever.ConnectionLimitRuleRetriever;
import org.apache.qpid.server.query.engine.retriever.DomainRetriever;
import org.apache.qpid.server.query.engine.retriever.EntityRetriever;
import org.apache.qpid.server.query.engine.retriever.SessionRetriever;
import org.apache.qpid.server.security.access.plugins.AclRule;
import org.apache.qpid.server.user.connection.limits.plugins.ConnectionLimitRule;

/**
 * Contains information about domain queried and retrieves steams of entities from domain.
 *
 * @param <T> Input parameter type
 * @param <R> Return parameter type
 * @param <C> ConfiguredObject descendant
 */
// sonar complains about underscores in variable names
@SuppressWarnings("java:S116")
public class FromExpression<T, R extends Stream<?>, C extends ConfiguredObject<?>> extends ConstantExpression<T, R>
{
    /**
     * Retrieves data from broker objects descending from ConfiguredObject
     */
    private final ConfiguredObjectRetriever<C> _configuredObjectRetriever = new ConfiguredObjectRetriever<>();

    /**
     * Retrieves data from AclRule/Rule entities
     */
    private final EntityRetriever<C> _aclRuleRetriever = new AclRuleRetriever<>();

    /**
     * Retrieves data from Binding entities
     */
    private final EntityRetriever<C> _bindingRetriever = new BindingRetriever<>();

    /**
     * Retrieves data from CertificateDetails entities
     */
    private final EntityRetriever<C> _certificateRetriever = new CertificateRetriever<>();

    /**
     * Retrieves data from ConnectionLimitRule entities
     */
    private final EntityRetriever<C> _connectionLimitRuleRetriever = new ConnectionLimitRuleRetriever<>();

    /**
     * Retrieves data from Domain entities
     */
    private final EntityRetriever<C> _domainRetriever = new DomainRetriever<>();

    /**
     * Retrieves data from Session entities
     */
    private final EntityRetriever<C> _sessionRetriever = new SessionRetriever<>();

    /**
     * Additional domains allowed to be queried
     */
    private final Map<Class<?>, EntityRetriever<C>> _allowedClasses = Map.of(
            AclRule.class, _aclRuleRetriever,
            Binding.class, _bindingRetriever,
            Certificate.class, _certificateRetriever,
            ConnectionLimitRule.class, _connectionLimitRuleRetriever,
            Domain.class, _domainRetriever,
            Session.class, _sessionRetriever);

    /**
     * Domain name
     */
    private final String _domain;

    /**
     * Domain alias
     */
    private final String _alias;

    /**
     * Domain class
     */
    private Class<?> _category;

    /**
     * Domain name
     */
    private Broker<?> _broker;

    /**
     * Constructor stores domain properties
     *
     * @param domain Domain name
     * @param alias Domain alias
     */
    public FromExpression(final String domain, final String alias)
    {
        super(null);
        _domain = domain;
        _alias = alias;
    }

    /**
     * Retrieves stream of data from domain
     *
     * @return Stream of data
     */
    @Override
    @SuppressWarnings("unchecked")
    public R get()
    {
        _broker = ctx().get(BROKER);

        if (Objects.equals(_domain, BROKER))
        {
            return (R) Stream.of(_broker);
        }

        if (ctx().contains(_domain))
        {
            final QueryExpression<T, R> query = ctx().get(_domain);
            return query.getSelect().apply(null);
        }

        _category = getDomainClass();

        if (_allowedClasses.containsKey(_category))
        {
            if (_category.isAssignableFrom(Domain.class))
            {
                return (R) Stream.concat(
                    _allowedClasses.keySet().stream().map(type -> Map.of("name", type.getSimpleName())),
                    _allowedClasses.get(_category).retrieve((C) _broker)
                );
            }
            return (R) _allowedClasses.get(_category).retrieve((C) _broker);
        }

        return (R) _configuredObjectRetriever.retrieve((C) _broker, (Class<C>) _category);
    }

    /**
     * Retrieves domain class
     *
     * @return Domain class
     */
    private Class<?> getDomainClass()
    {
        final Model brokerModel = _broker.getModel();
        return Stream.concat(_allowedClasses.keySet().stream(), brokerModel.getSupportedCategories().stream())
            .filter(type -> _domain.equalsIgnoreCase(type.getSimpleName())).findFirst()
            .orElseThrow(() -> QueryParsingException.of(Errors.VALIDATION.DOMAIN_NOT_SUPPORTED, _domain));
    }

    /**
     * Retrieves field names of a domain entity
     *
     * @return List of field names
     */
    @SuppressWarnings("unchecked")
    public List<String> getFieldNames()
    {
        if (Objects.equals(_domain, BROKER))
        {
            return new ArrayList<>(_broker.getModel().getTypeRegistry().getAttributeNames(Broker.class));
        }
        if (ctx().contains(_domain))
        {
            final QueryExpression<T, R> query = ctx().get(_domain);
            return query.getSelect().getProjections().stream().map(ProjectionExpression::getAlias).collect(Collectors.toList());
        }
        if (_allowedClasses.containsKey(_category))
        {
            return _allowedClasses.get(_category).getFieldNames();
        }
        return Stream.concat(_broker.getModel().getTypeRegistry().getAttributeNames((Class<C>)_category).stream(), Stream.of(STATISTICS))
            .collect(Collectors.toList());
    }

    /**
     * Retrieves accessor expressions for a domain entity
     *
     * @param aliases List of field names
     * @param selectExpression SelectExpression instance
     *
     * @param <Y> Return parameter type
     *
     * @return List of projection expressions
     */
    public <Y> List<ProjectionExpression<T, Y>> getProjections(
        final List<String> aliases,
        final SelectExpression<T, R> selectExpression
    )
    {
        return aliases.stream()
            .map(name -> AccessorExpressionFactory.<T, Y>delegating(name, name))
            .map(expression -> ProjectionExpressionFactory.projection(expression, expression.getAlias(), selectExpression.getOrdinal()))
            .collect(Collectors.toList());
    }

    /**
     * Returns domain alias
     *
     * @return Domain alias
     */
    @Override
    public String getAlias()
    {
        if (_alias == null)
        {
            return _domain;
        }
        return _alias;
    }

    @Override
    public String toString()
    {
        return _domain;
    }
}
