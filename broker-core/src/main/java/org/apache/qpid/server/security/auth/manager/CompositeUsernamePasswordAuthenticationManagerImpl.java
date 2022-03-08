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
package org.apache.qpid.server.security.auth.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.server.configuration.IllegalConfigurationException;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Container;
import org.apache.qpid.server.model.ManagedAttributeField;
import org.apache.qpid.server.model.ManagedObjectFactoryConstructor;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.security.auth.AuthenticationResult;
import org.apache.qpid.server.security.auth.sasl.PasswordSource;
import org.apache.qpid.server.security.auth.sasl.SaslNegotiator;
import org.apache.qpid.server.security.auth.sasl.SaslSettings;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Base64HashedNegotiator;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Base64HexNegotiator;
import org.apache.qpid.server.security.auth.sasl.crammd5.CramMd5Negotiator;
import org.apache.qpid.server.security.auth.sasl.plain.PlainNegotiator;
import org.apache.qpid.server.security.auth.sasl.scram.ScramNegotiator;
import org.apache.qpid.server.security.auth.sasl.scram.ScramSaslServerSource;
import org.apache.qpid.server.security.auth.sasl.scram.ScramSaslServerSourceAdapter;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

/**
 * Composite username / password authentication provider.
 *
 * Contains list of delegate authentication providers, which are assessed one by one during authentication process
 * until first successful authentication or until all authentication attempts fail.
 *
 * When two delegates share same SASL mechanism (e.g. PlainAuthenticationProvider and ScramSHA256AuthenticationManager
 * have in common SCRAM-SHA-256), implementation is resolved in runtime choosing the delegate containing username requested.
 * If user with same username is present in both delegates, authentication will be performed only against the first
 * delegate in the list.
 *
 */
public class CompositeUsernamePasswordAuthenticationManagerImpl
        extends AbstractAuthenticationManager<CompositeUsernamePasswordAuthenticationManagerImpl>
        implements CompositeUsernamePasswordAuthenticationManager<CompositeUsernamePasswordAuthenticationManagerImpl>
{

    /**
     * Mechanism name
     */
    @SuppressWarnings("unused")
    public static final String MECHANISM_NAME = "COMPOSITE";

    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeUsernamePasswordAuthenticationManagerImpl.class);

    /**
     * List of delegate authentication provider names
     */
    @SuppressWarnings("unused")
    @ManagedAttributeField
    private List<String> _delegates;

    /**
     * Authentication provider delegates
     */
    private final Set<UsernamePasswordAuthenticationProvider<?>> _authenticationProviders = new LinkedHashSet<>();

    /**
     * Available SASL negotiators
     */
    private final Map<String, Function<SaslSettings, SaslNegotiator>> _saslNegotiators = new HashMap<>();

    /**
     * Available scram adapters
     */
    private final Map<String, ScramSaslServerSourceAdapter> _scramAdapters = new HashMap<>();

    /**
     * HMAC names
     */
    private final Map<String, String> _hmacNames = new HashMap<>();

    /**
     * Digest names
     */
    private final Map<String, String> _digestNames = new HashMap<>();

    /**
     * List of supported SASL mechanisms
     */
    private List<String> _mechanisms = new ArrayList<>();

    /**
     * List of supported SASL mechanisms
     */
    private List<String> _secureOnlyMechanisms = new ArrayList<>();

    /**
     * List of supported SASL mechanisms
     */
    private List<String> _disabledMechanisms = new ArrayList<>();

    /**
     * Scram iterations count
     */
    final int scramIterationCount = getContextValue(
            Integer.class,
            AbstractScramAuthenticationManager.QPID_AUTHMANAGER_SCRAM_ITERATION_COUNT
    );

    /**
     * Constructor creates configured object
     *
     * @param attributes Attributes
     * @param container Parent container
     */
    @ManagedObjectFactoryConstructor()
    public CompositeUsernamePasswordAuthenticationManagerImpl(final Map<String, Object> attributes, final Container<?> container)
    {
        super(attributes, container);
    }

    /**
     * Initiates SCRAM adapters, delegates
     */
    @Override
    protected void postResolveChildren()
    {
        super.postResolveChildren();

        final PasswordSource passwordSource = getPasswordSource();

        _scramAdapters.put(
            ScramSHA1AuthenticationManager.MECHANISM,
            new ScramSaslServerSourceAdapter(
                scramIterationCount,
                ScramSHA1AuthenticationManager.HMAC_NAME,
                ScramSHA1AuthenticationManager.DIGEST_NAME,
                passwordSource
            )
        );

        _scramAdapters.put(
            ScramSHA256AuthenticationManager.MECHANISM,
            new ScramSaslServerSourceAdapter(
                scramIterationCount,
                ScramSHA256AuthenticationManager.HMAC_NAME,
                ScramSHA256AuthenticationManager.DIGEST_NAME,
                passwordSource
            )
        );

        // check for duplicates
        if (new HashSet<>(_delegates).size() != _delegates.size())
        {
            throw new IllegalConfigurationException("Composite authentication manager shouldn't contain duplicate names");
        }

        // prepare delegate authentication providers
        for (final String delegate: _delegates)
        {
            final AuthenticationProvider<?> authProvider = resolveDelegate(delegate);
            _authenticationProviders.add((UsernamePasswordAuthenticationProvider<?>) authProvider);
        }

        if (_authenticationProviders.isEmpty())
        {
            throw new IllegalConfigurationException("Composite authentication manager should contain at least one delegate");
        }

        // supported SASL mechanisms are prepared as intersection of delegates mechanisms
        _mechanisms = new ArrayList<>(_authenticationProviders.stream().findFirst().get().getMechanisms());
        _authenticationProviders.forEach(authProvider -> _mechanisms.retainAll(authProvider.getMechanisms()));
        _authenticationProviders.stream()
            .filter(authProvider -> authProvider.getDisabledMechanisms() != null)
            .forEach(authProvider -> _mechanisms.removeAll(authProvider.getDisabledMechanisms()));

        // secure only SASL mechanisms are prepared as union of delegates secure only mechanisms
        _secureOnlyMechanisms = Stream.concat(
            Optional.ofNullable(super.getSecureOnlyMechanisms()).orElse(Collections.emptyList()).stream(),
            _authenticationProviders.stream()
            .filter(authProvider -> authProvider.getSecureOnlyMechanisms() != null)
            .flatMap(authProvider -> authProvider.getSecureOnlyMechanisms().stream()))
            .distinct().collect(Collectors.toList());

        // disabled only SASL mechanisms are prepared as union of delegates disabled mechanisms
        _disabledMechanisms = Stream.concat(
            Optional.ofNullable(super.getDisabledMechanisms()).orElse(Collections.emptyList()).stream(),
            _authenticationProviders.stream()
            .filter(authProvider -> authProvider.getDisabledMechanisms() != null)
            .flatMap(authProvider -> authProvider.getDisabledMechanisms().stream()))
            .distinct().collect(Collectors.toList());
    }

    /**
     * Initializes SASL negotiators
     */
    @Override
    protected void onOpen()
    {
        super.onOpen();

        // initialize hmac names
        _hmacNames.put(ScramSHA1AuthenticationManager.MECHANISM, ScramSHA1AuthenticationManager.HMAC_NAME);
        _hmacNames.put(ScramSHA256AuthenticationManager.MECHANISM, ScramSHA256AuthenticationManager.HMAC_NAME);

        // initialize digest names
        _digestNames.put(ScramSHA1AuthenticationManager.MECHANISM, ScramSHA1AuthenticationManager.DIGEST_NAME);
        _digestNames.put(ScramSHA256AuthenticationManager.MECHANISM, ScramSHA256AuthenticationManager.DIGEST_NAME);

        // initialize available SASL negotiators
        _saslNegotiators.put(CramMd5Negotiator.MECHANISM, (saslSettings) -> new CramMd5Negotiator(getAuthenticationProviderStub(), saslSettings.getLocalFQDN(), getPasswordSource()));
        _saslNegotiators.put(CramMd5Base64HashedNegotiator.MECHANISM, (saslSettings) -> new CramMd5Base64HashedNegotiator(getAuthenticationProviderStub(), saslSettings.getLocalFQDN(), getPasswordSource()));
        _saslNegotiators.put(CramMd5Base64HexNegotiator.MECHANISM, (saslSettings) -> new CramMd5Base64HexNegotiator(getAuthenticationProviderStub(), saslSettings.getLocalFQDN(), getPasswordSource()));
        _saslNegotiators.put(PlainNegotiator.MECHANISM, (saslSettings) -> new PlainNegotiator(this));
        _saslNegotiators.put(ScramSHA1AuthenticationManager.MECHANISM, (saslSettings) -> new ScramNegotiator(this, getScramSaslServerSource(ScramSHA1AuthenticationManager.MECHANISM), ScramSHA1AuthenticationManager.MECHANISM));
        _saslNegotiators.put(ScramSHA256AuthenticationManager.MECHANISM, (saslSettings) -> new ScramNegotiator(this, getScramSaslServerSource(ScramSHA256AuthenticationManager.MECHANISM), ScramSHA256AuthenticationManager.MECHANISM));

    }

    /**
     * Validate changes
     *
     * @param proxyForValidation ConfiguredObject
     * @param changedAttributes Attribute names
     */
    @Override
    public void validateChange(final ConfiguredObject<?> proxyForValidation, final Set<String> changedAttributes)
    {
        super.validateChange(proxyForValidation, changedAttributes);
        final Collection<String> delegates = (Collection<String>) proxyForValidation.getAttribute("delegates");
        if (delegates.isEmpty())
        {
            throw new IllegalConfigurationException("Composite authentication manager should contain at least one delegate");
        }
        delegates.forEach(this::resolveDelegate);
    }

    /**
     * MD5 => ["PLAIN", "CRAM-MD5-HASHED", "CRAM-MD5-HEX"]
     * Plain => ["PLAIN", "CRAM-MD5", "SCRAM-SHA-1", "SCRAM-SHA-256"]
     * SCRAM-SHA-1 => ["PLAIN", "SCRAM-SHA-1"]
     * SCRAM-SHA-256 => ["PLAIN", "SCRAM-SHA-256"]
     * SimpleLDAP => ["PLAIN"]
     *
     * @return List of mechanism names
     */
    @Override
    public List<String> getMechanisms()
    {
        return Collections.unmodifiableList(_mechanisms);
    }

    /**
     * Returns list of available SASL mechanism names
     *
     * @param secure Secure flag
     *
     * @return List of mechanism names
     */
    @Override
    public List<String> getAvailableMechanisms(boolean secure)
    {
        final List<String> result = _mechanisms.stream()
            .filter(mechanism -> secure || !_secureOnlyMechanisms.contains(mechanism))
            .filter(mechanism -> !_disabledMechanisms.contains(mechanism))
            .collect(Collectors.toList());
        return Collections.unmodifiableList(result);
    }

    /**
     * Creates SASL negotiator based on available options
     *
     * @param mechanism Mechanism name
     * @param saslSettings SaslSettings
     * @param addressSpace NamedAddressSpace
     *
     * @return SaslNegotiator
     */
    @Override
    public SaslNegotiator createSaslNegotiator(
        final String mechanism,
        final SaslSettings saslSettings,
        final NamedAddressSpace addressSpace
    )
    {
        return _saslNegotiators.getOrDefault(mechanism, (settings) -> null).apply(saslSettings);
    }

    /**
     * Iterates over authentication provider delegates attempting authentication for each one.
     *
     * @param username username
     * @param password password
     *
     * @return AuthenticationResult
     */
    @Override
    public AuthenticationResult authenticate(String username, String password)
    {
        for (final UsernamePasswordAuthenticationProvider<?> authenticationProvider : _authenticationProviders)
        {
            final AuthenticationResult authResult = authenticationProvider.authenticate(username, password);
            if (AuthenticationResult.AuthenticationStatus.ERROR.equals(authResult.getStatus()))
            {
                LOGGER.debug(
                    "Authentication of user '{}' against '{}' failed",
                    username,
                    authenticationProvider.getClass().getSimpleName()
                );
                continue;
            }
            LOGGER.debug(
                "Authentication of user '{}' against '{}' succeeded",
                username,
                authenticationProvider.getClass().getSimpleName()
            );
            return authResult;
        }
        LOGGER.debug("All authentication attempts failed");
        return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR);
    }

    /**
     * Creates ScramSaslServerSource instance which will retrieve SaltAndPasswordKeys from the authentication
     * provider delegate containing requested username.
     *
     * @param mechanism SASL mechanism
     *
     * @return ScramSaslServerSource
     */
    private ScramSaslServerSource getScramSaslServerSource(String mechanism)
    {
        return new ScramSaslServerSource()
        {

            @Override
            public int getIterationCount()
            {
                return scramIterationCount;
            }

            @Override
            public String getDigestName()
            {
                return Optional.ofNullable(_digestNames.get(mechanism))
                    .orElseThrow(() -> new ConnectionScopedRuntimeException("Mechanism '" + mechanism + "' not supported"));
            }

            @Override
            public String getHmacName()
            {
                return Optional.ofNullable(_hmacNames.get(mechanism))
                    .orElseThrow(() -> new ConnectionScopedRuntimeException("Mechanism '" + mechanism + "' not supported"));
            }

            @Override
            public SaltAndPasswordKeys getSaltAndPasswordKeys(final String username)
            {
                return _authenticationProviders.stream()
                    .filter(authProvider -> authProvider instanceof ConfigModelPasswordManagingAuthenticationProvider<?>)
                    .filter(authProvider -> ((ConfigModelPasswordManagingAuthenticationProvider<?>)authProvider).getUser(username) != null)
                    .findFirst().map(authProvider ->
                    {
                        if (authProvider instanceof AbstractScramAuthenticationManager<?>)
                        {
                            return ((AbstractScramAuthenticationManager<?>) authProvider).getSaltAndPasswordKeys(username);
                        }
                        return _scramAdapters.get(mechanism).getSaltAndPasswordKeys(username);
                    }).orElse(_scramAdapters.get(mechanism).getSaltAndPasswordKeys(username));
            }
        };
    }

    /**
     * Resolves delegate authentication provider by name
     *
     * @param delegate Delegate name
     *
     * @return Delegate AuthenticationProvider
     */
    private AuthenticationProvider<?> resolveDelegate(final String delegate)
    {
        final Broker<?> broker = (Broker<?>) getParent();

        final Optional<AuthenticationProvider<?>> optAuthProvider = broker.getAuthenticationProviders().stream()
            .filter(provider -> provider.getName().equals(delegate)).findFirst();

        if (!optAuthProvider.isPresent())
        {
            throw new IllegalConfigurationException("Authentication provider '" + delegate + "' not found");
        }

        final AuthenticationProvider<?> authProvider = optAuthProvider.get();

        if (!(authProvider instanceof UsernamePasswordAuthenticationProvider<?>))
        {
            throw new IllegalConfigurationException("Authentication provider '" + delegate + "' is not UsernamePasswordAuthenticationProvider");
        }

        if (authProvider instanceof CompositeUsernamePasswordAuthenticationManager<?>)
        {
            throw new IllegalConfigurationException("Composite authentication providers shouldn't be nested");
        }

        return authProvider;
    }

    /**
     * Retrieves username from the first authentication provider containing user with matching username
     *
     * @return PasswordSource
     */
    private PasswordSource getPasswordSource()
    {
        return username -> _authenticationProviders.stream()
                .filter(authProvider -> authProvider instanceof ConfigModelPasswordManagingAuthenticationProvider)
                .filter(authProvider -> ((ConfigModelPasswordManagingAuthenticationProvider<?>) authProvider).getUser(username) != null)
                .findFirst()
                .map(authProvider -> (((ConfigModelPasswordManagingAuthenticationProvider<?>) authProvider)
                    .getPasswordSource().getPassword(username)))
                .orElse(null);
    }

    /**
     * Creates authentication provider stub used by some SASL negotiators
     *
     * @return ConfigModelPasswordManagingAuthenticationProvider stub
     */
    private <X extends ConfigModelPasswordManagingAuthenticationProvider<X>> ConfigModelPasswordManagingAuthenticationProvider<X> getAuthenticationProviderStub()
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(AuthenticationProvider.NAME, "AuthenticationProviderStub");
        attributes.put(AuthenticationProvider.ID, UUID.randomUUID());

        final CompositeUsernamePasswordAuthenticationManagerImpl parent = this;
        final PasswordSource passwordSource = getPasswordSource();

        return new ConfigModelPasswordManagingAuthenticationProvider<X>(attributes, (Container<?>) getParent())
        {

            @Override
            public AuthenticationResult authenticate(final String username, final String password)
            {
                return parent.authenticate(username, password);
            }

            @Override
            public PasswordSource getPasswordSource()
            {
                return passwordSource;
            }

            @Override
            protected String createStoredPassword(final String password)
            {
                throw new ConnectionScopedRuntimeException("SaslNegotiator isn't supposed to call createStoredPassword()");
            }

            @Override
            void validateUser(final ManagedUser managedUser)
            {
                throw new ConnectionScopedRuntimeException("SaslNegotiator isn't supposed to call validateUser()");
            }

            @Override
            public List<String> getMechanisms()
            {
                throw new ConnectionScopedRuntimeException("SaslNegotiator isn't supposed to call getMechanisms()");
            }

            @Override
            public SaslNegotiator createSaslNegotiator(
                final String mechanism,
                final SaslSettings saslSettings,
                final NamedAddressSpace addressSpace
            )
            {
                throw new ConnectionScopedRuntimeException("SaslNegotiator isn't supposed to call createSaslNegotiator()");
            }
        };
    }

    @Override
    public String toString()
    {
        return "CompositeAuthenticationManagerImpl {"
           + "_authenticationProviders=" + _authenticationProviders + '}';
    }

    @Override
    public List<String> getDelegates()
    {
        return _delegates;
    }
}
