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
package org.apache.qpid.tests.http.rest.model;

import static java.util.Collections.singletonMap;
import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import org.apache.qpid.server.logging.logback.VirtualHostFileLogger;
import org.apache.qpid.server.logging.logback.VirtualHostNameAndLevelLogInclusionRule;
import org.apache.qpid.server.model.AbstractConfiguredObject;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.User;
import org.apache.qpid.server.security.NonJavaKeyStore;
import org.apache.qpid.server.security.NonJavaTrustStore;
import org.apache.qpid.test.utils.tls.TlsResourceBuilder;
import org.apache.qpid.server.util.DataUrlUtils;
import org.apache.qpid.server.util.FileUtils;
import org.apache.qpid.test.utils.tls.KeyCertificatePair;
import org.apache.qpid.tests.http.HttpRequestConfig;
import org.apache.qpid.tests.http.HttpTestBase;

@HttpRequestConfig
public class ReadTest extends HttpTestBase
{
    private static final String QUEUE1_NAME = "myqueue1";
    private static final String QUEUE2_NAME = "myqueue2";
    private static final String QUEUE1_URL = String.format("queue/%s", QUEUE1_NAME);
    private static final String QUEUE2_URL = String.format("queue/%s", QUEUE2_NAME);

    @Before
    public void setUp()
    {
        getBrokerAdmin().createQueue(QUEUE1_NAME);
        getBrokerAdmin().createQueue(QUEUE2_NAME);
    }

    @Test
    public void readObject() throws Exception
    {
        final Map<String, Object> queue = getHelper().getJsonAsMap(QUEUE1_URL);
        assertThat(queue.get(ConfiguredObject.NAME), is(equalTo(QUEUE1_NAME)));
        assertThat(queue.get(ConfiguredObject.ID), is(notNullValue()));
    }

    @Test
    public void notFound() throws Exception
    {
        getHelper().submitRequest("queue/unknown", "GET", SC_NOT_FOUND);
    }

    @Test
    public void readObjectResponseAsList() throws Exception
    {
        final Map<String, Object> queue = getHelper().getJsonAsSingletonList(QUEUE1_URL + "?singletonModelObjectResponseAsList=true");
        assertThat(queue.get(ConfiguredObject.NAME), is(equalTo(QUEUE1_NAME)));
        assertThat(queue.get(ConfiguredObject.ID), is(notNullValue()));
    }

    @Test
    public void readAll() throws Exception
    {
        List<Map<String, Object>> list = getHelper().getJsonAsList("queue");
        assertThat(list.size(), is(equalTo(2)));
        Set<String> queueNames = list.stream()
                                     .map(map -> (String) map.get(ConfiguredObject.NAME))
                                     .collect(Collectors.toSet());
        assertThat(queueNames, containsInAnyOrder(QUEUE1_NAME, QUEUE2_NAME));
    }

    @Test
    public void readFilter() throws Exception
    {
        final Map<String, Object> queue1 = getHelper().getJsonAsMap(QUEUE1_URL);

        List<Map<String, Object>> list = getHelper().getJsonAsList(String.format("queue/?%s=%s",
                                                                                 ConfiguredObject.ID,
                                                                                 queue1.get(ConfiguredObject.ID)));
        assertThat(list.size(), is(equalTo(1)));
        final Map<String, Object> queue = list.get(0);
        assertThat(queue.get(ConfiguredObject.NAME), is(equalTo(QUEUE1_NAME)));
        assertThat(queue.get(ConfiguredObject.ID), is(notNullValue()));
    }

    @Test
    public void filterNotFound() throws Exception
    {
        List<Map<String, Object>> list = getHelper().getJsonAsList(String.format("queue/?%s=%s",
                                                                                 ConfiguredObject.ID,
                                                                                 UUID.randomUUID()));
        assertThat(list.size(), is(equalTo(0)));
    }

    @Test
    public void readHierarchy() throws Exception
    {
        final Map<String, Object> virtualHost = getHelper().getJsonAsMap("virtualhost?depth=2");
        assertThat(virtualHost.get(ConfiguredObject.ID), is(notNullValue()));
        assertThat(virtualHost.get(ConfiguredObject.NAME), is(equalTo(getVirtualHost())));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> queues = (List<Map<String, Object>>) virtualHost.get("queues");
        assertThat(queues.size(), is(equalTo(2)));
    }

    @Test
    public void excludeInheritedContext() throws Exception
    {
        final String hostContextKey = "myvhcontextvar";
        final String hostContextValue = UUID.randomUUID().toString();
        final Map<String, Object> hostUpdateAttrs = singletonMap(ConfiguredObject.CONTEXT,
                                                                 singletonMap(hostContextKey, hostContextValue));
        getHelper().submitRequest("virtualhost", "POST", hostUpdateAttrs, SC_OK);

        final String queueContextKey = "myqueuecontextvar";
        final String queueContextValue = UUID.randomUUID().toString();
        final Map<String, Object> queueUpdateAttrs = singletonMap(ConfiguredObject.CONTEXT,
                                                                  singletonMap(queueContextKey, queueContextValue));
        getHelper().submitRequest(QUEUE1_URL, "POST", queueUpdateAttrs, SC_OK);

        final Map<String, Object> queue = getHelper().getJsonAsMap(QUEUE1_URL);
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) queue.get("context");
        assertThat(context.size(), is(equalTo(1)));
        assertThat(context.get(queueContextKey), is(equalTo(queueContextValue)));

        final Map<String, Object> queue2 = getHelper().getJsonAsMap(QUEUE1_URL + "?excludeInheritedContext=false");
        @SuppressWarnings("unchecked")
        Map<String, Object> context2 = (Map<String, Object>) queue2.get("context");
        assertThat(context2.size(), is(greaterThanOrEqualTo(2)));
        assertThat(context2.get(queueContextKey), is(equalTo(queueContextValue)));
        assertThat(context2.get(hostContextKey), is(equalTo(hostContextValue)));
    }

    @Test
    public void actuals() throws Exception
    {
        final String queueContextKey = "myqueuecontextvar";
        final String queueContextValue = UUID.randomUUID().toString();

        final Map<String, Object> queueUpdateAttrs = new HashMap<>();
        queueUpdateAttrs.put(ConfiguredObject.DESCRIPTION, "${myqueuecontextvar}");
        queueUpdateAttrs.put(ConfiguredObject.CONTEXT, singletonMap(queueContextKey, queueContextValue));
        getHelper().submitRequest(QUEUE1_URL, "POST", queueUpdateAttrs, SC_OK);


        final Map<String, Object> queue = getHelper().getJsonAsMap(QUEUE1_URL);
        assertThat(queue.get(ConfiguredObject.DESCRIPTION), is(equalTo(queueContextValue)));

        final Map<String, Object> queueActuals = getHelper().getJsonAsMap(QUEUE1_URL + "?actuals=true");
        assertThat(queueActuals.get(ConfiguredObject.DESCRIPTION), is(equalTo("${myqueuecontextvar}")));
    }

    @Test
    public void wildcards() throws Exception
    {
        String rule1A = createLoggerAndRule("mylogger1", "myinclusionruleA");
        String rule1B = createLoggerAndRule("mylogger1", "myinclusionruleB");
        String rule2A = createLoggerAndRule("mylogger2", "myinclusionruleA");

        {
            List<Map<String, Object>> rules = getHelper().getJsonAsList("virtualhostloginclusionrule/*");
            assertThat(rules.size(), is(equalTo(3)));

            Set<String> ids = rules.stream().map(ReadTest::getId).collect(Collectors.toSet());
            assertThat(ids, containsInAnyOrder(rule1A, rule1B, rule2A));
        }

        {
            List<Map<String, Object>> rules = getHelper().getJsonAsList("virtualhostloginclusionrule/mylogger1/*");
            assertThat(rules.size(), is(equalTo(2)));

            Set<String> ids = rules.stream().map(ReadTest::getId).collect(Collectors.toSet());
            assertThat(ids, containsInAnyOrder(rule1A, rule1B));
        }

        {
            List<Map<String, Object>> rules = getHelper().getJsonAsList("virtualhostloginclusionrule/*/myinclusionruleA");
            assertThat(rules.size(), is(equalTo(2)));

            Set<String> ids = rules.stream().map(ReadTest::getId).collect(Collectors.toSet());
            assertThat(ids, containsInAnyOrder(rule1A, rule2A));
        }
    }

    @Test
    @HttpRequestConfig(useVirtualHostAsHost = false)
    public void secureAttributes() throws Exception
    {
        final String validUsername = getBrokerAdmin().getValidUsername();
        final Map<String, Object> user = getHelper().getJsonAsMap("user/plain/" + validUsername);
        assertThat(user.get(User.NAME), is(equalTo(validUsername)));
        assertThat(user.get(User.PASSWORD), is(equalTo(AbstractConfiguredObject.SECURED_STRING_VALUE)));
    }

    @Test
    @HttpRequestConfig(useVirtualHostAsHost = false)
    public void valueFilteredSecureAttributes() throws Exception
    {

        final KeyCertificatePair keyCertPair = generateCertKeyPair();
        final byte[] privateKey = keyCertPair.getPrivateKey().getEncoded();
        final byte[] cert = keyCertPair.getCertificate().getEncoded();
        final String privateKeyUrl = DataUrlUtils.getDataUrlForBytes(privateKey);
        final String certUrl = DataUrlUtils.getDataUrlForBytes(cert);

        final File privateKeyFile = File.createTempFile("foo" + System.currentTimeMillis(), "key");
        privateKeyFile.deleteOnExit();
        FileUtils.copy(new ByteArrayInputStream(privateKey), privateKeyFile);

        Map<String, Object> base = new HashMap<>();
        base.put(NonJavaKeyStore.TYPE, "NonJavaKeyStore");
        base.put(NonJavaKeyStore.CERTIFICATE_URL, certUrl);

        try
        {
            {
                final String storeUrl = "keystore/mystoreDataUrl";
                final Map<String, Object> attrs = new HashMap<>(base);
                attrs.put(NonJavaKeyStore.PRIVATE_KEY_URL, privateKeyUrl);
                getHelper().submitRequest(storeUrl, "PUT", attrs, SC_CREATED);

                final Map<String, Object> store = getHelper().getJsonAsMap(storeUrl);
                assertThat(store.get(NonJavaKeyStore.PRIVATE_KEY_URL),
                           is(equalTo(AbstractConfiguredObject.SECURED_STRING_VALUE)));

                getHelper().submitRequest(storeUrl, "DELETE", SC_OK);
            }

            {
                final String privateKeyFileUrl = privateKeyFile.toURI().toString();
                final String storeUrl = "keystore/mystoreFileUrl";
                final Map<String, Object> attrs = new HashMap<>(base);
                attrs.put(NonJavaKeyStore.TYPE, "NonJavaKeyStore");
                attrs.put(NonJavaKeyStore.PRIVATE_KEY_URL, privateKeyFileUrl);
                getHelper().submitRequest(storeUrl, "PUT", attrs, SC_CREATED);

                final Map<String, Object> store = getHelper().getJsonAsMap(String.format("%s?oversize=%d", storeUrl, privateKeyFileUrl.length()));
                assertThat(store.get(NonJavaKeyStore.PRIVATE_KEY_URL), is(equalTo(privateKeyFileUrl)));

                getHelper().submitRequest(storeUrl, "DELETE", SC_OK);
            }
        }
        finally
        {
            privateKeyFile.delete();
        }
    }

    @Test
    @HttpRequestConfig(useVirtualHostAsHost = false)
    public void oversizeAttribute() throws Exception
    {

        final byte[] encodedCert = generateCertKeyPair().getCertificate().getEncoded();
        final String dataUrl = DataUrlUtils.getDataUrlForBytes(encodedCert);

        final String storeUrl = "truststore/mystore";
        final Map<String, Object> attrs = new HashMap<>();
        attrs.put(NonJavaTrustStore.TYPE, "NonJavaTrustStore");
        attrs.put(NonJavaTrustStore.CERTIFICATES_URL, dataUrl);
        getHelper().submitRequest(storeUrl, "PUT", attrs, SC_CREATED);

        final Map<String, Object> store = getHelper().getJsonAsMap(storeUrl);
        assertThat(store.get(NonJavaTrustStore.CERTIFICATES_URL), is(equalTo(AbstractConfiguredObject.OVER_SIZED_ATTRIBUTE_ALTERNATIVE_TEXT)));

        final Map<String, Object> full = getHelper().getJsonAsMap(storeUrl +  String.format("?oversize=%d", dataUrl.length()));
        assertThat(full.get(NonJavaTrustStore.CERTIFICATES_URL), is(equalTo(dataUrl)));

        getHelper().submitRequest(storeUrl, "DELETE", SC_OK);
    }

    private String createLoggerAndRule(final String loggerName, final String inclusionRuleName) throws Exception
    {
        final String parentUrl = String.format("virtualhostlogger/%s", loggerName);
        Map<String, Object> parentAttrs = Collections.singletonMap(ConfiguredObject.TYPE, VirtualHostFileLogger.TYPE);

        int response = getHelper().submitRequest(parentUrl, "PUT", parentAttrs);
        assertThat(response, is(oneOf(SC_CREATED, SC_OK)));

        final String childUrl = String.format("virtualhostloginclusionrule/%s/%s", loggerName, inclusionRuleName);
        Map<String, Object> childAttrs = Collections.singletonMap(ConfiguredObject.TYPE, VirtualHostNameAndLevelLogInclusionRule.TYPE);
        getHelper().submitRequest(childUrl, "PUT", childAttrs, SC_CREATED);

        final Map<String, Object> child = getHelper().getJsonAsMap(childUrl);
        return (String) child.get(ConfiguredObject.ID);

    }

    private static String getId(Map<String, Object> object)
    {
        return ((String) object.get(ConfiguredObject.ID));
    }

    private KeyCertificatePair generateCertKeyPair() throws Exception
    {
        return TlsResourceBuilder.createSelfSigned("CN=foo");
    }
}
