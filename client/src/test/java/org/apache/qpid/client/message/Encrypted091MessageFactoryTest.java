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
package org.apache.qpid.client.message;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.security.auth.x500.X500Principal;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.framing.BasicContentHeaderProperties;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.test.utils.TestSSLConstants;
import org.apache.qpid.transport.ConnectionSettings;
import org.apache.qpid.transport.DeliveryProperties;
import org.apache.qpid.transport.MessageProperties;
import org.apache.qpid.transport.codec.BBEncoder;
import org.apache.qpid.util.BytesDataOutput;

public class Encrypted091MessageFactoryTest extends QpidTestCase
{
    public static final String TEXT_MESSAGE_CONTENT = "Test message";
    private Encrypted091MessageFactory _messageFactory;
    private byte[] _data = TEXT_MESSAGE_CONTENT.getBytes(StandardCharsets.UTF_8);;
    private BasicContentHeaderProperties _props;

    byte[] _secretKeyEncoded = "secretkeyencoded0123456890abcdef".getBytes(StandardCharsets.US_ASCII);
    byte[] _initializeVector = "initializevector".getBytes(StandardCharsets.US_ASCII);
    private byte[] _unencrypted;
    private byte[] _encryptedMessage;
    private MessageEncryptionHelper _encryptionHelper;
    private SecretKeySpec _secretKeySpec;
    private KeyStore _keyStore;
    private MessageFactoryRegistry _messageFactoryRegistry;

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        final AMQSession session = mock(AMQSession.class);
        _messageFactoryRegistry = MessageFactoryRegistry.newDefaultRegistry(session);
        _messageFactory = new Encrypted091MessageFactory(_messageFactoryRegistry);


        _props = new BasicContentHeaderProperties();
        _props.setContentType("text/plain");

        final int headerLength = _props.getPropertyListSize() + 2;
        _unencrypted = new byte[headerLength + _data.length];
        BytesDataOutput output = new BytesDataOutput(_unencrypted);
        output.writeShort((short) (_props.getPropertyFlags() & 0xffff));
        _props.writePropertyListPayload(output);



        System.arraycopy(_data,0,_unencrypted,headerLength,_data.length);

        _secretKeySpec = new SecretKeySpec(_secretKeyEncoded, "AES");
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, _secretKeySpec, new IvParameterSpec(_initializeVector));
        _encryptedMessage = cipher.doFinal(_unencrypted);
        _keyStore = KeyStore.getInstance("JKS");
        _keyStore.load(new FileInputStream(TestSSLConstants.KEYSTORE), TestSSLConstants.KEYSTORE_PASSWORD.toCharArray());

        final AMQConnection connection = mock(AMQConnection.class);
        final ConnectionSettings settings = mock(ConnectionSettings.class);

        when(session.getAMQConnection()).thenReturn(connection);
        when(connection.getConnectionSettings()).thenReturn(settings);
        when(settings.getEncryptionTrustStore(any(ConnectionSettings.RemoteStoreFinder.class))).thenReturn(_keyStore);
        when(settings.getEncryptionKeyStore()).thenReturn(_keyStore);
        when(settings.getEncryptionKeyStorePassword()).thenReturn(TestSSLConstants.KEYSTORE_PASSWORD);

        _encryptionHelper = new MessageEncryptionHelper(session);
        when(session.getMessageEncryptionHelper()).thenReturn(_encryptionHelper);

    }

    public void testDecryptsMessage() throws Exception
    {
        if(isStrongEncryptionEnabled())
        {
            final List<MessageEncryptionHelper.KeyTransportRecipientInfo> recipientInfo =
                    _encryptionHelper.getKeyTransportRecipientInfo(Collections.singletonList(((X509Certificate) _keyStore
                            .getCertificate(
                                    "app1")).getSubjectX500Principal().getName(
                            X500Principal.CANONICAL)), _secretKeySpec);

            List<List<Object>> recipientHeader = new ArrayList<>();
            for (MessageEncryptionHelper.KeyTransportRecipientInfo info : recipientInfo)
            {
                recipientHeader.add(info.asList());
            }

            BasicContentHeaderProperties props = new BasicContentHeaderProperties();
            props.getHeaders().setObject(MessageEncryptionHelper.ENCRYPTION_ALGORITHM_PROPERTY,
                                         MessageEncryptionHelper.DEFAULT_MESSAGE_ENCRYPTION_CIPHER_NAME);
            props.getHeaders().setObject(MessageEncryptionHelper.ENCRYPTED_KEYS_PROPERTY, recipientHeader);
            props.getHeaders().setObject(MessageEncryptionHelper.KEY_INIT_VECTOR_PROPERTY, _initializeVector);

            final AbstractJMSMessage message =
                    _messageFactory.createMessage(new AMQMessageDelegate_0_8(props, 1l),
                                                  ByteBuffer.wrap(_encryptedMessage));


            assertTrue("message is not a text message", message instanceof JMSTextMessage);
            assertEquals("Message content not as expected", TEXT_MESSAGE_CONTENT, ((JMSTextMessage) message).getText());
        }
    }

    private boolean isStrongEncryptionEnabled() throws NoSuchAlgorithmException
    {
        return Cipher.getMaxAllowedKeyLength("AES")>=256;
    }

}
