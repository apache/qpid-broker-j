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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.Collection;
import java.util.Iterator;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.security.auth.x500.X500Principal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.QpidException;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.framing.BasicContentHeaderProperties;
import org.apache.qpid.framing.ByteArrayDataInput;

public class Encrypted091MessageFactory extends AbstractJMSMessageFactory
{
    public static final String ENCRYPTED_0_9_1_CONTENT_TYPE = "application/qpid-0-9-1-encrypted";
    private static final Logger LOGGER = LoggerFactory.getLogger(Encrypted091MessageFactory.class);

    private final MessageFactoryRegistry _messageFactoryRegistry;

    public Encrypted091MessageFactory(final MessageFactoryRegistry messageFactoryRegistry)
    {
        _messageFactoryRegistry = messageFactoryRegistry;
    }

    @Override
    protected AbstractJMSMessage createMessage(final AbstractAMQMessageDelegate delegate, final ByteBuffer data)
            throws QpidException
    {
        SecretKeySpec secretKeySpec;
        String algorithm;
        byte[] initVector;
        try
        {


            try
            {
                if (delegate.hasProperty(MessageEncryptionHelper.ENCRYPTION_ALGORITHM_PROPERTY))
                {
                    algorithm = delegate.getProperty(MessageEncryptionHelper.ENCRYPTION_ALGORITHM_PROPERTY).toString();

                    if (delegate.hasProperty(MessageEncryptionHelper.KEY_INIT_VECTOR_PROPERTY))
                    {
                        Object ivObj = delegate.getProperty(MessageEncryptionHelper.KEY_INIT_VECTOR_PROPERTY);
                        if (ivObj instanceof byte[])
                        {
                            initVector = (byte[]) ivObj;
                        }
                        else
                        {
                            throw new QpidException("If the property '"
                                                   + MessageEncryptionHelper.KEY_INIT_VECTOR_PROPERTY
                                                   + "' is present, it must contain a byte array");
                        }
                    }
                    else
                    {
                        initVector = null;
                    }
                    if (delegate.hasProperty(MessageEncryptionHelper.ENCRYPTED_KEYS_PROPERTY))
                    {
                        Object keyInfoObj = delegate.getProperty(MessageEncryptionHelper.ENCRYPTED_KEYS_PROPERTY);
                        if (keyInfoObj instanceof Collection)
                        {
                            secretKeySpec = getContentEncryptionKey((Collection) keyInfoObj,
                                                                    algorithm,
                                                                    _messageFactoryRegistry.getSession());
                        }
                        else
                        {
                            throw new QpidException("An encrypted message must contain the property '"
                                                   + MessageEncryptionHelper.ENCRYPTED_KEYS_PROPERTY
                                                   + "'");
                        }
                    }
                    else
                    {
                        throw new QpidException("An encrypted message must contain the property '"
                                               + MessageEncryptionHelper.ENCRYPTED_KEYS_PROPERTY
                                               + "'");
                    }

                }
                else
                {
                    throw new QpidException("Encrypted message must carry the encryption algorithm in the property '"
                                           + MessageEncryptionHelper.ENCRYPTED_KEYS_PROPERTY
                                           + "'");
                }

                Cipher cipher = Cipher.getInstance(algorithm);
                cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, new IvParameterSpec(initVector));
                byte[] encryptedData;
                int offset;
                int length;
                if (data.hasArray())
                {
                    encryptedData = data.array();
                    offset = data.arrayOffset() + data.position();
                    length = data.remaining();
                }
                else
                {
                    encryptedData = new byte[data.remaining()];
                    data.duplicate().get(encryptedData);
                    offset = 0;
                    length = encryptedData.length;
                }
                final byte[] unencryptedBytes = decryptData(cipher, encryptedData, offset, length);

                BasicContentHeaderProperties properties = new BasicContentHeaderProperties();
                int payloadOffset;
                ByteArrayDataInput dataInput = new ByteArrayDataInput(unencryptedBytes);

                payloadOffset = properties.read(dataInput);


                final ByteBuffer unencryptedData =
                        ByteBuffer.wrap(unencryptedBytes, payloadOffset, unencryptedBytes.length - payloadOffset);

                final AbstractAMQMessageDelegate newDelegate =
                        new AMQMessageDelegate_0_8(properties, delegate.getDeliveryTag());
                newDelegate.setJMSDestination(delegate.getJMSDestination());


                final AbstractJMSMessageFactory unencryptedMessageFactory =
                        _messageFactoryRegistry.getMessageFactory(properties.getContentTypeAsString());

                return unencryptedMessageFactory.createMessage(newDelegate, unencryptedData);
            }
            catch (GeneralSecurityException | IOException e)
            {
                throw new QpidException("Could not decode encrypted message", e);
            }
        }
        catch(QpidException e)
        {
            LOGGER.error("Error when attempting to decrypt message " + delegate.getDeliveryTag() + " to address ("+delegate.getJMSDestination()+").  Message will be delivered to the client encrypted", e);
            return _messageFactoryRegistry.getDefaultFactory().createMessage(delegate, data);
        }

    }

    private byte[] decryptData(final Cipher cipher, final byte[] encryptedData, final int offset, final int length)
            throws IOException
    {
        final byte[] unencryptedBytes;
        try (CipherInputStream cipherInputStream = new CipherInputStream(new ByteArrayInputStream(encryptedData,
                                                                                                  offset,
                                                                                                  length), cipher))
        {
            byte[] buf = new byte[512];
            int pos = 0;
            int read;
            while ((read = cipherInputStream.read(buf, pos, buf.length - pos)) != -1)
            {
                pos += read;
                if (pos == buf.length)
                {
                    byte[] tmp = buf;
                    buf = new byte[buf.length + 512];
                    System.arraycopy(tmp, 0, buf, 0, tmp.length);
                }
            }
            unencryptedBytes= new byte[pos];
            System.arraycopy(buf, 0, unencryptedBytes, 0, pos);
        }
        return unencryptedBytes;
    }

    private SecretKeySpec getContentEncryptionKey(final Collection keyInfoObjList,
                                                  final String algorithm,
                                                  final AMQSession<?, ?> session)
            throws QpidException, GeneralSecurityException, IOException
    {

        for(Object keyInfoObject : keyInfoObjList)
        {
            try
            {
                Iterator iter = ((Collection)keyInfoObject).iterator();

                int type = ((Number)iter.next()).intValue();
                switch(type)
                {
                    case 1:
                        String keyEncryptionAlgorithm = (String) iter.next();
                        X500Principal issuer = new X500Principal((String)iter.next());
                        BigInteger serialNumber = new BigInteger((String)iter.next());
                        byte[] encryptedKey = (byte[])iter.next();

                        PrivateKey privateKey = getPrivateKey(session, issuer, serialNumber);
                        if(privateKey != null)
                        {
                            Cipher cipher = Cipher.getInstance(keyEncryptionAlgorithm);
                            cipher.init(Cipher.DECRYPT_MODE, privateKey);
                            byte[] decryptedData = decryptData(cipher, encryptedKey, 0, encryptedKey.length);
                            SecretKeySpec keySpec = new SecretKeySpec(decryptedData, algorithm.split("/")[0]);
                            return keySpec;
                        }
                        break;
                    default:
                        throw new QpidException("Invalid format of 'x-qpid-encrypted-keys' - unknown key info type: " + type);

                }
            }
            catch(ClassCastException e)
            {
                throw new QpidException("Invalid format of 'x-qpid-encrypted-keys'");
            }
        }
        return null;
    }

    private PrivateKey getPrivateKey(final AMQSession<?, ?> session,
                                     final X500Principal issuer,
                                     final BigInteger serialNumber)
            throws GeneralSecurityException, IOException
    {
        return session.getMessageEncryptionHelper().getEncryptionPrivateKey(issuer, serialNumber);
    }

}
