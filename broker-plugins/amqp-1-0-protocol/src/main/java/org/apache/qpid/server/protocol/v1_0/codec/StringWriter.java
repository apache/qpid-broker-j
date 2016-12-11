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

package org.apache.qpid.server.protocol.v1_0.codec;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringWriter extends SimpleVariableWidthWriter<String>
{
    private static final Charset ENCODING_CHARSET;
    private static final byte ONE_BYTE_CODE;
    private static final byte FOUR_BYTE_CODE;
    static
    {
        Charset defaultCharset = Charset.defaultCharset();
        if(defaultCharset.name().equals("UTF-16") || defaultCharset.name().equals("UTF-16BE"))
        {
            ENCODING_CHARSET = defaultCharset;
            ONE_BYTE_CODE = (byte) 0xa2;
            FOUR_BYTE_CODE = (byte) 0xb2;
        }
        else
        {
            ENCODING_CHARSET = StandardCharsets.UTF_8;
            ONE_BYTE_CODE = (byte) 0xa1;
            FOUR_BYTE_CODE = (byte) 0xb1;
        }
    }

    public StringWriter(final String value)
    {
        super(value.getBytes(ENCODING_CHARSET));
    }

    @Override
    protected byte getFourOctetEncodingCode()
    {
        return FOUR_BYTE_CODE;
    }

    @Override
    protected byte getSingleOctetEncodingCode()
    {
        return ONE_BYTE_CODE;
    }


    private static Factory<String> FACTORY = new Factory<String>()
                                            {

                                                @Override
                                                public ValueWriter<String> newInstance(final Registry registry,
                                                                                       final String object)
                                                {
                                                    return new StringWriter(object);
                                                }
                                            };

    public static void register(ValueWriter.Registry registry)
    {
        registry.register(String.class, FACTORY);
    }
}
