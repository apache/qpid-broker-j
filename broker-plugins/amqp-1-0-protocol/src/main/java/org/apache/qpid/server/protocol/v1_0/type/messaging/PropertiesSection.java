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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v1_0.codec.DescribedTypeConstructor;
import org.apache.qpid.server.protocol.v1_0.type.messaging.codec.PropertiesConstructor;

public class PropertiesSection extends AbstractSection<Properties, Properties>
{
    public PropertiesSection(final QpidByteBuffer encodedForm)
    {
        super(encodedForm);
    }

    PropertiesSection(final Properties properties)
    {
       super(properties);
    }

    PropertiesSection(final PropertiesSection propertiesSection)
    {
        super(propertiesSection);
    }

    @Override
    public PropertiesSection copy()
    {
        return new PropertiesSection(this);
    }

    @Override
    protected DescribedTypeConstructor<Properties> createNonEncodingRetainingSectionConstructor()
    {
        return new PropertiesConstructor();
    }

}
