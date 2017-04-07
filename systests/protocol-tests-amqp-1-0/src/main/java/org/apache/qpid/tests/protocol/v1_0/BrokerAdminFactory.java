/*
 * Copyright 2017 by Lorenz Quack
 *
 * This file is part of qpid-java-build.
 *
 *     qpid-java-build is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 2 of the License, or
 *     (at your option) any later version.
 *
 *     qpid-java-build is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with qpid-java-build.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.apache.qpid.tests.protocol.v1_0;

import java.util.Map;

import org.apache.qpid.server.plugin.QpidServiceLoader;

public class BrokerAdminFactory
{
    BrokerAdmin createInstance(String type)
    {
        Map<String, BrokerAdmin> adminFacades = new QpidServiceLoader().getInstancesByType(BrokerAdmin.class);
        BrokerAdmin brokerAdmin = adminFacades.get(type);
        if (brokerAdmin == null)
        {
            throw new RuntimeException(String.format("Could not find BrokerAdmin implementation of type '%s'", type));
        }
        return brokerAdmin;
    }
}
