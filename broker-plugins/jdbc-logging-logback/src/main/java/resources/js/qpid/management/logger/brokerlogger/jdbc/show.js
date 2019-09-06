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
define(["qpid/common/util",
        "dojo/query",
        "dojo/text!logger/jdbc/show.html",
        "qpid/management/store/pool/ConnectionPool",
        "dojo/domReady!"], function (util, query, template, ConnectionPool) {
    function BrokerJdbcLogger(params)
    {
        var that = this;
        if (template && params.hasOwnProperty("containerNode"))
        {
            util.parse(params.containerNode, template, function () {
                if (params.hasOwnProperty("metadata"))
                {
                    that.attributeContainers =
                        util.collectAttributeNodes(params.containerNode, params.metadata, "BrokerLogger", "Jdbc");
                    var container = query(".connectionPoolTypeAttributeContainer", params.containerNode)[0];
                    that.poolSettings = new ConnectionPool(container, params.management, params.modelObj);

                    if (params.hasOwnProperty("data"))
                    {
                        that.update(params.data);
                    }
                }

            });
        }
    }

    BrokerJdbcLogger.prototype.update = function (data) {
        util.updateAttributeNodes(this.attributeContainers, data);
        if (this.poolSettings)
        {
            this.poolSettings.update(data);
        }
    };

    return BrokerJdbcLogger;
});
