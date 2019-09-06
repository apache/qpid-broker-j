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
define(["dojo/dom-construct",
        "qpid/common/util",
        "dojo/parser",
        "dojo/text!logger/jdbc/add.html",
        "qpid/management/store/pool/ConnectionPool",
        "dojo/domReady!"],
    function (domConstruct, util, parser, template, ConnectionPool) {

    return {
        show: function (data) {
            var that = this;
            if (data.hasOwnProperty("containerNode"))
            {
                var containerNode = domConstruct.create("div", {innerHTML: template}, data.containerNode);
                parser.parse(containerNode)
                    .then(function () {
                        ConnectionPool.initPoolFieldsInDialog("addLogger", data);
                        util.applyToWidgets(data.containerNode, data.category, data.type, data.data, data.metadata);
                    });
            }
        }
    };
});
