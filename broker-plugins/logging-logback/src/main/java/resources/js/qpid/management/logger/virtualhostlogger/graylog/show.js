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
 *
 */
define(["qpid/common/util",
        "dojo/text!logger/graylog/show.html",
        "dojo/text!logger/graylog/showStaticField.html",
        "qpid/common/TypeTabExtension",
        "dojo/domReady!"],
    function (util, template, fieldTemplate, TypeTabExtension)
    {
        function Graylog(params)
        {
            const type = "Graylog";
            const category = "VirtualHostLogger";

            this.containerNode = params.containerNode;

            TypeTabExtension.call(this,
                params.containerNode,
                template,
                category,
                type,
                params.metadata,
                params.data);
        }

        util.extend(Graylog, TypeTabExtension);

        Graylog.prototype.update = function (restData)
        {
            util.updateAttributeNodes(this.attributeContainers, restData, util.updateBooleanAttributeNode,
                (containerObject, data, utl) => util.updateMapAttributeNode(containerObject, data, utl, fieldTemplate));
        };

        return Graylog;
    });
