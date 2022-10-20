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
define(["qpid/common/util",
        "dijit/registry",
        "dojo/query",
        "dojo/_base/lang",
        "dojo/text!logger/virtualhostlogger/show.html",
        "qpid/common/CategoryTabExtension",
        "dojo/domReady!"], function (util, registry, query, lang, template, CategoryTabExtension)
{
    function VirtualHostLogger(params)
    {
        var categoryExtensionParams = lang.mixin(params, {
            template: template,
            typeSpecificAttributesClassName: "typeSpecificAttributes",
            baseUrl: "qpid/management/logger/virtualhostlogger/"
        });
        CategoryTabExtension.call(this, categoryExtensionParams);
    }

    util.extend(VirtualHostLogger, CategoryTabExtension);

    VirtualHostLogger.prototype.postParse = function (containerNode)
    {
        this._resetStatisticsButton = registry.byNode(query(".resetStatistics", containerNode)[0]);
        this._resetStatisticsButton.on("click", lang.hitch(this, this.resetStatistics));
    }

    VirtualHostLogger.prototype.resetStatistics = function ()
    {
        util.resetStatistics(this.management,
                             this.modelObj,
                             this._resetStatisticsButton,
                             "VirtualHostLogger",
                             this.modelObj.name);
    }

    return VirtualHostLogger;
});
