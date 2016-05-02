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
        "dojo/query",
        "dojo/_base/lang",
        "dojo/text!logger/brokerlogger/show.html",
        "qpid/common/CategoryTabExtension",
        "dojo/domReady!"], function (util, query, lang, template, CategoryTabExtension)
{
    function BrokerLogger(params)
    {
        var categoryExtensionParams = lang.mixin(params, {
            template: template,
            typeSpecificAttributesClassName: "typeSpecificAttributes",
            baseUrl: "qpid/management/logger/brokerlogger/"
        });
        CategoryTabExtension.call(this, categoryExtensionParams);
    }

    util.extend(BrokerLogger, CategoryTabExtension);

    BrokerLogger.prototype.postParse = function (containerNode)
    {
        this.virtualHostLogEventExcludedCheckboxContainer = query(".virtualHostLogEventExcluded", containerNode)[0];
    }

    BrokerLogger.prototype.update = function (restData)
    {
        var data = restData || {};
        this.virtualHostLogEventExcludedCheckboxContainer.innerHTML =
            util.buildCheckboxMarkup(data.virtualHostLogEventExcluded);
        CategoryTabExtension.prototype.update.call(this, restData);
    }

    return BrokerLogger;
});
