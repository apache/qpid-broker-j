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
define(["dojo/query",
        "dijit/registry",
        "qpid/common/util",
        "dojo/store/Memory",
        "dijit/form/RadioButton",
        "dijit/form/FilteringSelect",
        "dijit/form/ValidationTextBox",
        "dijit/form/CheckBox"], function (query, registry, util, Memory)
{
    return {
        show: function (data)
        {
            var that = this;
            util.parseHtmlIntoDiv(data.containerNode, "authenticationprovider/simpleldap/add.html", function ()
            {
                that._postParse(data);
            });
        },
        _postParse: function (data)
        {
            var that = this;

            this.groupInfoAttributeName = registry.byId('ldapGroupInfoRadioButtonAttributeContentAttrName');
            this.groupInfoSearchContext = registry.byId('ldapGroupInfoRadioButtonQueryContentSearchContext');
            this.groupInfoSearchFilter = registry.byId('ldapGroupInfoRadioButtonQueryContentSearchFilter');
            this.groupInfoSubtreeSearch = registry.byId('ldapGroupInfoRadioButtonQueryContentSubtreeSearch');

            registry.byId("ldapGroupInfoRadioButtonNone").on("change", function(isChecked){
                if(isChecked){
                    that.groupInfoAttributeName.set('disabled', true);

                    that.groupInfoSearchContext.set('disabled', true);
                    that.groupInfoSearchFilter.set('disabled', true);
                    that.groupInfoSubtreeSearch.set('disabled', true);
                }
            }, true);
            registry.byId("ldapGroupInfoRadioButtonAttribute").on("change", function(isChecked){
                if(isChecked){
                    that.groupInfoAttributeName.set('disabled', false);

                    that.groupInfoSearchContext.set('disabled', true);
                    that.groupInfoSearchFilter.set('disabled', true);
                    that.groupInfoSubtreeSearch.set('disabled', true);
                }
            }, true);
            registry.byId("ldapGroupInfoRadioButtonQuery").on("change", function(isChecked){
                if(isChecked){
                    that.groupInfoAttributeName.set('disabled', true);

                    that.groupInfoSearchContext.set('disabled', false);
                    that.groupInfoSearchFilter.set('disabled', false);
                    that.groupInfoSubtreeSearch.set('disabled', false);
                }
            }, true);

            var obj = {
                type: "truststore",
                parent: {type: "broker"}
            };
            data.parent.management.load(obj, {excludeInheritedContext: true})
                .then(function (trustStores)
                {
                    that._initTrustStores(trustStores, data.containerNode);
                    that._initAuthenticationMethods(data.parent.management.metadata, data.containerNode);
                    if (data.data)
                    {
                        that._initFields(data.data, data.containerNode, data.parent.management.metadata);
                        if (data.data.groupAttributeName)
                        {
                            registry.byId("ldapGroupInfoRadioButtonAttribute").set('checked', true);
                        }
                        else if (data.data.groupSearchFilter || data.data.groupSearchContext)
                        {
                            registry.byId("ldapGroupInfoRadioButtonQuery").set('checked', true);
                        }
                    }
                }, util.xhrErrorHandler);
        },

        _preSubmit: function(formData)
        {
            if ("none" === formData.groupInfo)
            {
                formData.groupAttributeName = "";

                formData.groupSearchContext = "";
                formData.groupSearchFilter = "";
                formData.groupSubtreeSearchScope = false;
            }
            else if ("attribute" === formData.groupInfo)
            {
                formData.groupSearchContext = "";
                formData.groupSearchFilter = "";
                formData.groupSubtreeSearchScope = false;
            }
            else if ("query" === formData.groupInfo)
            {
                formData.groupAttributeName = "";
            }
            else
            {
                console.error("Unexpected value of 'groupInfo': " + formData.groupInfo);
            }
        },

        _initTrustStores: function (trustStores, containerNode)
        {
            var data = [];
            for (var i = 0; i < trustStores.length; i++)
            {
                data.push({
                    id: trustStores[i].name,
                    name: trustStores[i].name
                });
            }
            var trustStoresStore = new Memory({data: data});

            var trustStore = registry.byNode(query(".trustStore", containerNode)[0]);
            trustStore.set("store", trustStoresStore);
        },
        _initAuthenticationMethods: function (metadata, containerNode)
        {
            var attributes = metadata.getMetaData("AuthenticationProvider", "SimpleLDAP").attributes;
            var store = util.makeTypeStore(attributes.authenticationMethod.validValues);
            var authenticationMethod = registry.byId("ldapAuthenticationMethod");
            authenticationMethod.set("store", store);
            authenticationMethod.on("change", function (value) {
                registry.byId('ldapSearchUsername').set("required", (value === "SIMPLE"));
            });
        },
        _initFields: function (data, containerNode, metadata)
        {
            util.applyToWidgets(containerNode,
                "AuthenticationProvider",
                "SimpleLDAP",
                data,
                metadata);
        }
    };
});
