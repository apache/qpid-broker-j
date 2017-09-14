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
define(["dojo/dom",
        "dojo/parser",
        "dojo/query",
        "dojo/dom-construct",
        "dojo/_base/connect",
        "dojo/_base/window",
        "dojo/_base/event",
        "dojo/_base/json",
        "dijit/registry",
        "qpid/common/util",
        "qpid/common/properties",
        "qpid/common/updater",
        "dojo/text!plugin/showManagementHttp.html",
        "qpid/management/plugin/managementhttp/edit",
        "dijit/form/Button",
        "dijit/form/ValidationTextBox",
        "dijit/form/CheckBox",
        "dijit/form/NumberSpinner",
        "dojo/domReady!"],
    function (dom,
              parser,
              query,
              construct,
              connect,
              win,
              event,
              json,
              registry,
              util,
              properties,
              updater,
              template,
              editor)
    {

        function ManagementHttp(containerNode, pluginObject, controller, contentPane)
        {
            var that = this;
            this.node = construct.create("div", null, containerNode, "last");
            this.name = pluginObject.name;
            this.contentPane = contentPane;
            this.pluginObject = pluginObject;
            this.modelObj = pluginObject;
            this.controller = controller;
            this.management = controller.management;
            this.node.innerHTML = template;
            parser.parse(this.node)
                .then(function (instances)
                {
                    var meta = that.management.metadata;
                    var typeMetaData = meta.getMetaData("Plugin", "MANAGEMENT-HTTP");

                    var methodsMultiSelect = dom.byId("formEditHttpPlugin.corsAllowMethods");
                    var methodsValidValues = typeMetaData.attributes.corsAllowMethods.validValues;
                    var methodsValues = meta.extractUniqueListOfValues(methodsValidValues);
                    util.setMultiSelectOptions(methodsMultiSelect, methodsValues.sort());

                    that.managementHttpUpdater = new ManagementHttpUpdater(that);
                    that.managementHttpUpdater.update(function ()
                    {
                        updater.add(that.managementHttpUpdater)
                    });

                    var editButton = query(".editPluginButton", that.node)[0];
                    connect.connect(registry.byNode(editButton), "onClick", function (evt)
                    {
                        that.edit();
                    });
                });
        }

        ManagementHttp.prototype.close = function ()
        {
            updater.remove(this.managementHttpUpdater);
        };

        ManagementHttp.prototype.edit = function ()
        {
            editor.show(this.management, this.modelObj, this.managementHttpUpdater.pluginData);
        };

        function ManagementHttpUpdater(tabObject)
        {
            var node = tabObject.node;
            this.contentPane = tabObject.contentPane;
            this.controller = tabObject.controller;
            this.modelObj = tabObject.pluginObject;
            this.name = tabObject.pluginObject.name;
            this.httpBasicAuthenticationEnabled = query(".httpBasicAuthenticationEnabled", node)[0];
            this.httpsBasicAuthenticationEnabled = query(".httpsBasicAuthenticationEnabled", node)[0];
            this.sessionTimeout = query(".sessionTimeout", node)[0];
            this.httpsSaslAuthenticationEnabled = query(".httpsSaslAuthenticationEnabled", node)[0];
            this.httpSaslAuthenticationEnabled = query(".httpSaslAuthenticationEnabled", node)[0];
            this.compressResponses = query(".compressResponses", node)[0];
            this.corsAllowOrigins = query(".corsAllowOrigins", node)[0];
            this.corsAllowMethods = query(".corsAllowMethods", node)[0];
            this.corsAllowHeaders = query(".corsAllowHeaders", node)[0];
            this.corsAllowCredentials = query(".corsAllowCredentials", node)[0];
            this.management = this.controller.management;
        }

        ManagementHttpUpdater.prototype.update = function (callback)
        {
            if (!this.contentPane.selected && !callback)
            {
                return;
            }

            var that = this;

            function showBoolean(val)
            {
                return "<input type='checkbox' disabled='disabled' " + (val ? "checked='checked'" : "") + " />";
            }

            this.management.load(this.modelObj, {excludeInheritedContext: true})
                .then(function (data)
                {
                    that.pluginData = data;
                    that.httpBasicAuthenticationEnabled.innerHTML =
                        showBoolean(that.pluginData.httpBasicAuthenticationEnabled);
                    that.httpsBasicAuthenticationEnabled.innerHTML =
                        showBoolean(that.pluginData.httpsBasicAuthenticationEnabled);
                    that.httpsSaslAuthenticationEnabled.innerHTML =
                        showBoolean(that.pluginData.httpsSaslAuthenticationEnabled);
                    that.httpSaslAuthenticationEnabled.innerHTML =
                        showBoolean(that.pluginData.httpSaslAuthenticationEnabled);
                    that.compressResponses.innerHTML = showBoolean(that.pluginData.compressResponses);
                    that.sessionTimeout.innerHTML = that.pluginData.sessionTimeout;
                    that.httpSaslAuthenticationEnabled.innerHTML =
                        showBoolean(that.pluginData.httpSaslAuthenticationEnabled);
                    that.corsAllowOrigins.innerHTML = that.pluginData.corsAllowOrigins;
                    that.corsAllowMethods.innerHTML = that.pluginData.corsAllowMethods;
                    that.corsAllowHeaders.innerHTML = that.pluginData.corsAllowHeaders;
                    that.corsAllowCredentials.innerHTML = showBoolean(that.pluginData.corsAllowCredentials);

                    if (callback)
                    {
                        callback();
                    }
                }, function (error)
                {
                    util.tabErrorHandler(error, {
                        updater: that,
                        contentPane: that.contentPane,
                        tabContainer: that.controller.tabContainer,
                        name: that.modelObj.name,
                        category: "Plugin HTTP Management"
                    });
                });

        };

        return ManagementHttp;
    });
