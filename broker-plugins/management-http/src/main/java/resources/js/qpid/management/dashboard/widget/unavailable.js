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

define(["dojo/_base/declare",
        "dojo/_base/lang",
        "dojo/json",
        "dojo/Deferred",
        "dojo/Evented",
        "dojox/widget/Portlet",
        "qpid/common/MessageDialog",
        "qpid/common/util"],
    function (declare,
              lang,
              json,
              Deferred,
              Evented,
              Portlet,
              MessageDialog,
              util)
    {

        return declare(Evented, {
            portlet : null,

            constructor: function (kwargs)
            {
                this.widgetSettings = kwargs.widgetSettings || {};
            },
            createPortlet: function ()
            {
                var portlet = new Portlet({
                    title: "Unvailable",
                    content: "This widget is currently not available",
                    open: !this.widgetSettings.hidden,
                    onClose: lang.hitch(this, function ()
                    {
                        MessageDialog.confirm({
                            title: "Remove widget?",
                            message: "Are you sure you want to remove the widget?"
                                     + "<br/>"
                                     + "It might become available at a later point.",
                            confirmationId: "dashboard.confirmation.widget.delete"
                        })
                            .then(lang.hitch(this, function ()
                            {
                                this.emit("close");
                            }));
                    })
                });

                if (portlet.closeIcon)
                {
                    portlet.closeIcon.title = "Remove this query from the dashboard.";
                    util.stopEventPropagation(portlet.closeIcon, "mousedown");
                }

                if (portlet.arrowNode)
                {
                    portlet.arrowNode.title = "Maximise/minimise this widget.";
                    util.stopEventPropagation(portlet.arrowNode, "mousedown");
                }

                portlet.on("hide", lang.hitch(this, function(){
                    this.widgetSettings.hidden = true;
                    this.emit("change");
                }));
                portlet.on("show", lang.hitch(this, function(){
                    this.widgetSettings.hidden = false;
                    this.emit("change");
                }));

                this.portlet = portlet;
                var deferred = new Deferred();
                deferred.resolve(portlet);

                return deferred.promise;
            },
            destroy : function ()
            {
                if (this.portlet)
                {
                    this.portlet.destroyRecursive();
                }
            },
            getSettings: function ()
            {
                return this.widgetSettings;
            },
            activate: function ()
            {
                // noop
            },
            deactivate: function ()
            {
                // noop
            }
        });

    });
