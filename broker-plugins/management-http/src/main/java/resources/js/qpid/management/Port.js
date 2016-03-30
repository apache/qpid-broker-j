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
        "dojo/_base/connect",
        "dijit/registry",
        "dojox/html/entities",
        "qpid/common/properties",
        "qpid/common/updater",
        "qpid/common/util",
        "qpid/common/formatter",
        "qpid/management/addPort",
        "dojo/text!showPort.html",
        "dojo/domReady!"],
       function (dom, parser, query, connect, registry, entities, properties, updater, util, formatter, addPort, template) {

           function Port(name, parent, controller) {
               this.name = name;
               this.controller = controller;
               this.management = controller.management;
               this.modelObj = { type: "port", name: name, parent: parent};
           }

           Port.prototype.getTitle = function() {
               return "Port: " + this.name;
           };

           Port.prototype.open = function(contentPane) {
               var that = this;
               this.contentPane = contentPane;

                contentPane.containerNode.innerHTML = template;
                parser.parse(contentPane.containerNode).then(function(instances)
                {
                            that.portUpdater = new PortUpdater(that);

                            var deletePortButton = query(".deletePortButton", contentPane.containerNode)[0];
                            var node = registry.byNode(deletePortButton);
                            connect.connect(node, "onClick",
                                function(evt){
                                    that.deletePort();
                                });

                            var editPortButton = query(".editPortButton", contentPane.containerNode)[0];
                            var node = registry.byNode(editPortButton);
                            connect.connect(node, "onClick",
                                function(evt){
                                  that.showEditDialog();
                                });

                            that.portUpdater.update(function(){updater.add( that.portUpdater );});
                });
           };

           Port.prototype.close = function() {
               updater.remove( this.portUpdater );
           };


           Port.prototype.deletePort = function() {
               if(confirm("Are you sure you want to delete port '" + entities.encode(this.name) + "'?")) {
                   var that = this;
                   this.management.remove(this.modelObj).then(
                       function(data) {
                           that.contentPane.onClose()
                           that.controller.tabContainer.removeChild(that.contentPane);
                           that.contentPane.destroyRecursive();
                           that.close();
                       },
                       util.xhrErrorHandler);
               }
           }

           Port.prototype.showEditDialog = function() {
               var that = this;
               this.management.load(that.modelObj.parent)
               .then(function(data)
                     {
                         var brokerData= data[0];
                         addPort.show(that.management, that.modelObj, that.portUpdater.portData.type, brokerData.authenticationproviders, brokerData.keystores, brokerData.truststores);
                     },
                     util.xhrErrorHandler
               );
           }

           function PortUpdater(portTab)
           {
               var that = this;
               this.tabObject = portTab;
               this.management = portTab.controller.management;
               this.modelObj = portTab.modelObj;
               var containerNode = portTab.contentPane.containerNode;

               function findNode(name) {
                   return query("." + name, containerNode)[0];
               }

               function storeNodes(names)
               {
                  for(var i = 0; i < names.length; i++) {
                      that[names[i]] = findNode(names[i]);
                  }
               }

               storeNodes(["nameValue",
                           "stateValue",
                           "typeValue",
                           "portValue",
                           "authenticationProviderValue",
                           "protocolsValue",
                           "transportsValue",
                           "bindingAddressValue",
                           "keyStoreValue",
                           "needClientAuthValue",
                           "wantClientAuthValue",
                           "trustStoresValue",
                           "connectionCountValue",
                           "maxOpenConnectionsValue",
                           "authenticationProvider",
                           "bindingAddress",
                           "keyStore",
                           "needClientAuth",
                           "wantClientAuth",
                           "clientCertRecorderValue",
                           "trustStores",
                           "maxOpenConnections",
                           "threadPoolMinimum",
                           "threadPoolMaximum",
                           "threadPoolSize",
                           "threadPoolMinimumValue",
                           "threadPoolMaximumValue",
                           "threadPoolSizeValue",
                           "portTypeSpecificDetails",
                           "portAttributes"
                           ]);
           }

           PortUpdater.prototype.updateHeader = function()
           {
               function printArray(fieldName, object)
               {
                   var array = object[fieldName];
                   var data = "<div>";
                   if (array) {
                       for(var i = 0; i < array.length; i++) {
                           data+= "<div>" + entities.encode(array[i]) + "</div>";
                       }
                   }
                   return data + "</div>";
               }

              this.nameValue.innerHTML = entities.encode(String(this.portData[ "name" ]));
              this.stateValue.innerHTML = entities.encode(String(this.portData[ "state" ]));
              this.typeValue.innerHTML = entities.encode(String(this.portData[ "type" ]));
              this.portValue.innerHTML = entities.encode(String(this.portData[ "port" ]));
              this.authenticationProviderValue.innerHTML = this.portData[ "authenticationProvider" ] ? entities.encode(String(this.portData[ "authenticationProvider" ])) : "";
              this.protocolsValue.innerHTML = printArray( "protocols", this.portData);
              this.transportsValue.innerHTML = printArray( "transports", this.portData);
              this.bindingAddressValue.innerHTML = this.portData[ "bindingAddress" ] ? entities.encode(String(this.portData[ "bindingAddress" ])) : "" ;
              this.connectionCountValue.innerHTML = this.portData[ "connectionCount" ] ? entities.encode(String(this.portData[ "connectionCount" ])) : "0" ;
              this.maxOpenConnectionsValue.innerHTML = (this.portData[ "maxOpenConnections" ] && this.portData[ "maxOpenConnections" ] >= 0) ? entities.encode(String(this.portData[ "maxOpenConnections" ])) : "(no limit)" ;
              this.threadPoolMinimumValue.innerHTML = entities.encode(String(this.portData["threadPoolMinimum"] || ""));
              this.threadPoolMaximumValue.innerHTML = entities.encode(String(this.portData["threadPoolMaximum"] || ""));
              this.threadPoolSizeValue.innerHTML = entities.encode(String(this.portData["threadPoolSize"] || ""));

              this.keyStoreValue.innerHTML = this.portData[ "keyStore" ] ? entities.encode(String(this.portData[ "keyStore" ])) : "";
              this.needClientAuthValue.innerHTML = "<input type='checkbox' disabled='disabled' "+(this.portData[ "needClientAuth" ] ? "checked='checked'": "")+" />" ;
              this.wantClientAuthValue.innerHTML = "<input type='checkbox' disabled='disabled' "+(this.portData[ "wantClientAuth" ] ? "checked='checked'": "")+" />" ;
              this.trustStoresValue.innerHTML = printArray( "trustStores", this.portData);
              this.clientCertRecorderValue.innerHTML = this.portData[ "clientCertRecorder"]? entities.encode(String(this.portData[ "clientCertRecorder"])):"";

              var typeMetaData = this.management.metadata.getMetaData("Port", this.portData["type"]);

              this.authenticationProvider.style.display = "authenticationProvider" in typeMetaData.attributes ? "block" : "none";
              this.bindingAddress.style.display = "bindingAddress" in typeMetaData.attributes ? "block" : "none";
              this.keyStore.style.display = "keyStore" in typeMetaData.attributes ? "block" : "none";
              this.needClientAuth.style.display = "needClientAuth" in typeMetaData.attributes ? "block" : "none";
              this.wantClientAuth.style.display = "wantClientAuth" in typeMetaData.attributes ? "block" : "none";
              this.trustStores.style.display = "trustStores" in typeMetaData.attributes ? "block" : "none";

              var hasThreadPoolMinMaxSettings = "threadPoolMaximum" in typeMetaData.attributes;
              this.threadPoolMinimum.style.display = hasThreadPoolMinMaxSettings ? "block" : "none";
              this.threadPoolMaximum.style.display = hasThreadPoolMinMaxSettings ? "block" : "none";

              var hasThreadPoolSizeSettings = "threadPoolSize" in typeMetaData.attributes;
              this.threadPoolSize.style.display = hasThreadPoolSizeSettings ? "block" : "none";

              this.maxOpenConnections.style.display = "maxOpenConnections" in typeMetaData.attributes ? "block" : "none";

           };

           PortUpdater.prototype.update = function(callback)
           {

              var thisObj = this;

              this.management.load(this.modelObj).then(function(data)
                   {
                      thisObj.portData = data[0] || {};
                      thisObj.updateUI(thisObj.portData);
                      util.flattenStatistics( thisObj.portData );
                      if (callback)
                      {
                        callback();
                      }
                      thisObj.updateHeader();
                   },
                   function(error)
                   {
                      util.tabErrorHandler(error, {updater:thisObj,
                                                   contentPane: thisObj.tabObject.contentPane,
                                                   tabContainer: thisObj.tabObject.controller.tabContainer,
                                                   name: thisObj.modelObj.name,
                                                   category: "Port"});
                   });
           };

           PortUpdater.prototype.updateUI = function(data)
           {
               if (!this.details)
               {
                   var that = this;
                   require(["qpid/management/port/" + data.type.toLowerCase() + "/show"],
                       function (Details)
                       {
                           that.details = new Details({
                               containerNode: that.portAttributes,
                               typeSpecificDetailsNode: that.portTypeSpecificDetails,
                               metadata: that.tabObject.management.metadata,
                               data: data,
                               management: that.tabObject.management,
                               modelObj: that.tabObject.modelObj,
                               portUpdater: that.portUpdater
                           });
                       }
                   );
               }
               else
               {
                   this.details.update(data);
               }
           }


           return Port;
       });
