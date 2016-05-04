/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

define(["dojo/dom",
        "dojo/dom-construct",
        "dojo/_base/window",
        "dijit/registry",
        "dojo/parser",
        "dojo/_base/array",
        "dojo/_base/event",
        'dojo/_base/json',
        "dojo/store/Memory",
        "dijit/form/FilteringSelect",
        "dojo/query",
        "dojo/_base/connect",
        "qpid/common/util",
        "dojo/text!moveCopyMessages.html",
        "dojo/domReady!"],
    function (dom,
              construct,
              win,
              registry,
              parser,
              array,
              event,
              json,
              Memory,
              FilteringSelect,
              query,
              connect,
              util,
              template)
    {

        var moveMessages = {};

        var node = construct.create("div", null, win.body(), "last");

        var theForm;
        node.innerHTML = template;
        moveMessages.dialogNode = dom.byId("moveMessages");
        parser.instantiate([moveMessages.dialogNode]);
        moveMessages.dialog = registry.byId("moveMessages");
        moveMessages.submitButton = registry.byId("moveMessageSubmit");

        theForm = registry.byId("formMoveMessages");

        var cancelButton = registry.byId("moveMessageCancel");

        connect.connect(cancelButton, "onClick", function (evt)
        {
            event.stop(evt);
            moveMessages.dialog.hide();
        });

        theForm.on("submit", function (e)
        {

            event.stop(e);
            if (theForm.validate())
            {
                var destination = theForm.getValues()["queue"]
                var messageIds = moveMessages.data.messages
                var modelObj = {
                    type: "queue",
                    name: moveMessages.data.move ? "moveMessages" : "copyMessages",
                    parent: moveMessages.modelObj
                };
                var parameters = {
                    destination: destination,
                    messageIds: messageIds
                };
                moveMessages.management.update(modelObj, parameters)
                    .then(function (result)
                    {
                        moveMessages.dialog.hide();
                        if (moveMessages.next)
                        {
                            moveMessages.next();
                        }
                    });
                return false;
            }
            else
            {
                alert('Form contains invalid data.  Please correct first');
                return false;
            }

        });

        moveMessages.show = function (management, modelObj, data, next)
        {
            var that = this;
            moveMessages.modelObj = modelObj;
            moveMessages.management = management;
            moveMessages.data = data;
            moveMessages.next = next;
            registry.byId("formMoveMessages")
                .reset();

            var label = data.move ? "Move messages" : "Copy messages";
            moveMessages.submitButton.set("label", label);
            moveMessages.dialog.set("title", label);

            management.load({
                    type: "queue",
                    parent: modelObj.parent
                },
                {
                    depth: 0,
                    excludeInheritedContext: true
                })
                .then(function (data)
                {
                    var queues = [];
                    for (var i = 0; i < data.length; i++)
                    {
                        if (data[i].name != modelObj.name)
                        {
                            queues.push({
                                id: data[i].name,
                                name: data[i].name
                            });
                        }
                    }
                    var queueStore = new Memory({data: queues});

                    if (that.queueChooser)
                    {
                        that.queueChooser.destroy(false);
                    }
                    var queueDiv = dom.byId("moveMessages.selectQueueDiv");
                    var input = construct.create("input", {id: "moveMessagesSelectQueue"}, queueDiv);

                    that.queueChooser = new FilteringSelect({
                        id: "moveMessagesSelectQueue",
                        name: "queue",
                        store: queueStore,
                        searchAttr: "name"
                    }, input);

                    moveMessages.dialog.show();

                }, util.xhrErrorHandler);

        };

        return moveMessages;
    });
