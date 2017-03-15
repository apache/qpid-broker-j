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

define(["dojo/query",
        "dojo/_base/lang",
        "qpid/common/util",
        "dojox/grid/EnhancedGrid",
        "qpid/common/UpdatableStore",
        "dijit/registry",
        "dojo/domReady!"], function (query, lang, util, EnhancedGrid, UpdatableStore, registry)
{
    function addIdToCertificates(obj)
    {
        var certItems = [];
        var certDetails = obj.certificateDetails;
        for (var idx in certDetails)
        {
            var item = lang.mixin(certDetails[idx],
                {id: certDetails[idx].serialNumber + '|' + certDetails[idx].issuerName});
            certItems.push(item);
        }
        return certItems;
    }

    function ManagedCertificateStore(data)
    {
        this.fields = [];
        this.management = data.parent.management;
        this.modelObj = data.parent.modelObj;
        var containerNode = data.containerNode;
        var attributes = this.management.metadata.getMetaData("TrustStore", "ManagedCertificateStore").attributes;
        for (var name in attributes)
        {
            this.fields.push(name);
        }
        var that = this;
        var gridProperties = {
            height: 400,
            selectionMode: "extended",
            plugins: {
                indirectSelection: true,
                pagination: {
                    pageSizes: [10, 25, 50, 100],
                    description: true,
                    sizeSwitch: true,
                    pageStepper: true,
                    gotoButton: true,
                    maxPageStep: 4,
                    position: "bottom"
                }
            }
        };

        util.buildUI(data.containerNode,
            data.parent,
            "store/managedcertificatestore/show.html",
            this.fields,
            this,
            function ()
            {
                that.certificates = addIdToCertificates(that);
                that.certificatesGrid =
                    new UpdatableStore(that.certificates, query(".managedCertificatesGrid", containerNode)[0], [{
                        name: "Subject Name",
                        field: "subjectName",
                        width: "25%"
                    }, {
                        name: "Issuer Name",
                        field: "issuerName",
                        width: "25%"
                    }, {
                        name: "Serial #",
                        field: "serialNumber",
                        width: "10%"
                    }, {
                        name: "Valid From",
                        field: "validFrom",
                        width: "20%",
                        formatter: function (val)
                        {
                            return that.management.userPreferences.formatDateTime(val, {
                                addOffset: true,
                                appendTimeZone: true
                            });
                        }
                    }, {
                        name: "Valid Until",
                        field: "validUntil",
                        width: "20%",
                        formatter: function (val)
                        {
                            return that.management.userPreferences.formatDateTime(val, {
                                addOffset: true,
                                appendTimeZone: true
                            });
                        }
                    }], null, gridProperties, EnhancedGrid);
            });

        this.removeButton = registry.byNode(query(".removeCertificates", containerNode)[0]);
        this.removeButton.on("click", function (e)
        {
            that.removeCertificates()
        });

        this.addButton = registry.byNode(query(".addCertificate", containerNode)[0]);
        var addButton = this.addButton;
        var that = this;

        function uploadCertificate(cert)
        {
            var parentModelObj = that.modelObj;
            var modelObj = {
                type: parentModelObj.type,
                name: "addCertificate",
                parent: parentModelObj
            };
            var url = that.management.buildObjectURL(modelObj);

            that.management.post({url: url}, {certificate: cert})
                .then(uploadComplete, uploadError);
        }

        function uploadComplete()
        {
            addButton.set("disabled", false);
            addButton.reset();
        }

        function uploadError(error)
        {
            that.management.errorHandler(error);
            addButton.set("disabled", false);
            addButton.reset();
        }

        function onFileSelected()
        {
            if (addButton.domNode.children[0].files)
            {
                addButton.set("disabled", true);
                var file = addButton.domNode.children[0].files[0];
                var fileReader = new FileReader();
                fileReader.onload = function (evt)
                {
                    var result = fileReader.result;
                    if (result.indexOf("-----BEGIN CERTIFICATE-----") != -1)
                    {
                        uploadCertificate(result);

                    }
                    else
                    {
                        fileReader.onload = function (evt)
                        {
                            var binresult = fileReader.result;
                            binresult = binresult.substring(binresult.indexOf(",") + 1);
                            uploadCertificate(binresult);
                        };
                        fileReader.readAsDataURL(file);
                    }
                };
                fileReader.readAsText(file);
            }
        }

        if (window.FileReader)
        {
            this.addButton.on("change", onFileSelected);
        }
        else
        {
            this.addButton.set("disabled", true);
            this.addButton.domNode.style.display = "none";
        }

    }

    ManagedCertificateStore.prototype.removeCertificates = function ()
    {
        var data = this.certificatesGrid.grid.selection.getSelected();

        if (data.length)
        {
            this.removeButton.set("disabled", true);
            var parentModelObj = this.modelObj;
            var modelObj = {
                type: parentModelObj.type,
                name: "removeCertificates",
                parent: parentModelObj
            };
            var items = [];
            for (var i = 0; i < data.length; i++)
            {
                var parts = data[i].id.split("|");
                items.push({
                    issuerName: parts[1],
                    serialNumber: parts[0]
                });
            }
            var url = this.management.buildObjectURL(modelObj);
            this.management.post({url: url}, {certificates: items})
                .then(null, management.xhrErrorHandler)
                .always(lang.hitch(this, function ()
                {
                    this.removeButton.set("disabled", false);
                }));
        }
    };

    ManagedCertificateStore.prototype.update = function (data)
    {
        util.updateUI(data, this.fields, this);
        this.certificates = addIdToCertificates(data);
        this.certificatesGrid.update(this.certificates)
    };

    return ManagedCertificateStore;
});
