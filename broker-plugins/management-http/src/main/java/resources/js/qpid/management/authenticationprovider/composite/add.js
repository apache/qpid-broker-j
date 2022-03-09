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
define([
    'dojo/dom',
    'dojo/query',
    'dijit/registry',
    'qpid/common/util'
], function (
    dom,
    query,
    registry,
    util
)
{
    return {
        show: function (data)
        {
            util.parseHtmlIntoDiv(data.containerNode, 'authenticationprovider/composite/add.html', function ()
            {
                if (!!data.data)
                {
                    util.applyToWidgets(data.containerNode,
                        'AuthenticationProvider',
                        'Composite',
                        data.data,
                        data.metadata);
                }
                this.data = data;
                const that = this;
                this.management.load({type: 'authenticationprovider'}, {excludeInheritedContext: true, depth: 1})
                    .then(function (data)
                    {
                        const allowed = ['MD5', 'Plain', 'SCRAM-SHA-1', 'SCRAM-SHA-256', 'SimpleLDAP'];
                        const names = data
                            .filter(authProvider => allowed.includes(authProvider.type))
                            .map(authProvider => authProvider.name);

                        const authProviderNames = that.data?.data?.delegates
                            ? that.data?.data?.delegates.slice() : [];
                        for (const name of names)
                        {
                            if (!authProviderNames.includes(name))
                            {
                                authProviderNames.push(name);
                            }
                        }
                        const authProvidersMultiSelect = dom.byId('composite.delegates.container');
                        authProvidersMultiSelect.style = 'border: 1px solid lightgray; padding: .5em; width: 14em;';

                        for (const name of authProviderNames)
                        {
                            const row = document.createElement('div');
                            row.style = 'display: flex; justify-content: space-between; width: 15em; margin-bottom: .5em;';

                            const checkboxContainer = document.createElement('span');
                            checkboxContainer.style = 'display: flex;';

                            const checkbox = document.createElement('input');
                            checkbox.setAttribute('type', 'checkbox');
                            checkbox.setAttribute('data-dojo-type', 'dijit/form/CheckBox');
                            checkbox.setAttribute('id', name + '-checkbox');
                            checkbox.setAttribute('name', name + '-checkbox');
                            checkbox.style = 'cursor: pointer; margin-right: .5em;';
                            checkboxContainer.appendChild(checkbox);

                            const label = document.createElement('label');
                            label.setAttribute('for', name + '-checkbox');
                            label.style = 'cursor: pointer;';
                            label.innerHTML = name;
                            checkboxContainer.appendChild(label);

                            const buttonContainer = document.createElement('span');
                            buttonContainer.style = 'display: flex; padding-right: .5em;';

                            const up = document.createElement('button');
                            up.innerHTML = '&#9650;';
                            up.setAttribute('data-dojo-type', 'dijit/form/Button');
                            up.setAttribute('type', 'button');
                            up.addEventListener('click', (el) => moveUp(el.target));
                            up.style = 'width: 1.5em; height: 1.5em; margin-right: .2em; display: flex; '
                                       + 'align-content: center; align-items: center; justify-content: center;';
                            buttonContainer.appendChild(up);

                            const down = document.createElement('button');
                            down.innerHTML = '&#9660;';
                            down.setAttribute('data-dojo-type', 'dijit/form/Button');
                            down.setAttribute('type', 'button');
                            down.addEventListener('click',(el) => moveDown(el.target));
                            down.style = 'width: 1.5em; height: 1.5em; margin-right: .2em; display: flex; '
                                         + 'align-content: center; align-items: center; justify-content: center;';
                            buttonContainer.appendChild(down);

                            row.appendChild(checkboxContainer);
                            row.appendChild(buttonContainer);

                            authProvidersMultiSelect.appendChild(row);
                        }

                        if (that.data?.data?.delegates)
                        {
                            for (const delegate of that.data.data.delegates)
                            {
                                dom.byId(delegate + '-checkbox').setAttribute('checked', 'checked');
                            }
                        }

                    }, util.xhrErrorHandler);

                function moveUp(el) {
                    const row = el.parentNode.parentNode;
                    if (row.previousElementSibling)
                    {
                        row.parentNode.insertBefore(row, row.previousElementSibling);
                    }
                }

                function moveDown(el) {
                    const row = el.parentNode.parentNode;
                    if (row.nextElementSibling)
                    {
                        row.parentNode.insertBefore(row.nextElementSibling, row);
                    }
                }
            });
        },
        _preSubmit: function(formData)
        {
            let result = [];
            const rows = document.getElementById('composite.delegates.container').children;
            for (let i = 0; i < rows.length; i ++)
            {
                if (rows.item(i).firstChild?.firstChild?.checked)
                {
                    result.push(rows.item(i).firstChild?.innerText);
                }
            }
            formData.delegates = result;
        }
    };
});

