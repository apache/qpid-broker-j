#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

SCRIPT=$(readlink -f "$0")
SCRIPT_PATH=$(dirname "$SCRIPT")
DOC_PATH=$(readlink -f "$SCRIPT_PATH/../markdown")
declare -a ARTICLES=("architecture" "build-instructions" "consumer-queue-interactions" "code-guide" "release-instructions")

function npm-do { (PATH=$(npm bin):$PATH; eval $@;) }

function generate-toc {
    for ARTICLE in "${ARTICLES[@]}"
    do
       ARTICLE_PATH=${DOC_PATH}/${ARTICLE}.md
       if [ -f "${ARTICLE_PATH}" ]; then
         echo "Generating TOC for ${ARTICLE}.md"
         npm-do markdown-toc  ${ARTICLE_PATH} -i:
       else
         echo "Cannot find ${ARTICLE}.md at ${DOC_PATH}"
       fi
    done
}

if [ ! -f "${DOC_PATH}/index.md" ]; then
    echo "Cannot find developer guide 'index.md' at expected location '${DOC_PATH}'"
    exit 1
fi

if ! command npm -v >/dev/null 2>&1 ; then
    echo "This script uses 'npm' and 'markdown-toc' to generate table of contents for develop guide articles"
    echo "Please install npm or/and Node.js"
    exit 2
fi

if ! command echo "## header" | npm-do markdown-toc - >/dev/null 2>&1 ; then
    echo "This script uses 'npm' and 'markdown-toc' to generate table of contents for develop guide articles"
    echo "Please install 'markdown-toc': npm install --save markdown-toc"
    exit 3
fi

generate-toc
