#!/bin/sh
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

from_local_dist=
from_release=
local_dist_path=
qpid_version=
MY_NAME=$(basename "$0")

print_help()
{
  cat << END_OF_HELP
Usage: $MY_NAME [OPTION]...

 options:

  --release          Apache Qpid Broker-J release version to build with
  --local-dist-path  Path to the local Apache Qpid Broker-J distribution to build with
  --help, -h, -?     Print this help and exit

END_OF_HELP
}

parse_parameters()
{
  while [ $# -gt 0 ]; do
    case $1 in
      --local-dist-path)
          from_local_dist=true
          local_dist_path=$2
          shift;;
      --release)
          from_release=true
          qpid_version=$2
          shift;;
      --help | -h | -?)
          print_help; exit 0;;
      *)
          echo "Unknown parameter '$1'"
          exit 2;;
    esac
      shift
  done

  if [ -n "${local_dist_path}" ] && [ -n "${qpid_version}" ]; then
    echo "Please specify either building image from local installation or from a particular release version, but not both"
    exit 2
  fi

  if [ -z "${local_dist_path}" ] && [ -z "${qpid_version}" ]; then
    print_help
    echo "Please specify either building image from local installation or from a particular release version"
    exit 2
  fi

  if [ -n "${local_dist_path}" ]; then

    if [ ! -f "${local_dist_path}" ]; then
      echo "Local distribution file ${local_dist_path} not found"
      exit 1
    fi

    if [ "$(echo ${local_dist_path} | tail -c 7)" != "tar.gz" ]; then
      echo "Local distribution file ${local_dist_path} should be a tar.gz archive"
      exit 1
    fi
  fi
}

install()
{
  qpid_dist_dir="./qpid-broker-j"
  qpid_dist_file_name=

  # Prepare directory
  if [ ! -d "${qpid_dist_dir}" ]; then
    echo "Creating directory ${qpid_dist_dir}"
    mkdir -p "${qpid_dist_dir}"
  elif [ ! -z "$(find "${temp_dir}" -name "${qpid_version}" -type d -mmin +60)" ]; then
    echo "Cleaning up directory ${qpid_dist_dir}"
    rm -rf ${qpid_dist_dir}/*
  else
    echo "Using directory ${qpid_dist_dir}"
  fi

  if [ -n "${from_release}" ]; then

    qpid_dist_file_name="apache-qpid-broker-j-${qpid_version}-bin.tar.gz"

    # Check if the release is already available locally, if not try to download it
    if [ -z "$(ls -A ${qpid_dist_dir})" ]; then
      cdn="$(curl -s https://www.apache.org/dyn/closer.cgi\?preferred=true)/qpid/broker-j/${qpid_version}/binaries/"
      archive="https://archive.apache.org/dist/qpid/broker-j/${qpid_version}/binaries/"
      qpid_base_url=${cdn}
      curl_output="${qpid_dist_dir}/${qpid_dist_file_name}"

      # Fallback to the apache archive if the version doesn't exist on the CDN anymore
      if [ -z "$(curl -Is ${qpid_base_url}${qpid_dist_file_name} | head -n 1 | grep 200)" ]; then
        qpid_base_url=${archive}

        # If the archive also doesn't work then report the failure and abort
        if [ -z "$(curl -Is ${qpid_base_url}${qpid_dist_file_name} | head -n 1 | grep 200)" ]; then
          echo "Failed to find ${qpid_dist_file_name}. Tried both ${cdn} and ${archive}."
          exit 1
        fi
      fi

      echo "Downloading ${qpid_dist_file_name} from ${qpid_base_url}..."
      curl --progress-bar "${qpid_base_url}${qpid_dist_file_name}" --output "${curl_output}"

    fi

  elif [ -n "${from_local_dist}" ]; then

    qpid_dist_file_name=$(basename ${local_dist_path})
    qpid_version=$(echo "$qpid_dist_file_name" | sed -e 's/apache-qpid-broker-j-\(.*\)-bin.tar.gz/\1/')

    echo "Broker-J distribution file is $qpid_dist_file_name"
    echo "Broker-J version is $qpid_version"

    echo "Copying ${local_dist_path} to ${qpid_dist_dir}..."
    cp "$local_dist_path" "$qpid_dist_dir"

  else
    exit 2
  fi

  echo "Expanding ${qpid_dist_dir}/${qpid_dist_file_name}..."
  tar xzf "${qpid_dist_dir}"/"${qpid_dist_file_name}" --directory "${qpid_dist_dir}" --strip 1

  echo "Removing ${qpid_dist_dir}/${qpid_dist_file_name}..."
  rm -rf "${qpid_dist_dir}"/"${qpid_dist_file_name}"

  mkdir -p $qpid_dist_dir/${qpid_version}/docker/
  cp ./broker.acl "$qpid_dist_dir/${qpid_version}/docker/"
  cp ./*.json "$qpid_dist_dir/${qpid_version}/docker/"
  cp ./Containerfile "$qpid_dist_dir/${qpid_version}/docker/"
  cp -r $qpid_dist_dir/${qpid_version}/lib "$qpid_dist_dir/${qpid_version}/docker/"
  cp ./entrypoint.sh "$qpid_dist_dir/${qpid_version}/docker/"
}

print_instruction()
{
  cat <<HERE

Well done! Now you can continue with building the Docker image:

  # Go to $qpid_dist_dir/${qpid_version}/docker/
  $ cd $qpid_dist_dir/${qpid_version}/docker/

  # For Ubuntu with JRE 17
  $ docker build -f ./Containerfile -t qpid-ubuntu .

  # For Alpine with JRE 17
  $ docker build -f ./Containerfile --build-arg OS_NAME=alpine -t qpid-alpine .

Note: -t qpid-ubuntu and -t qpid-alpine are just a tag names for the purpose of this guide

For more info see README.md

HERE
  exit 0
}

# main
parse_parameters "$@" && install && print_instruction
