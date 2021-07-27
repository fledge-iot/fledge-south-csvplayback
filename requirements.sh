#!/usr/bin/env bash

##--------------------------------------------------------------------
## Copyright (c) 2019 Dianomic Systems Inc.
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##--------------------------------------------------------------------

set -e

os_name=$(grep -o '^NAME=.*' /etc/os-release | cut -f2 -d\" | sed 's/"//g')
os_version=$(grep -o '^VERSION_ID=.*' /etc/os-release | cut -f2 -d\" | sed 's/"//g')
echo "Platform is ${os_name}, Version: ${os_version}"

if [ "$os_name" = "Red Hat" ] || [ "$os_name" = "CentOS" ]; then
	echo "No installation of atlas base dev required."
else
  sudo apt-get install -y libatlas-base-dev
fi

ID=$(cat /etc/os-release | grep -w ID | cut -f2 -d"=")

if [ ${ID} = "mendel" ]; then
   sudo apt-get install -y python3-pandas
fi

./extras_install.sh