#!/usr/bin/env bash

os_name=$(grep -o '^NAME=.*' /etc/os-release | cut -f2 -d\" | sed 's/"//g')
os_version=$(grep -o '^VERSION_ID=.*' /etc/os-release | cut -f2 -d\" | sed 's/"//g')
echo "Platform is ${os_name}, Version: ${os_version}"
arch=`arch`
ID=$(cat /etc/os-release | grep -w ID | cut -f2 -d"=")

if [ ${ID} != "mendel" ]; then

 case $os_name in

  *"Red Hat"*)
    source scl_source enable rh-python36
    ;;

  *"CentOS"*)
    source scl_source enable rh-python36
    ;;

  *"Ubuntu"*)
    if [ ${arch} = "aarch64" ]; then
      python3 -m pip install --upgrade pip
    fi
    ;;

  esac

  python3 -m pip install pandas==1.1.5
fi
