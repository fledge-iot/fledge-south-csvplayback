#!/usr/bin/env bash

os_name=$(grep -o '^NAME=.*' /etc/os-release | cut -f2 -d\" | sed 's/"//g')
os_version=$(grep -o '^VERSION_ID=.*' /etc/os-release | cut -f2 -d\" | sed 's/"//g')
echo "Platform is ${os_name}, Version: ${os_version}"

ID=$(cat /etc/os-release | grep -w ID | cut -f2 -d"=")

if [ ${ID} != "mendel" ]; then

 case $os_name in

  *"Red Hat"*)
    echo "No installation of atlas base dev required."
    ;;

  *"CentOS"*)
    echo "No installation of atlas base dev required."
    ;;

  esac

  python3 -m pip install pandas==1.0.1
fi
