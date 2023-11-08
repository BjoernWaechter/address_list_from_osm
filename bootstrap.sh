#!/bin/bash

sudo yum update -y
sudo yum groupinstall -y "Development Tools"
sudo yum erase openssl-devel -y
sudo yum install -y openssl11 openssl11-devel bzip2-devel libffi-devel zlib-devel sqlite-devel cargo

cd /opt
sudo wget https://www.python.org/ftp/python/3.10.13/Python-3.10.13.tgz
sudo tar xzf Python-3.10.13.tgz
cd Python-3.10.13

sudo ./configure --enable-optimizations
sudo make altinstall
sudo alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.10 1
sudo alternatives --install /usr/bin/pip3 pip3 /usr/local/bin/pip3.10 1
sudo /usr/local/bin/pip3.10 install -v --upgrade pip

sudo /usr/local/bin/pip3.10 install -v venv-pack
sudo /usr/local/bin/pip3.10 install -v virtualenv
