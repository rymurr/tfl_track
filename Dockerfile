# Copyright 2013 Thatcher Peskens
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ubuntu:precise

maintainer Dockerfiles

run echo "deb http://archive.ubuntu.com/ubuntu precise main universe" > /etc/apt/sources.list
run apt-get update
run apt-get install -y build-essential 
run apt-get install -y python python-dev python-setuptools
run apt-get install -y supervisor
run easy_install pip

# install our code
add . /home/docker/code/

# setup all the configfiles
run ln -s /home/docker/code/supervisor-app.conf /etc/supervisor/conf.d/

# run pip install
run pip install setuptools --no-use-wheel --upgrade
run pip install -r /home/docker/code/app/requirements.txt


cmd ["supervisord", "-n"]

#run uwsgi
#run uwsgi -s /home/docker/code/app.sock --wsgi-file /home/docker/code/app/app.py --callable app
