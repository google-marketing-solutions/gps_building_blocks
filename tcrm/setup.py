# coding=utf-8
# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Setup script for integration testing."""

import os
import shutil
import setuptools
from setuptools.command.install import install

VERSION = '0.1.0'
DEFAULT_REQUIREMENT_DOC = 'requirements.txt'


class CustomizedInstall(install):
  """Customized Install Class for supporting pre-install plugin in prod env."""
  user_options = [('prod-env', None, 'Install tcrm in plugins for prod env')
                 ] + install.user_options

  def create_dummy_plugin_dir(self):
    """Creates plugins folder, and put hooks and operators into tcrm of plugins."""

    # default settings to create dummy plugin
    default_plugins_dir_name = 'plugins'
    default_plugin_name = 'tcrm'
    default_init_name = '__init__.py'
    dir_list = ['hooks', 'operators']

    # create dummy plugin for testing
    dummy_plugins_dir_path = os.path.join(os.getcwd(), default_plugins_dir_name)
    dummy_plugins_path = os.path.join(dummy_plugins_dir_path,
                                      default_plugin_name)
    dummy_plugin_init_path = os.path.join(dummy_plugins_path, default_init_name)
    if not os.path.exists(dummy_plugins_path):
      os.makedirs(dummy_plugins_path)
    if os.path.exists(dummy_plugin_init_path):
      os.remove(dummy_plugin_init_path)
    shutil.copy(default_init_name, dummy_plugin_init_path)

    # copy all files/dirs under hooks and operators into plugins/tcrm
    for src_dir_name in dir_list:
      src_path = os.path.join(os.getcwd(), src_dir_name)
      dst_path = os.path.join(dummy_plugins_path, src_dir_name)
      if os.path.exists(dst_path):
        shutil.rmtree(dst_path)
      shutil.copytree(src_path, dst_path)

  def initialize_options(self):
    """Adds prod environment option to default install options."""
    install.initialize_options(self)
    # prod_env default value is 0 by default
    # it can be changed by providing --prod-env argument
    self.prod_env = 0

  def run(self):
    """Adds create dummy plugin dir before performing installation process."""
    if self.prod_env == 1:
      self.create_dummy_plugin_dir()
    self.do_egg_install()


# get library dependencies from requirements.txt and pass it to deps variable
with open(DEFAULT_REQUIREMENT_DOC) as f:
  deps = f.read().splitlines()

# inject environment variable for airflow installation
os.environ['SLUGIFY_USES_TEXT_UNIDECODE'] = 'yes'

# main setup script
if __name__ == '__main__':
  setuptools.setup(
      name='TCRM',
      version=VERSION,
      description='Cloud Composer for Data',
      install_requires=deps,
      packages=setuptools.find_packages(include=['tests']),
      include_package_data=True,
      cmdclass={'install': CustomizedInstall},
      zip_safe=False)
