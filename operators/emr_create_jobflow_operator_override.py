# -*- coding: utf-8 -*-
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
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.utils import apply_defaults
from airflow.hooks import S3_hook
import logging
import shutil
import boto3
import os


class EmrCreateJobFlowOperatorOverride(EmrCreateJobFlowOperator):
    """
    This operator overrides the default EmrCreateJobFlowOperator
    TODO update custom variables

    Bootstrap:
        - by default picks bootstrap from local airflow repo (emr/bootstrap.sh). Pass var to use custom path.
        - install packages on emr : set true or false
    --------------------------------------------
    *From original operator EmrCreateJobFlowOperator
    Creates an EMR JobFlow, reading the config from the EMR connection.
    A dictionary of JobFlow overrides can be passed that override
    the config from the connection.

    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param emr_conn_id: emr connection to use
    :type emr_conn_id: str
    :param job_flow_overrides: boto3 style arguments to override
           emr_connection extra. (templated)
    :type steps: dict
    """
    template_fields = ['job_flow_overrides']
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            bootstrap_path='/home/vagrant/uk_dm_airflow/emr/bootstrap.sh',
            install_packages_on_emr=True,
            *args, **kwargs):
        super(EmrCreateJobFlowOperatorOverride, self).__init__(*args, **kwargs)
        # self.environment = os.environ['DEPLOYMENT_ENVIRONMENT']
        self.environment = 'prod'
        self.bootstrap_path = bootstrap_path
        self.install_packages_on_emr = str(install_packages_on_emr)  # sh script only takes string as parameter

    def execute(self, context):
        if self.environment not in ["dev", "prod"]:
            logging.error(f"Can't recognise deployment environment '{self.environment}'. \n"
                          "Review the environment variable 'DEPLOYMENT_ENVIRONMENT'")
            raise ValueError(f"self.environment = os.environ['DEPLOYMENT_ENVIRONMENT'] --> {self.environment}")

        # check if development/local environment
        if self.environment == 'dev':
            logging.info("EMR cluster running from development environment")

            # get user aws name
            client = boto3.client('sts')
            username = client.get_caller_identity()['Arn'].split(":", 5)[5].split("/", 1)[1].lower()

            # Create zipped archive of the local airflow repository
            airflow_repo_path = '/home/vagrant/uk_dm_airflow'
            zip_local_path = '/tmp/latest'
            shutil.make_archive(base_name='/tmp/latest',
                                format='zip',
                                root_dir=airflow_repo_path)
            logging.info(f"Zipped file location: {zip_local_path}")

            # Upload zipped airflow repository to user's s3 bucket
            hook = S3_hook.S3Hook(aws_conn_id=self.aws_conn_id)
            hook.load_file(f"{zip_local_path}.zip", f'{username}/spark_local/latest.zip',
                           bucket_name='grp-ds-users',
                           replace=True)

            logging.info(f"Airflow repo uploaded to user bucket. User: '{username}'")

            # Upload local bootstrap file to user s3 buckets
            bootstrap_path = self.bootstrap_path
            hook.load_file(bootstrap_path, f'{username}/spark_local/bootstrap.sh',
                           bucket_name='grp-ds-users',
                           replace=True)

            self.override_emr_template(username)
            return EmrCreateJobFlowOperator.execute(self, context)

        # Create cluster and return jobflow_id
        # Output the edited EMR template.
        self.job_flow_overrides['BootstrapActions'][0]['ScriptBootstrapAction']['Args'] = [
            f'{self.environment}', self.install_packages_on_emr]
        logging.info(self.job_flow_overrides)
        return EmrCreateJobFlowOperator.execute(self, context)  # Returns the jobflow id

    def override_emr_template(self, username):
        """
        Overrides emr template to specify deployment in development/local
        :param username: specifies the users aws username / s3 key
        :return:
        """
        bootstrap_s3_key = f"s3://grp-ds-users/{username}/spark_local/bootstrap.sh"
        self.job_flow_overrides['Name'] = f"{self.job_flow_overrides['Name']}_{self.environment}"
        self.job_flow_overrides['BootstrapActions'][0]['ScriptBootstrapAction']['Path'] = bootstrap_s3_key
        logging.info("Bootstrap settings: \n"
                     f"Environment: {self.environment} \n"
                     f"Bootstrap s3 location: {bootstrap_s3_key} \n"
                     f"Install packages and libraries: {self.install_packages_on_emr}")
        self.job_flow_overrides['BootstrapActions'][0]['ScriptBootstrapAction']['Args'] = [
            f'{self.environment}', self.install_packages_on_emr, f'{username}']
