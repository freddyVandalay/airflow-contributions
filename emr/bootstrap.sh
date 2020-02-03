#!/bin/bash

# This bash script installs the required packages to bootstrap an EMR cluster, plus
# move all the deployed project folder to EMR Cluster.

# If the script is run in local environment (vagrant), the deployment bucket will point to the current users
# latest local airflow repo in s3 e.g. s3://grp-ds-users/first.name/local_spark/local_latest.zip

# The script accepts to arguments which is passed on as bootstrap args when emr clusters is build:
#                               $1 represents the environment
#                               $2 represents true or false to install extra packages and libraries
#                               $3 represents the aws username

# You can any required packages for your project in this bash script and make sure to reference this file
# in your DAG bootstrap configuration.
# TODO add comments

if [ "$1" = "dev" ]
  then
    # Development environment
    DEPLOY_BUCKET='grp-ds-users'
    DEPLOY_KEY=$3'/spark_local/latest.zip'
    echo 'Running in Development environment'
  else
    # Production environment
    DEPLOY_BUCKET='airflow-deployments'
    DEPLOY_KEY='latest.zip'
    echo 'Running Production environment'
fi
RESOURCES_BUCKET='projects'
RESOURCES_KEY='resources'

echo "Deployment bucket: s3://$DEPLOY_BUCKET/$DEPLOY_KEY"

# Install awscli (later of versions emr release require this)
# *required to upload local airflow repo to emr
sudo pip install awscli==1.16.26

#############################################################
#  MOVE DEPLOYMENT FOLDER TO EMR CLUSTER                    #
#############################################################
# Get the whole deployed project from S3 and move it to the EMR cluster
# and then unzip it.
echo "Copying airlfow repository to EMR"
aws s3 cp s3://$DEPLOY_BUCKET/$DEPLOY_KEY /home/hadoop/
unzip -o /home/hadoop/latest.zip -d /home/hadoop/

#############################################################
#  LIBRARIES AND PACKAGES REQUIRED IN EMR
# *optional via parameter in EmrCreateJobFlowOperatorOverride operator
#############################################################
if [ "$2" = "True" ]
then
  # Install any required packages in EMR servers
  echo "Installing LIBRARIES AND PACKAGES REQUIRED IN EMR"
  sudo yum -y install unzip

  # Install packages requiter in EMR cluster
  # Beware of psycopg2: newer versions give installation error due to old pip version on cluster
  # Old versions of boto3, botocore and awscli can cause bootstrap to fail with newer emr release label versions
  sudo pip install boto3==1.10.19 \
                   botocore==1.13.22 \
                   credstash==1.15.0 \
                   pandas==0.22.0 \
                   psycopg2==2.7.4 \
                   pyarrow==0.10.0 \
                   SQLAlchemy==1.1.15 \
                   sqlalchemy-redshift==0.5.0

  # Get mysql repository and install it
  aws s3 cp s3://$RESOURCES_BUCKET/$RESOURCES_KEY/mysql-connector-python-2.1.5-1.el7.x86_64.rpm /home/hadoop/
  sudo yum -y localinstall /home/hadoop/mysql-connector-python-2.1.5-1.el7.x86_64.rpm
fi
