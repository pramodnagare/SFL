#!/usr/bin/env bash
airflow db init
airflow users create -r Admin -u admin -f admin -l admin -p admin -e pramodnagare1993@gmail.com
airflow webserver