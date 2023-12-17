#!/bin/bash
git config --global --add safe.directory '*'

eval "$(ssh-agent -s)"
ssh-add /root/.ssh/id_ed25519

pushd /src
sudo docker compose down
sudo docker compose build

git pull origin deploy_airflow
git checkout deploy_airflow

sudo docker compose up -d
popd
