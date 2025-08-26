#!/usr/bin/env bash

sudo bash -c 'pip completion --bash > /etc/bash_completion.d/pip'
pip install -r requirements.txt

curl https://install.duckdb.org | sh

# Needed by VSCode SQL Tools
# TODO needs to be installed in SQL Tools extension dir ~/.local/share/vscode-sqltools/
# npm install duckdb-async@0.10.2

# Bash completion for DBT
# curl https://raw.githubusercontent.com/fishtown-analytics/dbt-completion.bash/master/dbt-completion.bash > /tmp/dbt-completion.bash
# echo 'source /tmp/dbt-completion.bash' >> ~/.profile
# sudo mkdir -p /etc/bash_completion.d
# sudo echo 'source /tmp/.dbt-completion.bash' > /etc/bash_completion.d/dbt