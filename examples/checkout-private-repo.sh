#!/usr/bin/env bash

set -x

# get secret names from config if they exist
secret_name=$(python -c "from cpg_utils.config import get_config; print(get_config(print_config=False).get('infrastructure', {}).get('git_credentials_secret_name', ''))")
secret_project=$(python -c "from cpg_utils.config import get_config; print(get_config(print_config=False).get('infrastructure', {}).get('git_credentials_secret_project', ''))")

if [ ! -z "$secret_name" ] && [ ! -z "$secret_project" ]; then
    # configure git credentials store if credentials are set
    echo 'Configuring private repo'
    gcloud --project $secret_project secrets versions access --secret $secret_name latest > ~/.git-credentials
    git config --global credential.helper "store"
fi

git clone https://github.com/populationgenomics/transfer-private.git

ls -lGh transfer-private