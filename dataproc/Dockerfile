FROM ubuntu:20.04

RUN apt update && \
    apt install -y --no-install-recommends apt-transport-https ca-certificates curl gnupg zip git make g++ openjdk-8-jdk-headless libopenblas-base liblapack3 python3-pip rsync && \
    # === Google Cloud SDK ===
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt update && apt install -y google-cloud-sdk && \
    apt clean && rm -rf /var/lib/apt/lists/* && \
    # === Hail ===
    git clone https://github.com/hail-is/hail.git && \
    cd hail/hail && \
    # 0.2.63 is the last known working version:
    # - 0.2.64 has a gcsfs version issue.
    # - 0.2.65 and above suffer from random __C3799RGContainer_GRCh38 crashes.
    git checkout 0.2.63 && \
    # Add the `--service-account` parameter to `hailctl dataproc start`, which was added after 0.2.63.
    git cherry-pick -n ecd8292889b02c0bdc803d0462f4a37f30392fba && \
    # Add the `--scopes` parameter to `hailctl dataproc start`, which was added after 0.2.63.
    git cherry-pick -n 5659445fd5bca8a15f902f4e41dd8ba19a34757d && \
    # Install locally, avoiding the need for a pip package.
    # DEPLOY_REMOTE avoids a dev suffix being appended to dataproc initialization paths.
    make install DEPLOY_REMOTE=1 && \
    cd ../.. && \
    rm -rf hail
