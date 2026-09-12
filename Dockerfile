FROM astrocrpublic.azurecr.io/runtime:3.2-3

USER root

RUN apt-get update \
    && apt-get install -y git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER astro