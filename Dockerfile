# This is intended to run in Local Development (dev) and Github Actions (staging/prod)
# BUILD_ENV options (dev, staging, prog) dev for local testing and staging for github actions testing on prod ready code
ARG BUILD_ENV="prod"
ARG MAINTAINER="kimn@ssi.dk;"

#---------------------------------------------------------------------------------------------------
# Base for dev environement
#---------------------------------------------------------------------------------------------------
FROM continuumio/miniconda3:4.8.2 as build_dev
ONBUILD COPY /lib/bifrostlib /bifrost/lib/bifrostlib
ONBUILD WORKDIR /bifrost/lib/bifrostlib/
ONBUILD RUN \
    pip install -r requirements.txt; \
    pip install --no-cache -e file:///bifrost/lib/bifrostlib;

#---------------------------------------------------------------------------------------------------
# Base for production environment
#---------------------------------------------------------------------------------------------------
FROM continuumio/miniconda3:4.8.2 as build_prod
ONBUILD WORKDIR /bifrost/lib/bifrostlib
ONBUILD COPY ./ ./
ONBUILD RUN \
    pip install --no-cache -e file:///bifrost/lib/bifrostlib/

#---------------------------------------------------------------------------------------------------
# Base for staging environment (prod with tests)
#---------------------------------------------------------------------------------------------------
FROM continuumio/miniconda3:4.8.2 as build_staging
ONBUILD WORKDIR /bifrost/lib/bifrostlib
ONBUILD COPY ./ ./
ONBUILD RUN \
    pip install -r requirements.txt \
    pip install --no-cache -e file:///bifrost/lib/bifrostlib/


#---------------------------------------------------------------------------------------------------
# Details
#---------------------------------------------------------------------------------------------------
FROM build_${BUILD_ENV}
LABEL \
    description="Docker environment for bifrostlib" \
    environment="${BUILD_ENV}" \
    maintainer="${MAINTAINER}"

#---------------------------------------------------------------------------------------------------
# Additional programs for all environments
#---------------------------------------------------------------------------------------------------
# N/A

#---------------------------------------------------------------------------------------------------
# Additional resources
#---------------------------------------------------------------------------------------------------
# N/A

#---------------------------------------------------------------------------------------------------
# Run and entry commands
#---------------------------------------------------------------------------------------------------
# WORKDIR /bifrost/lib/bifrostlib
