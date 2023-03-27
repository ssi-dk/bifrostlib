# This is intended to run in Local Development (dev) and Github Actions (test/prod)
# BUILD_ENV options (dev, test, prod) dev for local testing and test for github actions testing on prod ready code
ARG BUILD_ENV="prod"
ARG MAINTAINER="kimn@ssi.dk;"

#---------------------------------------------------------------------------------------------------
# Base for dev environement
#---------------------------------------------------------------------------------------------------
FROM continuumio/miniconda3:22.11.1 as build_dev
ONBUILD COPY /lib/bifrostlib /bifrost/lib/bifrostlib
ONBUILD WORKDIR /bifrost/lib/bifrostlib/
ONBUILD RUN \
    pip install -r requirements.txt; \
    pip install --no-cache -e file:///bifrost/lib/bifrostlib;

#---------------------------------------------------------------------------------------------------
# Base for production environment
#---------------------------------------------------------------------------------------------------
FROM continuumio/miniconda3:22.11.1 as build_prod
ONBUILD WORKDIR /bifrost/lib/bifrostlib
ONBUILD COPY ./ ./
ONBUILD RUN \
    pip install file:///bifrost/lib/bifrostlib/

#---------------------------------------------------------------------------------------------------
# Base for test environment (prod with tests)
#---------------------------------------------------------------------------------------------------
FROM continuumio/miniconda3:22.11.1 as build_test
ONBUILD WORKDIR /bifrost/lib/bifrostlib
ONBUILD COPY ./ ./
ONBUILD RUN \
    pip install -r requirements.txt \
    pip install file:///bifrost/lib/bifrostlib/


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
