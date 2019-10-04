FROM rocker/tidyverse:latest

RUN apt-get update && apt-get install -y --allow-downgrades --allow-remove-essential --allow-change-held-packages --allow-unauthenticated --no-install-recommends --no-upgrade \
  curl \
  zip

## Install packages

RUN install2.r -s remotes
COPY DESCRIPTION /pkg/DESCRIPTION
RUN Rscript -e "remotes::install_deps(pkgdir = '/pkg/', dependencies=TRUE, upgrade = 'always', force = FALSE, quiet = FALSE)"


