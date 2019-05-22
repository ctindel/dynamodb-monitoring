FROM python:3.7.3-stretch
ENV SHELL /bin/bash

USER root

COPY src /tmp/src

RUN apt-get -y update && \
    apt-get install -y lsb-release iproute2 sudo vim curl build-essential && \
    curl -sL https://deb.nodesource.com/setup_8.x | sudo -E bash - && \
    apt-get install -y nodejs awscli && \
    npm install -g serverless@1.43.0 && \
    apt-get install -y git && \
    chmod 755 /tmp/src/deploy.sh && \
    chmod 755 /tmp/src/remove.sh

CMD ["/bin/bash"]
ENTRYPOINT ["/bin/bash", "-c"]
