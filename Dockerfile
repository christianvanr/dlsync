FROM adoptopenjdk:11-jre-hotspot

# Update and install dependencies
RUN apt update && \
    apt-get -y install coreutils python3-venv jq 

# Install AWS CLI
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install awscli

# DlSync app
RUN mkdir /opt/app
WORKDIR /opt/app
COPY build/libs/dlsync-*.jar dlsync.jar