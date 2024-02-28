FROM phusion/baseimage:0.10.1

# Ensure UTF-8
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

ENV HOME /root

RUN /etc/my_init.d/00_regen_ssh_host_keys.sh

CMD ["/sbin/my_init"]

RUN apt-get update

# Install Node JS.
RUN curl -sL https://deb.nodesource.com/setup_6.x | bash - && \
    apt-get update && \
    apt-get -y install nodejs

# Install Google Cloud Functions Emulator.
RUN npm install -g @google-cloud/functions-emulator
ADD conf/config.json /root/.config/configstore/@google-cloud/functions-emulator/config.json
RUN mkdir ${HOME}/functions

# Add init scripts.
COPY init.d/ /etc/my_init.d
RUN find /etc/my_init.d -type f -exec chmod +x {} \;

# Add deploy script
COPY scripts/deploy.sh ${HOME}
RUN chmod +x ${HOME}/deploy.sh

# Clean up.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

EXPOSE 8010