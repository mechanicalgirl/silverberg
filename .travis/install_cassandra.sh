#!/usr/bin/env bash

if [[ "${CASS_VERSION}" == 1.2* ]]; then

    echo "Installing Cassandra ${CASS_VERSION}"

    sudo service cassandra stop
    sudo rm -rf /var/lib/cassandra/*

    if [ "${CASS_VERSION}" == "1.2.19" ]; then
        wget http://www.apache.org/dyn/closer.cgi?path=/cassandra/1.2.19/apache-cassandra-1.2.19-bin.tar.gz && tar -xvzf apache-cassandra-1.2.19-bin.tar.gz && sudo sh apache-cassandra-1.2.19/bin/cassandra -Xss512k
    else
        wget http://archive.apache.org/dist/cassandra/${CASS_VERSION}/apache-cassandra-${CASS_VERSION}-bin.tar.gz && tar -xvzf apache-cassandra-${CASS_VERSION}-bin.tar.gz && sudo sh apache-cassandra-${CASS_VERSION}/bin/cassandra -Xss512k
    fi

fi
