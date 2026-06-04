## Instructions using confluentinc-kafka-connect-oracle-xstream-cdc-source-1.4.1-PATCHED.zip :
- Stop the Connect service 
- Remove the old Oracle XStream Connector plugin. For Linux, this location is normally under /usr/share/confluent-hub-components,
Back up and remove the existing connector plugin 
```
cd  /usr/share/confluent-hub-components
rm -rf  confluentinc-kafka-connect-oracle-xstream-cdc-source
```
- Unzip the confluentinc-kafka-connect-oracle-xstream-cdc-source-1.4.1-PATCHED.zip
```
unzip confluentinc-kafka-connect-oracle-xstream-cdc-source-1.4.1-PATCHED.zip
```

- Copy the ojdbc8.jar and xstream.jar from Oracle Client Installation or existing plugin
```
cp ojdbc8.jar xstreams.jar .
```

- Start the connect service


## Instructions using kafka-connect-oracle-xstream-cdc-source-1.4.1.jar (if XStream 1.4.1 is already installed) :
- Stop the Connect service 
- To use the binary, you will need to find the location of the connector code. For Linux, this location is normally under /usr/share/confluent-hub-components
```
cd  /usr/share/confluent-hub-components
```

- Remove or back up the existing binary
```
cd confluentinc-kafka-connect-oracle-xstream-cdc-source/lib
rm kafka-connect-oracle-xstream-cdc-source-1.4.1.jar 
```
- Copy the downloaded binary
```
cp ~/kafka-connect-oracle-xstream-cdc-source-1.4.2-SNAPSHOT.jar .
```
- Start the connect service
