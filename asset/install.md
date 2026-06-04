## Instructions using confluentinc-kafka-connect-oracle-xstream-cdc-source-1.4.1-PATCHED.zip :
- Stop the Connect worker
   
- Backup and remove the old Oracle XStream Connector from connect plugin path. For Linux, this location is normally under /usr/share/confluent-hub-components.
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

- Start the connect worker




