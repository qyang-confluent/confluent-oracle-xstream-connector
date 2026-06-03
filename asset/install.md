## Instructions:
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
