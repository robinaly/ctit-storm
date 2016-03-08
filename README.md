# ctit-storm
Template project for storm applications running on the CTIT cluster of the University of Twente.

## Compilation
The project can be compiled as follows
```bash
cd ctit-storm
mvn package
```

## Create New Topologies
To create a new topology, follow these steps:
```bash
# create additional bolts
...
# copy an existing topology
cp src/main/java/nl/utwente/bigdata/KafkaPrinter.java src/main/java/nl/utwente/bigdata/<name>.java 
# edit the new file to use the new topology
```
