# blabel
A library for skolemising (or canonicalising) blank node labels in RDF graphs.

## Compilation
You need [Maven](https://maven.apache.org/) installed
### To build a .jar
```bash
$ mvn clean package
```
### To install the .jar into your local Maven repository
````bash
$ mvn clean install
````

## Usage
### CLI
````bash
$ java -jar blabel-0.2.0-SNAPSHOT-jar-with-dependencies.jar
````
### As Maven dependency
````xml
<dependency>
  <groupId>cl.uchile.dcc</groupId>
  <artifactId>blabel</artifactId>
  <version>0.2.0-SNAPSHOT</version>
</dependency>
````

See the following page for more details: http://blabel.github.io/
