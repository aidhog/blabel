# blabel
A library for skolemising (or canonicalising) blank node labels in RDF graphs.

## Compilation

You need [Maven](https://maven.apache.org/) and JDK 8+ installed.

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

To get a list of utilities:

```bash
$ java -jar blabel-0.2.0-SNAPSHOT-jar-with-dependencies.jar
usage: cl.uchile.dcc.blabel.cli.Main
missing <utility> arg where <utility> one of
	LabelRDFGraph: Run labelling over an RDF graph encoded as N-Triples
	RunNQuadsTest: [Testing] Compute the canonical graphs in a quads file
	Control: [Testing] Run a control experiment to time parsing a quads file
	RunSyntheticEvaluation: [Testing] Run synthetic benchmark
	AnalyseNQuadsResults: [Testing]
	UndirectedGraphToRDF: [Testing]
```

To get help for the main (skolemization) utility:

```bash
$ java -jar blabel-0.2.0-SNAPSHOT-jar-with-dependencies.jar LabelRDFGraph -h
***ERROR: class org.apache.commons.cli.MissingOptionException: Missing required options: io
usage: parameters:
 -b         output labels as blank nodes
 -ddp       don't distinguish partitions [isomorphic blank node partitions
            will be removed; by default they are distinguished and kept]
 -h         print help
 -i <arg>   input file [enter 'std' for stdin]
 -igz       input is GZipped
 -l         lean beforehand
 -lo        lean only, do not label
 -o <arg>   output file [enter 'std' for stdout]
 -ogz       output should be GZipped
 -p <arg>   string prefix to append to label [make sure it's valid for URI
            or blank node!] [default empty string])
 -s <arg>   hashing scheme: 0:md5 1:murmur3_128 2:sha1 3:sha256 4:sha512
            (default Hashing.md5())
 -upp       keep blank nodes unique per partition, not graph [blank nodes
            are labelled only using information from the partition; by default the
            entire graph is encoded in the blank node label including ground triples]
time elapsed 17 ms
```

To skolemize a file `input.nt` into a file `output.nt`, using a prefix `https://example.com/.well-known/genid/` and the SHA256 hashing algorithm:

```sh
java -jar blabel/target/blabel-0.2.0-SNAPSHOT-jar-with-dependencies.jar LabelRDFGraph \
   -i input.nt -s 3 -p 'https://example.com/.well-known/genid/' -o output.nt
```

### As Maven dependency

```xml
<dependency>
  <groupId>cl.uchile.dcc</groupId>
  <artifactId>blabel</artifactId>
  <version>0.2.0-SNAPSHOT</version>
</dependency>
```

See the following page for more details: http://blabel.github.io/
