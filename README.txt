Running Pig
-----------

WARNING: this is a very old and trivial experiment, at the moment it does not
even compile with the new Apache Jena release(s).

It is shared here, just to show how trivial it is to load/store N-Triples or
N-Quads files with Pig.


./bin/pig -help
./bin/pig -version
./bin/pig -x local ./src/main/piglatin/helloworld.pig
./bin/pig -x mapreduce ./src/main/piglatin/helloworld.pig


...

Maven
-----

Once you have installed Maven, you can have fun with the following commands:

  mvn -Declipse.workspace=/opt/workspace eclipse:add-maven-repo
  mvn eclipse:clean eclipse:eclipse -DdownloadSources=true -DdownloadJavadocs=true
  mvn dependency:resolve
  mvn compile
  mvn test
  mvn package
  mvn site
  mvn install
  mvn deploy


