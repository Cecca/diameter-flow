try-java-small: java/BVGraphToEdges.class
	rm -rf /tmp/scratch
	cd java && java -ea -cp webgraph-3.6.3.jar:slf4j-api-1.7.26.jar:dsiutils-2.6.2.jar:fastutil-8.3.0.jar:jsap-2.1.jar:. BVGraphToEdges \
	~/.graph-datasets/51AC668FED74DC19BF9552375D5BF3428D4DC89D9EC248AC094C966711618781/cnr-2000-hc \
	/tmp/out.bin

try-java: java/BVGraphToEdges.class
	cd java && java -Xmx4G -cp webgraph-3.6.3.jar:slf4j-api-1.7.26.jar:dsiutils-2.6.2.jar:fastutil-8.3.0.jar:jsap-2.1.jar:. BVGraphToEdges \
	~/.graph-datasets/EDA383F35CE3CC7832DE1301A2E972E33FC39307B9B14E4F70A7916B2D575D6E/it-2004-hc \
	/tmp/out.bin

java/BVGraphToEdges.class: java/BVGraphToEdges.java
	cd java && javac -cp webgraph-3.6.3.jar:slf4j-api-1.7.26.jar:dsiutils-2.6.2.jar:fastutil-8.3.0.jar:jsap-2.1.jar BVGraphToEdges.java
