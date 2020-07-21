DATE=`date +%Y-%m-%d.%H.%m`

.PHONY: default
default: build-java test-rust

.PHONY: test-rust
test-rust:
	cargo test

.PHONY: build-java
build-java: deps java/BVGraphToEdges.class

.PHONY: install
install:
	cargo install --path diameter --force

.PHONY: deps
deps: java/webgraph-3.6.3.jar java/dsiutils-2.6.5.jar

try-java-small: java/BVGraphToEdges.class
	rm -rf /tmp/out.bin
	cd java && java -ea -cp webgraph-3.6.3.jar:slf4j-api-1.7.26.jar:dsiutils-2.6.2.jar:fastutil-8.3.0.jar:jsap-2.1.jar:. BVGraphToEdges \
	~/.graph-datasets/51AC668FED74DC19BF9552375D5BF3428D4DC89D9EC248AC094C966711618781/cnr-2000-hc \
	/tmp/out.bin

try-java: java/BVGraphToEdges.class
	rm -rf /tmp/out.bin
	cd java && java -Xmx4G -cp webgraph-3.6.3.jar:slf4j-api-1.7.26.jar:dsiutils-2.6.2.jar:fastutil-8.3.0.jar:jsap-2.1.jar:. BVGraphToEdges \
	~/.graph-datasets/EDA383F35CE3CC7832DE1301A2E972E33FC39307B9B14E4F70A7916B2D575D6E/it-2004-hc \
	/tmp/out.bin

convert-twitter: java/BVGraphToEdges.class
	cd java && java -Xmx4G -cp webgraph-3.6.3.jar:slf4j-api-1.7.26.jar:dsiutils-2.6.2.jar:fastutil-8.3.0.jar:jsap-2.1.jar:. BVGraphToEdges \
	/mnt/ssd/graphs/3CAEEB13A5DAC1044EEE5E0624398251D9AD49F3646194AA31DEC95DE360B6B8/twitter-2010-hc \
	/mnt/ssd/tmp/out.bin

java/BVGraphToEdges.class: java/BVGraphToEdges.java
	cd java && javac -cp webgraph-3.6.3.jar:slf4j-api-1.7.30.jar:dsiutils-2.6.5.jar:fastutil-8.3.1.jar:jsap-2.1.jar BVGraphToEdges.java

java/webgraph-3.6.3-bin.tar.gz:
	cd java && curl -O http://webgraph.di.unimi.it/webgraph-3.6.3-bin.tar.gz

java/webgraph-deps.tar.gz:
	cd java && curl -O http://webgraph.di.unimi.it/webgraph-deps.tar.gz

java/webgraph-3.6.3.jar: java/webgraph-3.6.3-bin.tar.gz
	cd java && tar -xvf webgraph-3.6.3-bin.tar.gz --strip-components=1 \
	 webgraph-3.6.3/webgraph-3.6.3.jar

java/dsiutils-2.6.5.jar: java/webgraph-deps.tar.gz
	cd java && tar -xvf webgraph-deps.tar.gz \
	 dsiutils-2.6.5.jar \
	 fastutil-8.3.1.jar \
	 jsap-2.1.jar \
	 slf4j-api-1.7.30.jar

.PHONY: analysis
analysis:
	R -e "drake::r_make()"
	cp export/* ~/Dropbox/Lavoro/Diameter/Algorithms-MDPI/include
	cp dashboard.html docs/dashboard.html

.PHONY: sync
sync:
	cp diameter-results.sqlite diameter-results.sqlite.bak.${DATE}
	rsync --progress eridano11:diameter-results.sqlite .
