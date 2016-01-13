Log server
==========

This repository is an experiment to use Kafka as a kind of multitenant streaming database that
end-user clients can connect to.

Usage
-----

This project is set up to build with Java 8 and Maven. Make sure you have those installed.

[Download the latest version of Apache Kafka](http://kafka.apache.org/downloads.html)
(binary package) and unpack it to a directory of your choice. You can then start the ZooKeeper and
Kafka services from within the directory where you unpacked it:

    $ bin/zookeeper-server-start.sh config/zookeeper.properties &
    $ bin/kafka-server-start.sh config/server.properties &
    $ bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic events --partitions 16 --replication-factor 1

Then you can compile and run this project as follows:

    $ mvn package
    $ mvn integration-test

Then open a web browser at [localhost:8080](http://localhost:8080/).

License
-------

Copyright 2016, University of Cambridge.
Licensed under the Apache License, Version 2.0 (see `LICENSE`).

Unless required by applicable law or agreed to in writing, software distributed under the License is
distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations under the
License.
