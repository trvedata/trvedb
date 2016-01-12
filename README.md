Log server
==========

This repository is an experiment to use Kafka as a kind of multitenant streaming database that
end-user clients can connect to.

Building
--------

This project depends on
[Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/KIP-28+-+Add+a+processor+client),
which is not currently published to Maven, so you have to build it from source. Check out and build
Kafka at the path `../kafka` relative to this project directory:

    $ git clone http://git-wip-us.apache.org/repos/asf/kafka.git ../kafka
    $ cd ../kafka
    $ gradle
    $ ./gradlew jar

You can then start the ZooKeeper and Kafka services from within the Kafka directory:

    $ bin/zookeeper-server-start.sh config/zookeeper.properties &
    $ bin/kafka-server-start.sh config/server.properties &

Then you can compile and run this project as follows:

    $ mvn initialize
    $ mvn package
    $ mvn integration-test

License
-------

Copyright 2016, University of Cambridge.
Licensed under the Apache License, Version 2.0 (see `LICENSE`).

Unless required by applicable law or agreed to in writing, software distributed under the License is
distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations under the
License.
