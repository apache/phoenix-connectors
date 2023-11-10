<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

![logo](https://phoenix.apache.org/images/phoenix-logo-small.png)

<b>[Apache Phoenix](http://phoenix.apache.org/)</b> enables OLTP and operational analytics in Hadoop for low latency applications. Visit the Apache Phoenix website <b>[here](http://phoenix.apache.org/)</b>. This repo contains connectors for third party libraries to access data stored in Phoenix/HBase. 

Copyright ©2019 [Apache Software Foundation](http://www.apache.org/). All Rights Reserved. 

## Building Apache Phoenix Connectors

This repository will build jars for the different Phoenix connectors.

```
$ mvn package
```

### Building against specific Phoenix version
To build a release of Phoenix Connectors which packages a specific version of Phoenix, specify the `phoenix.version` system property to indicate a specific Phoenix version.


When specifying `phoenix.version`, also specify the HBase version to be used
by the corresponding `hbase-two.version` system property.
Similarly you can override the `hadoop-three.version` system properties if necessary.

```
$ mvn package -Dphoenix-five.version=5.1.0-SNAPSHOT
```

### Running integration tests

`mvn package` will run the unit tests while building, but it will not run the integration test suite.

The IT suite is run when executing `mvn install` or `mvn verify`. The Phoenix version specified
with `phoenix.version` is used for running the integration tests.

```
$ mvn install -Dphoenix-five.version=5.1.0-SNAPSHOT -Dhbase-one.version=2.1.9 -Dhadoop-three.version=3.0.3
```
