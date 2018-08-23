<!---
  HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
  
  (c) 2016-2018 Hortonworks, Inc. All rights reserved.
  
  This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms
  of the Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party
  authorized to distribute this code.  If you do not have a written agreement with Hortonworks or with
  an authorized and properly licensed third party, you do not have any rights to this code.
  
  If this code is provided to you under the terms of the AGPLv3: A) HORTONWORKS PROVIDES THIS CODE TO YOU
  WITHOUT WARRANTIES OF ANY KIND; (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH
  RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY
  AND FITNESS FOR A PARTICULAR PURPOSE; (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY,
  OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING FROM OR RELATED TO THE CODE; AND (D) WITH RESPECT
  TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY DIRECT,
  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
  DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
  OR LOSS OR CORRUPTION OF DATA.
-->
# Streams Messaging Manager

### Build

* Clone the repository from github with `git clone https://github.com/hortonworks/streams-messaging-manager`
* Go to the root directory of the cloned repository and run `mvn clean install && cd dist && mvn clean package`
* Under `$SMM_REPO_ROOT_DIR/dist/target` directory one can find a zip and a tar file, copy any one of them to your desired location and unzip it

### Setup

* After following the steps to build the repository go into the directory obtained by unzipping the binary
* Configure your environment by editing `conf/streams-messaging-manager.yaml`. Following are the minimal configuration that needs to be edited to bring up the service
    * **kafkaBootstrapServers**: a list of kafka brokers separated by comma
    * **schemaRegistryUrl**: url to schema registry service
    * **db.type**, **dataSource.url**, **dataSource.user**, **dataSource.password** under **storageProviderConfiguration**: properties to configure database
    * **ams.timeline.metrics.hosts**, **ams.timeline.metrics.port** under **KafkaMetricsConfig**: properties to connect to ambari metric service
* Once the required configuration has been updated if required create the necessary database, users and permissions against the target database type
* Run `sh bootstrap/bootstrap_storage.sh migrate` command to trigger SQL migration on the target database
* Once the SQL migration has gone through, run `sh bin/streams-messaging-manager.sh start` to start up the service
* Logs for the service can be found under `logs` directory
* To stop the service run `sh bin/streams-messaging-manager.sh stop`

### Contributing

If you wish to make contributions to this project, refer to [CONTRIBUTING.md](https://github.com/hortonworks/dps_platform/blob/master/CONTRIBUTING.md) to DPS for more information.

### License

The DPS Platform and it's associated DPS applications are made available under the terms
of the [GNU Affero General Public License (GNU AGPLv3)](COPYING).
