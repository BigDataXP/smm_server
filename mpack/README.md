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
#### SMM Ambari Management Pack

#### Install maven 3.5.3
```
wget http://mirrors.gigenet.com/apache/maven/maven-3/3.5.5/binaries/apache-maven-3.5.3-bin.tar.gz
su -c "tar -zxvf apache-maven-3.5.3-bin.tar.gz -C /opt/"
export M2_HOME=/opt/apache-maven-3.5.3
export M2=$M2_HOME/bin
PATH=$M2:$PATH
echo "export M2_HOME=/opt/apache-maven-3.5.3" >> ~/.bashrc
echo "export M2=$M2_HOME/bin" >> ~/.bashrc
echo "PATH=$M2:$PATH" >> ~/.bashrc
```

#### Clone git
```
yum install -y git
git clone https://github.com/hortonworks/streams-messaging-manager.git ;  cd streams-messaging-manager;
```

#### Build Instructions:
```
cd mpack
mvn versions:set -DnewVersion=${smm_version}
mvn clean package
# SMM management pack will be created at target/smm-ambari-mpack-1.0.0.0-SNAPSHOT.tar.gz
```

##### Example:
```
cd mpack
curr_smm_version="49"
export JAVA_HOME=$(find /usr/jdk64 -iname 'jdk1.8*' -type d)
export M2_HOME=/opt/apache-maven-3.5.3
export M2=$M2_HOME/bin
PATH=$M2:$PATH
export smm_version=1.0.0.0.${curr_smm_version}
mvn versions:set -DnewVersion=${smm_version}
mvn clean package

#this will build the below tarball
ls -la target/smm-ambari-mpack-1.0.0.0-SNAPSHOT.tar.gz
```

### SMM can only be installed on HDP only, HDF only or HDF on HDP cluster.

- Install SMM mpack
```
ambari-server install-mpack --mpack=/path/to/smm-ambari-mpack-1.0.0.0-SNAPSHOT.tar.gz --verbose
```

- Start ambari-server
```
ambari-server restart
```

- update public repo URL
```
Go to Stack and Versions Section on Ambari, click Versions -> Manage Versions, Click HDP or HDF version link. Update SMM link with its public repo link.
```

- Add SMM
```
Go to Add Dashboard, Add Service Section, Add Streams Messaging Manager. For further info, please refer to docs.hortonworks.com
```