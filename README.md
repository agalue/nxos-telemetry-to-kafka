# nxos-telemetry-to-kafka

The idea of this project is to implement a gRPC server in order to stream telemetry statistics from a [Cisco Nexus](https://www.cisco.com/go/nexus) device, and then send these statistics to a topic in [Kafka](http://kafka.apache.org/).

The gRPC service definition for the Nexus devices is defined [here](https://github.com/CiscoDevNet/nx-telemetry-proto).

Even if the gRPC gRPCMdtDialout  service is defined as bi-directional, this application focus only on the client streaming part; in other words, receiving data from the Nexus switch.

Using UDP is another way to obtain telemetry data from Cisco, but due to the physical size limitation of UDP (65,507 bytes according to [Wikipedia](https://en.wikipedia.org/wiki/User_Datagram_Protocol)), it it not possible to send large groups of telemetry data through UDP, due to the hierarchical structure of the "NX-API or DMI sources" and the potential amount of resources involved on big switches. This is due to the fact that each UDP packet is independent of each other (it is self contained), and the switch won't split the data into multiple packets.

These limitations force the administrator of the switch to define hundreds if not thousands of sensor path entries on the telemetry configuration to guarantee that the amount of data fits the UDP packet.

For example,

```
telemetry
  destination-group 100
    ip address 192.168.0.253 port 50001 protocol UDP encoding GPB 
  sensor-group 200
    path sys/intf/phys-[eth1/1]/dbgIfHCIn depth 0
    path sys/intf/phys-[eth1/1]/dbgIfHCOut depth 0
    path sys/intf/phys-[eth1/2]/dbgIfHCIn depth 0
    path sys/intf/phys-[eth1/2]/dbgIfHCOut depth 0
...
    path sys/intf/phys-[eth10/96]/dbgIfHCIn depth 0
    path sys/intf/phys-[eth10/96]/dbgIfHCOut depth 0
  subscription 300
    dst-grp 100
    snsr-grp 200 sample-interval 300000
```

Also, due to the MTU settings across the network infrastructure between the Nexus and the recipient (in this case Kafka), that also applies as a limitation for choosing what to send over UDP, even when using GPB as the encoding protocol (as the payload is smaller than JSON, besides another benefits).

Fortunately this is not a limitation when using gRPC as the transport protocol, and it is possible to send a huge section of the telemetry data on a single sensor definition, which simplifies the maintenance of the telemetry configuration on the switches.

For example,

```
telemetry
  destination-group 100
    ip address 192.168.0.253 port 50001 protocol UDP encoding GPB 
  sensor-group 200
	  path sys/intf depth unbounded
  subscription 300
    dst-grp 100
    snsr-grp 200 sample-interval 300000
```

This application requires to pass the Kafka Bootstrap Server string, the Kafka Topic on which the data will be stored, and the port on which the gRPC server will be listening for telemetry data. It also provides a flag in case you want to see the human readable version of the GPB data on the logs (for testing or debugging purposes).

As there are 2 possible encoding protocols (GPB or JSON), when printing the data on the logs, it will try GPB first, then it will try JSON, as unlike UDP, the payload exracted from MdtDialoutArgs doesn’t tell you which kind of data you’re receiving. For UDP, the first 6 bytes contain that information as illustrated [here](https://www.cisco.com/c/en/us/td/docs/switches/datacenter/nexus9000/sw/7-x/programmability/guide/b_Cisco_Nexus_9000_Series_NX-OS_Programmability_Guide_7x/b_Cisco_Nexus_9000_Series_NX-OS_Programmability_Guide_7x_chapter_011000.html).

# OpenNMS

Starting with Horizon 22, OpenNMS support streaming telemetry from NX-OS devices over UDP using GPB only. When using Minions, it is possible to put Kafka in the middle of the communication.

Due to the complexity associated with Java dependencies on a stand-alone JVM and inside an OSGi container (which is what Minion uses), this application has been modified from its the original implementation, in order to send the data to Kafka in the same way on which an OpenNMS Minion would do it.

In other words, this application is basically bypassing the Minion by doing exactly what it should do to put telemetry data on Kafka. In order to do this, the data obtained from the Nexus device (which has to be in GPB, as OpenNMS doesn’t support JSON) should be wrapped into another GPB packet designed for processing Telemetry through the Sink pattern. The definition of this file is included here (telemetry.proto), and it was re-compiled to avoid depending on the OpenNMS JARs (as a proto file is the contract between GPB/gRPC applications).

As a side note, `telemetry.proto` file is not compliant with the protocol definition, as it is mandatory to specify the version. The following is being assumed:

```
syntax="proto2"
```

Otherwise, it cannot be compiled.

For this reason, besides the original requirements, the Minion ID and the Minion Location are required, to make sure that OpenNMS will be able receive, decode, process and persist the data correctly.

In terms of OpenNMS configuration, use the exact same settings you would use if you were using an UDP listener, because the adapter will get the same data.

Check the [official docs](http://docs.opennms.org/opennms/releases/22.0.0/guide-admin/guide-admin.html#_cisco_nx_os_telemetry) for more information about the telemetry feature in OpenNMS.

This project can serve as a reference in order to formally implement the Listener interface which is part of the OpenNMS Telemetry API (see the content of the opennms directory), but handling the dependencies to have gRPC working smoothly within OpenNMS or a Minion is not easy.

# Integration Challenges

Here are some facts to keep in mind, when trying to do it:

* OpenNMS H22 depends on Protobuf 2, but gRPC 1.12.0 depends on Protobuf 3 (Minions only loads Protobuf 2 within Karaf)
* Protobuf 3.5.1 depends on Guava 19, but gRPC 1.12.0 depends on Guava 20 (none of these versions of Guava are present in OpenNMS)
* OpenNMS H22 depends on Gson 2.8, but gRPC 1.12.0 depends on Gson 2.7
* Gson 2.7 is a valid OSGi bundle, but 2.8 doesn’t (fixed on 2.8.1, which seems to work with gRPC 1.12.0)
* Protobuf 3.5.1 JARs are OSGi bundles.
* gRPC 1.12.0 JARs are non-OSGi bundles (the wrap:mvn protocol is required).
* All gRPC dependencies, except for Guava, Probuf and Gson are non-OSGi bundles.
* grpc-netty-shaded is required to avoid conflicts with the Netty version used by OpenNMS. Also, this is the version to use when TLS/SSL is required (at least for 1.12.0).
* OpenNMS H22 uses Netty 4.1.9.Final while grpc-netty relies on 4.1.22.Final

During preliminary experiments, the gRPC listener was running within OpenNMS, after replacing Guava and Protobuf with their respective latest versions. Unfortunately, due to how Karaf/OSGi handle dependencies I was not able to have it running within a Minion.

This application is ready to use and can be used in conjunction with the Minion to provide the missing capabilities to OpenNMS.

Now, handling the generated GPB object that Cisco produces is unfortunately not easy. OpenNMS provides a helper class called NxosGpbParserUtil, used when writing the required Groovy script to convert the telemetry data into a CollectionSet.

This helper class was designed on such way that it simplify the life of the developer when using UDP as the transport protocol on the Nexus Switch. Unfortunately, when sending larger metric sets using gRPC, the structure is a little bit different, and the current status of the helper class requires some tuning. Fortunately, the missing methods can be added on the Groovy script (see the content of the opennms directory, for an example).

# Requirements

* Oracle JDK 8
* Maven 3

# Compilation

```SHELL
mvn install
```

The generated JAR with dependencies (onejar) contains everything needed to execute the application.

# Usage

```SHELL
$ java -jar grpc2kafka-1.0.0-SNAPSHOT-onejar.jar
```

For more details:

```SHELL
$ java -jar grpc2kafka-1.0.0-SNAPSHOT-onejar.jar -h
usage: grpc2kafka
 -b,--bootstrap-servers <arg>   Kafka bootstrap server list.
                                Default: 127.0.0.1:9092
 -d,--debug                     Show message on logs.
 -h,--help                      Show this help.
 -l,--minion-location <arg>     OpenNMS Minion Location.
 -m,--minion-id <arg>           OpenNMS Minion ID.
 -p,--port <arg>                gRPC server listener port.
                                Default: 50051
 -t,--topic <arg>               Kafka destination topic name.
                                Default: OpenNMS.Sink.Telemetry-NXOS
```

It is recommended to run this application on the same machine where the OpenNMS Minion is running, and make sure to provide the appropriate settings. It is important that the Minion is using Kafka for the Sink pattern in order to have this solution working.
                                
