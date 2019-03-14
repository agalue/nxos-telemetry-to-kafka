# nxos-telemetry-to-kafka

The idea of this project is to implement a [gRPC](https://grpc.io/) server in order to stream telemetry statistics from a [Cisco Nexus](https://www.cisco.com/go/nexus) device, and then send these statistics to a topic in [Kafka](http://kafka.apache.org/).

The gRPC service definition for the Nexus devices is defined [here](https://github.com/CiscoDevNet/nx-telemetry-proto).

Even if the gRPC `gRPCMdtDialout` service is defined as bi-directional, this application focus only on the client streaming part; in other words, receiving data from the Nexus switch.

Using UDP is another way to obtain telemetry data from Cisco, but due to the physical size limitation of UDP (65,507 bytes according to [Wikipedia](https://en.wikipedia.org/wiki/User_Datagram_Protocol)), it it not possible to send large groups of telemetry data through UDP, due to the hierarchical structure of the data (obtained through `NX-API`, or data management engine `DME`), and the potential amount of resources involved on big switches. This is due to the fact that each UDP packet is independent of each other (it is self contained), and the switch won't split the data into multiple packets.

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
    ip address 192.168.0.253 port 50001 protocol gRPC encoding GPB
  sensor-group 200
    path sys/intf depth unbounded
  subscription 300
    dst-grp 100
    snsr-grp 200 sample-interval 300000
```

This application requires to pass the Kafka Bootstrap Server string, the Kafka Topic on which the data will be stored, and the port on which the gRPC server will be listening for telemetry data. It also provides a flag in case you want to see the human readable version of the GPB data on the logs (for testing or debugging purposes).

As there are 2 possible encoding protocols (GPB or JSON), when printing the data on the logs, it will try GPB first, then it will try JSON, as unlike UDP, the payload exracted from MdtDialoutArgs doesn’t tell you which kind of data you’re receiving. For UDP, the first 6 bytes contain that information as illustrated [here](https://www.cisco.com/c/en/us/td/docs/switches/datacenter/nexus9000/sw/7-x/programmability/guide/b_Cisco_Nexus_9000_Series_NX-OS_Programmability_Guide_7x/b_Cisco_Nexus_9000_Series_NX-OS_Programmability_Guide_7x_chapter_011000.html).

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
$ java -jar grpc2kafka-1.0.0-SNAPSHOT-jar-with-dependencies.jar --help
usage: grpc2kafka
 -b,--bootstrap-servers <arg>   Kafka bootstrap server list.
                                Default: 127.0.0.1:9092
 -d,--debug                     Show message on logs.
 -h,--help                      Show this help.
 -j,--json                      Convert GPB payload to JSON prior send it
                                to Kafka.
 -p,--port <arg>                gRPC server listener port.
                                Default: 50051
 -t,--topic <arg>               Kafka destination topic name.
                                Default: nxos-telemetry-grpc
```
                                
