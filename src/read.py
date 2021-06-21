import sys
import time
import json
import geoip2.database
from memsql.common import database

k = 0
reader = geoip2.database.Reader('/root/GeoLite2-City.mmdb')
asnreader = geoip2.database.Reader('/root/GeoLite2-ASN.mmdb')

# MEMSQL CONNECTION
HOST = "127.0.0.1"
PORT = 3306
USER = "root"
PASSWORD = "testpw"
DATABASE = "vflow"

connection = database.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE, options={'auth_plugin':'mysql_native_password'})


def clear_old_records():
    query = "delete from samples where datetime < date_sub(NOW(), interval -2 hour);"
    connection.execute(query)
    return


if not connection.connected():
    print('can not connect to the database')
else:
    try:
        buff = ''
        while True:
            buff += sys.stdin.read(1)
            if buff.endswith('\n'):
                #print(buff)
                #print(k)
                try:
                    flows = json.loads(buff)
                except Exception as e:
                    print(e, buff)
                exported_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                              time.localtime(flows["Header"]["ExportTime"]))

                try:
                    for flow in flows["DataSets"]:
                        sourceIPAddress = "unknown"
                        destinationIPAddress = "unknown"
                        bgpSourceAsNumber = "unknown"
                        bgpDestinationAsNumber = "unknown"
                        protocolIdentifier = 0
                        sourceTransportPort = 0
                        destinationTransportPort = 0
                        tcpControlBits = "unknown"
                        ipNextHopIPAddress = "unknown"
                        octetDeltaCount = 0
                        ingressInterface = 0
                        egressInterface = 0
                        direction = ""
                        switch = 0
                        AsNumber = ""
                        Country = ""

                        for field in flow:
                            try:
                                if field["I"] in [214]:
                                    continue
                                elif field["I"] in [8, 27]:
                                    sourceIPAddress = field["V"]
                                    AsNumber = asnreader.asn(field["V"]).autonomous_system_number
                                    if AsNumber == 57844:
                                        direction = "upload"
                                        switch = 1
                                    else:
                                        direction = "download"
                                        Country = reader.city(field["V"]).country.iso_code

                                elif field["I"] in [12, 28]:
                                    destinationIPAddress = field["V"]
                                    if switch == 1:
                                        AsNumber = asnreader.asn(field["V"]).autonomous_system_number
                                        Country = reader.city(field["V"]).country.iso_code

                                elif field["I"] in [15, 62]:
                                    ipNextHopIPAddress = field["V"]
                                elif field["I"] == 16:
                                    bgpSourceAsNumber = field["V"]
                                elif field["I"] == 17:
                                    bgpDestinationAsNumber = field["V"]
                                elif field["I"] == 14:
                                    ingressInterface = field["V"]
                                elif field["I"] == 10:
                                    egressInterface = field["V"]
                                elif field["I"] == 7:
                                    sourceTransportPort = field["V"]
                                elif field["I"] == 11:
                                    destinationTransportPort = field["V"]
                                elif field["I"] == 4:
                                    protocolIdentifier = field["V"]
                                elif field["I"] == 6:
                                    tcpControlBits = field["V"]
                                elif field["I"] == 1:
                                    octetDeltaCount = field["V"]

                            except Exception as e:
                                #print(f'error:{e}')
                                pass

                        out = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
                              % (
                                  flows["AgentID"],
                                  sourceIPAddress,
                                  destinationIPAddress,
                                  ipNextHopIPAddress,
                                  bgpSourceAsNumber,
                                  bgpDestinationAsNumber,
                                  protocolIdentifier,
                                  sourceTransportPort,
                                  destinationTransportPort,
                                  tcpControlBits,
                                  ingressInterface,
                                  egressInterface,
                                  octetDeltaCount,
                                  exported_time,
                              )

                        mm = "insert into samples (src,dst,proto,srcPort,dstPort,tcpFlags,bytes,datetime,AsNumber,country,direction) values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}' ) ;".format(
                            sourceIPAddress, destinationIPAddress, protocolIdentifier, sourceTransportPort,
                            destinationTransportPort, tcpControlBits, octetDeltaCount, exported_time, AsNumber, Country,
                            direction)

                        connection.execute(mm)
                        #print(out)
                        #print(mm)
                        #print(f'types asn,country,direction:{type(AsNumber), type(Country), type(direction)}')

                except Exception as e:
                    print(f'error:{e} buff:{buff}')
                    pass

                buff = ''
                #k = k + 1
                #break
    except KeyboardInterrupt:
        #sys.stdout.flush()
        pass
    print(f'{k} line read')
