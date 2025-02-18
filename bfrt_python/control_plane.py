import json
import time
from datetime import datetime,timezone
import threading
import socket
import atexit

p4 = bfrt.Per_Flow_Collector.pipe

long_flows = {}
number_of_long_flows = 0
sketches_reset_lock = 0

P4_start_time = datetime.now(timezone.utc).timestamp()

host = socket.gethostname()    

sock_60002 = socket.socket()         # New flow thread
port = 60002                 
sock_60002.connect((host, port))

sock_60003 = socket.socket()         # RTT thread
port = 60003                 
sock_60003.connect((host, port))

sock_60004 = socket.socket()         # Throughput thread   
port = 60004                  
sock_60004.connect((host, port))

sock_60005 = socket.socket()         # Queuing delay thread   
port = 60005                 
sock_60005.connect((host, port))

sock_60006 = socket.socket()         # Retransmission thread   
port = 60006                 
sock_60006.connect((host, port))



def send_data(metric_name,metric_value, socket, metric_samples_per_second=250,to_sleep=True,time_deduction_offset=0,source_ip=None,flow_id=None,dst_ip=None,limited_by="Network"):
    import time,ipaddress
    from datetime import datetime,timezone

    metric_value = "{:.4f}".format(metric_value)
    stats = {"metric_name": metric_name, metric_name:metric_value}
    stats["per_flow_measurement"] ={"type": metric_name}
    stats["report_time"] = datetime.utcfromtimestamp(datetime.now(timezone.utc).timestamp()).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    if (source_ip != None):
        stats["source_ip"] = source_ip
    if (dst_ip != None):
        stats["dst_ip"] = dst_ip
    if (flow_id != None):
        stats["flow_id"] = flow_id
    stats = json.dumps(stats)
    stats +="\n"
    socket.send(stats.encode())
    if(to_sleep):
        time.sleep(1)

def new_long_flow(dev_id, pipe_id, direction, parser_id, session, msg):
    import ipaddress
    from datetime import datetime,timezone
    import time
    global p4
    global long_flows
    global P4_start_time
    global number_of_long_flows
    global sock_60002
    
    for digest in msg:
        flow_id = digest['flow_id']
        rev_flow_id = digest['rev_flow_id']
        src_IP = str(ipaddress.ip_address(digest['flow_source_IP']))
        dst_IP = str(ipaddress.ip_address(digest['flow_destination_IP']))
        src_port = digest['flow_source_port']
        dst_port = digest['flow_destination_port']
        
        flow_start_time = float(p4.Ingress.flow_start_end_time.get\
                     (REGISTER_INDEX=flow_id, from_hw=True, print_ents=False).data[b'Ingress.flow_start_end_time.flow_start_time'][1])/10**6 + P4_start_time
        
        long_flows[digest["flow_id"]] = {'rev_flow_id':rev_flow_id,'src_IP':src_IP,'dst_IP':dst_IP,'src_port':src_port, 'dst_port':dst_port,"flow_start_time":flow_start_time \
                                ,"per_flow_measurement": {'type':"long_flow"},'flow_id':digest["flow_id"],"number_of_bytes":0, \
                                    "old_retr":0,"bloating_number":0,"total_packets":0,"counter":0,"prev_throughput":0}       
        number_of_long_flows+=1
        report = {'flow_id':flow_id, 'src_IP':src_IP,'dst_IP':dst_IP,'src_port':src_port, 'dst_port':dst_port,"per_flow_measurement": {'type':"new_flow_report"},'p_count':1 }
        report["report_time"] = datetime.utcfromtimestamp(datetime.now(timezone.utc).timestamp()).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        report = json.dumps(report)
        report +="\n"
        sock_60002.send(report.encode())
        
        p4.Ingress.counted_flow.add_with_meter(flow_id=digest['flow_id'], ENTRY_TTL = 500)
    return 0

def long_flow_timeout(dev_id, pipe_id, direction, parser_id, entry):
    import json
    from datetime import datetime,timezone
    global p4
    global long_flows
    global P4_start_time
    global number_of_long_flows
    try:

        flow_id = entry.key[b'meta.flow_id']
        
        number_of_bytes = float(p4.Ingress.Sample_length.get\
                             (REGISTER_INDEX=flow_id, from_hw=True, print_ents=False).data[b'Ingress.Sample_length.f1'][1])
        end_time = float(p4.Ingress.flow_start_end_time.get\
                        (REGISTER_INDEX=flow_id, from_hw=True, print_ents=False).data[b'Ingress.flow_start_end_time.flow_end_time'][1])/10**6 + P4_start_time
        duration = end_time - long_flows[flow_id]['flow_start_time']
        p4.Ingress.flow_start_end_time.mod(REGISTER_INDEX=flow_id, flow_start_time=0)
        p4.Ingress.flow_start_end_time.mod(REGISTER_INDEX=flow_id, flow_end_time=0)
        p4.Egress.total_packets.mod(REGISTER_INDEX=flow_id, f1=0)
        
        long_flows[flow_id]['flow_end_time'] = end_time
        long_flows[flow_id]['duration'] = duration
        long_flows[flow_id]['number_of_bytes'] = number_of_bytes
        long_flows[flow_id]['bitrate'] = (long_flows[flow_id]['number_of_bytes'])*8/long_flows[flow_id]['duration']
        report =long_flows[flow_id]
        report["report_time"] = datetime.utcfromtimestamp(datetime.now(timezone.utc).timestamp()).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        report = json.dumps(report)
        report += "\n"
        del long_flows[flow_id]
        number_of_long_flows-=1
    except Exception as e:
            print(e)

def reset_sketches():
    global p4
    import time
    global long_flows
    global sketches_reset_lock
    
    while (True):
        time.sleep(4)
        if(sketches_reset_lock):
            p4.Ingress.sketch0.clear()
            p4.Ingress.sketch1.clear()
            p4.Ingress.sketch2.clear()

            p4.Ingress.reg_table_1.clear() 
        else:
            time.sleep(1)
            sketches_reset_lock = 1

def queue_delay_thread():

    import time

    global send_data
    global p4
    global long_flows
    global sock_60005
 
    metric_name = "queue_occupancy"

    while (1):
        try:
            if(len(long_flows)>0):    
                queue_delay = p4.Egress.queue_delays.get(REGISTER_INDEX=0, from_hw=True, print_ents=False).data[b'Egress.queue_delays.f1'][1]
                queue_delay = (queue_delay) / (50000000)
                metric_value = queue_delay
                send_data(metric_name,metric_value,to_sleep=False,socket = sock_60005)
            time.sleep(0.005)
        except KeyError:
            pass
        except Exception as e:
            print("An error occurred in queue_delay_thread :", str(e))
            break

    return 0

def rtt_thread():
    import time

    global send_data
    global p4
    global long_flows
    global sock_60003 
    metric_name = "rtt"

    while (1):
        try:
            for flow_id in long_flows.copy():  
                rtt = float(p4.Ingress.rtt.get\
                                        (REGISTER_INDEX=long_flows[flow_id]["rev_flow_id"],from_hw=True, print_ents=False).data[b'Ingress.rtt.f1'][1])
                metric_value = rtt/1e9
                send_data(metric_name,metric_value,flow_id=flow_id,socket=sock_60003,to_sleep=False,dst_ip=long_flows[flow_id]["dst_IP"],source_ip=long_flows[flow_id]["src_IP"])
            time.sleep(1)
        except KeyError:
            pass
        except Exception as e:
            print("Error occured in the rtt_thread: ",e)
            break

    return 0

def calculate_jains_fairness(bandwidth_allocations):
    num_flows = len(bandwidth_allocations)
    sum_bandwidth = sum(bandwidth_allocations)
    if sum_bandwidth > 0:
        sum_squared_bandwidth = sum([x ** 2 for x in bandwidth_allocations])
        
        fairness_index = (sum_bandwidth ** 2) / (num_flows * sum_squared_bandwidth)
        return fairness_index
    else:
        return -1

def throughput_thread():

    import time
    from datetime import datetime
    
    global calculate_jains_fairness
    global send_data
    global p4
    global long_flows
    global sock_60004
 
    metric_name = "throughput"

    while (1):
        try:
            throughput_list = []
            for flow_id in long_flows.copy():    
                total_bytes = float(p4.Ingress.Sample_length.get\
                                        (REGISTER_INDEX=flow_id,from_hw=True, print_ents=False).data[b'Ingress.Sample_length.f1'][1])
                
                new_bytes = total_bytes - long_flows[flow_id]["number_of_bytes"]+ long_flows[flow_id]["bloating_number"]*4294967296
                if (new_bytes < 0):
                    new_bytes = total_bytes + 4294967296 - long_flows[flow_id]["number_of_bytes"] + long_flows[flow_id]["bloating_number"]*4294967296 
                    long_flows[flow_id]["bloating_number"] += 1
                metric_value = (new_bytes)*8
                throughput_list.append(metric_value)
                
                link_utilization = sum(throughput_list)/9.9e9 if sum(throughput_list)/9.9e9 < 1 else 1
                send_data("link_utilization",link_utilization,to_sleep=False,flow_id=flow_id,socket=sock_60004)
                send_data("new_bytes",new_bytes,to_sleep=False,flow_id=flow_id,socket=sock_60004,dst_ip=long_flows[flow_id]["dst_IP"],source_ip=long_flows[flow_id]["src_IP"])

                # send_data("total_bytes",long_flows[flow_id]["bloating_number"]*4294967296 + total_bytes,to_sleep=False,flow_id=flow_id,socket=sock_60004,dst_ip=long_flows[flow_id]["dst_IP"],source_ip=long_flows[flow_id]["src_IP"])
                
                long_flows[flow_id]["number_of_bytes"] += new_bytes
                send_data(metric_name,metric_value,to_sleep=False,flow_id=flow_id,socket=sock_60004,dst_ip=long_flows[flow_id]["dst_IP"],source_ip=long_flows[flow_id]["src_IP"])
            
            fairenss_index = calculate_jains_fairness(throughput_list)
            if fairenss_index > -1:
                send_data("fairenss_index",fairenss_index,to_sleep=False,socket=sock_60004)

            time.sleep(1)
        except KeyError:
            pass
        except Exception as e:
            print("Error occured in the throughput_thread: ",e)
            break

    return 0

def retr_thread():

    import time

    global send_data
    global p4
    global long_flows
    global sock_60006
 
    metric_name = "retr"
 
    while (1):
        try:
            for flow_id in long_flows.copy():     
                retr = float(p4.Ingress.retr.get\
                                        (REGISTER_INDEX=flow_id,from_hw=True, print_ents=False).data[b'Ingress.retr.f1'][1])
                total_packets = float(p4.Egress.total_packets.get\
                                        (REGISTER_INDEX=flow_id,from_hw=True, print_ents=False).data[b'Egress.total_packets.f1'][1])
                new_total_packets = total_packets - long_flows[flow_id]["total_packets"]
                send_data("total_number_of_packets",new_total_packets,to_sleep=False,flow_id=flow_id,dst_ip=long_flows[flow_id]["dst_IP"],source_ip=long_flows[flow_id]["src_IP"],socket=sock_60006)
                
                if(total_packets == long_flows[flow_id]["total_packets"]):
                    metric_value = 0
                else:
                    metric_value = retr - long_flows[flow_id]["old_retr"]
                    retr_percentage = ((retr - long_flows[flow_id]["old_retr"]) / (total_packets - long_flows[flow_id]["total_packets"]))*100 
                    long_flows[flow_id]["total_packets"] = total_packets
                    send_data("retr_percentage",retr_percentage,to_sleep=False,flow_id=flow_id,dst_ip=long_flows[flow_id]["dst_IP"],source_ip=long_flows[flow_id]["src_IP"],socket=sock_60006)

                long_flows[flow_id]["old_retr"] = retr
                send_data(metric_name,metric_value,to_sleep=False,flow_id=flow_id,dst_ip=long_flows[flow_id]["dst_IP"],socket=sock_60006)
            time.sleep(1)
        except KeyError:
            pass
        except Exception as e:
            print("Error occured in the retr_thread: ",e)
            break

    return 0

def number_of_flows():

    import time

    global send_data
    global p4
    global number_of_long_flows
 
    metric_name = "number_of_flows"

    while (1):
        metric_value = number_of_long_flows
        send_data(metric_name,metric_value,to_sleep=False)
        time.sleep(1)

    return 0
 
try:
    p4.Ingress.counted_flow.idle_table_set_notify(enable=True, callback=long_flow_timeout, interval=200, min_ttl=400, max_ttl=1000)
    p4.IngressDeparser.new_long_flow_digest.callback_register(new_long_flow)
except:
    print('Error registering callback')

periodic_reset = threading.Thread(target=reset_sketches, name="reset_sketches")

th_queue_delay_thread = threading.Thread(target=queue_delay_thread, name="queue_delay_thread")

th_rtt_thread = threading.Thread(target=rtt_thread, name="th_rtt_thread")

th_throughput_thread = threading.Thread(target=throughput_thread , name="th_throughput_thread")


th_retr_thread = threading.Thread(target=retr_thread, name="th_retr_thread")
# th_number_of_flows = threading.Thread(target=number_of_flows, name="number_of_flows")


periodic_reset.start()

th_queue_delay_thread.start()

th_rtt_thread.start()

th_throughput_thread.start()

th_retr_thread.start()
# th_number_of_flows.start()

i=0
while(1):
   
    if(number_of_long_flows==0):
        i+=1
    else:
        i=0

    if(i==3): #10):
        sketches_reset_lock = 0
        try:
            i=0
            long_flows.clear()
        
            p4.Ingress.calc_flow_id.clear() 
            p4.Ingress.calc_rev_flow_id.clear()    
            p4.Ingress.copy32_1.clear()    
            p4.Ingress.copy32_2.clear()    
            p4.Ingress.crc16_1.clear()    
            p4.Ingress.crc16_2.clear()    
            p4.Ingress.crc32_1.clear()    
            p4.Ingress.crc32_2.clear()     
            
            long_flows.clear()
            p4.Ingress.sketch0.clear()
            p4.Ingress.sketch1.clear()
            p4.Ingress.sketch2.clear()

        except Exception as e:
            print(e)
    
    time.sleep(1)