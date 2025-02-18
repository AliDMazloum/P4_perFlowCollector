/* -*- P4_16 -*- */

#include <core.p4>
#include <tna.p4>
#include "Headers_and_Definitions.p4"



struct flow_digest_t {
    bit<11> flow_id;
	bit<11> rev_flow_id;
    bit<32> flow_source_IP;
    bit<32> flow_destination_IP;
    bit<16> flow_source_port;
    bit<16> flow_destination_port;	
}

struct flow_timing_t {
    bit<32> flow_start_time;
    bit<32> flow_end_time;
}

struct flow_sampling_t {
    bit<32> prev_sampling_time;
    bit<32> flow_sampling_rate;
} 
  

/*************************************************************************
 **************  I N G R E S S   P R O C E S S I N G   *******************
 *************************************************************************/

    /***********************  H E A D E R S  ************************/
    struct my_ingress_headers_t {
        ethernet_h   ethernet;
        ipv4_h       ipv4;
        tcp_h        tcp;
        udp_h        udp;
        rtp_h        rtp;

    }
    /******  G L O B A L   I N G R E S S   M E T A D A T A  *********/
    struct my_ingress_metadata_t {
        bit<11> flow_id;
		bit<11> rev_flow_id;
        
        bit<32> flow_sample_length;

        bit<32> flow_end_time;

        bit<32> check_to_sample;

		bit<9> in_port;
		

        bit<13> index_sketch0;
		bit<13> index_sketch1;
		bit<13> index_sketch2;
		bit<13> index_sketch3;

		bit<1> value_sketch0;
		bit<1> value_sketch1;
		bit<1> value_sketch2;
		bit<1> value_sketch3;

        bit<32> expected_ack;
        bit<32> pkt_signature;

        bit<16> hashed_location_1;
        bit<16> hashed_location_2;

        bit<32> table_1_read;
        bit<32> rtt;
        bit<32> lost;
		
		bit<8> lock;
		bit<32> seq_hash;

        bool pkt_type;
    
        bit<32> tmp_1;
        bit<32> tmp_2;
        bit<32> tmp_3;
        bit<32> total_hdr_len_bytes; 
        bit<32> total_body_len_bytes;

        bit<32> total_before;
		bit<32> total_after;

        bit<32> flow_current_time;

        bit<1> last_was_ack;
        bit<32> last_ack_arrival;

    }
    /***********************  P A R S E R  **************************/
    parser IngressParser(packet_in        pkt,
        /* User */
        out my_ingress_headers_t          hdr,
        out my_ingress_metadata_t         meta,
        /* Intrinsic */
        out ingress_intrinsic_metadata_t  ig_intr_md)
        {
            /* This is a mandatory state, required by Tofino Architecture */
            state start {
                pkt.extract(ig_intr_md);
                pkt.advance(PORT_METADATA_SIZE);
                transition parse_ethernet;
            }

            state parse_ethernet {
                pkt.extract(hdr.ethernet);
                transition select(hdr.ethernet.ether_type) {
                    ETHERTYPE_IPV4:  parse_ipv4;
                    default: accept;
                }
            }

            state parse_ipv4 {
                pkt.extract(hdr.ipv4);
                transition select(hdr.ipv4.protocol) {
                    6: parse_tcp;
                    0x11: parse_udp;
                    default: accept;
                }
            }

            state parse_tcp {
                pkt.extract(hdr.tcp);
                transition accept;
            }

            state parse_udp {
                pkt.extract(hdr.udp);
                transition parse_rtp;
            }
            
            state parse_rtp {
                pkt.extract(hdr.rtp);
                transition accept;
            }


        }


    /***************** M A T C H - A C T I O N  *********************/
    control Ingress(
        /* User */
        inout my_ingress_headers_t                       hdr,
        inout my_ingress_metadata_t                      meta,
        /* Intrinsic */
        in    ingress_intrinsic_metadata_t               ig_intr_md,
        in    ingress_intrinsic_metadata_from_parser_t   ig_prsr_md,
        inout ingress_intrinsic_metadata_for_deparser_t  ig_dprsr_md,
        inout ingress_intrinsic_metadata_for_tm_t        ig_tm_md)
        {
            
            // ***********************************************************************************************
            // *************************        H  A S H I N G       *****************************************
            // ***********************************************************************************************
            Hash<bit<11>>(HashAlgorithm_t.CRC16) hash;
                action apply_hash() {
                    meta.flow_id = hash.get({
                        hdr.ipv4.src_addr,
                        hdr.ipv4.dst_addr,
                        hdr.ipv4.protocol,
                        hdr.tcp.src_port,
                        hdr.tcp.dst_port
                                        });
                                    }

                table calc_flow_id {
                    actions = {
                        apply_hash;
                    }
                    const default_action = apply_hash();
                }
    
            Hash<bit<11>>(HashAlgorithm_t.CRC16) rev_hash;
                action apply_rev_hash() {
                    meta.rev_flow_id = rev_hash.get({
                        hdr.ipv4.dst_addr,
                        hdr.ipv4.src_addr,
                        hdr.ipv4.protocol,
                        hdr.tcp.dst_port,
                        hdr.tcp.src_port
                    });
                }
                table calc_rev_flow_id {
                    actions = {
                        apply_rev_hash;
                    }
                    const default_action = apply_rev_hash();
                }

            // ***********************************************************************************************
            // *************************     E N D   H  A S H I N G       ************************************
            // ***********************************************************************************************

            // ***********************************************************************************************
            // *************************     S T A R T   C M S       *****************************************
            // ***********************************************************************************************  
            SKETCH_REGISTER(0);
            SKETCH_REGISTER(1);
            SKETCH_REGISTER(2);

            CRCPolynomial<bit<13>>( 0x04C11DB7, true, false, false, 13w0xFFFF, 13w0xFFFF) crc10d_1; 
            CRCPolynomial<bit<13>>( 0xEDB88320, true, false, false, 13w0xFFFF, 13w0xFFFF) crc10d_2; 
            CRCPolynomial<bit<13>>( 0x1A83398B, true, false, false, 13w0xFFFF, 13w0xFFFF) crc10d_3; 

            SKETCH_COUNT(0, HashAlgorithm_t.CUSTOM, crc10d_1)
            SKETCH_COUNT(1, HashAlgorithm_t.CUSTOM, crc10d_2)
            SKETCH_COUNT(2, HashAlgorithm_t.CUSTOM, crc10d_3)
            
            action meter(){
            }

            // This table stores the ID of the active flows
            // idle_timeout is enabled to automatically remove unactive flows and notify the control plane 
            table counted_flow {
                key = {
                    meta.flow_id: exact;
                }
                actions = {
                    meter;
                    NoAction;
                }
                size = 2024;
                const default_action = NoAction;
                idle_timeout = true;
            }

            // ***********************************************************************************************
            // *************************     E N D   C M S       *********************************************
            // ***********************************************************************************************


            // ***********************************************************************************************
            // *************************     GET THROUGHPUT PER FLOW       ***********************************
            // ***********************************************************************************************

            Register<bit<32>, _>(2048) Sample_length;
            RegisterAction<bit<32>, _, bit<32>>(Sample_length) update_Sample_length = {
                void apply(inout bit<32> register_data) {
                    register_data = register_data + (bit<32>)hdr.ipv4.total_len;
                }
            };

            action exec_update_Sample_length(){
                update_Sample_length.execute(meta.flow_id);
            }          

            // ***********************************************************************************************
            // ***********************************************************************************************
            // ***********************************************************************************************

            // ***********************************************************************************************
            // ********************************         R T T       *****************************************
            // ***********************************************************************************************

                action drop() {
                    ig_dprsr_md.drop_ctl = 0x1; // Drop packet.
                }  
                action mark_SEQ(){
                    meta.pkt_type=PKT_TYPE_SEQ;
                }
                action mark_ACK(){
                    meta.pkt_type=PKT_TYPE_ACK;
                }
                action drop_and_exit(){
                    drop();exit;
                }
                
                // Decide packet is a data packet or an ACK
                
                table tb_decide_packet_type {
                    key = {
                        hdr.tcp.flags: ternary;
                        hdr.ipv4.total_len: range;
                    }
                    actions = {
                        mark_SEQ;
                        mark_ACK;
                        drop_and_exit;
                    }
                    default_action = mark_SEQ();
                    size = 512;
                    const entries = {
                        (TCP_FLAGS_S,_): mark_SEQ();
                        (TCP_FLAGS_S+TCP_FLAGS_A,_): mark_ACK();
                        (TCP_FLAGS_A, 0..80 ): mark_ACK();
                        (TCP_FLAGS_A+TCP_FLAGS_P, 0..80 ): mark_ACK();
                        (_,100..1600): mark_SEQ();
                        (TCP_FLAGS_R,_): drop_and_exit();
                        (TCP_FLAGS_F,_): drop_and_exit();
                    }
                }
                
                // Calculate the expected ACK number for a data packet.
                // Formula: expected ACK=SEQ+(ipv4.total_len - 4*ipv4.ihl - 4*tcp.data_offset)
                // For SYN/SYNACK packets, add 1 to e_ack
                
                Hash<bit<32>>(HashAlgorithm_t.IDENTITY) copy32_1;
                Hash<bit<32>>(HashAlgorithm_t.IDENTITY) copy32_2;
                action compute_eack_1_(){
                    meta.tmp_1=copy32_1.get({26w0 ++ hdr.ipv4.ihl ++ 2w0});
                }
                action compute_eack_2_(){
                    meta.tmp_2=copy32_2.get({26w0 ++ hdr.tcp.data_offset ++ 2w0});
                }
                action compute_eack_3_(){
                    meta.tmp_3=16w0 ++ hdr.ipv4.total_len;
                }
                action compute_eack_4_(){
                    meta.total_hdr_len_bytes=(meta.tmp_1+meta.tmp_2);
                }
                action compute_eack_5_(){
                    meta.total_body_len_bytes=meta.tmp_3 - meta.total_hdr_len_bytes;
                }
                action compute_eack_6_(){
                    meta.expected_ack=hdr.tcp.seq_no + meta.total_body_len_bytes;
                }
                
                action compute_eack_last_if_syn(){
                    meta.expected_ack=meta.expected_ack + 1;
                    // could save 1 stage here by folding this into "++ 2w0" as "++ 2w1"
                }
                
                // Calculate 32-bit packet signature, to be stored into hash tables
                
                Hash<bit<32>>(HashAlgorithm_t.CRC32) crc32_1;
                Hash<bit<32>>(HashAlgorithm_t.CRC32) crc32_2;
                action get_pkt_signature_SEQ(){
                    meta.pkt_signature=crc32_1.get({
                        hdr.ipv4.src_addr, hdr.ipv4.dst_addr,
                        hdr.tcp.src_port, hdr.tcp.dst_port,
                        meta.expected_ack
                    });
                }
                action get_pkt_signature_ACK(){
                    meta.pkt_signature=crc32_2.get({
                        hdr.ipv4.dst_addr,hdr.ipv4.src_addr, 
                        hdr.tcp.dst_port,hdr.tcp.src_port, 
                        hdr.tcp.ack_no
                    });
                }
                
                // Calculate 16-bit hash table index
                        
                Hash<bit<16>>(HashAlgorithm_t.CRC16) crc16_1;
                Hash<bit<16>>(HashAlgorithm_t.CRC16) crc16_2;
                action get_location_SEQ(){
                    meta.hashed_location_1=crc16_1.get({
                        //4w0,
                        hdr.ipv4.src_addr, hdr.ipv4.dst_addr,
                        hdr.tcp.src_port, hdr.tcp.dst_port,
                        meta.expected_ack//,
                        //4w0
                    });
                }
                action get_location_ACK(){
                    meta.hashed_location_1=crc16_2.get({
                        //4w0,
                        hdr.ipv4.dst_addr,hdr.ipv4.src_addr, 
                        hdr.tcp.dst_port,hdr.tcp.src_port, 
                        hdr.tcp.ack_no//,
                        //4w0
                    });
                }
                
                // Self-expiry hash table, each entry stores a signature and a timestamp
                
                #define TIMESTAMP ig_intr_md.ingress_mac_tstamp[31:0]
                #define TS_EXPIRE_THRESHOLD (200*1000*1000)
                //50ms
                #define TS_LEGITIMATE_THRESHOLD (2000*1000*1000)
                
                
                Register<paired_32bit,_>(32w65536) reg_table_1;
                
                RegisterAction<paired_32bit, _, bit<32>>(reg_table_1) table_1_insert= {  
                    void apply(inout paired_32bit value, out bit<32> rv) {          
                        rv = 0;                                                    
                        paired_32bit in_value;                                          
                        in_value = value;                 
                        
                        bool existing_timestamp_is_old = (TIMESTAMP-in_value.hi)>TS_EXPIRE_THRESHOLD;
                        bool current_entry_empty = in_value.lo==0;
                        
                        if(existing_timestamp_is_old || current_entry_empty)
                        {
                            value.lo=meta.pkt_signature;
                            value.hi=TIMESTAMP;
                            rv=1;
                        }
                    }                                                              
                };
                
                action exec_table_1_insert(){
                    meta.table_1_read=table_1_insert.execute(meta.hashed_location_1);
                }
                
                RegisterAction<paired_32bit, _, bit<32>>(reg_table_1) table_1_tryRead= {  
                    void apply(inout paired_32bit value, out bit<32> rv) {    
                        rv=0;
                        paired_32bit in_value;                                          
                        in_value = value;     
                        
                        #define current_entry_matched (in_value.lo==meta.pkt_signature)
                        #define timestamp_legitimate  ((TIMESTAMP-in_value.hi)<TS_LEGITIMATE_THRESHOLD)
                        
                        if(current_entry_matched && timestamp_legitimate)
                        {
                            value.lo=0;
                            rv=in_value.hi;
                            value.hi=0;
                        }
                    }                                                              
                };
                
                action exec_table_1_tryRead(){ 
                    meta.table_1_read=table_1_tryRead.execute(meta.hashed_location_1);
                }
                

            // ***********************************************************************************************
            // *************************     E N D   R T T       *********************************************
            // ***********************************************************************************************

            // ***********************************************************************************************
            // ********************************    START   RETR       ****************************************
            // ***********************************************************************************************

                Register<bit<32>, _>(2048) last_seq;
                RegisterAction<bit<32>, _, bit<32>>(last_seq) read_store_last_seq = {
                    void apply(inout bit<32> register_data, out bit<32> result) {
                        if(hdr.tcp.seq_no + 65535 < register_data) {
                            register_data = meta.expected_ack;
                        }
                        
                        if(hdr.tcp.seq_no < register_data ) {
                            result = register_data;
                        } else
                        {
                            register_data = meta.expected_ack;
                        }
                        
                    }
                };

                action exec_read_store_last_seq(){
                    meta.lost=read_store_last_seq.execute(meta.flow_id);
                }


                Register<bit<32>, _>(2048) retr;
                RegisterAction<bit<32>, _, bit<1>>(retr) update_retr = {
                    void apply(inout bit<32> register_data) {
                        if(meta.lost != 0) {
                            register_data = register_data + 1;
                        }
                    }
                };
                action exec_update_retr(){
                    update_retr.execute(meta.flow_id);	
                }

                Register<bit<32>, _>(2048) rtt;
                RegisterAction<bit<32>, _, bit<1>>(rtt) update_rtt = {
                    void apply(inout bit<32> register_data) {
                            register_data = meta.rtt;
                    }
                };
                action exec_update_rtt(){
                    update_rtt.execute(meta.flow_id);	
                }


            // ***********************************************************************************************
            // ********************************     END    RETR       ****************************************
            // ***********************************************************************************************                

            //This register stores the starting and ending time of flows
                Register<flow_timing_t, _>(2048) flow_start_end_time;
                RegisterAction<flow_timing_t, _, bit<32>>(flow_start_end_time) update_flow_start_end_time = {
                    void apply(inout flow_timing_t register_data,out bit<32> result) {
                        if (register_data.flow_start_time == 0){
                            register_data.flow_start_time = ig_prsr_md.global_tstamp[41:10];
                        }
                        else{
                            register_data.flow_end_time = ig_prsr_md.global_tstamp[41:10];
                        }
                    }
                };

            apply {
                hdr.tcp.in_port = ig_intr_md.ingress_port;
                //Port 140 is the one receiving the duplicated packets for calculating the queuing delay
                if(hdr.ipv4.isValid() && hdr.tcp.isValid() && ig_intr_md.ingress_port != 140){

                    // Apply hash functions to get the ID of the flows
                    calc_flow_id.apply();
                    calc_rev_flow_id.apply();

                    // Update the flow start/end time
                    update_flow_start_end_time.execute(meta.flow_id);

                    // Update the flow's number of bytes
                    exec_update_Sample_length();	

                    // Apply CMS to get new long flows
                    apply_hash_0();
                    apply_hash_1();
                    apply_hash_2();

                    exec_read_sketch0();
                    exec_read_sketch1();
                    exec_read_sketch2();
                    

                    // Check if the packet dose ont belongs to an active flow (i.e., counted_flow.apply() ==  miss)
                    // If the packet belongs to new flow and the flow is a long flow (which is decided by the three sketches),
                    // the data plane notifies the control plane that a new is detected through digest
                    if(counted_flow.apply().miss){
                        if( meta.value_sketch0 == 1 && meta.value_sketch1 == 1 && meta.value_sketch2 == 1 ) { //&& meta.value_sketch3 == 1) { #ig_intr_md.ingress_port != 132 &&
                            ig_dprsr_md.digest_type = 0; 
                        }
                    }
                                            

                                    
                    // RTT calculation
                    tb_decide_packet_type.apply();
                        
                    // compute e_ack
                    if(meta.pkt_type==PKT_TYPE_SEQ){
                        compute_eack_1_();
                        compute_eack_2_();
                        compute_eack_3_();
                        compute_eack_4_();
                        compute_eack_5_();
                        compute_eack_6_();
                        if(hdr.tcp.flags==TCP_FLAGS_S){
                            compute_eack_last_if_syn();
                        } else {
                            exec_read_store_last_seq();
                        }                            
                        get_pkt_signature_SEQ();
                        get_location_SEQ();
                        exec_table_1_insert();
                        exec_update_retr();
                    }
                    else{
                        get_pkt_signature_ACK();
                        get_location_ACK();
                        exec_table_1_tryRead();
                        if(meta.table_1_read==0){
                            meta.rtt=0;                       
                        }else {
                            meta.rtt = (TIMESTAMP-meta.table_1_read);
                            exec_update_rtt();
                        }
                    }
                }
                if(ig_tm_md.ucast_egress_port != 192) {
			        ig_tm_md.ucast_egress_port=148; // just to go to egress
		        }
            }
        }

    /*********************  D E P A R S E R  ************************/
    control IngressDeparser(packet_out pkt,
        /* User */
        inout my_ingress_headers_t                       hdr,
        in    my_ingress_metadata_t                      meta,
        /* Intrinsic */
        in    ingress_intrinsic_metadata_for_deparser_t  ig_dprsr_md)
        {
            Digest<flow_digest_t>() new_long_flow_digest;

            apply {
                if (ig_dprsr_md.digest_type == 0) {
                    new_long_flow_digest.pack({meta.flow_id, meta.rev_flow_id,hdr.ipv4.src_addr, hdr.ipv4.dst_addr, hdr.tcp.src_port, hdr.tcp.dst_port});
                }
                pkt.emit(hdr);
            }
        }


/*************************************************************************
 ****************  E G R E S S   P R O C E S S I N G   *******************
 *************************************************************************/

    /***********************  H E A D E R S  ************************/
    struct my_egress_headers_t {
        ethernet_h   ethernet;
        ipv4_h       ipv4;
        tcp_h        tcp;
    }
    /********  G L O B A L   E G R E S S   M E T A D A T A  *********/
    struct my_egress_metadata_t {
        bit<32> packet_hash;
        bit<32> packet_queue_delay;		
        bit<11> flow_id;
    }
    /***********************  P A R S E R  **************************/
    parser EgressParser(packet_in        pkt,
        /* User */
        out my_egress_headers_t          hdr,
        out my_egress_metadata_t         meta,
        /* Intrinsic */
        out egress_intrinsic_metadata_t  eg_intr_md)
        {
            /* This is a mandatory state, required by Tofino Architecture */
            state start {
                pkt.extract(eg_intr_md);
                transition parse_ethernet;
            }

            state parse_ethernet {
                pkt.extract(hdr.ethernet);
                transition select(hdr.ethernet.ether_type) {
                    ETHERTYPE_IPV4:  parse_ipv4;
                    default: accept;
                }
            }

            state parse_ipv4 {
                pkt.extract(hdr.ipv4);
                transition select(hdr.ipv4.protocol) {
                    6: parse_tcp;
                    default: accept;
                }
            }

            state parse_tcp {
                pkt.extract(hdr.tcp);
                transition accept;
            }
        }

    /***************** M A T C H - A C T I O N  *********************/
    control Egress(
        /* User */
        inout my_egress_headers_t                          hdr,
        inout my_egress_metadata_t                         meta,
        /* Intrinsic */
        in    egress_intrinsic_metadata_t                  eg_intr_md,
        in    egress_intrinsic_metadata_from_parser_t      eg_prsr_md,
        inout egress_intrinsic_metadata_for_deparser_t     eg_dprsr_md,
        inout egress_intrinsic_metadata_for_output_port_t  eg_oport_md)
        {

            Hash<bit<11>>(HashAlgorithm_t.CRC16) hash;
                action apply_hash() {
                    meta.flow_id = hash.get({
                        hdr.ipv4.src_addr,
                        hdr.ipv4.dst_addr,
                        hdr.ipv4.protocol,
                        hdr.tcp.src_port,
                        hdr.tcp.dst_port
                    });
                }
                table calc_flow_id {
                    actions = {
                        apply_hash;
                    }
                    const default_action = apply_hash();
                }
                
                
                // ------------------------- QUEUE DELAY--------------------------------------
                // ---------------------------------------------------------------------------
                
                Hash<bit<32>>(HashAlgorithm_t.CRC32) packet_hash;
                action apply_packet_hash() {
                    meta.packet_hash = packet_hash.get({
                        meta.flow_id,
                        hdr.tcp.seq_no
                    });
                }
                table calc_packet_hash {
                    actions = {
                        apply_packet_hash;
                    }
                    const default_action = apply_packet_hash();
                }
                Register<bit<32>, bit<17>>(100000) packets_timestamp;
                RegisterAction<bit<32>, bit<17>, bit<32>>(packets_timestamp) update_packets_timestamp = {
                    void apply(inout bit<32> register_data) {
                            register_data = eg_prsr_md.global_tstamp[31:0];
                    }
                };
                action exec_update_packets_timestamp(){
                    update_packets_timestamp.execute(meta.packet_hash[16:0]);
                }
                RegisterAction<bit<32>, bit<17>, bit<32>>(packets_timestamp) calc_queue_delay_packet = {
                    void apply(inout bit<32> register_data, out bit<32> result) {
                        if(eg_prsr_md.global_tstamp[31:0] > register_data && eg_prsr_md.global_tstamp[31:0] - register_data < 50000000) {
                            result = eg_prsr_md.global_tstamp[31:0] - register_data;
                        } else {
                            result = 0;
                        }
                    }
                };
                action exec_calc_queue_delay_packet(){
                    meta.packet_queue_delay = calc_queue_delay_packet.execute(meta.packet_hash[16:0]);
                }
                
                // Averaging
                Lpf<value_t, bit<1>>(size=1) lpf_queue_delay_1;
                Lpf<value_t, bit<1>>(size=1) lpf_queue_delay_2;
                value_t lpf_queue_delay_input;
                value_t lpf_queue_delay_output_1;
                value_t lpf_queue_delay_output_2;

                
                
                Register<bit<32>, _>(2048) queue_delays;
                RegisterAction<bit<32>, _, bit<32>>(queue_delays) update_queue_delays = {
                    void apply(inout bit<32> register_data) {
                        register_data = meta.packet_queue_delay;
                    }
                };
                action exec_update_queue_delays(){
                    update_queue_delays.execute(0);
                }

                Register<bit<32>, _>(2048) total_packets;
                RegisterAction<bit<32>, _, bit<32>>(total_packets) update_total_packets = {
                    void apply(inout bit<32> register_data) {
                        register_data = register_data + 1;
                    }
                };
                action exec_update_total_packets(){
                    update_total_packets.execute(meta.flow_id);
                }


                apply {
                    calc_flow_id.apply();
                    calc_packet_hash.apply();
                    if (hdr.tcp.in_port == 148) {
                        exec_update_total_packets();
                        exec_update_packets_timestamp();
                    }
                    else if(hdr.tcp.in_port == 140) {
                        exec_calc_queue_delay_packet();
                        if(meta.packet_queue_delay != 0) {
                            lpf_queue_delay_input = (value_t)meta.packet_queue_delay;
                            lpf_queue_delay_output_1 = lpf_queue_delay_1.execute(lpf_queue_delay_input, 0);
                            lpf_queue_delay_output_2 = lpf_queue_delay_2.execute(lpf_queue_delay_output_1, 0);
                            meta.packet_queue_delay = lpf_queue_delay_output_2;
                            exec_update_queue_delays();

                        }
                    }
                    else if(hdr.tcp.in_port == 132){
                        exec_update_total_packets();
                    }
                    eg_dprsr_md.drop_ctl = 0;
                
                }
            }
        

    /*********************  D E P A R S E R  ************************/
    control EgressDeparser(packet_out pkt,
        /* User */
        inout my_egress_headers_t                       hdr,
        in my_egress_metadata_t                      meta,
        /* Intrinsic */
        in    egress_intrinsic_metadata_for_deparser_t  eg_dprsr_md)
        {
            apply {
                pkt.emit(hdr);
            }
        }


/************ F I N A L   P A C K A G E ******************************/
Pipeline(
    IngressParser(),
    Ingress(),
    IngressDeparser(),
    EgressParser(),
    Egress(),
    EgressDeparser()
) pipe;

Switch(pipe) main;



