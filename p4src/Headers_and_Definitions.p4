/*************************************************************************
 ************* C O N S T A N T S    A N D   T Y P E S  *******************
**************************************************************************/
const bit<16> ETHERTYPE_TPID = 0x8100;
const bit<16> ETHERTYPE_IPV4 = 0x0800;
typedef bit<32> value_t;

#define SKETCH_BUCKET_LENGTH 8192 
#define SKETCH_CELL_BIT_WIDTH 32
#define THRESH 100000


// Count Min Skitch is used to classify long flows

#define SKETCH_REGISTER(num) Register<bit<SKETCH_CELL_BIT_WIDTH>, _>(SKETCH_BUCKET_LENGTH) sketch##num
#define SKETCH_COUNT(num, algorithm, crc) Hash<bit<13>>(algorithm, crc) hash##num; \
action apply_hash_##num() { \
    meta.index_sketch##num = hash##num.get({ \
        hdr.ipv4.src_addr, \
        hdr.ipv4.dst_addr, \
        hdr.ipv4.protocol, \
        hdr.tcp.src_port, \
        hdr.tcp.dst_port \
    }); \
}\
RegisterAction<bit<SKETCH_CELL_BIT_WIDTH>, _, bit<1>>(sketch##num) read_sketch##num = {\
    void apply(inout bit<SKETCH_CELL_BIT_WIDTH> register_data, out bit<1> result) { \
        register_data = register_data +1; \
        if (register_data > THRESH) {\
            result = 1; \
        } else {\
            result = 0; \
        }\
    } \
}; \
action exec_read_sketch##num() { \
    meta.value_sketch##num = read_sketch##num.execute(meta.index_sketch##num); \
} 


/*************************************************************************
 ***********************  H E A D E R S  *********************************
 *************************************************************************/

/*  Define all the headers the program will recognize             */
/*  The actual sets of headers processed by each gress cn differ */

/* Standard ethernet header */
header ethernet_h {
    bit<48>   dst_addr;
    bit<48>   src_addr;
    bit<16>   ether_type;
}

header vlan_tag_h {
    bit<3>   pcp;
    bit<1>   cfi;
    bit<12>  vid;
    bit<16>  ether_type;
}

header ipv4_h {
    bit<4>   version;
    bit<4>   ihl;
    bit<8>   diffserv;
    bit<16>  total_len;
    bit<16>  identification;
    bit<3>   flags;
    bit<13>  frag_offset;
    bit<8>   ttl;
    bit<8>   protocol;
    bit<16>  hdr_checksum;
    bit<32>  src_addr;
    bit<32>  dst_addr;
}

header tcp_h {
    bit<16> src_port;
    bit<16> dst_port;
    bit<32> seq_no;
    bit<32> ack_no;
    bit<4> data_offset;
    bit<4> res;
    bit<8> flags;
    bit<16> window;
    bit<16> checksum;
    bit<16> urgent_ptr;
	bit<9> in_port;
	bit<7> dummy;
}

header udp_h {
	bit<16> src_port;
    bit<16> dst_port;
    bit<16> hdr_length;
    bit<16> checksum;
}

header rtp_h {
    bit<2>   version;
    bit<1>   padding;
    bit<1>   extension;
    bit<4>   CSRC_count;
    bit<1>   marker;
    bit<7>   payload_type;
    bit<16>  sequence_number;
    bit<32>  timestamp;
    bit<32>  SSRC;
}

struct RTT_digest_t {
    bit<8> flow_id; 
    bit<32> rtt;
}

struct paired_32bit {
    bit<32> lo;
    bit<32> hi;
}

#define PKT_TYPE_SEQ true
#define PKT_TYPE_ACK false
typedef bit<8> tcp_flags_t;
const tcp_flags_t TCP_FLAGS_F = 1;
const tcp_flags_t TCP_FLAGS_S = 2;
const tcp_flags_t TCP_FLAGS_R = 4;
const tcp_flags_t TCP_FLAGS_P = 8;
const tcp_flags_t TCP_FLAGS_A = 16;