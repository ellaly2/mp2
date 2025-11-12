"""
Authors: Ella Ly (301580649), Sophia Chipeco (301595881)
"""

import struct

class Packet:
    # Flags
    FLAG_SYN = 0x01
    FLAG_ACK = 0x02
    FLAG_FIN = 0x04
    FLAG_DATA = 0x08
    
    # Header: seq(4) + ack(4) + flags(1) + window(1) + length(2) + checksum(2) = 14 bytes
    HEADER_FORMAT = '!IIBBHH'
    HEADER_SIZE = 14
    
    def __init__(self, seq_num=0, ack_num=0, flags=0, window=0, data=b''):
        self.seq_num = seq_num
        self.ack_num = ack_num
        self.flags = flags
        self.window = window
        self.data = data
        self.checksum = 0
    
    def serialize(self):
        payload_length = len(self.data)
        
        # Pack without checksum
        header = struct.pack(self.HEADER_FORMAT, self.seq_num, self.ack_num,
                            self.flags, self.window, payload_length, 0)
        
        # Calculate checksum
        self.checksum = self._calc_checksum(header + self.data)
        
        # Repack with checksum
        header = struct.pack(self.HEADER_FORMAT, self.seq_num, self.ack_num,
                            self.flags, self.window, payload_length, self.checksum)
        
        return header + self.data
    
    @classmethod
    def deserialize(cls, data):
        if len(data) < cls.HEADER_SIZE:
            raise ValueError("Packet too short")
        
        # Unpack header
        seq, ack, flags, window, length, checksum = struct.unpack(
            cls.HEADER_FORMAT, data[:cls.HEADER_SIZE])
        
        # Extract payload
        payload = data[cls.HEADER_SIZE:cls.HEADER_SIZE + length]
        
        # Create packet
        pkt = cls(seq, ack, flags, window, payload)
        pkt.checksum = checksum
        
        # Verify checksum
        if not pkt.verify_checksum(data):
            raise ValueError("Checksum failed - corrupted packet")
        
        return pkt
    
    def _calc_checksum(self, data):
        if len(data) % 2 == 1:
            data += b'\x00'
        
        total = 0
        for i in range(0, len(data), 2):
            word = (data[i] << 8) + data[i + 1]
            total += word
            total = (total & 0xFFFF) + (total >> 16)
        
        return ~total & 0xFFFF
    
    def verify_checksum(self, raw_data):
        header = bytearray(raw_data[:self.HEADER_SIZE])
        struct.pack_into('!H', header, 12, 0)
        expected = self._calc_checksum(bytes(header) + self.data)
        return self.checksum == expected
    
    def is_syn(self): return bool(self.flags & self.FLAG_SYN)
    def is_ack(self): return bool(self.flags & self.FLAG_ACK)
    def is_fin(self): return bool(self.flags & self.FLAG_FIN)
    def is_data(self): return bool(self.flags & self.FLAG_DATA)
    
    def __repr__(self):
        flags = []
        if self.is_syn(): flags.append("SYN")
        if self.is_ack(): flags.append("ACK")
        if self.is_fin(): flags.append("FIN")
        if self.is_data(): flags.append("DATA")
        return f"Packet(seq={self.seq_num}, ack={self.ack_num}, flags={','.join(flags)}, win={self.window}, len={len(self.data)})"