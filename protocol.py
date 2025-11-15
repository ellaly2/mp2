"""
Authors: Ella Ly (301580649), Sophia Chipeco (301595881)
"""

import time
import random
import socket
from collections import deque
from packet import Packet

# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    # Network
    MAX_PAYLOAD_SIZE = 512
    BUFFER_SIZE = 2048
    
    # Windows
    INITIAL_CWND = 1
    MAX_CWND = 255
    RECEIVER_WINDOW = 64
    SSTHRESH_INIT = 32
    
    # RTT Estimation 
    ALPHA = 0.125
    BETA = 0.25
    TIMEOUT_MULTIPLIER = 4
    INITIAL_TIMEOUT = 1.0
    MIN_TIMEOUT = 0.2
    MAX_TIMEOUT = 60.0
    
    # Congestion Control
    DUPLICATE_ACK_THRESHOLD = 3
    
    # Channel Simulation
    PACKET_LOSS_RATE = 0
    BIT_ERROR_RATE = 0.0   
    
    # Connection
    MAX_SYN_RETRIES = 5
    CONNECTION_TIMEOUT = 2.0


# =============================================================================
# UTILITIES
# =============================================================================

class Timer:
    def __init__(self, timeout):
        self.timeout = timeout
        self.start_time = None
    
    def start(self):
        self.start_time = time.time()
    
    def stop(self):
        self.start_time = None
    
    def reset(self, timeout=None):
        if timeout: self.timeout = timeout
        self.start_time = time.time()
    
    def is_expired(self):
        return self.start_time and (time.time() - self.start_time >= self.timeout)
    
    def elapsed(self):
        return time.time() - self.start_time if self.start_time else 0



class UnreliableChannel:
    
    def __init__(self, sock, loss_rate=None, error_rate=None):
        self.socket = sock
        self.loss_rate = loss_rate if loss_rate is not None else Config.PACKET_LOSS_RATE
        self.error_rate = error_rate if error_rate is not None else Config.BIT_ERROR_RATE
 
    
    def send(self, data, address):

        
        # Simulate packet loss
        if random.random() < self.loss_rate:
            print(f"[CHANNEL] Dropped packet to {address}")
            return
        
        # Simulate bit errors
        if random.random() < self.error_rate:
            data = self._corrupt(data)
            print(f"[CHANNEL] Corrupted packet to {address}")
        
        self.socket.sendto(data, address)
    
    def recv(self, buffer_size=Config.BUFFER_SIZE):
        data, address = self.socket.recvfrom(buffer_size)
        return data, address
    
    def _corrupt(self, data):
        data = bytearray(data)
        for _ in range(random.randint(1, 3)):
            pos = random.randint(0, len(data) - 1)
            data[pos] ^= random.randint(1, 255)
        return bytes(data)


# =============================================================================
# SENDER 
# =============================================================================

class Sender:
    
    def __init__(self, channel, initial_seq=0):
        self.channel = channel
        
        # Sequence numbers
        self.base = initial_seq
        self.next_seq_num = initial_seq
        
        # Window
        self.window = {} 
        
        # Flow control
        self.receiver_window = Config.RECEIVER_WINDOW
        
        # Congestion control
        self.cwnd = Config.INITIAL_CWND
        self.ssthresh = Config.SSTHRESH_INIT
        self.duplicate_ack_count = 0
        self.last_ack = initial_seq
        self.in_fast_recovery = False
        
        # RTT estimation
        self.estimated_rtt = Config.INITIAL_TIMEOUT
        self.dev_rtt = 0
        self.timeout_interval = Config.INITIAL_TIMEOUT
        
        # Timer
        self.timer = Timer(self.timeout_interval)
        
        # Send buffer
        self.send_buffer = deque()
        
        print(f"[SENDER] Initialized: base={self.base}, cwnd={self.cwnd}, ssthresh={self.ssthresh}")
    
    def send_data(self, data):
        offset = 0
        while offset < len(data):
            chunk = data[offset:offset + Config.MAX_PAYLOAD_SIZE]
            self.send_buffer.append(chunk)
            offset += len(chunk)
        print(f"[SENDER] Queued {len(data)} bytes ({len(self.send_buffer)} packets)")
    
    def can_send(self):
        effective_window = min(int(self.cwnd), self.receiver_window)
        return len(self.window) < effective_window and len(self.send_buffer) > 0
    
    def send_packet(self, remote_addr):
        if not self.send_buffer:
            return
        
        data = self.send_buffer.popleft()
        packet = Packet(self.next_seq_num, 0, Packet.FLAG_DATA, 0, data)
        
        self.channel.send(packet.serialize(), remote_addr)
        self.window[self.next_seq_num] = (packet, time.time())
        
        if self.next_seq_num == self.base:
            self.timer.start()
        
        print(f"[SEND] seq={self.next_seq_num}, cwnd={self.cwnd:.2f}, ssthresh={self.ssthresh}, in_flight={len(self.window)}")
        
        self.next_seq_num += len(data)
    
    def handle_ack(self, ack_packet):
        ack_num = ack_packet.ack_num
        self.receiver_window = ack_packet.window
        
        if ack_num > self.base:
            self._handle_new_ack(ack_num)
        elif ack_num == self.base and ack_num == self.last_ack:
            self._handle_duplicate_ack(ack_num)
        
        self.last_ack = ack_num
    
    def _handle_new_ack(self, ack_num):
        # Update RTT
        if self.base in self.window:
            _, send_time = self.window[self.base]
            sample_rtt = time.time() - send_time
            self._update_rtt(sample_rtt)
        
        # Remove ACKed packets
        for seq in list(self.window.keys()):
            if seq < ack_num:
                del self.window[seq]
        
        self.base = ack_num
        self.duplicate_ack_count = 0
        
        # Update timer
        if self.window:
            self.timer.reset(self.timeout_interval)
        else:
            self.timer.stop()
        
        # Congestion control
        if self.in_fast_recovery:
            self.cwnd = self.ssthresh
            self.in_fast_recovery = False
            print(f"[CC] Exit fast recovery, cwnd={self.cwnd:.2f}")
        elif self.cwnd < self.ssthresh:
            self.cwnd += 1 
            print(f"[CC] Slow start: cwnd={self.cwnd:.2f}")
        else:
            self.cwnd += 1.0 / self.cwnd  
            print(f"[CC] Congestion avoidance: cwnd={self.cwnd:.2f}")
        
        self.cwnd = min(self.cwnd, Config.MAX_CWND)
    
    def _handle_duplicate_ack(self, ack_num):
        self.duplicate_ack_count += 1
        
        if self.duplicate_ack_count == Config.DUPLICATE_ACK_THRESHOLD:
            print(f"[FAST-RETRANSMIT] Triple dup ACK for {ack_num}")
            self._fast_retransmit()
    
    def _fast_retransmit(self):
        
        if self.base in self.window and hasattr(self, 'remote_addr'):
            packet, _ = self.window[self.base]
            self.channel.send(packet.serialize(), self.remote_addr)
            print(f"[RETRANSMIT] seq={self.base} (fast)")
        
        # Fast recovery
        self.ssthresh = max(self.cwnd / 2, 2)
        self.cwnd = self.ssthresh + Config.DUPLICATE_ACK_THRESHOLD
        self.in_fast_recovery = True
        print(f"[CC] Fast recovery: ssthresh={self.ssthresh:.2f}, cwnd={self.cwnd:.2f}")
    
    def check_timeout(self, remote_addr):
        """Check for timeout"""
        if self.timer.is_expired() and self.window:
            print(f"[TIMEOUT] base={self.base}")
            
            # Retransmit all (Go-Back-N)
            for seq in sorted(self.window.keys()):
                packet, _ = self.window[seq]
                self.channel.send(packet.serialize(), remote_addr)
                print(f"[RETRANSMIT] seq={seq} (timeout)")
                self.window[seq] = (packet, time.time())
            
            # Timeout recovery
            self.ssthresh = max(self.cwnd / 2, 2)
            self.cwnd = Config.INITIAL_CWND
            self.in_fast_recovery = False
            self.duplicate_ack_count = 0
            print(f"[CC] Timeout: ssthresh={self.ssthresh:.2f}, cwnd={self.cwnd:.2f}")
            
            self.timeout_interval = min(self.timeout_interval * 2, Config.MAX_TIMEOUT)
            self.timer.reset(self.timeout_interval)
    
    def _update_rtt(self, sample_rtt):
        if self.estimated_rtt == Config.INITIAL_TIMEOUT and self.dev_rtt == 0:
            self.estimated_rtt = sample_rtt
            self.dev_rtt = sample_rtt / 2
        else:
            self.dev_rtt = (1 - Config.BETA) * self.dev_rtt + Config.BETA * abs(sample_rtt - self.estimated_rtt)
            self.estimated_rtt = (1 - Config.ALPHA) * self.estimated_rtt + Config.ALPHA * sample_rtt
        
        self.timeout_interval = self.estimated_rtt + Config.TIMEOUT_MULTIPLIER * self.dev_rtt
        self.timeout_interval = max(Config.MIN_TIMEOUT, min(self.timeout_interval, Config.MAX_TIMEOUT))
    
    def has_pending_data(self):
        return len(self.send_buffer) > 0 or len(self.window) > 0


# =============================================================================
# RECEIVER 
# =============================================================================

class Receiver:
    
    def __init__(self, channel, initial_seq=0):
        self.channel = channel
        self.expected_seq = initial_seq
        self.receive_buffer = bytearray()
        self.buffer_size = Config.RECEIVER_WINDOW * Config.MAX_PAYLOAD_SIZE
        
        print(f"[RECEIVER] Initialized: expected_seq={self.expected_seq}")
    
    def handle_packet(self, packet, sender_addr):
     
        seq = packet.seq_num
        
        if seq == self.expected_seq:
            # In-order
            self.receive_buffer.extend(packet.data)
            self.expected_seq += len(packet.data)
            print(f"[ACCEPT] seq={seq}, new_expected={self.expected_seq}")
            self._send_ack(sender_addr)
            
        elif seq < self.expected_seq:
            # Duplicate
            print(f"[DUPLICATE] seq={seq}")
            self._send_ack(sender_addr)
            
        else:
            # Out-of-order
            print(f"[OUT-OF-ORDER] seq={seq} > expected={self.expected_seq}, DISCARDING")
            self._send_ack(sender_addr)
    
    def _send_ack(self, sender_addr):
        used = len(self.receive_buffer)
        available = self.buffer_size - used
        rwnd = min(max(0, available // Config.MAX_PAYLOAD_SIZE), 255)
        
        ack = Packet(0, self.expected_seq, Packet.FLAG_ACK, rwnd, b'')
        self.channel.send(ack.serialize(), sender_addr)
        print(f"[ACK] ack={self.expected_seq}, rwnd={rwnd}")
    
    def get_received_data(self):
        data = bytes(self.receive_buffer)
        self.receive_buffer.clear()
        return data
    
    def has_data(self):
        return len(self.receive_buffer) > 0