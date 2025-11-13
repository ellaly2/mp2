"""
Authors: Ella Ly (301580649), Sophia Chipeco (301595881)
"""

import socket
import time
import sys
from packet import Packet
from protocol import Config, UnreliableChannel, Sender, Receiver

# =============================================================================
# CLIENT
# =============================================================================

class PRTPClient:
    """Client using PRTP protocol"""
    
    def __init__(self, server_host, server_port):
        self.server_addr = (server_host, server_port)
        
        # Create UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.1)
        self.sock.bind(('', 0))
        
        # Create channel and sender
        self.channel = UnreliableChannel(self.sock)
        self.sender = None
        self.connected = False
        
        print(f"[CLIENT] Initialized on {self.sock.getsockname()}")
    
    def connect(self):
        print(f"[CLIENT] Connecting to {self.server_addr}...")
        
        initial_seq = 1000
        
        # Send SYN
        syn = Packet(initial_seq, 0, Packet.FLAG_SYN, 0, b'')
        
        for attempt in range(Config.MAX_SYN_RETRIES):
            self.channel.send(syn.serialize(), self.server_addr)
            print(f"[CLIENT] Sent SYN (attempt {attempt + 1})")
            
            # Wait for SYN-ACK
            start = time.time()
            while time.time() - start < Config.CONNECTION_TIMEOUT:
                try:
                    data, addr = self.channel.recv()
                    packet = Packet.deserialize(data)
                    
                    if packet.is_syn() and packet.is_ack():
                        print(f"[CLIENT] Received SYN-ACK")
                        
                        # Send ACK
                        ack = Packet(initial_seq + 1, packet.seq_num + 1, Packet.FLAG_ACK, 0, b'')
                        self.channel.send(ack.serialize(), self.server_addr)
                        print(f"[CLIENT] Sent ACK - Connected!")
                        
                        # Create sender
                        self.sender = Sender(self.channel, initial_seq + 1)
                        self.sender.remote_addr = self.server_addr
                        self.connected = True
                        return True
                        
                except socket.timeout:
                    continue
                except ValueError as e:
                    print(f"[CLIENT] Error: {e}")
                    continue
        
        print("[CLIENT] Connection failed")
        return False
    
    def send_file(self, filename):
        if not self.connected:
            print("[CLIENT] Not connected")
            return
        
        try:
            with open(filename, 'rb') as f:
                data = f.read()
            
            print(f"[CLIENT] Sending {len(data)} bytes from {filename}")
            start_time = time.time()
            
            # Queue data
            self.sender.send_data(data)
            
            # Main loop
            while self.sender.has_pending_data():
                # Send packets
                while self.sender.can_send():
                    self.sender.send_packet(self.server_addr)
                
                # Check timeout
                self.sender.check_timeout(self.server_addr)
                
                # Receive ACKs
                try:
                    pkt_data, addr = self.channel.recv()
                    packet = Packet.deserialize(pkt_data)
                    if packet.is_ack():
                        self.sender.handle_ack(packet)
                except socket.timeout:
                    pass
                except ValueError:
                    pass
            
            elapsed = time.time() - start_time
            throughput = len(data) / elapsed if elapsed > 0 else 0
            
            print(f"[CLIENT] Transfer complete!")
            print(f"[CLIENT] Time: {elapsed:.2f}s, Throughput: {throughput:.2f} bytes/sec")
            
            
        except FileNotFoundError:
            print(f"[CLIENT] File not found: {filename}")
        except Exception as e:
            print(f"[CLIENT] Error: {e}")
    
    def close(self):

        if self.connected:
            # Send FIN
            fin = Packet(self.sender.next_seq_num, 0, Packet.FLAG_FIN, 0, b'')
            self.channel.send(fin.serialize(), self.server_addr)
            print("[CLIENT] Sent FIN")
        
        self.sock.close()


# =============================================================================
# SERVER
# =============================================================================

class PRTPServer:

    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        
        # Create UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.1)
        self.sock.bind((host, port))
        
        # Create channel
        self.channel = UnreliableChannel(self.sock)
        self.receiver = None
        self.client_addr = None
        
        print(f"[SERVER] Listening on {host}:{port}")
    
    def accept_connection(self):
        print("[SERVER] Waiting for connection...")
        
        while True:
            try:
                data, addr = self.channel.recv()
                packet = Packet.deserialize(data)
                
                if packet.is_syn() and not packet.is_ack():
                    print(f"[SERVER] Received SYN from {addr}")
                    self.client_addr = addr
                    client_initial_seq = packet.seq_num
                    
                    # Send SYN-ACK
                    initial_seq = 2000
                    syn_ack = Packet(initial_seq, packet.seq_num + 1, 
                                    Packet.FLAG_SYN | Packet.FLAG_ACK, 0, b'')
                    self.channel.send(syn_ack.serialize(), addr)
                    print(f"[SERVER] Sent SYN-ACK")
                    
                    # Wait for ACK
                    start = time.time()
                    while time.time() - start < Config.CONNECTION_TIMEOUT:
                        try:
                            data2, addr2 = self.channel.recv()
                            packet2 = Packet.deserialize(data2)
                            
                            if addr2 == addr and packet2.is_ack():
                                print(f"[SERVER] Received ACK - Connected!")
                                self.receiver = Receiver(self.channel, client_initial_seq + 1)
                                return True
                                
                        except socket.timeout:
                            continue
                        except ValueError:
                            continue
         
                    print(f"[SERVER] Timeout waiting for ACK, but connection established")
                    self.receiver = Receiver(self.channel, client_initial_seq + 1)
                    return True
                    
            except socket.timeout:
                continue
            except ValueError as e:
                print(f"[SERVER] Error: {e}")
                continue
    
    def receive_data(self, output_file=None):
        print("[SERVER] Receiving data...")
        
        last_activity = time.time()
        timeout = 5.0
        
        while True:
            try:
                data, addr = self.channel.recv()
                
                if addr != self.client_addr:
                    continue
                
                packet = Packet.deserialize(data)
                last_activity = time.time()
                
                if packet.is_data():
                    self.receiver.handle_packet(packet, addr)
                elif packet.is_fin():
                    print("[SERVER] Received FIN")
                    break
                    
            except socket.timeout:
                if time.time() - last_activity > timeout:
                    print("[SERVER] Timeout - transfer complete")
                    break
            except ValueError as e:
                print(f"[SERVER] Corrupted packet: {e}")
        
        # Get received data
        received_data = self.receiver.get_received_data()
        
        # Save to file
        if output_file:
            with open(output_file, 'wb') as f:
                f.write(received_data)
            print(f"[SERVER] Saved {len(received_data)} bytes to {output_file}")
        
        print(f"[SERVER] Received total: {len(received_data)} bytes")
        

        
        return received_data
    
    def close(self):
        self.sock.close()


# =============================================================================
# MAIN
# =============================================================================

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Server: python main.py server")
        print("  Client: python main.py client <filename>")
        print("\nTest scenarios (edit Config in protocol.py):")
        print("  Config.PACKET_LOSS_RATE = 0.0   # Perfect network")
        print("  Config.PACKET_LOSS_RATE = 0.05  # 5% loss")
        print("  Config.PACKET_LOSS_RATE = 0.15  # 15% loss")
        sys.exit(1)
    
    mode = sys.argv[1]
    
    if mode == 'server':
        server = PRTPServer('localhost', 9999)
        file_count = 1
        
        try:
            while True:  # Keep server running
                
                # Accept new connection
                if server.accept_connection():
                    # Receive data
                    output_file = f'received_file_{file_count}.dat'
                    server.receive_data(output_file)
                    file_count += 1
                    
                    # Reset receiver for next connection
                    server.receiver = None
                    server.client_addr = None
                    
                    print(f"\n[SERVER] Connection closed. Waiting for next client...")
                else:
                    print("[SERVER] Connection failed, continuing to listen...")
                    
        except KeyboardInterrupt:
            print("\n[SERVER] Shutting down...")
        finally:
            server.close()
    
    elif mode == 'client':
        if len(sys.argv) < 3:
            print("Error")
            sys.exit(1)
        
        filename = sys.argv[2]
        client = PRTPClient('localhost', 9999)
        
        try:
            if client.connect():
                client.send_file(filename)
                time.sleep(1)  # Wait for final ACKs
        finally:
            client.close()
    
    else:
        print(f"Unknown mode: {mode}")
        print("Use 'server' or 'client'")


if __name__ == '__main__':
    main()