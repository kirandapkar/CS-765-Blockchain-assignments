import socket
from threading import Thread

file_name = "outputseed.txt"


class Seed(Thread):
    def __init__(self, host, port):
        Thread.__init__(self, name="seed_thread")
        self.host = host
        self.port = port
        self.peer_list = []

    def register_peer(self, address):
        self.peer_list.append(address)
        with open(file_name, 'a') as f:
            s = f"{self.host}:{self.port} : Peer Addition: {address[0]}:{address[1]}"
            print(s)
            f.write(s + '\n')

    def handle_new_request(self, sock, msgs):  # <Sender.IP>:<Sender.Port>
        peer_address = (msgs[0], int(msgs[1]))
        s = ""
        for peer in self.peer_list:
            if peer != peer_address:
                s += f"{peer[0]}:{peer[1]},"
        sock.sendall(bytes(s, 'utf-8'))  # Send peer_list to new connected peer
        sock.shutdown(1)

        self.register_peer(peer_address)

    def handle_dead_node(self, msgs):  # <DeadNode.IP>:<DeadNode.Port>:<self.timestamp>:<sender.IP>:<sender.Port>
        dn_ip, dn_port, ts, s_ip, s_port = msgs
        dn = tuple([dn_ip, int(dn_port)])
        if dn in self.peer_list:
            self.peer_list.remove(dn)
            s = f"{self.host}:{self.port} : Dead Node {dn_ip}:{dn_port} reported by {s_ip}:{s_port}"
            print(s)
            with open(file_name, 'a') as f:
                f.write(s + '\n')
        else:
            pass  # When dead node doesn't exist in peer list or has been removed earlier, ignore

    def handle_connection(self, connection):
        full_msg = ""
        while True:
            data = connection.recv(16)
            full_msg += data.decode('utf-8')
            if not data:
                full_msg = full_msg.strip()
                break
        msgs = full_msg.split(":")
        if(msgs[0] == "New Node"):
            self.handle_new_request(connection, msgs[1:])
        elif(msgs[0] == "Dead Node"):
            self.handle_dead_node(msgs[1:])
        else:
            print("Invalid message received")
        connection.close()

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host, self.port))
        sock.listen(10)
        while True:
            try:
                connection, _ = sock.accept()
                Thread(target=self.handle_connection, args=(connection,)).start()
            except KeyboardInterrupt:
                connection.close()
                break

        sock.close()


# Start every seed from config.txt file (all seeds are considered to be on localhost)
if __name__ == '__main__':
    file = open("config.txt", "r")
    lines = file.readlines()
    file.close()
    seeds = []
    for line in lines:
        host, port = line.strip().split()
        seed = Seed(host, int(port))
        seed.start()
        seeds.append(seed)
