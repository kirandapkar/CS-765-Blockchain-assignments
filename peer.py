from socket import *
import random
from threading import Thread
import time
from hashlib import sha256
from collections import defaultdict

LIVENESS_SLEEP_TIME = 13
GOSSIP_SLEEP_TIME = 5

class Peer(Thread):

    def __init__(self, ip_address, port):
        Thread.__init__(self, name="Peer Thread for {}".format(port))
        self.ip_address = ip_address
        self.port = port

        self.node = socket(AF_INET, SOCK_STREAM)
        self.node.bind(tuple([ip_address, port]))
        self.node.listen(5)

        self.seed_list = self.get_seeds()
        self.register_to_seeds()
        print(f"Seed list for {port}: ", self.seed_list)

        self.message_list = defaultdict(lambda: False)

    def get_seeds(self):
        file = open("config.txt", "r")
        lines = file.readlines()
        file.close()
        n = len(lines)
        k = int(n / 2) + 1
        random_seeds = random.sample(lines, k)
        seed_list = []
        for x in random_seeds:
            seed_list.append(tuple(x.strip().split()))
        seed_list = [(a, int(b)) for (a, b) in seed_list]
        return seed_list

    def register_to_seeds(self):
        plist = []
        for seed in self.seed_list:
            s = socket(AF_INET, SOCK_STREAM)
            s.connect(seed)
            # Format to register new node assumed as New Node:<Sender.IP>:<Sender.Port>
            s.sendall(bytes("New Node:" + self.ip_address + ":" + str(self.port), 'utf-8'))
            s.shutdown(1)
            msg = ""
            while True:
                data = s.recv(16)
                msg += data.decode('utf-8')
                if not data:
                    msg = msg.strip().split(",")
                    msg.remove('')
                    plist.extend(msg)
                    break
            s.close()

        if plist == []:  # If empty list is received
            self.peers = []
            return
        plist = list(set(plist))  # union of all received peers

        self.peers = [tuple(i.split(":")) for i in plist]
        self.peers = [(a, int(b)) for (a, b) in self.peers]
        self.peers = self.get_working_peers_list(self.peers)

        print(f"Connected peers: ", self.peers)

    def get_working_peers_list(self, plist):
        random.shuffle(plist)
        final_peers = []
        for peer in plist:
            s = socket(AF_INET, SOCK_STREAM)
            try:
                s.connect(peer)
                final_peers.append(peer)
            except ConnectionRefusedError:
                # To check liveness of failed nodes so that seeds can remove them
                Thread(target=self.liveness, args=(peer,)).start()
            finally:
                s.close()
                if len(final_peers) == 4:
                    break

        return final_peers

    def run(self):
        Thread(target=self.gossip).start()
        for peer in self.peers:
            Thread(target=self.liveness, args=(peer,)).start()

        while True:
            try:
                connection, _ = self.node.accept()
                Thread(target=self.handle_connection, args=(connection,)).start()
            except KeyboardInterrupt:
                connection.close()
                break

        self.node.close()
        print(f"Peer {self.ip_address} : {self.port} stopped")

    def handle_connection(self, connection):
        full_msg = ""
        while True:
            data = connection.recv(16)
            full_msg += data.decode('utf-8')
            if not data:
                full_msg = full_msg.strip()
                break
        if full_msg != "":
            self.handle_msg(connection, full_msg)

        connection.close()

    def handle_msg(self, sock, msg):
        msgs = msg.split(":")
        if(msgs[0] == "Liveness Request"):
            # self.write_to_file(msg) # To write liveness msg to file
            ts, ip, port = msgs[1:]
            s = "Liveness Reply:" + ts + ":" + ip + ":" + port + ":" + self.ip_address + ":" + str(self.port)
            sock.sendall(bytes(s, 'utf-8'))
            sock.shutdown(1)
        else:
            msgs = msg.split(";")  # Format is <msg;sender_ip;sender_port>
            original_msg = msgs[0]
            has = sha256(original_msg.encode()).hexdigest()
            if self.message_list[has]:
                return
            self.write_to_file(msg)
            self.message_list[has] = True
            sender = (msgs[1], int(msgs[2]))
            for peer in self.peers:
                if peer != sender:
                    Thread(target=self.send_gossip_msg, args=(peer, original_msg)).start()

    def gossip(self):
        if self.peers == []:
            return
        for n in range(1, 11):
            msg = str(time.time()) + ":" + self.ip_address + ":" + str(self.port) + ":" + str(n)
            has = sha256(msg.encode()).hexdigest()
            self.message_list[has] = True

            for peer in self.peers:
                Thread(target=self.send_gossip_msg, args=(peer, msg)).start()
            time.sleep(GOSSIP_SLEEP_TIME)

    def send_gossip_msg(self, peer, msg):
        try:
            s = socket(AF_INET, SOCK_STREAM)
            s.connect(peer)
            msg += f";{self.ip_address};{self.port}"  # Pass sender address alongwith original msg
            s.sendall(bytes(msg, 'utf-8'))
            s.shutdown(1)
        except ConnectionRefusedError:
            pass  # Msg won't be passed and the parallely running liveness threads will check for liveness
        finally:
            s.close()

    def liveness(self, peer):
        flag = 0
        while True:
            timestamp = str(time.time())
            msg = "Liveness Request:" + timestamp + ":" + self.ip_address + ":" + str(self.port)
            s = socket(AF_INET, SOCK_STREAM)
            try:
                s.connect(peer)
            except ConnectionRefusedError:
                flag += 1
                if flag == 3:
                    self.report(peer)
                    s.close()
                    break
                else:
                    time.sleep(LIVENESS_SLEEP_TIME)
                    continue

            s.sendall(bytes(msg, 'utf-8'))
            s.shutdown(1)
            msg = ""

            while True:
                data = s.recv(16)
                msg += data.decode('utf-8')
                if not data:
                    msg = msg.strip()
                    break

            if msg is None:
                flag += 1
                if flag == 3:
                    self.report(peer)
                    s.close()
                    break
            else:
                msg = msg.split(":")
                if not(msg[0] == "Liveness Reply" and msg[1] == timestamp and self.ip_address == msg[2] and self.port == int(msg[3]) and peer[0] == msg[4] and peer[1] == int(msg[5])):
                    flag += 1
                    if flag == 3:
                        self.report(peer)
                        s.close()
                        break
                else:
                    flag = 0
            s.close()

            # print("Sleep start")
            time.sleep(LIVENESS_SLEEP_TIME)
            # print("Sleep ends")

    def report(self, peer):
        try:
            self.peers.remove(peer)
        except ValueError:
            pass  # When node received on registering with seed is not live
        msg = "Reporting Dead Node message"
        self.write_to_file(msg)
        for seed in self.seed_list:
            s = socket(AF_INET, SOCK_STREAM)
            s.connect(seed)
            timestamp = str(time.time())
            msg = "Dead Node:" + peer[0] + ":" + str(peer[1]) + ":" + timestamp + \
                ":" + self.ip_address + ":" + str(self.port)
            s.sendall(bytes(msg, 'utf-8'))
            s.shutdown(1)
            s.close()

    def write_to_file(self, msg):
        print(msg)
        msg = f"{self.ip_address}:{self.port} : " + msg
        with open("outputpeer.txt", 'a') as f:
            f.write(msg + '\n')


if __name__ == "__main__":
    # host = input("Enter host : ")
    host = "127.0.0.1"  # localhost taken to be default for this assignment
    port = int(input("Enter port value "))
    peer = Peer(host, port)
    peer.start()
