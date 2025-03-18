import socket
import json

class ComputeClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.client_socket = None

    def start(self):
        """Démarre le client et se connecte au serveur"""
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.client_socket.connect((self.host, self.port))
            print(f"Connecté au serveur {self.host}:{self.port}")
            self.run()
        except Exception as e:
            print(f"Erreur de connexion au serveur : {e}")
        finally:
            self.client_socket.close()

    def run(self):
        """Gère la communication avec le serveur"""
        try:
            while True:
                message = self.receive_message()
                if message is None:
                    print("Connexion au serveur perdue.")
                    break
                print("Message reçu :", message)
        except Exception as e:
            print(f"Erreur lors de la communication : {e}")

    def receive_message(self):
        """Reçoit un message au format JSON du serveur"""
        try:
            message_size_bytes = self.client_socket.recv(4)
            if not message_size_bytes:
                return None
            
            message_size = int.from_bytes(message_size_bytes, byteorder='big')
            chunks = []
            bytes_received = 0

            while bytes_received < message_size:
                chunk = self.client_socket.recv(min(message_size - bytes_received, 4096))
                if not chunk:
                    raise ConnectionError("Connexion interrompue pendant la réception")
                chunks.append(chunk)
                bytes_received += len(chunk)

            message_bytes = b''.join(chunks)
            print("Données brutes reçues :", message_bytes)  # Pour le débogage
            
            # Tentez de décoder le message et gérez les erreurs
            try:
                message_json = message_bytes.decode('utf-8')
                return json.loads(message_json)
            except UnicodeDecodeError as e:
                print(f"Erreur de décodage UTF-8 : {e}")
                print("Données brutes reçues (non décodées) :", message_bytes)
                return None
            except json.JSONDecodeError as e:
                print(f"Erreur de décodage JSON : {e}")
                return None
        except Exception as e:
            print(f"Erreur lors de la réception du message: {e}")
            return None

if __name__ == "__main__":
    client = ComputeClient(host='192.168.215.121', port=9000)
    client.start()