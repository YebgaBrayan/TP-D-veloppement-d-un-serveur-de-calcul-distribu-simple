import socket
import json
import threading
import queue
import time
import uuid
import logging
import random

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ComputeServer:
    def _init_(self, host: str = '0.0.0.0', port: int = 9000):
        self.host = host
        self.port = port
        self.server_socket = None
        self.clients = {}  # {client_id: (socket, address)}
        self.tasks_queue = queue.Queue()
        self.results = {}
        self.running = False

    def start(self):
        """Démarre le serveur et attend les connexions clients"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True

        logging.info(f"Serveur démarré sur {self.host}:{self.port}")

        # Threads pour distribuer les tâches et générer des tâches de test
        threading.Thread(target=self.distribute_tasks, daemon=True).start()
        threading.Thread(target=self.generate_test_tasks, daemon=True).start()

        try:
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    threading.Thread(target=self.handle_client, args=(client_socket, address), daemon=True).start()
                except Exception as e:
                    logging.error(f"Erreur lors de l'acceptation d'une connexion : {e}")
        except KeyboardInterrupt:
            logging.info("Arrêt du serveur...")
        finally:
            self.stop()

    def stop(self):
        """Arrête le serveur et ferme toutes les connexions"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()

        for client_id, (socket, _) in self.clients.items():
            try:
                socket.close()
            except Exception as e:
                logging.warning(f"Erreur lors de la fermeture de la connexion avec {client_id}: {e}")

        logging.info("Serveur arrêté")

    def handle_client(self, client_socket: socket.socket, address: tuple):
        """Gère la communication avec un client"""
        client_id = str(uuid.uuid4())
        self.clients[client_id] = (client_socket, address)
        logging.info(f"Nouveau client connecté: {address} (ID: {client_id})")

        try:
            self.send_message(client_socket, {
                "type": "register_ack",
                "id": client_id,
                "message": "Bienvenue au service de calcul distribué"
            })

            while self.running:
                try:
                    message = self.receive_message(client_socket)
                    if not message:
                        break
                    if self.validate_message(message):
                        self.process_client_message(client_id, message)
                    else:
                        logging.warning(f"Message invalide reçu du client {client_id}: {message}")
                except ConnectionError:
                    break
                except Exception as e:
                    logging.error(f"Erreur lors du traitement d'un message du client {client_id}: {e}")
                    break
        finally:
            logging.info(f"Client déconnecté: {address} (ID: {client_id})")
            client_socket.close()
            self.clients.pop(client_id, None)

    def validate_message(self, message: dict) -> bool:
        """Valide le message reçu du client"""
        required_keys = {"type", "id"}
        return required_keys.issubset(message.keys())

    def process_client_message(self, client_id: str, message: dict):
        """Traite un message reçu d'un client"""
        msg_type = message.get("type")

        if msg_type == "register":
            logging.info(f"Client {client_id} s'est enregistré avec les capacités: {message.get('capabilities', {})}")

        elif msg_type == "result":
            task_id = message.get("id")
            result = message.get("payload", {}).get("result")

            logging.info(f"Résultat reçu pour la tâche {task_id} du client {client_id}: {result}")
            self.results[task_id] = {
                "client_id": client_id,
                "result": result,
                "timestamp": time.time()
            }

        elif msg_type == "status":
            socket, _ = self.clients[client_id]
            self.send_message(socket, {
                "type": "status_response",
                "id": str(uuid.uuid4()),
                "payload": {
                    "server_status": "running",
                    "clients_connected": len(self.clients),
                    "tasks_pending": self.tasks_queue.qsize(),
                    "results_received": len(self.results)
                }
            })

    def distribute_tasks(self):
        """Distribue les tâches aux clients disponibles"""
        while self.running:
            try:
                if not self.clients:
                    time.sleep(1)
                    continue

                try:
                    task = self.tasks_queue.get(block=True, timeout=1)
                except queue.Empty:
                    continue

                # Sélectionne un client aléatoirement
                client_ids = list(self.clients.keys())
                if not client_ids:
                    self.tasks_queue.put(task)  # Remet la tâche dans la file
                    time.sleep(1)
                    continue

                client_id = random.choice(client_ids)
                client_socket, _ = self.clients[client_id]

                try:
                    self.send_message(client_socket, task)
                    logging.info(f"Tâche {task['id']} envoyée au client {client_id}")
                except Exception as e:
                    logging.error(f"Échec d'envoi de tâche au client {client_id}: {e}")
                    self.tasks_queue.put(task)  # Remet la tâche dans la file
            except Exception as e:
                logging.error(f"Erreur dans le distributeur de tâches: {e}")

    def generate_test_tasks(self):
        """Génère des tâches de test (calcul de nombres premiers)"""
        ranges = [(10000, 15000), (15000, 20000), (20000, 25000), 
                  (25000, 30000), (30000, 35000), (35000, 40000)]

        for i, (start, end) in enumerate(ranges):
            task_id = f"prime_task_{i}"
            task = {
                "type": "task",
                "id": task_id,
                "payload": {
                    "type": "find_primes",
                    "range_start": start,
                    "range_end": end
                }
            }
            self.tasks_queue.put(task)
            logging.info(f"Tâche créée: recherche des nombres premiers entre {start} et {end}")

        logging.info(f"{len(ranges)} tâches générées et mises en file d'attente")

    def send_message(self, sock: socket.socket, message: dict):
        """Envoie un message au format JSON au client"""
        try:
            message_json = json.dumps(message)
            message_bytes = message_json.encode('utf-8')
            message_size = len(message_bytes)
            sock.sendall(message_size.to_bytes(4, byteorder='big'))
            sock.sendall(message_bytes)
        except Exception as e:
            logging.error(f"Erreur lors de l'envoi du message: {e}")
            raise ConnectionError(f"Erreur lors de l'envoi du message: {e}")

    def receive_message(self, sock: socket.socket) -> dict:
        """Reçoit un message au format JSON du client"""
        try:
            message_size_bytes = sock.recv(4)
            if not message_size_bytes:
                return None

            message_size = int.from_bytes(message_size_bytes, byteorder='big')
            chunks = []
            bytes_received = 0

            while bytes_received < message_size:
                chunk = sock.recv(min(message_size - bytes_received, 4096))
                if not chunk:
                    raise ConnectionError("Connexion interrompue pendant la réception")
                chunks.append(chunk)
                bytes_received += len(chunk)

            message_bytes = b''.join(chunks)
            message_json = message_bytes.decode('utf-8')
            return json.loads(message_json)
        except Exception as e:
            logging.error(f"Erreur lors de la réception du message: {e}")
            raise ConnectionError(f"Erreur lors de la réception du message: {e}")

if _name_ == "_main_":
    server = ComputeServer()
    server.start()