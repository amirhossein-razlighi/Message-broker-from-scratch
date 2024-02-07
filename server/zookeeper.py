from broker import Broker

class ZooKeeper(Broker):

    def __init__(self, broker_ids):
        super().__init__()
        self.brokers = {broker_id: {'status': 'DOWN'} for broker_id in broker_ids}

    def add_broker(self, broker_id):
        if broker_id not in self.brokers:
            self.brokers[broker_id] = {'status': 'DOWN'}
            print(f"Broker {broker_id} added.")
        else:
            print(f"Error: Broker {broker_id} already exists.")

    def remove_broker(self, broker_id):
        if broker_id in self.brokers:
            del self.brokers[broker_id]
            print(f"Broker {broker_id} removed.")
        else:
            print(f"Error: Broker {broker_id} not found.")

    def update_broker_status(self, broker_id, status):
        if broker_id in self.brokers:
            self.brokers[broker_id]['status'] = status
        else:
            print(f"Error: Broker {broker_id} not found.")

    def get_active_brokers(self):
        active_brokers = [broker_id for broker_id, data in self.brokers.items() if data['status'] == 'UP']
        return active_brokers

    def start_broker(self, broker_id):
        self.update_broker_status(broker_id, 'UP')
        print(f"Broker {broker_id} started.")

    def stop_broker(self, broker_id):
        self.update_broker_status(broker_id, 'DOWN')
        print(f"Broker {broker_id} stopped.")

