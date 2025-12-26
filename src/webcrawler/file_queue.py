import logging
import os

from json_serializer import JsonSerializer, JsonSerializerClass
from directory_helper import Directory

level=logging.DEBUG
logging.basicConfig(level=level)

class QueueUnderFlow(Exception): pass
class QueueOverFLow(Exception): pass

class FileQueue:
    def __init__(self, queue_dir, queue_name="queue") -> None:
        self.queue_dir = os.path.join(queue_dir, queue_name)
        self.queue_dir_obj = Directory(dir=self.queue_dir)
        self.queue_file=os.path.join(self.queue_dir, f"{queue_name}.json")
        class JsonQueueSerializer(JsonSerializerClass):
            front=str
            rear=str
            size=int
            def __str__(self) -> str:
                return f"front: {self.front}, rear: {self.rear}, size: {self.size}"
        self.queue=JsonQueueSerializer(front="", rear="", size=0)
        self.json_serializer=JsonSerializer(JsonQueueSerializer)
        if not os.path.exists(self.queue_file):
            logging.info(f"{queue_name} file not found! Creating: {self.queue_file} with default: {self.queue}")
            self.json_serializer.dump(self.queue, self.queue_file)
        else:
            logging.info(f"{queue_name} file found! Loading!")
            self.queue=self.json_serializer.load(self.queue_file)

    def is_empty(self):
        return self.queue.size==0

    def get_size(self):
        return self.queue.size
    
    def get_front(self):
        if self.queue.front:
            return self.queue_dir_obj.readlines_from_file(self.queue.front)
        else:
            raise QueueUnderFlow("Queue is empty!")

    def get_rear(self):
        if self.queue.rear:
            return self.queue_dir_obj.readlines_from_file(self.queue.rear)
        else:
            raise QueueUnderFlow("Queue is empty!")

    def dequeue_bulk(self):
        logging.debug(f"Dequeuing!")
        logging.debug(f"queue: {self.queue}")
        if self.queue.front:
            logging.debug(f"Queue front: {self.queue.front}")
            items_list=[]
            tmp=self.queue_dir_obj.readlines_from_file(self.queue.front)
            for item in tmp:
                items_list.append(item.strip("\n"))
            self.queue_dir_obj.delete_file(self.queue.front)
            seq_no=int(self.queue.front.split(".")[0])
            new_front=f"{seq_no+1:010d}.dat"
            if self.queue.front==self.queue.rear:
                self.queue.front=None
                self.queue.rear=None
            else:
                self.queue.front=new_front
            self.queue.size-=len(items_list)
            logging.debug(f"queue: {self.queue}")
            self.json_serializer.dump(self.queue, self.queue_file)
            return items_list
        else:
            raise QueueUnderFlow("Queue is empty!")
        
    def enqueue_bulk(self, items_list):
        logging.debug(f"Enqueuing bulk with size: {len(items_list)}")
        logging.debug(f"queue: {self.queue}")
        seq_no=-1
        if self.queue.rear:
            seq_no=int(self.queue.rear.split(".")[0])
        tmp=[]
        for item in items_list:
            tmp.append(f"{item}\n")
        logging.debug(f"Enqueuing file: {seq_no+1:010d}.dat")
        self.queue_dir_obj.writelines_to_file(f"{seq_no+1:010d}.dat", data=tmp)
        self.queue.rear=f"{seq_no+1:010d}.dat"
        if not self.queue.front:
            self.queue.front=self.queue.rear
        self.queue.size+=len(items_list)
        logging.debug(f"queue: {self.queue}")
        self.json_serializer.dump(self.queue, self.queue_file)


if __name__ == "__main__":
    from config import data_dir
    q1=FileQueue(queue_dir=data_dir, queue_name="test_queue")
    items_list=['Hello', 'World', 'Bye']
    q1.enqueue_bulk(items_list)
    q1.enqueue_bulk(items_list)
    q1.enqueue_bulk(items_list)
    q1.enqueue_bulk(items_list)
    q1.enqueue_bulk(items_list)