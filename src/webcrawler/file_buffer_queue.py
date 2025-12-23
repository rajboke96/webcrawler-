import logging
import os

# level=logging.DEBUG
# logging.basicConfig(level=level)

class QueueUnderFlow(Exception): pass
class QueueOverFLow(Exception): pass

class FileQueue:
    def __init__(self, queue_dir) -> None:
        self.queue_dir = queue_dir
        if not os.path.exists(queue_dir):
            logging.info(f"Queue dir not found! Creating: {queue_dir}")
            os.makedirs(queue_dir)

    def is_empty(self):
        queued_files=sorted(os.listdir(self.queue_dir))
        logging.debug(f"Is empty: {queued_files==[]}")
        return queued_files==[]

    def dequeue_bulk(self):
        logging.debug(f"Dequeuing!")
        queued_files=sorted(os.listdir(self.queue_dir))
        logging.debug(f"queued_files: {queued_files}")
        if queued_files:
            items_list=[]
            first_queue_file_path=f"{self.queue_dir}/{queued_files[0]}"
            with open(first_queue_file_path, "r") as f:
                tmp=f.readlines()
                for item in tmp:
                    items_list.append(item.strip("\n"))
            os.remove(first_queue_file_path)
            return items_list
        else:
            raise QueueUnderFlow("Queue is empty!")
        
    def enqueue_bulk(self, items_list):
        logging.debug(f"Enqueuing bulk with size: {len(items_list)}")
        queued_files=sorted(os.listdir(self.queue_dir))
        logging.debug(f"queued_files: {queued_files}")
        seq_no=-1
        if queued_files:
            seq_no=int(queued_files[-1].split(".")[0])
        enqueue_filepath=f"{self.queue_dir}/{seq_no+1:010d}.dat"
        logging.debug(f"Enqueuing: {enqueue_filepath}")
        tmp=[]
        for item in items_list:
            tmp.append(f"{item}\n")
        with open(enqueue_filepath, "w") as f:
            f.writelines(tmp)

if __name__ == "__main__":
    from config import data_dir
    q=FileQueue(queue_dir=f"{data_dir}/test_queue")
    items_list=['Hello\n', 'World\n', 'Bye\n']
    q.enqueue_bulk(items_list)
    # q.enqueue_bulk(items_list)
    # q.enqueue_bulk(items_list)
    # q.enqueue_bulk(items_list)
    # print(q.dequeue_bulk())