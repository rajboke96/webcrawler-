import os
import logging

class Directory:
    def __init__(self, dir) -> None:
        self.dir=dir
        if not os.path.exists(dir):
            logging.info(f"Directory not found! Creating: {dir}")
            os.makedirs(dir)
    
    def write_to_file(self, file_name, txt):
        with open(f"{self.dir}/{file_name}", "w") as f:
            f.write(txt)

    def writelines_to_file(self, file_name, data):
        with open(f"{self.dir}/{file_name}", "w") as f:
            f.writelines(data)

    def read_from_file(self, file_name):
        with open(f"{self.dir}/{file_name}", "r") as f:
            return f.read()

    def readlines_from_file(self, file_name):
        with open(f"{self.dir}/{file_name}", "r") as f:
            return f.readlines()

    def delete_file(self, file_name):
        os.remove(f"{self.dir}/{file_name}")
