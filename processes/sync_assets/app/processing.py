import os
import shutil

class Processing:
    # Init
    def __init__(self):
        self.load_env_variables()
        pass
    
    ############################################################
    # Loaders

    def load_env_variables(self):
        self.src_path = os.getenv('path', '/usr/src/app/assets/')
        self.dst_path = os.getenv('path', '/usr/src/app/shared/assets/')

    def print_data(self, path):
        print('print_data in', path)
        
        dir = os.path.dirname(path)

        if os.path.exists(dir):
            files_and_dirs = os.listdir(dir)
            print("Contents of the directory:")
            for item in files_and_dirs:
                print(item)

            print()
            for dirpath, dirnames, filenames in os.walk(dir):
                print(f'Current directory: {dirpath}')
                for filename in filenames:
                    print(f'File: {filename}')
                for dirname in dirnames:
                    print(f'Directory: {dirname}')

        pass

    ############################################################
    # Methods

    def execute_process(self):
        print('execute_process')

        # Define the source and destination paths
        source_folder = self.src_path  # Mounted folder path
        volume_folder = self.dst_path  # Volume folder path

        # Ensure the destination folder exists
        os.makedirs(volume_folder, exist_ok=True)

        # Copy all contents from the source to the destination folder
        for item in os.listdir(source_folder):
            source_item = os.path.join(source_folder, item)
            dest_item = os.path.join(volume_folder, item)

            # If the item is a file, copy it
            if os.path.isfile(source_item):
                shutil.copy2(source_item, dest_item)

            # If the item is a directory, copy the directory recursively
            elif os.path.isdir(source_item):
                shutil.copytree(source_item, dest_item)
        pass

    ############################################################
        
    def execute(self):
        self.print_data(self.src_path)
        self.execute_process()
        self.print_data(self.dst_path)
        pass