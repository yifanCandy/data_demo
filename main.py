import logging
import logging.config
import configparser
import yaml
import time
from datetime import datetime
from data_logic import *

class MainEntry:
    def __init__(self):
        print('enter into __init__.')
        # self.initlog()
        # self.data_logic = Data_logic(self.logger)
    '''
    def initlog(self):
        print('enter into initlog.')
        with open('./conf/logging_config.yaml', 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file.read())
            logging.config.dictConfig(config)
        self.logger = logging.getLogger('data_migration')
        self.logger.info("initlog func end.")
    '''
    
    def demo_test(self):  
        while True:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f'this is demo test. current_time:{current_time}')
            time.sleep(5)

# business main
def main():
    main_instance = MainEntry()
    main_instance.demo_test()

if __name__ == "__main__":
    main()
    


        

        






        

    
