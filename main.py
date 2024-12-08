import logging
import logging.config
import configparser
import yaml
from data_logic import *

class MainEntry:
    def __init__(self):
        self.initlog()
        self.data_logic = Data_logic(self.logger)

    def initlog(self):
        print('enter into initlog.')
        with open('./conf/logging_config.yaml', 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file.read())
            logging.config.dictConfig(config)
        self.logger = logging.getLogger('data_migration')
        self.logger.info("initlog func end.")
    
    def demo_test(self, config):  
        self.data_logic.demo_test(config)


# business main
def main():
    mainEntry = MainEntry()
    config = configparser.ConfigParser()
    config.read("./conf/conf.ini")
    mainEntry.demo_test(config)
      

if __name__ == "__main__":
    main()
    


        

        






        

    
