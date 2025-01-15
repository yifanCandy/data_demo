import time
from datetime import datetime

class MainEntry:
    def __init__(self):
        print('enter into __init__.')
    
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
    


        

        






        

    
