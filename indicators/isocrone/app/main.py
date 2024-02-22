from indicator import Indicator

def read_root():
    return {"message": "Hello World"}

def main():
    indicator = Indicator()
    indicator.exec()

if __name__ == '__main__':
    main()
