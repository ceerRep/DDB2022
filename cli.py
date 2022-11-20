import cmd, sys
import socket
import readline

class ServerShell(cmd.Cmd):
    intro = 'Welcome to the server.\n'
    prompt = '(sql) '
    file = None

    def __init__(self, server: str, port: intro) -> None:
        super().__init__()
        self.file = socket.create_connection((server, port))

    def default(self, line: str):
        self.file.send(line.encode('utf-8'))
        output = self.file.recv(8192)
        print(output.decode('utf-8'))


    def close(self):
        if self.file:
            self.file.close()
            self.file = None

def parse(arg):
    'Convert a series of zero or more numbers to an argument tuple'
    return tuple(map(int, arg.split()))

if __name__ == '__main__':
    ServerShell(sys.argv[1], sys.argv[2]).cmdloop()
