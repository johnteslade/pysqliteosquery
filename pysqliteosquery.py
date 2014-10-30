import os, sys, time
import apsw
import psutil

class PortsSource(object):
    """ List of ports """

    def Create(self, db, modulename, dbname, tablename, *args):
        columns = ['pid', 'name', 'username']
        data = []
        counter = 1

        for proc in psutil.process_iter():
            try:
                for conn in proc.get_connections(kind="inet"):
                    if conn.status == "LISTEN":
                        counter += 1
                        data.append( (counter, proc.pid, conn.laddr[0], conn.laddr[1]))
            except psutil.AccessDenied:
                pass

        schema = "CREATE TABLE {} (pid integer, address text, port integer)".format(dbname)
        return schema, Table(columns, data)
    Connect = Create

class ProcessSource(object):
    """ List of processes """

    def Create(self, db, modulename, dbname, tablename, *args):
        columns = ['pid', 'name', 'username']
        data = []
        counter = 1

        for proc in psutil.process_iter():
            try:
                pinfo = proc.as_dict(attrs=columns)
                counter += 1
                data.append([counter] + [ pinfo[col] for col in columns ])
            except psutil.NoSuchProcess:
                pass

        schema = "CREATE TABLE {} (pid integer, name text, username text)".format(dbname)
        return schema, Table(columns, data)
    Connect = Create

class Table(object):
    """ Represents a table """
    def __init__(self, columns, data):
        self.columns = columns
        self.data = data

    def BestIndex(self, *args):
        return None

    def Open(self):
        return Cursor(self)

    def Disconnect(self):
        pass

    Destroy = Disconnect

class Cursor(object):
    """ Represents a cursor """
    def __init__(self, table):
        self.table = table

    def Filter(self, *args):
        self.pos = 0

    def Eof(self):
        return self.pos >= len(self.table.data)

    def Rowid(self):
        return self.table.data[self.pos][0]

    def Column(self, col):
        return self.table.data[self.pos][col + 1]

    def Next(self):
        self.pos += 1

    def Close(self):
        pass

if __name__ == "__main__":

    connection = apsw.Connection("dbfile")
    cursor = connection.cursor()

    connection.createmodule("processsource", ProcessSource())
    connection.createmodule("portssource", PortsSource())

    cursor.execute("DROP TABLE IF EXISTS sysproc; CREATE VIRTUAL TABLE sysproc USING processsource()")
    cursor.execute("DROP TABLE IF EXISTS listening_ports; CREATE VIRTUAL TABLE listening_ports USING portssource()")

    for row in cursor.execute("SELECT * from sysproc limit 10"):
        print row

    print "------------"

    for row in cursor.execute("SELECT * from listening_ports limit 10"):
        print row

    print "------------"

    for row in cursor.execute("select distinct process.name, listening.port, process.pid from sysproc as process join listening_ports as listening on process.pid = listening.pid where listening.address = '127.0.0.1';"):
        print row

