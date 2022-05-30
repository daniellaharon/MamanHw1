from typing import List

import psycopg2

import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


# ON DELETE CASCADE constraint is used in MySQL to delete
# the rows from the child table automatically,
# when the rows from the parent table are deleted
# DONE
def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"

                     "CREATE TABLE File( "
                     "file_id INTEGER PRIMARY KEY NOT NULL CHECK(file_id > 0),"
                     "file_type TEXT NOT NULL,"
                     "file_size INTEGER NOT NULL CHECK(file_size >= 0));"

                     "CREATE TABLE Disk( "
                     "disk_id INTEGER PRIMARY KEY NOT NULL CHECK(disk_id > 0),"
                     "disk_manufacturing_company TEXT NOT NULL,"
                     "disk_speed INTEGER NOT NULL CHECK(disk_speed >= 0),"
                     "disk_free_space INTEGER NOT NULL CHECK(disk_free_space >= 0),"
                     "disk_cost_per_byte INTEGER NOT NULL CHECK(disk_cost_per_byte >= 0));"

                     "CREATE TABLE RAM( "
                     "ram_id INTEGER PRIMARY KEY NOT NULL CHECK(ram_id > 0),"
                     "ram_size INTEGER NOT NULL CHECK(ram_size >= 0),"
                     "ram_company TEXT NOT NULL);"

                     "CREATE TABLE FilesInDisk( "
                     "disk_id INTEGER NULL CHECK(disk_id > 0), "
                     "file_id INTEGER NOT NULL CHECK(file_id > 0),"
                     "PRIMARY KEY (disk_id, file_id),"
                     "FOREIGN KEY(disk_id) REFERENCES Disk(disk_id) ON DELETE CASCADE,"
                     "FOREIGN KEY(file_id) REFERENCES File(file_id) ON DELETE CASCADE);"

                     "CREATE TABLE RAMInDisk( "
                     "disk_id INTEGER NULL CHECK(disk_id > 0),"
                     "ram_id INTEGER NOT NULL CHECK(ram_id > 0),"
                     "PRIMARY KEY (disk_id, ram_id),"
                     "FOREIGN KEY(disk_id) REFERENCES Disk(disk_id) ON DELETE CASCADE,"
                     "FOREIGN KEY(ram_id) REFERENCES RAM(ram_id) ON DELETE CASCADE);"

                     "COMMIT")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Status.ALREADY_EXISTS
    except Exception as e:
        print(e)
        return Status.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return Status.OK


# Clears the tables for your solution (leaves tables in place but without any data)
# DONE
def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("TRUNCATE File, Disk,RAM ,FilesInDisk, RamInDisk")
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:
        return Status.ERROR
    # except Exception as e:
    #     print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


# should we add IF EXISTS table_name CASCADE?
# NOTDONW
def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DROP TABLE IF EXISTS File CASCADE;"
                     "DROP TABLE IF EXISTS Disk CASCADE;"
                     "DROP TABLE IF EXISTS RAM CASCADE;"
                     "DROP TABLE IF EXISTS FilesInDisk CASCADE;"
                     "DROP TABLE IF EXISTS RAMInDisk CASCADE;"
                     "COMMIT")
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:
        return Status.ERROR
    # except Exception as e:
    #     print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


# DONE
def addFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO File(file_id, file_type, file_size) "
                        "VALUES({id},{type},{size})").format(id=sql.Literal(file.getFileID()),
                                                             type=sql.Literal(file.getType()),
                                                             size=sql.Literal(file.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Status.BAD_PARAMS
    except psycopg2.errors.InvalidTextRepresentation:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:
        return Status.ERROR
    # except Exception as e:
    #     print(e)
    #     return Status.BAD_PARAMS
    finally:
        conn.close()
    return Status.OK


# DONE
def getFileByID(fileID: int) -> File:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * "
                                             "FROM File "
                                             "WHERE File.file_id={id}".format(id=fileID))
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:
        return Status.ERROR
    # except Exception as e:
    #     print(e)
    #     return Status.BAD_PARAMS
    finally:
        conn.close()
        if len(result.rows):
            return File(result.rows[0][0], result.rows[0][1], result.rows[0][2])
        return File.badFile()


# DONE
def deleteFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"

                        # contains the size of the file with file_id
                        "CREATE VIEW size_of_file AS "
                        "SELECT file_size "
                        "FROM File"
                        "WHERE file_id = {file_id}; "

                        # if the file is on any of the disks, the disk_id will be in this view
                        "CREATE VIEW disk_contains_file AS "
                        "SELECT disk_id "
                        "FROM FilesInDisk "
                        "WHERE file_id = {file_id}; "

                        # updating the free space of the disk after the deletion of file from it
                        "UPDATE Disk "
                        "SET disk_free_space = "
                        "disk_free_space + "
                        "(SELECT SUM(file_size) FROM size_of_file) "
                        "WHERE Disk.disk_id IN "
                        "(SELECT disk_id FROM disk_contains_file);"

                        # deleting the file_id tuple from the File relation.
                        # will be deleted from the FilesInDisk too, as we defined the table with ON DELETE CASCADE"
                        "DELETE FROM File "
                        "WHERE  file_id = {file_id} ;"

                        "COMMIT;").format(file_id=sql.Literal(file.getFileID()))

        conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:  # file does not exist so the return value is OK
        print(e)
        return Status.OK
    except DatabaseException.CHECK_VIOLATION as e:  # file does not exist so the return value is OK
        print(e)
        return Status.OK
    except DatabaseException.UNIQUE_VIOLATION as e:  # file does not exist so the return value is OK
        print(e)
        return Status.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:  # file does not exist so the return value is OK
        print(e)
        return Status.OK
    except Exception as e:
        print(e)
        return Status.ERROR
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()
    return Status.OK


# DONE
def addDisk(disk: Disk) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Disk(disk_id, disk_manufacturing_company, disk_speed ,disk_free_space, disk_cost_per_byte)"
            "VALUES({id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte})").format(
            id=sql.Literal(disk.getDiskID()),
            manufacturing_company=sql.Literal(disk.getCompany()),
            speed=sql.Literal(disk.getSpeed()),
            free_space=sql.Literal(disk.getFreeSpace()),
            cost_per_byte=sql.Literal(disk.getCost()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Status.BAD_PARAMS
    except psycopg2.errors.InvalidTextRepresentation:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:
        return Status.ERROR
    # except Exception as e:
    #     print(e)
    #     return Status.BAD_PARAMS
    finally:
        conn.close()
    return Status.OK


# DONE
def getDiskByID(diskID: int) -> Disk:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * "
                                             "FROM Disk "
                                             "WHERE Disk.disk_id={id}".format(id=diskID))
        conn.commit()
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:
        return Status.ERROR
    # except Exception as e:
    #     print(e)
    #     return Status.BAD_PARAMS
    finally:
        conn.close()
        if len(result.rows):
            return Disk(result.rows[0]["disk_id"], result.rows[0]["disk_manufacturing_company"],
                        result.rows[0]["disk_speed"], result.rows[0]["disk_free_space"],
                        result.rows[0]["disk_cost_per_byte"])
        return Disk.badDisk()


# check return values and correctness
# NOT DONE
def deleteDisk(diskID: int) -> Status:
    conn = None
    rows_effected, res = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disk "
                        "WHERE Disk.disk_id = {disk_id}").format(disk_id=sql.Literal(diskID))
        rows_effected, res = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.ERROR
    except Exception as e:
        return Status.ERROR
    finally:
        conn.close()
    if rows_effected == 0:
        return Status.NOT_EXISTS
    return Status.OK


# DONE
def addRAM(ram: RAM) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO File(ram_id, ram_size, ram_company) "
                        "VALUES({id}, {type}, {size})").format(
            id=sql.Literal(ram.getRamID()),
            type=sql.Literal(ram.getSize()),
            size=sql.Literal(ram.getCompany()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Status.BAD_PARAMS
    except psycopg2.errors.InvalidTextRepresentation:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:
        return Status.ERROR
    # except Exception as e:
    #     print(e)
    #     return Status.BAD_PARAMS
    finally:
        conn.close()
    return Status.OK


# DONE
def getRAMByID(ramID: int) -> RAM:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * "
                                             "FROM File "
                                             "WHERE Ram.ram_id={id}".format(id=ramID))
        conn.commit()
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:
        return Status.ERROR
    # except Exception as e:
    #     print(e)
    #     return Status.BAD_PARAMS
    finally:
        conn.close()
        if len(result.rows):
            return RAM(result.rows[0]["ram_id"], result.rows[0]["ram_size"], result.rows[0]["ram_company"])
        return RAM.badRAM()


# check return values and correctness
# NOT DONE
def deleteRAM(ramID: int) -> Status:
    conn = None
    rows_effected, res = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RAM "
                        "WHERE RAM.RAM_id = {RAM_id}").format(id=sql.Literal(ramID))
        rows_effected, res = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Status.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        return Status.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Status.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.ERROR
    except Exception as e:
        return Status.ERROR
    finally:
        conn.close()
    if rows_effected == 0:
        return Status.NOT_EXISTS
    return Status.OK


# DONE
def addDiskAndFile(disk: Disk, file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO File(id_file, type_file, size_file)"
                        "VALUES({file_id}, {file_type}, {file_size});"

                        "INSERT INTO Disk(disk_id, disk_manufacturing_company, disk_speed ,"
                        "disk_free_space, disk_cost_per_byte)"
                        "VALUES({id_disk}, {manufacturing_company_disk}, {speed_disk}, "
                        "{free_space_disk}, {cost_per_byte_disk});"

                        "COMMIT;").format(id_file=sql.Literal(file.getFileID()),
                                          type_file=sql.Literal(file.getType()),
                                          size_file=sql.Literal(file.getSize()),
                                          id_disk=sql.Literal(disk.getDiskID()),
                                          manufacturing_company_disk=sql.Literal(disk.getCompany()),
                                          speed_disk=sql.Literal(disk.getSpeed()),
                                          free_space_disk=sql.Literal(disk.getFreeSpace()),
                                          cost_per_byte_disk=sql.Literal(disk.getCost()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Status.BAD_PARAMS
    except psycopg2.errors.InvalidTextRepresentation:
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS
    except DatabaseException.UNKNOWN_ERROR:
        return Status.ERROR
    # except Exception as e:
    #     print(e)
    #     return Status.BAD_PARAMS
    finally:
        conn.close()
    return Status.OK


# DONE
def addFileToDisk(file: File, diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        # if the file doesn't exist in File() relation or disk_id in Disk relation,
                        # this will make sure we will get a Foreign Key exception
                        "INSERT INTO FilesInDisk(disk_id, file_id) "
                        "VALUES({disk_id} ,{file_id});"
                        # deleting the tuple (added just for the FK check)
                        "DELETE FROM FilesInDisk "
                        "WHERE  FilesInDisk.disk_id = {disk_id} "
                        "AND  FilesInDisk.file_id = {file_id};"

                        "INSERT INTO FilesInDisk(file_id, disk_id) "
                        "(SELECT File.file_id, Disk.disk_id "
                        "FROM File, Disk "
                        "WHERE Disk.disk_id = {disk_id} AND File.file_id = {file_id}"
                        "AND File.file_size <= Disk.disk_free_space); "

                        # changing the free space in the disk after the insert (only if we successfully added the file)
                        "UPDATE Disk"
                        "SET disk_free_space = "
                        "disk_free_space - "
                        "(SELECT SUM(File.file_size)"
                        "FROM File"  # file_size is available only in File table
                        "WHERE File.file_id = {file_id})"
                        "WHERE Disk.disk_id = {disk_id}"
                        "AND EXISTS (SELECT file_id "
                        "FROM File "
                        "WHERE File.file_id = {file_id});"

                        "COMMIT;").format(disk_id=sql.Literal(diskID), file_id=sql.Literal(file.getFileID()))
        rows_effected, res = conn.execute(query)
        conn.commit()

    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:  # if file_id or disk_id doesnt exist
        return Status.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:  # if file_id already is on disk_id
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:  # file_size id greater than disk_free_space
        return Status.BAD_PARAMS
    except DatabaseException.ConnectionInvalid:
        return Status.ERROR
    except DatabaseException.UNKNOWN_ERROR:
        return Status.BAD_PARAMS
    except Exception as e:
        return Status.BAD_PARAMS

    finally:
        conn.close
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    return Status.OK


# check return values here and also the correctness of the sql query
def addRAMToDisk(ramID: int, diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        # if the ram_id doesn't exist in RAM() relation or disk_id in Disk() relation,
                        # this will make sure we will get a Foreign Key exception
                        "INSERT INTO RAMInDisk(disk_id , ram_id) "
                        "VALUES({disk_id} , {ram_id});"
                        # deleting the tuple (added just for the FK check)
                        "DELETE FROM RAMInDisk "
                        "WHERE  RAMInDisk.disk_id = {disk_id} "
                        "AND  RAMInDisk.ram_id = {ram_id};"

                        "INSERT INTO RAMInDisk(disk_id, ram_id) "
                        "(SELECT Disk.disk_id RAM.ram_id "
                        "FROM Disk, RAM "
                        "WHERE Disk.disk_id = {disk_id} AND RAM.ram_id = {ram_id});"

                        "COMMIT").format(disk_id=sql.Literal(diskID), ram_id=sql.Literal(ramID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION:  # if ram_id or disk_id doesnt exist
        return Status.NOT_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:  # if ram_id is already a part of disk_id
        return Status.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid:
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        return Status.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION:
        return Status.BAD_PARAMS
    except Exception as e:
        return Status.BAD_PARAMS

    finally:
        conn.close()
    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


# return values and correctness,  # maybe add HAVING here
# NOT DONE
def averageFileSizeOnDisk(diskID: int) -> float:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT AVG(file_size) "
                        "FROM File "
                        "WHERE File.file_id IN"
                        "(SELECT file_id "
                        "FROM FilesInDisk "
                        "WHERE disk_id={disk_id} "
                        "GROUP BY disk_id,file_id").format(disk_id=sql.Literal(diskID))  # maybe add HAVING here?
        rows_effected, result = conn.execute(query)
        conn.commit()
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid:
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return -1
    except DatabaseException.UNIQUE_VIOLATION:
        return -1

    except DatabaseException.NOT_NULL_VIOLATION:
        return -1
    except DatabaseException.CHECK_VIOLATION:
        return -1

    except Exception as e:
        return -1
    finally:
        conn.close()

    if result.rows[0][0] is not None:
        return result.rows[0][0]
    return 0


# return values and correctness,  # maybe add HAVING here
# NOT DONE
def diskTotalRAM(diskID: int) -> int:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT SUM(ram_size) "
                        "FROM RAM "
                        "WHERE RAM.ram_id IN"
                        "(SELECT ram_id "
                        "FROM RAMInDisk "
                        "WHERE disk_id={disk_id} "
                        "GROUP BY disk_id,ram_id").format(disk_id=sql.Literal(diskID))  # maybe add HAVING here
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid:
        return -1
    except DatabaseException.NOT_NULL_VIOLATION:
        return -1
    except DatabaseException.CHECK_VIOLATION:
        return -1
    except DatabaseException.UNIQUE_VIOLATION:
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return -1
    except Exception as e:
        return -1
    finally:
        conn.close()

    if result.rows[0][0] is not None:
        return result.rows[0][0]

    return 0


# check if the JOIN is right
# DONE
def getCostForType(type: str) -> int:
    # Returns the total amount of money paid (cost per unit * size) for saving type files across all disks.
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"

                        # creating a view of IDs and sizes of FILES from the file_type specified.
                        "CREATE VIEW files_id_size AS"
                        "SELECT file_id, file_size"
                        "FROM File"
                        "GROUP BY file_type, file_id"
                        "HAVING file_type = {file_type);"

                        # creating a view of IDs of cost_per_byte of DISKS 
                        "CREATE VIEW disks_id_cost_per_byte AS"
                        "SELECT disk_id, disk_cost_per_byte"
                        "FROM Disk;"

                        # three-way INNER-JOIN to get a view of file_size of the files from the specified type
                        # and disk_cost_per_byte of the disk the files are on.
                        "CREATE VIEW fileSizes_diskCosts AS"
                        "SELECT file_size, disk_cost_per_byte "
                        "FROM ((FilesInDisk"
                        "INNER JOIN files_id_size ON FilesInDisk.file_id = files_id_size.file_id)"
                        "INNER JOIN disks_id_cost_per_byte ON FilesInDisk.disk_id = disks_id_cost_per_byte.disk_id)"

                        "SELECT SUM(file_size * disk_cost_per_byte) "
                        "FROM fileSizes_diskCosts"

                        "COMMIT;").format(file_type=sql.Literal(type))
        rows_effected, res = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid:
        return -1
    except DatabaseException.NOT_NULL_VIOLATION:
        return -1
    except DatabaseException.CHECK_VIOLATION:
        return -1
    except DatabaseException.UNIQUE_VIOLATION:
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return -1
    except Exception as e:
        return -1

    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    if res.rows[0][0] is not None:
        return res.rows[0][0]

    return 0


# DONE
def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    # Returns a List (up to size 5) of files’ IDs that can be added to the disk with diskID as singles - not all
    # together (even if they’re already on the disk). The list should be ordered by IDs in descending order.

    conn = None
    rows_effected = 0
    res = Connector.ResultSet()

    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT file_id "
                        "FROM File "
                        "WHERE File.file_size <= "
                        "(SELECT SUM(disk_free_space) "
                        "FROM Disk "
                        "WHERE Disk.disk_id = {disk_id}) "
                        "ORDER BY file_id DESC LIMIT 5").format(disk_id=sql.Literal(diskID))
        rows_effected, res = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid:
        return []
    except DatabaseException.NOT_NULL_VIOLATION:
        return []
    except DatabaseException.CHECK_VIOLATION:
        return []
    except DatabaseException.UNIQUE_VIOLATION:
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return []
    except Exception as e:
        return []

    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    # making a list out of the tuples in res
    res_list = []
    for row in res.rows:
        res_list.append(int(row[0]))
    return res_list


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


# should check if the disk_id exist in Disk relation??
def isCompanyExclusive(diskID: int) -> bool:
    # Returns whether the disk with diskID is manufactured by the same company as all its RAMs
    conn = None
    rows_effected = 0
    res = Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        # should check if the disk_id exist in Disk relation??

                        # creating a view of the companies of all RAMs in the disk with disk_id
                        "CREATE VIEW company_of_ramInDisk AS "
                        "SELECT DISTINCT ram_company "
                        "FROM RAM "
                        "WHERE RAM.ram_id IN "
                        "(SELECT ram_id "
                        "FROM RAMInDisk "
                        "GROUP BY disk_id, ram_id "
                        "HAVING disk_id = {disk_id}); "

                        # creating a table of the company names of the manufacturers of the disks
                        "CREATE VIEW company_of_disks AS "
                        "SELECT disk_manufacturing_company "
                        "FROM Disk"
                        "WHERE Disk.disk_id = {disk_id};"

                        # a view of all the companies that are different between the disks and rams
                        "CREATE VIEW result_view AS "
                        "SELECT disk_manufacturing_company "
                        "FROM company_of_disks, company_of_ramInDisk "
                        "WHERE company_of_disks.disk_manufacturing_company != company_of_ramInDisk.ram_company;"
                        # counting the different companies. 
                        # if zero - all the rams on disk are from the same company as the disk itself
                        "SELECT COUNT(disk_manufacturing_company) "
                        "FROM result_view "
                        "COMMIT;").format(disk_id=sql.Literal(diskID))

        rows_effected, res = conn.execute(query)
        conn.commit()

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return False
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return False
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return False
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return False
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return False
    except Exception as e:
        print(e)
        return False
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    if res.rows[0][0] != 0:  # NOT ALL the rams on disk are from the same company as the disk itself.
        return False

    return True


def getConflictingDisks() -> List[int]:
    # Returns a list containing conflicting disks' IDs (no duplicates).
    # Disks are conflicting if and only if they save at least one identical file.
    # The list should be ordered by diskIDs in ascending order.
    conn = None
    rows_effected, res = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"


                        "COMMIT;")
        rows_effected, res = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return []
    except Exception as e:
        print(e)
        return []
    finally:
        conn.close()

    res_list = []

    if res.rows:  # res is not empty
        for row in res.rows:
            res_list.append(int(row[0]))
        return res_list

    # res is empty - didn't find anything, return empty list
    return []


def mostAvailableDisks() -> List[int]:
    # Returns a list of up to 5 disks' IDs that can save the most files (as singles).
    # A disk can save a file if and only if the file’s size is not larger than the free space on disk
    # (even if it’s already saved on the disk).
    # The list should be ordered by:
    # • Main sort by number of files in descending order.
    # • Secondary sort by disk's speed in descending order.
    # • Final sort by diskID in ascending order.

    return []


def getCloseFiles(fileID: int) -> List[int]:
    return []


if __name__ == '__main__':
    dropTables()
    createTables()
    addFile(File(12, "test", 10))
    getFileByID(12)
    clearTables()
    getFileByID(12)
