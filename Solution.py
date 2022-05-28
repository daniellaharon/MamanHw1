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
        conn.execute("BEGIN;"
                     "TRUNCATE File;"
                     "TRUNCATE Disk;"
                     "TRUNCATE RAM;"
                     "TRUNCATE FilesInDisk;"
                     "TRUNCATE RAMInDisk;")
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
        query = sql.SQL("INSERT INTO File(file_id,file_type,file_size) "
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
            return File(result.rows[0]["file_id"], result.rows[0]["file_type"], result.rows[0]["file_size"])
        return File.badFile()


def deleteFile(file: File) -> Status:
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


# NOT DONE
def deleteDisk(diskID: int) -> Status:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Disk WHERE Disk.disk_id={id}").format(id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(query)
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


# NOT DONE
def deleteRAM(ramID: int) -> Status:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM RAM WHERE RAM.ram_id={id}").format(id=sql.Literal(ramID))
        rows_effected, _ = conn.execute(query)
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


def addFileToDisk(file: File, diskID: int) -> Status:
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    return Status.OK


# check return values here and also the correctness of the sql query
def addRAMToDisk(ramID: int, diskID: int) -> Status:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO RAMInDisk(disk_id, ram_id) "
            "(SELECT Disk.disk_id RAM.ram_id "
            "FROM Disk, RAM "
            "WHERE Disk.disk_id = {disk_id} AND RAM.ram_id = {ram_id});"

            "COMMIT").format(disk_id=sql.Literal(diskID),
                             ram_id=sql.Literal(ramID))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Status.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
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


# return values and corretness
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


# return values and corretness
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
                        "GROUP BY disk_id,ram_id").format(disk_id=sql.Literal(diskID)) # maybe add HAVING here
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


def getCostForType(type: str) -> int:
    return 0


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseFiles(fileID: int) -> List[int]:
    return []


if __name__ == '__main__':
    createTables()
    # dropTables()
    # addFile(File(12, "test", 10))
