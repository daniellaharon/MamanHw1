from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "CREATE TABLE File(file_id INTEGER PRIMARY KEY NOT NULL CHECK(file_id > 0), file_type TEXT NOT NULL, file_size INTEGER NOT NULL CHECK(file_size >= 0));"
                     "CREATE TABLE Disk(disk_id INTEGER PRIMARY KEY NOT NULL CHECK(disk_id > 0), disk_manufacturing_company TEXT NOT NULL, disk_speed INTEGER NOT NULL CHECK(disk_speed >= 0)), disk_free_space INTEGER NOT NULL CHECK(disk_free_space >= 0)), disk_cost_per_byte INTEGER NOT NULL CHECK(disk_cost_per_byte >= 0));"
                     "CREATE TABLE RAM(ram_id INTEGER PRIMARY KEY NOT NULL CHECK(ram_id > 0), ram_size INTEGER NOT NULL CHECK(ram_size >= 0)), ram_company TEXT NOT NULL);"
                     "COMMIT")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DELETE FROM File;"
                     "DELETE FROM Disk;"
                     "DELETE FROM RAM;"
                     "COMMIT")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DROP TABLE File;"
                     "DROP TABLE Disk;"
                     "DROP TABLE RAM;"
                     "COMMIT")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO File(file_id, file_type, file_size) VALUES({id}, {type}, {size})").format(id=sql.Literal(file.getFileID()),
                                                                                                                type=sql.Literal(file.getType(),
                                                                                                                size=file.getSize()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return Status.OK


def getFileByID(fileID: int) -> File:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM File WHERE File.file_id={id}".format(id=fileID))
        conn.commit()
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        if len(result.rows):
            return File(result.rows[0]["file_id"], result.rows[0]["file_type"], result.rows[0]["file_size"])
        return File.badFile()


def deleteFile(file: File) -> Status:
    return Status.OK


def addDisk(disk: Disk) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Disk(disk_id, disk_manufacturing_company, disk_speed ,disk_free_space, disk_cost_per_byte) VALUES({id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte})").format(id=sql.Literal(disk.getDiskID()),
                                                                                                                                                                                                                        manufacturing_company=sql.Literal(disk.getCompany(),
                                                                                                                                                                                                                        speed=disk.getSpeed(),
                                                                                                                                                                                                                        free_space=disk.setFreeSpace(),
                                                                                                                                                                                                                        cost_per_byte=disk.getCost()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return Status.OK


def getDiskByID(diskID: int) -> Disk:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Disk WHERE Disk.disk_id={id}".format(id=diskID))
        conn.commit()
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        if len(result.rows):
            return Disk(result.rows[0]["disk_id"], result.rows[0]["disk_manufacturing_company"], result.rows[0]["disk_speed"], result.rows[0]["disk_free_space"], result.rows[0]["disk_cost_per_byte"])
        return Disk.badDisk()


def deleteDisk(diskID: int) -> Status:
    return Status.OK


def addRAM(ram: RAM) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO File(ram_id, ram_size, ram_company) VALUES({id}, {type}, {size})").format(id=sql.Literal(ram.getRamID()),
                                                                                                                type=sql.Literal(ram.getSize(),
                                                                                                                size=ram.getCompany()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return Status.OK


def getRAMByID(ramID: int) -> RAM:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM File WHERE Ram.ram_id={id}".format(id=ramID))
        conn.commit()
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        if len(result.rows):
            return RAM(result.rows[0]["ram_id"], result.rows[0]["ram_size"], result.rows[0]["ram_company"])
        return RAM.badRAM()


def deleteRAM(ramID: int) -> Status:
    return Status.OK


def addDiskAndFile(disk: Disk, file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("BEGIN;"
                        "INSERT INTO File(id_file, type_file, size_file) VALUES({file_id}, {file_type}, {file_size});"
                        "INSERT INTO Disk(disk_id, disk_manufacturing_company, disk_speed ,disk_free_space, disk_cost_per_byte) VALUES({id_disk}, {manufacturing_company_disk}, {speed_disk}, {free_space_disk}, {cost_per_byte_disk});"
                        "COMMIT;").format(id_file=sql.Literal(file.getFileID()),
                                            type_file=sql.Literal(file.getType(),
                                            size_file=file.getSize(),
                                            id_disk=disk.getDiskID(),
                                            manufacturing_company_disk=disk.getCompany(),
                                            speed_disk=disk.getSpeed(),
                                            free_space_disk=disk.setFreeSpace(),
                                            cost_per_byte_disk=disk.getCost()))
        rows_effected, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return Status.OK


def addFileToDisk(file: File, diskID: int) -> Status:
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    return Status.OK


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def averageFileSizeOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
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
