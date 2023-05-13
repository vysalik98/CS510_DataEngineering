import time
import psycopg2
import argparse
import re
import csv

DBname = "storage"
DBuser = "postgres"
DBpwd = "8424"
TableName = "CensusData"
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = True  # indicates whether the DB table should be (re)-created


def initialize():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()

    global Datafile
    Datafile = args.datafile
    global CreateDB
    CreateDB = args.createtable


# read the input data file into a list of row strings
def readdata(fname):
    print(f"readdata: reading from File: {fname}")
    with open(fname, mode="r") as fil:
        dr = csv.DictReader(fil)

        rowlist = []
        for row in dr:
            rowlist.append(row)

    return rowlist


# convert list of data rows into list of SQL 'COPY ...' commands
def getSQLcmnds(rowlist):
    with open(Datafile, mode="r") as fil:
        dr = csv.reader(fil)
        next(dr)  # skip the header row
        with dbconnect() as conn:
            with conn.cursor() as cur:
                cur.copy_from(
                    fil,
                    TableName,
                    sep=",",
                    null="",
                    columns=[
                        "CensusTract",
                        "State",
                        "County",
                        "TotalPop",
                        "Men",
                        "Women",
                        "Hispanic",
                        "White",
                        "Black",
                        "Native",
                        "Asian",
                        "Pacific",
                        "Citizen",
                        "Income",
                        "IncomeErr",
                        "IncomePerCap",
                        "IncomePerCapErr",
                        "Poverty",
                        "ChildPoverty",
                        "Professional",
                        "Service",
                        "Office",
                        "Construction",
                        "Production",
                        "Drive",
                        "Carpool",
                        "Transit",
                        "Walk",
                        "OtherTransp",
                        "WorkAtHome",
                        "MeanCommute",
                        "Employed",
                        "PrivateWork",
                        "PublicWork",
                        "SelfEmployed",
                        "FamilyWork",
                        "Unemployment",
                    ],
                )
    return []


# connect to the database
def dbconnect():
    connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
    )
    connection.autocommit = True
    return connection


# create the target table
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
                        DROP TABLE IF EXISTS {TableName};
                        CREATE TABLE {TableName} (
                                CensusTract         NUMERIC,
                                State               TEXT,
                                County              TEXT,
                                TotalPop            INTEGER,
                                Men                 INTEGER,
                                Women               INTEGER,
                                Hispanic            DECIMAL,
                                White               DECIMAL,
                                Black               DECIMAL,
                                Native              DECIMAL,
                                Asian               DECIMAL,
                                Pacific             DECIMAL,
                                Citizen             INTEGER,
                                Income              INTEGER,
                                IncomeErr           INTEGER,
                                IncomePerCap        INTEGER,
                                IncomePerCapErr     INTEGER,
                                Poverty             DECIMAL,
                                ChildPoverty        DECIMAL,
                                Professional        DECIMAL,
                                Service             DECIMAL,
                                Office              DECIMAL,
                                Construction        DECIMAL,
                                Production          DECIMAL,
                                Drive               DECIMAL,
                                Carpool             DECIMAL,
                                Transit             DECIMAL,
                                Walk                DECIMAL,
                                OtherTransp         DECIMAL,
                                WorkAtHome          DECIMAL,
                                MeanCommute         DECIMAL,
                                Employed            INTEGER,
                                PrivateWork         DECIMAL,
                                PublicWork          DECIMAL,
                                SelfEmployed        DECIMAL,
                                FamilyWork          DECIMAL,
                                Unemployment        DECIMAL
                        );
                        ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
                        CREATE INDEX idx_{TableName}_State ON {TableName}(State);
                """
        )


def load(conn, filename):
    with conn.cursor() as cursor:
        print(f"Loading data from {filename}")
        start = time.perf_counter()

        with open(filename, "r") as f:
            reader = csv.reader(f, delimiter=",")
            next(reader)  # Skip the header row
            cursor.copy_from(f, TableName, sep=",")

        conn.commit()

        elapsed = time.perf_counter() - start
        print(f"Finished Loading. Elapsed Time: {elapsed:0.4} seconds")


def main():
    initialize()
    conn = dbconnect()
    rlis = readdata(Datafile)
    cmdlist = getSQLcmnds(rlis)

    if CreateDB:
        createTable(conn)

    load(conn, Datafile)


if __name__ == "__main__":
    main()
