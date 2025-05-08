import random
import shutil
import csv
from datetime import datetime, timedelta
import os
import duckdb


class Process():
    def __init__(self):
        self.FolderForTheoreticalFiles = "FolderForTheoreticalFiles"
        self.DatePartitionedFiles = "DatePartitionedFiles"
        self.DatabaseName = "local.duckdb"
        self.ticker_ids = [f"stk_{n:03}" for n in range(1, 201)]

    def create_tables(self):

        column_defs_price = ', '.join(f"{ticker} DECIMAL" for ticker in self.ticker_ids)
        column_defs_volume = ', '.join(f"{ticker} INT" for ticker in self.ticker_ids)

        with duckdb.connect(self.DatabaseName) as conn:
            conn.execute(f"""
        DROP TABLE IF EXISTS prices;

        CREATE TABLE prices (
        date DATE,
        {column_defs_price}  
        );
        """)
            conn.execute(f"""
            
            DROP TABLE IF EXISTS volume;
            CREATE TABLE volume (
                                        
                                        date DATE,
                                       {column_defs_volume});
                        """)

    def _GenerateTestFilesWithIncompleteData(self):
        headers = ["date", "id", "price", "trade_volume"]
        start_date = datetime.strptime("2025-01-01", "%Y-%m-%d")
        num_days = 30
        date_list = [(start_date + timedelta(days=i)).date() for i in range(num_days)]
        stock_ids = [f"stk_{x:03}" for x in range(10)]
        print(stock_ids)
        for date in date_list:
            for stock_id in stock_ids:
                random_file_choice = f"{self.FolderForTheoreticalFiles}/{random.randint(1, 10)}.csv"
                random_volume = f"{random.randint(100, 9999)}"
                random_price = f"{round(random.uniform(1.00, 300.00), 2)}"

                file_exists = os.path.exists(random_file_choice)
                with open(f"{random_file_choice}", "a") as file:
                    writer = csv.DictWriter(file, headers)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow({"date": date, "id": stock_id, "price": random_price, "trade_volume": random_volume})

    def SetupTestEnvironment(self) -> None:
        """This function holds all the logic to set up needed folders
            and then fills test data in csv format
            and also creates "production" tables
        """
        shutil.rmtree(self.FolderForTheoreticalFiles, ignore_errors=True)
        shutil.rmtree(self.DatePartitionedFiles, ignore_errors=True)
        os.makedirs(self.FolderForTheoreticalFiles)
        os.makedirs(self.DatePartitionedFiles)

        self._GenerateTestFilesWithIncompleteData()
        self.create_tables()

    def _GetBaseDataStructure(self) -> dict:
        """
        :return: {'stk_001': None, 'stk_002': None, 'stk_003': None, 'stk_004': None,
        'stk_005': None, 'stk_006': None, 'stk_007': None, 'stk_008': None,
        'stk_009': None, 'stk_010': None, 'stk_011': None,..
        }
        """
        BaseDataStructure = {}
        for n in range(1, 201):
            BaseDataStructure.setdefault(f"stk_{n:03}", None)
        return BaseDataStructure

    def PartitionFilesByDate(self) -> None:
        """
        This convoluted function reads the theoretical 100 gb files split into 10 files
        then partitions them by new files name by date.
        :return:
        """
        for dirpath, dirnames, filenames in os.walk(self.FolderForTheoreticalFiles):
            for filename in filenames:
                with (open(f"{self.FolderForTheoreticalFiles}/{filename}", "r") as file):
                    reader = csv.DictReader(file)
                    for line in reader:
                        partitioned_file_name = f"{self.DatePartitionedFiles}/{line['date']}.csv"
                        partioned_file_exists = os.path.exists(partitioned_file_name)
                        partitioned_file_headers = ["date", "id", "trade_volume", "price"]
                        with open(partitioned_file_name, "a") as partitioned_file:
                            partitioned_file_writer = csv.DictWriter(partitioned_file, fieldnames=partitioned_file_headers)
                            if not partioned_file_exists:
                                partitioned_file_writer.writeheader()
                            partitioned_file_writer.writerow({"date": line["date"],
                                                              "id": line["id"],
                                                              "price": line["price"],
                                                              "trade_volume": line["trade_volume"]})

    def create_temp_tables(self, conn):
        """
        This private function takes a duckdb connection
        and then creates 2 temp tables representing volume ata, and price data
        :param conn:
        """
        conn.execute("""
                        DROP TABLE IF EXISTS temp_prices;
                         CREATE TEMP TABLE temp_prices (
                             id TEXT,
                             date DATE,
                             price DECIMAL);


                             DROP TABLE IF EXISTS temp_volume;
                             CREATE TEMP TABLE temp_volume (
                                                         id TEXT,
                                                         date DATE,
                                                         trade_volume INTEGER);
                         """

                     )

    def ETLPartitionedFiles(self) -> None:
        """
        read through each partitioned file
        then will be inserted into temp tables
        a special pivot query will be run against said tables
        and inserted into "production" tables
        """
        volume_data = []
        price_data = []
        for dirpath, dirnames, filenames in os.walk(self.FolderForTheoreticalFiles):
            for filename in filenames:
                with (open(f"{self.FolderForTheoreticalFiles}/{filename}", "r") as file):
                    reader = csv.DictReader(file)
                    for line in reader:
                        volume_data.append((line["date"], line["id"], line["trade_volume"]))
                        price_data.append((line["date"], line["id"], line["price"]))
                    with duckdb.connect(self.DatabaseName) as conn:
                        self.create_temp_tables(conn)
                        conn.executemany("INSERT INTO temp_volume ( date,id, trade_volume) VALUES (?, ?, ?)", volume_data)
                        conn.executemany("INSERT INTO temp_prices (date,id,price) VALUES (?, ?, ?)", price_data)
                        columns = ",".join(self.ticker_ids)
                        conn.execute(f"""
                        BEGIN;
                                INSERT INTO prices
                                SELECT *
                                FROM (
                                    SELECT date, id, price
                                    FROM temp_prices
                                )
                                PIVOT (
                                    MAX(price) FOR id IN (
                                       {columns}
                                    )
                                )
                                ;
                                
                                INSERT INTO volume
                                SELECT *
                                FROM (
                                    SELECT date, id, trade_volume
                                    FROM temp_volume
                                )
                                PIVOT (
                                    MAX(trade_volume) FOR id IN (
                                       {columns}
                                    )
                                )
                                ;
                    COMMIT;
                    """)

    def run(self):
        self.SetupTestEnvironment()
        self.PartitionFilesByDate()
        self.ETLPartitionedFiles()


if __name__ == "__main__":
    p = Process()
    p.run()
