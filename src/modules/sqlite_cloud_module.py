# pip install sqlitecloud
import sqlitecloud
from pydantic_settings import BaseSettings, SettingsConfigDict
from yachalk import chalk #Print to STDOUT using colors
from datetime import datetime
import pathlib # Pathlib for file path manipulations
from typing import List, Dict, Optional, Union
import logging

class DatabaseConfig(BaseSettings):
    """
    Configurazione database che può essere inizializzata con file .env personalizzati.
    """
    VIES_PROD_DATABASE_URL: str
    VIES_PROD_DATABASE_APIKEY: str

    # Configurazione di default
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True
    )

    @classmethod
    def from_env_file(cls, env_file: Union[str, pathlib.Path]) -> 'DatabaseConfig':
        """
        Crea un'istanza DatabaseConfig da un file .env specifico.
        
        Args:
            env_file: Percorso al file .env da utilizzare
            
        Returns:
            Istanza configurata di DatabaseConfig
        """
        # Crea una classe dinamica con il file .env specificato
        class DynamicDatabaseConfig(cls):
            model_config = SettingsConfigDict(
                env_file=str(env_file),
                env_file_encoding="utf-8",
                case_sensitive=True
            )
        
        return DynamicDatabaseConfig()


def ensure_logging_configured() -> bool:
    """
    Verifica che il logging sia configurato, altrimenti configura un setup di base.
    Returns:
        bool: True se il logging è configurato, False se è stato configurato un setup di base.
    Raises:
        None
    """
    root_logger = logging.getLogger()
    
    # Se non ci sono handler configurati
    if not root_logger.handlers:
        # Configurazione di emergenza
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(filename)s:%(funcName)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler('sqlite_module.log')#,  # File di backup
                #logging.StreamHandler()  # Console
            ]
        )
        logger.warning("Logging non configurato dal programma principale. Usando configurazione di fallback.")
        return False
    return True

# Crea il logger per questo modulo
logger = logging.getLogger(__name__)
# Verifica la configurazione all'avvio del modulo
ensure_logging_configured()

def load_database_config(env_filepath: pathlib.Path, print_mode: str = "OFF") -> DatabaseConfig:
    """
    Carica la configurazione del database dal file .env specificato.
    
    Args:
        env_filepath: Percorso al file .env
        print_mode: "ON" per stampare i valori, "OFF" per non stamparli
        
    Returns:
        Istanza DatabaseConfig configurata
        
    Raises:
        ValueError: Se c'è un errore nel caricamento della configurazione
    """
    try:
        # Usa il metodo classmethod (RACCOMANDATO)
        dbconfig = DatabaseConfig.from_env_file(env_filepath)        
    except Exception as e:
        raise ValueError(f"**Error loading database configuration: {e}")
        logger.error(f"**Error loading database configuration: {e}")
    else:
        if print_mode == "ON":
            print(f"-> Database URL: {dbconfig.VIES_PROD_DATABASE_URL}")
            print(f"-> Database API Key: {dbconfig.VIES_PROD_DATABASE_APIKEY}")
    
    return dbconfig


def open_database(db_config, print_mode="OFF") -> sqlitecloud.Connection:
    """
    Open a connection to the SQLite Cloud database using the provided configuration.
    Args:
        db_config: An instance of DatabaseConfig containing the database URL and API key.
        print_mode: "ON" to print connection status, "OFF" to suppress output.        
    Returns:
        db_conn: A connection object to the SQLite Cloud database.
    """

    #connection_string = db_info["db_url"] + db_info["db_api_key"]
    connection_string = db_config.VIES_PROD_DATABASE_URL + db_config.VIES_PROD_DATABASE_APIKEY
    try:
        db_conn = sqlitecloud.connect(connection_string)
    # Handle errors        
    except sqlitecloud.Error as errMsg:
        logger.error(f"**Error occurred while opening database: {errMsg}")
        raise ValueError(f"**Error occurred while opening database: {errMsg}")
    # Check if the connection is success
    else:
        logger.debug(f"SQLite Cloud connection opened successfully: {db_conn}")
        if print_mode == "ON":
            print(chalk.yellow(f"-> SQLite Cloud connection opened successfully!"))
    return db_conn


def get_database_info(db_conn, print_mode="OFF") -> Dict[str, Optional[str]]:
    """
    Retrieves information about the SQLite Cloud database.
    Args:
        db_conn: The database connection object.
        print_mode: "ON" to print database info, "OFF" to suppress output.
    Returns:
        A dictionary containing the SQLite Cloud version.
    Raises:
        ValueError: If an error occurs while retrieving database information.
    """
    try:
        cursor = db_conn.cursor()
    except Exception as errMsg:
        logger.error(f"**Error occurred while getting database info: {errMsg}")
        raise ValueError(f"**Error occurred while getting database info: {errMsg}")
    else:
        # Get Sqlite Cloud database version
        cursor.execute("SELECT sqlite_version();")
        sqlite_version = cursor.fetchone()    
 
        if print_mode == "ON":
            print(chalk.yellow(f"-> SQLite Cloud version: {sqlite_version}"))
        logger.debug(f"SQLite Cloud version: {sqlite_version}")
        # Close the cursor      
        cursor.close()
        
        return {
            "sqlite_version": sqlite_version[0] if sqlite_version else None
        }


def execute_query(db_conn, query:str, values=None, print_mode="OFF") -> None:
    """
    Executes a SQL query on the SQLite Cloud database.
    Args:
        db_conn: The database connection object.
        query: The SQL query to execute.
        values: Optional tuple of values to bind to the query.
        print_mode: "ON" to print status messages, "OFF" to suppress output.
    Returns:
        None
    Raises:
        ValueError: If an error occurs while executing the query.
    """
    
    cursor = db_conn.cursor()
    try:
        if values == None:
            cursor.execute(query)
        else:    
            cursor.execute(query, values)
    # Handle errors           
    except Exception as errMsg:
        raise ValueError(f"**Error occurred executing query {query}: {errMsg}")
        logger.error(f"**Error occurred executing query {query}: {errMsg}")
    else:
        db_conn.commit()
        #logger.debug(f"Query executed successfully: {query}")
        logger.debug(f"Query executed successfully: {query}")
        if print_mode == "ON":        
            print(chalk.yellow(f"-> Query executed successfully: {query}"))            
    finally:
        cursor.close()


def insert_vies_record(db_conn, row, print_mode="OFF") -> dict:
    """
    Inserts a single VIES record into the database.
    Args:
        db_conn: The database connection object.
        row: A dictionary containing the VIES record data.
        print_mode: "ON" to print status messages, "OFF" to suppress output.
    Returns:
        A dictionary with the status and message of the operation.
    """
    f_return = dict()
    cursor = db_conn.cursor()
    query = """INSERT INTO partners (partner_id, partner_name, v_land, v_vatnr, v_cname, v_status, v_errmsg, v_reqdate, cpudate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
    cpudate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Create a list of tuples to insert
    values = (row['vat_country_code']+row["vat_number"], 
              row['vat_description'], 
              row['vies_country_code'], 
              row['vies_vatnr'], 
              row['vies_company_name'], 
              row['vies_status'], 
              row['vies_err_msg'], 
              row['vies_reqdate'], 
              cpudate)
    try:
        execute_query(db_conn, query, values)
    except Exception as errMsg:
        #raise ValueError(f"**Error occurred while inserting row {values}: {errMsg}")
        logger.error(f"**Error occurred while inserting row {values}: {errMsg}")
        if print_mode == "ON":
            print(chalk.bg_red(f"-> Error inserting row: {values}"))
        f_return["status"] = False
        f_return["message"] = str(errMsg)
        return f_return   
    else:
        db_conn.commit() 
        if print_mode == "ON":
            print(chalk.yellow(f"-> Row inserted successfully: \n{values}"))  
        logger.debug(f"Row inserted successfully: {values}")
    finally:
        cursor.close()
    
    f_return["status"] = True
    f_return["message"] = "Row inserted successfully"
    return f_return  


def insert_vies_records(db_conn, df_input, print_mode="OFF") -> dict:
    """
    Inserts multiple VIES records into the database from a DataFrame.
    Args:
        db_conn: The database connection object.
        df_input: A pandas DataFrame containing the VIES records to insert.
        print_mode: "ON" to print status messages, "OFF" to suppress output.
    Returns:
        A dictionary with the status and message of the operation.
    """

    cursor = db_conn.cursor()
    data = list()
    f_return = dict()
    
    for index, row in df_input.iterrows():
        # Create a list of tuples to insert

        t_rec = (
            row['in_ccode']+row["in_vatnr"], 
            row['in_pdesc'], 
            row['vies_ccode'], 
            row['vies_vatnr'], 
            row['vies_company_name'], 
            row['vies_status'], 
            row['vies_err_msg'], 
            row['vies_reqdate'],
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        data.append(t_rec)

    # Insert data
    sql_statement = """INSERT INTO partners (partner_id, partner_name, v_land, v_vatnr, v_cname, v_status, v_errmsg, v_reqdate, cpudate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """
    try:
        cursor.executemany(sql_statement, data)    
    except Exception as errMsg:
        #raise ValueError(f"**Error occurred while inserting row {values}: {errMsg}")
        logger.error(f"**Error occurred while inserting rows: {errMsg}")
        if print_mode == "ON":
            print(chalk.bg_red(f"-> Error inserting rows: {data}"))
        f_return["status"] = False
        f_return["message"] = str(errMsg)
        return f_return   
    else: 
        db_conn.commit()
        logger.debug(f"Rows inserted successfully: {data}")
        if print_mode == "ON":
            print(chalk.yellow(f"-> Row inserted successfully: \n{data}"))                 
    finally:
        cursor.close()
    
    f_return["status"] = True
    f_return["message"] = "Rows inserted successfully"
    return f_return  


def close_database(db_conn, print_mode="OFF") -> None:
    """
    Closes the SQLite Cloud database connection.
    Args:
        db_conn: The database connection object to close.
        print_mode: "ON" to print status messages, "OFF" to suppress output.
    Returns:
        None
    Raises:
        ValueError: If an error occurs while closing the database connection.
    """

    if db_conn:
        try:
            db_conn.close()
        # Handle errors        
        except sqlitecloud.Error as errMsg:
            #logger.error(f"**Error occurred while closing database connection {db_conn}: {errMsg}")
            raise ValueError(f"**Error occurred while closing database connection {db_conn}: {errMsg}")
        else:
            #logger.debug(f"SQLite Cloud connection closed successfully: {db_conn}")
            if print_mode == "ON":
                print(chalk.yellow(f"-> SQLite Cloud connection closed successfully: {db_conn}"))


def main():
    logger = logging.getLogger(__name__)
    # Load database configuration
    dbconfig = load_database_config(print_mode="ON")
    print(type(dbconfig))
    # Open database connecti
    #db_conn = open_database(dbconfig, print_mode="ON")
    
    #Close database connection
    #db_conn.close()

if __name__ == "__main__":
    main()


