# Built-in modules
import sys
import time
import datetime
from typing import List, Dict
from datetime import date # Date for date manipulations
import pathlib # Pathlib for file path manipulations
import os # OS for file path manipulations
import argparse #Parse script arguments
import csv # CSV for reading CSV files
import logging #Log at debug level
from typing import List, Dict, Optional, Union
from enum import Enum
import configparser # ConfigParser for reading configuration files

# Thrird party libraries
from pydantic import BaseModel, Field, ValidationError 
from zeep import Client # SOAP client library
from yachalk import chalk #Print to STDOUT using colors
import fitz  # Importa PyMuPDF
import pandas as pd # DataFrame for data manipulations
from tqdm import tqdm # Progress bar for long operations --> Alternative: alive-progress
from playsound3 import playsound # Play sound


# Other modules
import modules.sqlite_cloud_module
import modules.reportlab_module

#GLOBAL APP CONSTANTS
RECORD_SEPARATOR = '*' * 100
STANDARD_RECORD_SLEEP_TIME = 5 #sleep 5 seconds every record
LONG_RECORD_SLEEP_TIME = 10 #sleep 10 seconds every record
BATCH_SIZE = 100 #size of batch
BATCH_SLEEP_TIME = 30 #sleep 30 seconds every batch

# Module constants
__author__ = "Riccardo Baravelli"
__version__ = "1.0"

# Definiamo un'enumerazione per i codici paese consentiti
class EUContryCode(str, Enum):
    AUSTRIA = "AT"
    BELGIO = "BE"
    BULGARIA = "BG"
    CIPRO = "CY"
    REP_CECA = "CZ"
    GERMANIA = "DE"
    DANIMARCA = "DK"
    ESTONIA = "EE"
    GRECA = "EL"
    SPAGNA = "ES"
    FINLANDIA = "FI"
    FRANCIA = "FR"
    CROAZIA = "HR"
    UNGHERIA = "HU"
    IRLANDA = "IE"
    ITALIA = "IT"
    LITUANIA = "LT"
    LUSSEMBURGO = "LU"
    LETTONIA = "LV"
    MALTA = "MT"
    OLANDA = "NL"
    POLONIA = "PL"
    PORTOGALLO = "PT"
    ROMANIA = "RO"
    SVEZIA = "SE"
    SLOVENIA = "SI"
    SLOVACCHIA = "SK"
    REGNO_UNITO = "XI"

# Definiamo il modello Pydantic per la validazione
class Partner(BaseModel):
    partner_description: str = Field(min_length=1, frozen=True)
    country_code: EUContryCode
    vat_nr: str = Field(..., max_length=12)


def setup_logging(log_filepath: str, log_filemode: str, log_level: str) -> None:
    """ Function that sets up the logging configuration """
    
    # Converte la stringa del livello in costante logging
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    # Ottiene il livello numerico, default a INFO se non riconosciuto
    numeric_level = level_map.get(log_level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=numeric_level,  # <-- Usa il parametro invece di logging.INFO fisso
        format='%(asctime)s - {%(filename)s}:{%(funcName)s} - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',  # <-- Formato data/ora personalizzato
        handlers=[
            logging.FileHandler(log_filepath, log_filemode)#,
            #logging.StreamHandler()
        ]
    )


def check_file_existance(path: pathlib) -> bool:
    """ Function tha returns True if a input string is a file and the file exists """    
    if os.path.exists(path) and os.path.isfile(path):
        return True
    else:
        return False    


def check_folder_existance(path: pathlib) -> bool:
    """ Function tha returns True if a input string is a folder and the folder exists """   
    if os.path.exists(path) and os.path.isdir(path):
        return True
    else:
        return False  


def decode_program_parameters() -> argparse.Namespace:
    """Function that decodes CLI parameters""" 
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                    usage='%(prog)s -c <config_file>',
                                    description='Python client that calls VIES SOAP Web Service to check vat number validity.',
                                    allow_abbrev=False,
                                    epilog='Enjoy the program!')
    
    # Default config file path
    #config_default_filepath = os.path.join(pathlib.Path(sys.argv[0]).parent.parent, 'app_config.ini')
    
    # Config file as optional parameter with -c flag
    parser.add_argument('-c', '--config',
                    action='store', 
                    required=True,
                    dest='config_file',
                    type=str, 
                    help='App configuration filepath (required)')   
   
    args = parser.parse_args()    
    return args


def read_app_config(config_path: Optional[Union[str, pathlib.Path]]) -> Dict:
    """ Function that reads the application configuration file and returns a dictionary with the values """
    # Create a ConfigParser object
    config = configparser.ConfigParser()
    # Set default configuration path if not provided
    if config_path is None:
        script_parent = pathlib.Path(sys.argv[0]).parent
        config_path = os.path.join(script_parent, 'app_config.ini')
    
    # Check if the configuration file exists
    if check_file_existance(config_path) == False:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    # Read the configuration file
    try:
        # Read the configuration file
        config.read(config_path)
              
    except configparser.Error as e:
        #raise configparser.Error(f"Error parsing configuration file {config_path}: {e}")
        print(f"{chalk.bg_red('Error: configuration file not found!')}: {config_path}")
        config_values = {}
        config_return = {
            "status": False,
            "message": f"Configuration file not found: {config_path}"
        }
    else:
        # Access values from the configuration file
        try:
            vies_checkvat_endpoint = config.get('VIES_ENDPOINTS', 'VIES_CHECK_VAT_SERVICE_ENDPOINT')
        except Exception as errMsg:
            print(chalk.red_bright(f'Error: VIES_CHECK_VAT_SERVICE_ENDPOINT not found!'))
            sys.exit(-255)
        try:
            vies_status_endpoint = config.get('VIES_ENDPOINTS', 'VIES_STATUS_SERVICE_ENDPOINT')
        except Exception as ErrMsg:
            print(chalk.red_bright(f'Error: VIES_STATUS_SERVICE_ENDPOINT not found!'))
            sys.exit(-255) 
        database_store_active = config.getboolean('DATABASE','database_store_active', fallback=False)
        env_filepath_default = os.path.join(pathlib.Path(sys.argv[0]).parent, '.env')
        env_filepath = config.get('DATABASE','env_filepath', fallback=env_filepath_default)
        
        data_source_dir_default = os.path.join(pathlib.Path(sys.argv[0]).parent, 'data')
        data_source_dir = config.get('APPLICATION', 'data_source_dir', fallback=data_source_dir_default)
        
        data_dest_dir_default = os.path.join(pathlib.Path(sys.argv[0]).parent, 'data')        
        data_dest_dir = config.get('APPLICATION', 'data_dest_dir', fallback=data_dest_dir_default)
        
        sound_active_default = False
        sound_active = config.getboolean('SOUND', 'sound_active',fallback=sound_active_default)
        
        sound_filepath_default = os.path.join(pathlib.Path(sys.argv[0]).parent, 'sounds', 'success.wav')
        sound_filepath = config.get('SOUND', 'sound_filepath', fallback=sound_filepath_default)
        
        log_mode = config.get('LOGGING', 'log_level', fallback='INFO')
        log_filepath_default = os.path.join(pathlib.Path(sys.argv[0]), sys.argv[0].replace('.py', '.log'))
        log_filepath = config.get('LOGGING','log_filepath', fallback=log_filepath_default)
        
        # Return a dictionary with the retrieved values
        config_values = {
        'vies_checkvat_endpoint': vies_checkvat_endpoint,
        'vies_status_endpoint': vies_status_endpoint,
        'data_source_dir': pathlib.Path(data_source_dir),
        'data_dest_dir': pathlib.Path(data_dest_dir),
        'sound_active': sound_active,
        'sound_filepath': pathlib.Path(sound_filepath),
        'log_mode': log_mode,
        'log_filepath': pathlib.Path(log_filepath),
        'database_store_active': database_store_active,
        'env_filepath': pathlib.Path(env_filepath)
        }
        config_return = {
            "status": True,
            "message": ""
        }

        return {'config_values': config_values, 'config_return': config_return}        


def vow_check_status_service(wsdl_endpoint) -> Dict:
    """ Function that checks the VIES service status """

    logger = logging.getLogger("vies_check_vat")
    f_return = dict()
    try:
        client = Client(wsdl=wsdl_endpoint)
    except Exception as e:
        #print(f"{chalk.bg_red('VIES service STATUS unreachable!')}: {str(e)}")
        logger.error(f"VIES service STATUS unreachable!")
        f_return["status"] = False
        f_return["message"] = str(e)
        return f_return
    try:
        result = client.service.checkStatus()
        vow_status = result['vow']['available']
        if vow_status == True:
            print(f"Vies on the Web status: {chalk.bg_green('AVAILABLE')}")
        else:    
            print(f"Vies on the Web status: {chalk.bg_red('UNAVAILABLE')}")
            logger.error(f"VIES service STATUS UNAVAILABLE!")

        member_elements = result['memberStates']['memberState']
        for element in member_elements:
            if element['availability'] == "Available":
                print(f"Member State: {element['countryCode']} --> {chalk.green_bright('AVAILABLE')}")
            else:
                print(f"Member State: {element['countryCode']} --> {chalk.red_bright('UNAVAILABLE')}")
    except Exception as e:
        print(f"{chalk.bg_red('Error checking VIES status!')}: {str(e)}")
        logger.error(f"Error checking VIES status: {str(e)}")
        f_return["status"] = False
        f_return["message"] = str(e)
        return f_return       

    f_return["status"] = True
    f_return["message"] = ""
    return f_return


def vow_check_vat_validity_service(wsdl_endpoint: str, vat_country_code: str, vat_number: str, vat_description="") -> Dict[str, Dict]:
    """ Function that checks the VIES service for VAT validity """

    chk_vat_info = dict()
    chk_vat_return = dict()
    logger = logging.getLogger("vies_check_vat")
    
    try:
        client = Client(wsdl=wsdl_endpoint)
    except Exception as errMsg:
        print(f"{chalk.bg_red('VIES service CHECK_VAT unreachable!')}: {str(errMsg)}")
        logger.error(f"VIES service CHECK_VAT unreachable: {str(errMsg)}")
        chk_vat_return["status"] = False
        errMsg = 'VIES service CHECK_VAT unreachable!'
        chk_vat_return["message"] = str(errMsg)
        return {"vies_vat_info": chk_vat_info, "vies_vat_return": chk_vat_return}
    else:
        try:
            result = client.service.checkVat(vat_country_code, vat_number)

        except Exception as errMsg:
            logger = logger.error("Error checking VAT validity: %s", str(errMsg))
            today = date.today()
            chk_vat_info["vies_reqdate"] = today.strftime("%Y-%m-%d")
            chk_vat_info["vies_err_msg"] = str(errMsg)
            chk_vat_info["vies_ccode"] = ""
            chk_vat_info["vies_vatnr"] = ""
            chk_vat_info["vies_company_name"] = ""
            chk_vat_info["vies_company_address"] = ""
            chk_vat_info["vies_status"] = ""
            chk_vat_info["vat_description"] = vat_description
            chk_vat_info["vat_country_code"] = vat_country_code
            chk_vat_info["vat_number"] = vat_number
            chk_vat_return["status"] = True
            chk_vat_return["message"] = ""
            return {"vies_vat_info": chk_vat_info, "vies_vat_return": chk_vat_return}

        else:
            chk_vat_info["vies_reqdate"] = result.requestDate
            chk_vat_info["vies_err_msg"] = ""
            chk_vat_info["vies_ccode"] = result.countryCode
            chk_vat_info["vies_vatnr"] = result.vatNumber
            chk_vat_info["vies_company_name"] = result.name
            chk_vat_info["vies_company_address"] = result.address        
            chk_vat_info["vies_status"] = result.valid
            chk_vat_info["vat_description"] = vat_description
            chk_vat_info["vat_country_code"] = vat_country_code
            chk_vat_info["vat_number"] = vat_number
            chk_vat_return["status"] = True
            chk_vat_return["message"] = ""
            return {"vies_vat_info": chk_vat_info, "vies_vat_return": chk_vat_return}

def vies_check_vat_service(wsdl_endpoint: str, df_in: pd.DataFrame, sleep_time: int) -> pd.DataFrame:
    """ Function that checks the VIES service for VAT validity """

    logger = logging.getLogger("vies_check_vat")

    # Check if the input DataFrame is empty
    if df_in.empty:
        print(chalk.red_bright("Input DataFrame is empty. No records to process."))
        return df_in

    # Fase 1: Raggruppo per paese e assegno un indice progressivo a ciascun record di ogni paese
    df_in['gruppo_idx'] = df_in.groupby('in_ccode').cumcount()

    # Fase 2: Ordino prima per l'indice di gruppo e poi per il paese 
    # (in caso di parità nell'indice di gruppo)
    df_distribuito = df_in.sort_values(['gruppo_idx', 'in_ccode']).reset_index(drop=True)

    # Fase3: Rimuovo la colonna di appoggio
    df_distribuito = df_distribuito.drop('gruppo_idx', axis=1)
    
    # Utilizzo tqdm con un iteratore
    total_records = len(df_in)
    # Ciclo per ogni riga del dataframe
    # Creo la barra di avanzamento
    with tqdm(total=total_records, desc="Elabororation...", colour='CYAN') as pbar:
        for index, row in df_distribuito.iterrows():
            print(f"[{index+1}] Check validity: {row['in_ccode']}{row['in_vatnr']}")
            vies_rec = vow_check_vat_validity_service(wsdl_endpoint, row['in_ccode'], row['in_vatnr'], row['in_pdesc'])
            if vies_rec["vies_vat_return"]["status"] == False:
                print(f'Error processing row {index}: {vies_rec["vies_vat_return"]["message"]}')
                df_in.at[index, 'vies_err_msg'] = vies_rec["vies_vat_return"]["message"]
            else:
                # Update dataram columns with VIES data
                df_distribuito.at[index, 'vies_ccode'] = vies_rec["vies_vat_info"]["vies_ccode"]
                df_distribuito.at[index, 'vies_vatnr'] = vies_rec["vies_vat_info"]["vies_vatnr"] 
                df_distribuito.at[index, 'vies_company_name'] = vies_rec["vies_vat_info"]["vies_company_name"]
                df_distribuito.at[index, 'vies_reqdate'] = vies_rec["vies_vat_info"]["vies_reqdate"]
                df_distribuito.at[index, 'vies_err_msg'] = vies_rec["vies_vat_info"]["vies_err_msg"]   

                if vies_rec["vies_vat_info"]["vies_status"] == True:
                    df_distribuito.at[index, 'vies_status'] = 'VALID'
                    print(f"-> Status: {chalk.green_bright(df_distribuito.at[index, 'vies_status'])}")
                else:
                    df_distribuito.at[index, 'vies_status'] = 'INVALID'
                    print(f"-> Status: {chalk.red_bright(df_distribuito.at[index, 'vies_status'])}")
                    print(f"-> Error message: {chalk.red_bright(df_distribuito.at[index, 'vies_err_msg'])}")    

                # Attesa per evitare di sovraccaricare l'API (buona pratica)
                time.sleep(sleep_time)
                pbar.update(1)

    # Fase 4: Ordino nuovamente il dataframe di output per LINE_NR
    df_out = df_distribuito.sort_values(['file_input', 'line_nr'], ascending=[True, True]).reset_index(drop=True)
    return df_out

def display_vies_check_result(vat_result: dict, record_index: int) -> None:
    """ Function that displays the VIES check result """
    print()
    print(RECORD_SEPARATOR)
    print(f"Record: {record_index}")    
    print(f"Partner Id: {chalk.yellow_bright(vat_result['in_ccode'])}{chalk.yellow_bright(vat_result['in_vatnr'])}")
    print(f"Partner Name: {vat_result['in_pdesc']}")
    if vat_result["vies_status"]== True:
        print(f"[VIES] Status: {chalk.green_bright('VALID')}")
    elif vat_result["vies_status"]== False:
        print(f"[VIES] Status: {chalk.red_bright('INVALID')}")
    else:
        print(f"[VIES] Status: {vat_result['vies_status']}")

    print(f"[VIES] Country code: {vat_result['vies_ccode']}")
    print(f"[VIES] VAT nr: {vat_result['vies_vatnr']}")    
    print(f"[VIES] Company name: {vat_result['vies_company_name']}")
    print(f"[VIES] Company address: {vat_result['vies_company_address']}")
    print(f"[VIES] Error Message: {chalk.red_bright(vat_result['vies_err_msg'])}")    
    print(f"[VIES] Request date: {vat_result['vies_reqdate']}")


def create_pdf_report(output_pdf_name, vat_records_list):
    """
    Creates a PDF file containing the data from a list of VAT dictionaries,
    with colored output for the 'validy_check' field.

    Args:
        output_pdf_name (str): The name of the PDF file to create.
        vat_records_list (list): A list of dictionaries containing the VAT data.
    """
    document = fitz.open()  # Create a new PDF document

    # --- 1. Create and populate the cover page ---
    cover_page = document.new_page()  # Add a new page
    page_width = cover_page.rect.width
    page_height = cover_page.rect.height

    # Define fonts
    normal_font = "Helvetica"
    bold_font = "Helvetica-Bold"

    x_margin = 50
    y_margin = 50
    line_spacing = 10
    current_y_cover = y_margin

    # Main Title with underline
    main_title = "VIES Vat Report"
    title_font_size = 24
    cover_page.insert_text(
        (x_margin, current_y_cover),
        main_title,
        fontsize=title_font_size,
        color=fitz.pdfcolor["navy"]
    )
    
    # Add underline for the title
    title_width = fitz.get_text_length(main_title, fontname=normal_font, fontsize=title_font_size)
    underline_y = current_y_cover + 5  # Position slightly below the text
    cover_page.draw_line(
        (x_margin, underline_y),
        (x_margin + title_width, underline_y),
        color=fitz.pdfcolor["navy"],
        width=1.5
    )
    current_y_cover += title_font_size + line_spacing * 3  # Space after the main title

    # Set the starting Y for metadata block
    current_y_metadata_block_start = current_y_cover + line_spacing * 3

    # --- Report Metadata - Aligned Labels and Values ---
    metadata_font_size = 12
    metadata_x_start = x_margin  # Start of the label column
    gap_between_columns = 10  # Space between the label column and the value column
    line_vertical_padding = 5  # Padding between lines

    # Metadata content
    metadata_lines = [
        (f"Program:", os.path.basename(__file__)),
        (f"Input File:", os.path.basename(output_pdf_name).replace(".pdf", ".csv")),
        (f"Output File:", os.path.basename(output_pdf_name)),
        (f"Generation Date:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        (f"Number of Records:", str(len(vat_records_list)))
    ]

    # --- STEP 1: Calculate the maximum width of labels ---
    max_label_width = 0
    for label, _ in metadata_lines:
        label_width = fitz.get_text_length(
            label,
            fontname=bold_font,  # Use bold font for calculation
            fontsize=metadata_font_size
        )
        max_label_width = max(max_label_width, label_width)

    # --- STEP 2: Calculate the FIXED starting point for the value column ---
    value_column_x_start = metadata_x_start + max_label_width + gap_between_columns

    # Initialize Y position for the first metadata line ONLY ONCE
    current_y_metadata_line = current_y_metadata_block_start

    for label, value in metadata_lines:
        # 1. Insert the Label (Bold)
        cover_page.insert_text(
            (metadata_x_start, current_y_metadata_line),
            label,
            fontsize=metadata_font_size,
            color=fitz.pdfcolor["black"],
            fontname=bold_font
        )

        # 2. Insert the Value (Normal)
        cover_page.insert_text(
            (value_column_x_start, current_y_metadata_line),
            str(value),
            fontsize=metadata_font_size,
            color=fitz.pdfcolor["black"],
            fontname=normal_font
        )

        # Move to the next line
        current_y_metadata_line += metadata_font_size + line_vertical_padding

    # --- 2. Create and populate content pages with VAT records ---
    content_page_initial_y = y_margin  # Starting Y for content on subsequent pages
    content_x_margin = 50
    record_spacing = 25
    line_spacing = 15

    current_page = document.new_page()  # Add the first content page
    current_y = content_page_initial_y

    normal_font_size = 10

    # Keys in the desired order for printing
    ordered_keys = ["in_ccode", "in_vatnr", "in_pdesc", "vies_ccode", "vies_vatnr", "vies_status", "vies_err_msg", "vies_rqt_date"]

    # Define colors
    green_color = (0, 128/255, 0)  # Green for valid
    red_color = (1, 0, 0)          # Red for invalid
    black_color = (0, 0, 0)        # Black for regular text

    # Define display format for each key
    key_display_format = {
        "vies_ccode": {"prefix": "[VIES] Country code: ", "color": black_color},
        "vies_vatnr": {"prefix": "[VIES] Vat nr: ", "color": black_color},
        "vies_status": {"prefix": "[VIES] Validity Check: ", "color": None},  # Special case, color depends on value
        "vies_err_msg": {"prefix": "[VIES] Error message: ", "color": None},  # Special case, color depends on value
        "vies_rqt_date": {"prefix": "[VIES] Request date: ", "color": black_color},
        # Default case will be handled in the loop
    }

    for i, data_dict in enumerate(vat_records_list):
        # Add a subtitle for each record
        subtitle = f"Record {i + 1}"
        subtitle_font_size = 12
        
        # Calculate space needed for the subtitle and all lines of the current record
        space_needed_for_record_block = subtitle_font_size + line_spacing * len(ordered_keys) + record_spacing
        
        # Check if we need a new page for this record
        if current_y + space_needed_for_record_block > current_page.rect.height - y_margin:
            current_page = document.new_page()  # Add a new page
            current_y = content_page_initial_y  # Reset vertical position
            
        # Insert the record subtitle
        current_page.insert_text(
            (content_x_margin, current_y),
            subtitle,
            fontsize=subtitle_font_size,
            color=fitz.pdfcolor["blue"]
        )
        current_y += subtitle_font_size  # Space after subtitle (senza riga vuota)

        # Iterate over the specific keys for each dictionary in the list
        for key in ordered_keys:
            if key in data_dict:
                value = data_dict[key]
                
                # Get display format for this key (or use default)
                format_info = key_display_format.get(key, {"prefix": f"{key.replace('_', ' ').title()}: ", "color": black_color})
                prefix = format_info["prefix"]
                text_color = format_info["color"]
                
                # Handle special cases
                if key == "vies_status":
                    if value is True:  # Valid
                        display_text = prefix + "VALID"
                        text_color = green_color
                    else:  # Invalid
                        display_text = prefix + "INVALID"
                        text_color = red_color
                elif key == "vies_err_msg":
                    display_text = prefix + str(value)
                    text_color = red_color if value else black_color
                else:
                    # Standard case
                    display_text = prefix + str(value)
                
                # Check if there's space on the current page for this line
                if current_y + line_spacing > current_page.rect.height - y_margin:
                    current_page = document.new_page()  # Add a new page
                    current_y = content_page_initial_y  # Reset vertical position
                
                # Insert the text with appropriate color
                current_page.insert_text(
                    (content_x_margin, current_y),
                    display_text,
                    fontsize=normal_font_size,
                    color=text_color
                )
                
                current_y += line_spacing  # Move down for the next line
            else:
                print(f"Warning (Record {i+1}): key '{key}' not found in the provided dictionary.")
        
        # Add separator line after EACH record (including the last one)
        separator_width = page_width - 2 * content_x_margin

        # Check if there's space for the separator line
        if current_y + 5 > current_page.rect.height - y_margin:
            current_page = document.new_page()
            current_y = content_page_initial_y
        else:
            # Draw separator line
            current_page.draw_line(
                (content_x_margin, current_y),
                (content_x_margin + separator_width, current_y),
                color=black_color,
                width=1.0
            )
            
            # Add adequate space after the separator line
            current_y += 20  # Aumentato da 10 a 20 per maggiore spaziatura

        # If adding spacing pushes to next page, add a new page
        if current_y > current_page.rect.height - y_margin:
            current_page = document.new_page()
            current_y = content_page_initial_y
                
            # If adding minimal spacing pushes to next page, add a new page
            if current_y > current_page.rect.height - y_margin:
                current_page = document.new_page()
                current_y = content_page_initial_y
            
            # If adding record spacing pushes to next page, add a new page
            if current_y > current_page.rect.height - y_margin:
                current_page = document.new_page()
                current_y = content_page_initial_y

    # --- 3. Add page numbers to every page ---
    total_pages = document.page_count
    page_number_font_size = 8
    page_number_y = page_height - y_margin + page_number_font_size  # Position near bottom margin

    for page_num in range(total_pages):
        page = document.load_page(page_num)
        page_number_text = f"Pagina {page_num + 1} di {total_pages}"
        
        # Calculate text width for right alignment
        text_width = fitz.get_text_length(
            page_number_text,
            fontname=normal_font,
            fontsize=page_number_font_size
        )
        
        # Position text right-aligned
        page_number_x = page_width - x_margin - text_width
        
        page.insert_text(
            (page_number_x, page_number_y),
            page_number_text,
            fontsize=page_number_font_size,
            color=fitz.pdfcolor["gray"]
        )

    # --- 4. Save and close the document ---
    document.save(output_pdf_name)
    document.close()


def print_prg_header(opt_msg: str = '') -> None:
    """ Function that prints program header to STDOUT"""  
    STDOUT_SEP_LEN = 120
    STDOUT_SEP_CHR = '='  
    print()
    print(chalk.blue_bright(STDOUT_SEP_LEN * STDOUT_SEP_CHR))
    print(chalk.blue_bright('Running Python script:', os.path.basename(__file__), ' - Version: ', repr(__version__)))
    print(chalk.blue_bright('Author:', repr(__author__)))
    curr_time = time.localtime()
    print(chalk.blue_bright('Start time: ', time.strftime('%Y-%m-%d %H:%M:%S', curr_time)))
    if opt_msg != '':
        print(chalk.blue_bright(opt_msg))    
    print(chalk.blue_bright(STDOUT_SEP_LEN * STDOUT_SEP_CHR))
    print()


def print_prg_footer(opt_msg: str = '') -> None:
    """Function that prints program footer to STDOUT"""
    STDOUT_SEP_LEN = 120
    STDOUT_SEP_CHR = '='
    print()
    print(chalk.blue_bright(STDOUT_SEP_LEN * STDOUT_SEP_CHR))
    curr_time = time.localtime()
    print(chalk.blue_bright('End time  : ', time.strftime('%Y-%m-%d %H:%M:%S', curr_time)))
    if opt_msg != '':
        print(chalk.blue_bright(opt_msg))
    print(chalk.blue_bright(STDOUT_SEP_LEN * STDOUT_SEP_CHR))


def validate_csv_line(row_index: int, row: list) -> Dict:
    funct_return = dict()
    if len(row) != 3:
        errMsg = f"**Error line {row_index}: number of column {len(row)} is wrong!"
        funct_return["status"] = False
        funct_return["message"] = str(errMsg)
        return funct_return

    try:
        partner = Partner(
            partner_description=row[0],
            country_code=row[1],
            vat_nr=row[2]
        )
    except ValidationError as errMsg:
        msg = f"**Error line {row_index}:"
        funct_return["status"] = False
        funct_return["message"] = msg + str(errMsg)
        return funct_return
    else:
        funct_return["status"] = True
        funct_return["message"] = ""
        return funct_return
    
def main() -> None:
    """ Main function """
 
    # Start time counter
    tic = time.perf_counter()

    # Decode CLI parameters: 
    cli_parameters = list()
    cli_parameters = decode_program_parameters()
    p_program_name = pathlib.Path(os.path.abspath(sys.argv[0]))
    p_config_filepath = pathlib.Path(cli_parameters.config_file.strip())

    # Check if the configuration file exists
    if check_file_existance(p_config_filepath) == False:
        print(f'\n❌Error: unable to open source folder {p_config_filepath}')
        sys.exit(-255)
        
    # Read application configuration file
    app_config = read_app_config(p_config_filepath)
    if app_config['config_return']['status'] == False:
        print(chalk.bg_red(f"\nError reading configuration file: {app_config['config_return']['message']}"))
        sys.exit(-255)
    else:
        source_folder = app_config['config_values']['data_source_dir']
        dest_folder = app_config['config_values']['data_dest_dir']
        vies_checkvat_endpoint = app_config['config_values']['vies_checkvat_endpoint']
        vies_status_endpoint = app_config['config_values']['vies_status_endpoint']
        database_store_active = app_config['config_values']['database_store_active']
        env_filepath = app_config['config_values']['env_filepath']
        log_level = app_config['config_values']['log_mode']
        log_filepath = app_config['config_values']['log_filepath']
        sound_active = app_config['config_values']['sound_active']
        sound_filepath = app_config['config_values']['sound_filepath'] 
    
    # Setup Logging
    setup_logging(log_filepath, 'w', log_level)  # Configurazione GLOBALE
    logger = logging.getLogger("vies_check_vat")  # Ottieni il logger globale

    # if log_level == 'DEBUG':  
    #     logger.setLevel(logging.DEBUG)
    # elif log_level == 'INFO':
    #     logger.setLevel(logging.INFO)
    # elif log_level == 'ERROR':
    #     logger.setLevel(logging.ERROR)         
    # file_handler = logging.FileHandler(log_filepath, mode = 'a')
    # formatter = logging.Formatter('[%(asctime)s] - {%(filename)s:%(funcName)s} - %(message)s', datefmt='%Y-%d-%m %H:%M:%S')

    # file_handler.setFormatter(formatter)
    # logger.addHandler(file_handler)

    logger.info('| START PROGRAM!') 

    # Print program header to STDOUT  
    print_prg_header()
    
    # Play sound
    if sound_active:
        if os.path.exists(sound_filepath):
            # Play sounds from disk
            playsound(sound_filepath)

    # Print decoded parameters
    print(chalk.bg_grey(f'APP parameters:'))
    print(chalk.gray(f'Program name       : {pathlib.Path(p_program_name)}'))
    print(chalk.gray(f'Logging level      : {log_level}'))
    print(chalk.gray(f'Logging filepath   : {pathlib.Path(log_filepath)}'))
    print(chalk.gray(f'Database store     : {database_store_active}'))
    print(chalk.gray(f'Env filepath       : {pathlib.Path(env_filepath)}'))    
    print(chalk.gray(f'Sound active       : {sound_active}'))
    print(chalk.gray(f'Sound filepath     : {pathlib.Path(sound_filepath)}'))
    print(chalk.gray(f'Source CSV folder  : {pathlib.Path(source_folder)}'))
    print(chalk.gray(f'Output folder      : {pathlib.Path(dest_folder)}'))
    print()

    # Check input parameters
    logger.info(f'| Check configuration file: {p_config_filepath}')   
    if check_folder_existance(source_folder) == False:
        print(chalk.red_bright(f'\n❌ Error: unable to open source folder {source_folder}'))
        logger.error(f'**Error: unable to open source folder {source_folder}')
        sys.exit(-255)
    
    # Check if source folder is empty
    logger.info('| Check VIES service STATUS...') 
    print(chalk.bg_grey(f'Check VIES service STATUS:'))
    funct_result = vow_check_status_service(vies_status_endpoint)
    if funct_result["status"] == False:
        print(chalk.bg_red(f'\n❌ Error: VIES service STATUS unreachable -> {funct_result["message"]}'))
        logger.error(f'**Error: VIES service STATUS unreachable -> {funct_result["message"]}')        
        sys.exit(-255)
    
    print(chalk.bg_grey(f'\nGet files list from source folder:'))
    logger.info(f'| Get files list from source folder {pathlib.Path(source_folder)}...')
    p = pathlib.Path(source_folder)
    source_file_lst = [x for x in p.iterdir() if x.is_file() and x.suffix == '.csv']      
    print(f'\n#Number of files in source folder {pathlib.Path(source_folder)}: {len(source_file_lst)}')    
  
    file_counter = 0
    for file_input in source_file_lst:
        file_counter += 1
        print(chalk.cyan_bright(f'\nProcessing file [{file_counter}]: {os.path.basename(file_input)}'))
        logger.info(f'| Processing file [{file_counter}]: {os.path.basename(file_input)}')
        with open(file_input, mode ='r', encoding='utf-8') as filein:
            # Read CSV file
            try:
                csvFile = csv.reader(filein, delimiter=';')
            except Exception as e:
                print(chalk.bg_red(f'\n❌ Error: unable to open input CSV file {file_input}: {str(e)}'))
                logger.error(f'**Error: unable to open input CSV file {file_input} -> {str(e)}')
                sys.exit(-255)

            else:
                # Skip header line
                header = next(csvFile, None)
                nr_header_record = 1

            # Initialize variables
            nr_correct_record = 0
            nr_wrong_record = 0
            f_input_lst = list()
            line_nr_lst = list()
            partner_desc_lst = list()
            country_code_lst = list()
            vat_nr_lst = list()

            for i, row in enumerate(csvFile, start=1):
                logger.debug(f'| Validate line [{i}]') 
                funct_result = validate_csv_line(i, row)
                if funct_result["status"] == False:
                    print(chalk.red_bright(f'Row {i}: {funct_result["message"]}'))
                    logger.error(f'| {funct_result["message"]}')
                    nr_wrong_record += 1
                    continue
                else:
                    # Load record in a dataframe
                    f_input_lst.append(os.path.basename(file_input))
                    line_nr_lst.append(i)
                    partner_desc_lst.append(row[0])
                    country_code_lst.append(row[1])
                    vat_nr_lst.append(row[2])
                    logger.debug(f'| Partner: {row[1]}{row[2]}')                    
                    nr_correct_record += 1
            
            # End of for loop
            # Check if there are correct records
            if nr_correct_record == 0:
                continue

            data = {
                'file_input': f_input_lst,
                'line_nr': line_nr_lst,
                'in_ccode': country_code_lst,
                'in_vatnr': vat_nr_lst,
                'in_pdesc': partner_desc_lst                
            }
            

            df_in = pd.DataFrame.from_dict(data)
            df_out = pd.DataFrame()
            print(chalk.grey(f'\nNr. of correct record: {len(df_in)}\n'))
            logger.info(f'| Nr. of right records: {len(df_in)}')

            print(chalk.bg_grey(f'Check VIES validity for [{len(df_in)}] records:'))
            logger.info(f'| Check VIES validity for {len(df_in)} records...')
            df_tmp = vies_check_vat_service(vies_checkvat_endpoint, df_in, STANDARD_RECORD_SLEEP_TIME)
            df_out = pd.concat([df_out, df_tmp])
            df_err = pd.DataFrame()
            # Check if there are errors in the VIES service
            # If there are 'MS_MAX_CONCURRENT_REQ' errors, call the VIES service again
            df_err = df_out[(df_out['vies_status'] == 'INVALID') & (df_out['vies_err_msg'] =='MS_MAX_CONCURRENT_REQ')] 
            print(chalk.grey(f'\nNr. of processed records with error: {len(df_err)}\n'))           
            if len(df_err) > 0:
                # Drop errors from df_out
                df_out = df_out.drop(df_err.index)
                # Rerun errors in the VIES service
                print(chalk.yellow(f'Reprocessing [{len(df_err)}] records:'))
                logger.info(f'| Reprocessing {len(df_err)} records...')
                df_tmp = vies_check_vat_service(vies_checkvat_endpoint, df_err, LONG_RECORD_SLEEP_TIME)
                # Concatenate the results
                df_out = pd.concat([df_out, df_tmp])                
                df_out = df_out.sort_values(['file_input', 'line_nr'], ascending=[True, True]).reset_index(drop=True)

            # Create PDF report
            print(chalk.bg_grey(f'\nCreate PDF Report:'))            
            logger.info(f'| Create PDF report...')
            pdf_out_file = os.path.join(pathlib.Path(source_folder), os.path.basename(file_input).replace('.csv', '.pdf'))
            report_tab_columns = [
                'line_nr',
                'in_ccode', 
                'in_vatnr', 
                'in_pdesc', 
                'vies_ccode', 
                'vies_vatnr', 
                'vies_status', 
                'vies_err_msg'
                ]
            report_title = f"VIES VAT CONTROLLER"
            report_cover_vars = {
                "Program name": os.path.basename(__file__),
                "Source file": file_input,
                "PDF file": pdf_out_file,
                "Number of records": str(len(df_out))#,
                #"Generation date": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                }
            
            modules.reportlab_module.create_vat_controller_pdf(df_out, pdf_out_file, report_tab_columns, report_title, report_cover_vars)
            print(chalk.cyan_bright(f'PDF Report created: {pdf_out_file}'))
            logger.info(f'| PDF Report created: {os.path.basename(pdf_out_file)}')
 
            if len(df_out)>0 and database_store_active == True:
                # Save VIES check results to SQLiteCloud
                print(chalk.bg_grey(f'\nSave data to SQLiteCloud:'))  
                logger.info('| Open SQLiteCloud database...')
                
                # Load SQLiteCloud database configuration
                db_config = modules.sqlite_cloud_module.load_database_config(env_filepath)
                
                # Open SQLiteCloud database connection
                db_conn = modules.sqlite_cloud_module.open_database(db_config)
                if db_conn == None:
                    print(chalk.bg_red(f'\n❌ Error: Database connection failed!'))
                    logger.error(f'**Error: Database connection failed!')
                    sys.exit(-255)
                else:
                    #print(chalk.green_bright(f'\nSqliteCloud connection OK!'))
                    logger.info(f'| SqliteCloud connection OK!')
                    modules.sqlite_cloud_module.get_database_info(db_conn,"OFF")
                # Save VIES check to SQLiteCloud
                logger.info(f'| Save VIES check to SQLiteCloud...') 
                f_return = modules.sqlite_cloud_module.insert_vies_records(db_conn, df_out)
                if f_return["status"] == False:
                    print(chalk.bg_red(f'\n❌ Error: unable to save VIES records to SQLite Cloud-> {f_return["message"]}'))
                    logger.error(f'**Error: unable to save VIES records to SQLite Cloud -> {f_return["message"]}')        
                    sys.exit(-255)
                else:
                    print(chalk.cyan_bright(f'VIES records saved to SQLite Cloud!'))
                    logger.info(f'| VIES records saved to SQLite Cloud!')

                logger.info('| Close SQLiteCloud database...')
                modules.sqlite_cloud_module.close_database(db_conn)
       
    # Play sound
    if sound_active:
        if os.path.exists(sound_filepath):
            # Play sounds from disk
            playsound(sound_filepath)

    toc = time.perf_counter()
    print_prg_footer(f"Program duration: {toc - tic:0.2f} seconds")
    logger.info(f'| END PROGRAM!') 


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(chalk.yellow_bright(f"\nUsage: python.exe {sys.argv[0]} -c <config_file_path>"))
        sys.exit(1)
    else:
        main()

