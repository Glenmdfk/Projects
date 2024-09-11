# if you wanna running this code you need to install openpyxl, lxml, html5lib
# libraries
from datetime import datetime as dt
import inspect, warnings, logging
from time import sleep
import pandas as pd
# selenium
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium import webdriver
# self libraries
import Libs.basic_function as bs
import Libs.db_conn as db_conn
# global variables and config
default_download = 'C:\\App\\Download'
file_generated = 'C:/App/Download/DOF.xlsx'
#configuration
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.ERROR)
#DEBUG, INFO, WARNING, ERROR, CRITICAL

# start definition
class scraping:
    # initial count
    def __init__(self):
        self.trans = 0
        self.error = 0
    # error handling
    def handle_error(self, msg, error):
        bs.time_trans(msg, error)
        logging.error('Error on : ' + error)
        self.error += 1
    # variable definition
    def variables(self):
        try:
            url = 'https://dof.gob.mx/'
            initial_date = bs.vars_date()[4]
            end_date = bs.vars_date()[1]
            bs.time_trans('variables has been definied')
            self.trans += 1
            return url, initial_date, end_date
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # directories validation
    def path_val(self):
        try:
            #Lista vacia
            path_list = list()
            #Variables de directorios
            path_download = 'C:/App/Download'
            path_processed = 'C:/App/Processed'
            #Agregar a la lista de directorios
            path_list.append(path_download)
            path_list.append(path_processed)
            self.trans += 1
            return path_list
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # scraping over page
    def new_scraping(self, url, initial_date, end_date):
        try:
            # scraping preferences
            driver = webdriver.Firefox()
            prefs = {"download.default_directory" : default_download}
            options = Options()
            options.add_argument('--start-maximized')
            options.add_experimental_option("prefs", prefs)
            
            # start execution
            driver.get(url)
            sleep(3)
            bs.time_trans('Buscar tipo de cambio')
            driver.find_element(By.CSS_SELECTOR, 'a[href="indicadores.php"]').click()
            sleep(3)
            bs.time_trans('Escribir fecha inicial: ' + initial_date)
            driver.find_element(By.ID, 'dfecha').send_keys(initial_date)
            sleep(3)
            bs.time_trans('Escribir fecha final: '+ end_date)
            driver.find_element(By.ID, 'hfecha').send_keys(end_date)
            sleep(3)
            bs.time_trans('Presionar boton de consulta')
            driver.find_element(By.CSS_SELECTOR, 'img[alt="consultar"]').click()
            sleep(3)
            
            bs.time_trans('Leyendo tabla en HTML')
            recording_table = driver.find_element(By.CLASS_NAME, 'Tabla_borde')
            content = recording_table.get_attribute("outerHTML")
            
            # end of running
            driver.quit()
            self.trans += 1
            bs.time_trans('Scraping has ended')
            return content
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # reading table
    def render_table(self, content):
        try:
            df_list = pd.read_html(content, header=0)
            df_dof = df_list[0]
            df_dof.to_excel(file_generated, index = False)
            bs.time_trans('File has been generated')
            self.trans += 1
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # read excel file
    def transform_excel(self):
        try:
            name_cols = ['fecha','precio']
            df_to_load = pd.read_excel('C:/App/Download/DOF.xlsx', names=name_cols, header=0)
            bs.time_trans('File is ready to load')
            self.trans += 1
            return df_to_load
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # clean table sql
    def clean_sql(self, initial_date, end_date, conn, cursor):
        try:
            date_ini = dt.strptime(initial_date, '%d/%m/%Y').date()
            date_end = dt.strptime(end_date, '%d/%m/%Y').date()
            query = """DELETE FROM "gob.dollar_by_date"
                WHERE fecha BETWEEN '""" + str(date_ini) + """' AND '""" + str(date_end) + """';"""
            cursor.execute(query)
            conn.commit()
            bs.time_trans('gob.dollar_by_date has been cleaned from ' + str(date_ini) + ' to ' + str(date_end))
            self.trans += 1
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # load to sql
    def load_to_sql(self, df_to_load, engine):
        try:
            table = 'gob.dollar_by_date'
            df_to_load.to_sql(table, engine, index=False, if_exists='append')
            bs.time_trans(table + ' has been updated')
            self.trans += 1
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # run all function
    def load(self, proc):
        try:
            engine, conn, cursor = db_conn.sqlite_with_alchemy()
            path_list = scraping.path_val(self)
            bs.path_validator(path_list)
            url, initial_date, end_date = scraping.variables(self)
            content = scraping.new_scraping(self, url, initial_date, end_date)
            scraping.render_table(self, content)
            df_to_load = scraping.transform_excel(self)
            scraping.clean_sql(self, initial_date, end_date, conn, cursor)
            scraping.load_to_sql(self, df_to_load, engine)
            conn.close()
        except Exception as error:
            status = 'error'
            msg_proc = 'Caugth this error: ' + repr(error)
        else:
            status = 'ok'
            msg_proc = 'Nothing went wrong'
        finally:
            print('transacciones: ' + str(self.trans) + ', errores: ' + str(self.error))
            return status, msg_proc

# start execution
def main():
    run_class = scraping()
    run_process = 'scraping'
    result = run_class.load(run_process)
main()