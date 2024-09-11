# if you wanna running this code you need to install openpyxl, lxml, html5lib
# libraries
from datetime import datetime as dt
import inspect, warnings
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
dict_month = {1:'Enero', 2:'Febrero', 3:'Marzo', 4:'Abril', 5:'Mayo', 6:'Junio'
    ,7:'Julio', 8:'Agosto', 9:'Septiembre', 10:'Octubre', 11:'Noviembre', 12:'Diciembre'}
#configuration
warnings.filterwarnings("ignore")
# logging.basicConfig(level=logging.ERROR) #DEBUG, INFO, WARNING, ERROR, CRITICAL

# start definition
class scraping:
    # initial count
    def __init__(self):
        self.trans = 0
        self.error = 0
    # transaction handling
    def handle_trans(self, msg):
        bs.time_trans(msg)
        self.trans += 1
    # error handling
    def handle_error(self, msg, error):
        bs.time_trans(msg, error)
        self.error += 1
    # variable definition
    def variables(self):
        try:
            url = 'https://dof.gob.mx/'
            initial_date = bs.vars_date()[4]
            end_date = bs.vars_date()[1]
            self.handle_trans('variables has been definied')
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
            self.handle_trans('Los directiorios fueron validados')
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
            self.handle_trans('Scraping has ended')
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
            self.handle_trans('File has been generated')
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # run all function
    def load(self, proc):
        try:
            path_list = scraping.path_val(self)
            bs.path_validator(path_list)
            url, initial_date, end_date = scraping.variables(self)
            content = scraping.new_scraping(self, url, initial_date, end_date)
            scraping.render_table(self, content)
        except Exception as error:
            status = 'error'
            msg_proc = 'Caugth this error: ' + repr(error)
        else:
            status = 'ok'
            msg_proc = 'Nothing went wrong'
        finally:
            print('The class ' + proc + ' is finished.')
            print('transacciones: ' + str(self.trans) + ', errores: ' + str(self.error))
            return status, msg_proc

class sqlite_load:
    # initial count
    def __init__(self):
        self.trans = 0
        self.error = 0
    # transaction handling
    def handle_trans(self, msg):
        bs.time_trans(msg)
        self.trans += 1
    # error handling
    def handle_error(self, msg, error):
        bs.time_trans(msg, error)
        self.error += 1
    # read excel file
    def transform_excel(self):
        try:
            name_cols = ['fecha','precio']
            df_to_load = pd.read_excel('C:/App/Download/DOF.xlsx', names=name_cols, header=0)
            self.handle_trans('File is ready to load')
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
            self.handle_trans('gob.dollar_by_date has been cleaned from ' + str(date_ini) + ' to ' + str(date_end))
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # load to sql
    def load_to_sql(self, df_to_load, engine):
        try:
            table = 'gob.dollar_by_date'
            df_to_load.to_sql(table, engine, index=False, if_exists='append')
            self.handle_trans(table + ' has been updated')
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # run all function
    def load(self, proc):
        try:
            engine, conn, cursor = db_conn.sqlite_with_alchemy()
            url, initial_date, end_date = scraping.variables(self)
            df_to_load = sqlite_load.transform_excel(self)
            sqlite_load.clean_sql(self, initial_date, end_date, conn, cursor)
            sqlite_load.load_to_sql(self, df_to_load, engine)
            conn.close()
        except Exception as error:
            status = 'error'
            msg_proc = 'Caugth this error: ' + repr(error)
        else:
            status = 'ok'
            msg_proc = 'Nothing went wrong'
        finally:
            print('The class ' + proc + ' is finished.')
            print('transacciones: ' + str(self.trans) + ', errores: ' + str(self.error))
            return status, msg_proc

class generate_excel:
    # initial count
    def __init__(self):
        self.trans = 0
        self.error = 0
    # transaction handling
    def handle_trans(self, msg):
        bs.time_trans(msg)
        self.trans += 1
    # error handling
    def handle_error(self, msg, error):
        bs.time_trans(msg, error)
        self.error += 1
    # read sql table
    def read_sql(self, engine):
        try:
            query = 'SELECT* FROM "gob.dollar_by_date"'
            df_raw = pd.read_sql(query, engine)
            df_dollar = df_raw.rename(columns={'fecha': 'fecha_raw', 'precio': 'precio'})
            df_dollar['fecha'] = pd.to_datetime(df_dollar['fecha_raw'], format='%d-%m-%Y')
            df_dollar['año'] = df_dollar['fecha'].dt.year
            df_dollar['mes'] = df_dollar['fecha'].dt.month
            df_dollar['mes_nv'] = df_dollar['fecha'].dt.month.apply(lambda x: f"{x:02d}")
            df_dollar['nombre_mes'] = df_dollar['mes'].map(dict_month)
            df_dollar['año_mes'] = df_dollar['nombre_mes'] + ' ' + df_dollar['año'].astype(str)
            df_dollar['año_mes_filtro'] = df_dollar['año'].astype(str) + df_dollar['mes_nv'].astype(str) + '_' + df_dollar['nombre_mes']
            self.handle_trans('SQL table is ready to process')
            return df_dollar
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # define the file names
    def file_names(self, df_dollar):
        try:
            list_month = df_dollar['año_mes_filtro'].unique().tolist()
            path_processed = 'C:/App/Processed/'
            dict_files = {}
            for month in list_month:
                name_file = path_processed + month + '.xlsx'
                dict_files.update({month:name_file})
            self.handle_trans('Lists has been generated')
            return dict_files
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # filter and generate excel files
    def generate_files(self, dict_files, df_dollar):
        try:
            for month in dict_files:
                df_filtered = df_dollar[['fecha','precio','nombre_mes','año','año_mes']][df_dollar['año_mes_filtro'] == month]
                df_filtered.to_excel(dict_files[month], index = False, sheet_name = month)
            self.handle_trans('Files in excel has been generated')
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # generate unified file
    def generate_unified(self, dict_files, df_dollar):
        try:
            file_name = 'C:/App/Processed/dollar_unificado.xlsx'
            unificadoo = pd.ExcelWriter(file_name)
            with unificadoo as writer:
                # genera el archivo resumido
                df_unificado = df_dollar[['fecha','precio','nombre_mes','año','año_mes']]
                df_unificado.to_excel(writer, index=False, sheet_name = 'Precio_dolar')
                # genera una hoja para cada mes
                for month in dict_files:
                    df_filtered = df_dollar[['fecha','precio','nombre_mes','año','año_mes']][df_dollar['año_mes_filtro'] == month]
                    df_filtered.to_excel(writer, index = False, sheet_name = month)
            self.handle_trans('Unified file excel has been generated')
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # run all function
    def load(self, proc):
        try:
            engine, conn, cursor = db_conn.sqlite_with_alchemy()
            df_dollar = generate_excel.read_sql(self, engine)
            conn.close()
            dict_files = generate_excel.file_names(self, df_dollar)
            generate_excel.generate_files(self, dict_files, df_dollar)
            generate_excel.generate_unified(self, dict_files, df_dollar)
        except Exception as error:
            status = 'error'
            msg_proc = 'Caugth this error: ' + repr(error)
        else:
            status = 'ok'
            msg_proc = 'Nothing went wrong'
        finally:
            print('The class ' + proc + ' is finished.')
            print('transacciones: ' + str(self.trans) + ', errores: ' + str(self.error))
            return status, msg_proc


# start execution
dict_clases = {'scraping':scraping(), 'sqlite_load':sqlite_load(), 'generate_excel':generate_excel()}
def main():
    for clase in dict_clases:
        print('Starting to run ' + clase)
        run_process = clase
        run_class = dict_clases[clase]
        result = run_class.load(run_process)
main()