# if you wanna running this code you need to install openpyxl, lxml, html5lib
# libraries
import inspect, warnings
from time import sleep
import pandas as pd
# selenium
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium import webdriver
# self libraries
import Libs.basic_function as bs
# global variables and config
default_download = 'C:\\App\\Download'
warnings.filterwarnings("ignore")
# start definition
class Scrapping:
    # initial count
    def __init__(self):
        self.trans = 0
        self.error = 0
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
            bs.time_trans(msg, error)
            self.error += 1
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
            bs.time_trans(msg, error)
            self.error += 1
    # scrapping over page
    def new_scrapping(self, url, initial_date, end_date):
        try:
            # scrapping preferences
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
            bs.time_trans('Scrapping has ended')
            return content
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            bs.time_trans(msg, error)
            self.error += 1
    # reading table
    def read_table(self, content):
        try:
            df_list = pd.read_html(content, header = 0)
            df_dof = df_list[0]
            df_dof.to_excel('C:/App/Download/DOF.xlsx', index = False)
            bs.time_trans('File has been generated')
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            bs.time_trans(msg, error)
            self.error += 1
    #run all function
    def load(self, proc):
        try:
            path_list = Scrapping.path_val(self)
            bs.path_validator(path_list)
            url, initial_date, end_date = Scrapping.variables(self)
            content = Scrapping.new_scrapping(self, url, initial_date, end_date)
            Scrapping.read_table(self, content)
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
    run_class = Scrapping()
    run_process = 'Scrapping'
    result = run_class.load(run_process)
main()