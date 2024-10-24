# libraries
import inspect
import pandas as pd
# self libraries
import Libs.basic_function as bs
import Libs.db_conn as db_conn
# variabes
csv_file = 'Files/NFLX_daily_data.csv'

# starting scrapping
class mongo_insert:
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
    # mongo connection definition
    def db_definition(self):
        try:
            client = db_conn.mongosh_with_lib()
            database = client['DataManagement']
            collection = database['tbl_netflix_daily']
            self.handle_trans('mongo connecction is ready')
            return collection
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # convert csv file to json structure
    def csv_to_json(self):
        try:
            data = pd.read_csv(csv_file, header=0)
            json_data = data.to_dict('records')
            self.handle_trans('Json structure is ready')
            return json_data
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # json to mongo
    def json_to_mongo(self, json_data, collection):
        try:
            collection.insert_many(json_data)
            self.handle_trans('Data has been inserted')
        except Exception as error:
            msg = 'Error on ' + inspect.currentframe().f_code.co_name
            self.handle_error(msg, error)
    # run all function
    def load(self, proc):
        try:
            collection = mongo_insert.db_definition(self)
            json_data = mongo_insert.csv_to_json(self)
            mongo_insert.json_to_mongo(self, json_data, collection)
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

def main():
    run_process = 'mongo_insert'
    run_class = mongo_insert()
    result = run_class.load(run_process)
main()