#libraries
from datetime import datetime as dt, date, timedelta as td

def time_trans(message, error = 0):
    try:
        time_trans = dt.strftime(dt.now(), '%d/%m/%Y %H:%M:%S')
        if repr(error) == '0':
            print(time_trans + ': ' + message)
        else:
            print(time_trans + ': ' + message + ': ' + repr(error))
    except Exception as error:
        print('Error en el formato del mensaje')
def vars_date(variable = 0):
    try:
        today = date.today()
        day_over = today - td(days = 1) #día vencido
        first_day_month = date(year = day_over.year, month = day_over.month, day = 1)
        first_day_year = date(year = day_over.year, month = 1, day = 1)
        first_day_over_year = date(year = day_over.year - 1, month =1, day = 1)
        #Conversión a string para scrapping
        today_str = dt.strftime(today, '%d/%m/%Y') #día string
        day_over_str = dt.strftime(day_over, '%d/%m/%Y') #día vencido string
        firs_day_m_str = dt.strftime(first_day_month, '%d/%m/%Y') #primer día del mes string
        firs_day_oy_str = dt.strftime(first_day_year, '%d/%m/%Y') #primer día del año string
        firs_day_y_str = dt.strftime(first_day_over_year, '%d/%m/%Y') #primer día del año vencido string
        return today_str, day_over_str, firs_day_m_str, firs_day_oy_str, firs_day_y_str
    except Exception as error:
        print('Error en la definición de variables')