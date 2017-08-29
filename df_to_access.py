import pandas as pd
import datetime as dt
import pyodbc
import os

def load_data(excel_file):
    now = dt.datetime.now()      
    table_name = ['IIF Historico','IRF Historico','Benchmark Historico']

    #Columnas access
    iifc = '(V,[Op V],[Op Int V],FV,C,[Op C],[Op Int C],FC,Rte,Folio,Instrumento,Emisor,Liq,D,Rescate,Moneda,Dias,Tasa,Captacion,[Tipo Emisor],Hora,Fecha)'
    irfc = '(V,[Op V],[Op Int V],FV,C,[Op C],[Op Int C],FC,Rte,Folio,Instrumento,Liq,D,Cantidad,Reaj,Plazo,Duration,Precio,TIR,Monto,Hora,Fecha,[Monto Liq],Familia,[Moneda Liq])'
    bchmc = '(Indice,Benchmark,[10:10 am],[1:20 pm],Ultimo,Mayor,Menor,[Nro Negocios],[Monto $],Fecha)'
    fields = [iifc,irfc,bchmc]

    #Dataframes
    IIF = pd.read_excel(open(excel_file,"rb"), sheetname="IIF")
    IRF = pd.read_excel(open(excel_file,"rb"), sheetname="IRF")
    Bch = pd.read_excel(open(excel_file,"rb"), sheetname="Benchmark")
    date = IIF['Fecha'][0]
    Bch['Fecha'] = date

    dfs = [IIF,IRF,Bch]

    #Access connection
    connStr = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        r"DBQ=C:\Users\Jose Pedro\Dropbox\projects\Python\Zspread\datosSebra.accdb;"
        )
    cnxn = pyodbc.connect(connStr)
    cursor = cnxn.cursor()

    for i in range(3):
        print('Table %s in proccess... %i of %i.' %(table_name[i],(i+1),len(table_name)))
        print('Changing date and hour format...')
        dfs[i] = fix_for_sql(dfs[i])
        #tmp = dfs[i].values.tolist()
        send_to_access(dfs[i],table_name[i],fields[i],cursor,cnxn)

    send_to_access(date,'Fechas Datos','(Fecha)',cursor,cnxn)
    print('Date data saved.')
    time = dt.datetime.now() - now
    print('All tables saved succesfully. Execution time: %s' % str(time))
    #Insert date
    cnxn.close()

def fix_for_sql(df):
    print('Changing formats for SQL...')
    df = df.fillna(0)
    try:
        df['Fecha'] = df['Fecha'].astype(str)
        df['Hora'] = df['Hora'].astype(str)
        df['Plazo'] = df['Plazo'].astype(str)
    except:
        pass
    return df

def send_to_access(df,table_name,fields,cursor,cnxn):
    #df as DataFrame, table_name as string, fields as string(access), cursor as pyodbc cursor and cnxc as pyodbc connect
    if isinstance(df, pd.DataFrame):
        tmp = df.values.tolist()
        try:
            for j in range(0,len(tmp)):
                query = 'INSERT INTO ['+ table_name +'] ' + str(fields) +' VALUES '
                #data = ','.join([str(tuple(x)) for x in tmp])
                if len(tmp)>1:
                    data = str(tuple(tmp[j]))
                    query = query + data + ';'
                else:
                    data = '('+str(tmp)+')'
                    query = query + data + ';'
                #print(query)
                cursor.execute(query)
        except:
            print('Error: Table %s not saved. \n' %table_name)
            raise
        else:
            cnxn.commit()
            print('Table %s saved succesfully. \n' %table_name)
    else:
        tmp = df
        try:
            query = 'INSERT INTO ['+ table_name +'] ' + str(fields) +' VALUES '
            #data = ','.join([str(tuple(x)) for x in tmp])
            data = '(\''+str(tmp)+'\')'
            query = query + data + ';'
            print(query)
            cursor.execute(query)
        except:
            print('Error: Table %s not saved. \n' %table_name)
            raise
        else:
            cnxn.commit()
            print('Table %s saved succesfully. \n' %table_name)

def populate_db(file_path):
    directory = os.fsencode(file_path)
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith(".xlsx"):
            excel_file = os.path.join(os.fsdecode(directory), filename)
            load_data(excel_file)
        else:
            continue
