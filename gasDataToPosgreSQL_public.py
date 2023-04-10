# ####################################################################### #
# APP: program to save the panda with spain gas station data to postgres  #
# Year:2022                                                               #
#                                                                         #
# ####################################################################### #
# libraries 

from datetime import datetime
import psycopg2 #to install with pip3 install psycopg2-binary
import sys  # to take input argument from shell.
from takeGasPrices import takeGasPrices

# Connection to database data.
database_ip="xxx.xxx.xxx.xxx" # your server database IP
database_port=5432 # your postgres port (by default 5432)
database_user='user' # your postgres user
database_password='pass' #your postgres pass
database_db='db' #your gas db name

# ####################################################################### #
# Data Base functions to avoid complex main program reading.              #
# ####################################################################### #


# ####################################################################### #
# FUNTION: conectar_db                                                    #
# DESCRIPTION: Generate a connection to the database (postgreSQL)         #
# INPUT: Data needed to connect and the inital connection query           #
# OUTPUT: Cursor and Connection,  print error if happens                  #
# ####################################################################### #
def conectar_bd(PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB, PS_QUERY):
    # """Funcion para conectar con la base de datos, mandamos los datos de conexion y la consulta,
    # devolvemos un array con cursor y connector"""
    # realizamos la conexión
    
    try:
        # Conectarse a la base de datos
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB)
        conn = psycopg2.connect(connstr)

        # Abrir un cursor para realizar operaciones sobre la base de datos
        cur = conn.cursor()

        # Ejecutamos la peticion
        cur.execute(PS_QUERY)


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    return cur, conn
# ####################################################################### #
# FUNTION: cerrar_conexion_bbdd                                           #
# DESCRIPTION: Close the connection                                       #
# INPUT: Data needed to close                                             #
# OUTPUT: Nothing                                                         #
# ####################################################################### #

def cerrar_conexion_bbdd(PS_CURSOR, PS_CONN):
    # Cerrar la conexión con la base de datos
    PS_CURSOR.close()
    PS_CONN.close()




#############################################
## MAIN
#############################################

# Let's call to the function that fetch the data.

df = takeGasPrices('')
# conectamos a bbdd
print ("Connecting with database ...")
cur, con = conectar_bd(database_ip, database_port, database_user, database_password, database_db, "select 1")    # escribimos

print ("Inserting rows please wait ...")
for i in range(len(df)):
    Date = df.iloc[i]['Date']
    Datetimeformated = datetime.strptime(Date, "%d/%m/%Y %H:%M:%S")
    InsertTimeGasStation = 'insert into gasprices values ('
    InsertTimeGasStation += "'"+ df.iloc[i]['PostalCode']+ "',"
    InsertTimeGasStation += "'"+ df.iloc[i]['Address']+ "',"
    InsertTimeGasStation += "'"+ df.iloc[i]['Schedule']+ "',"
    InsertTimeGasStation += str(df.iloc[i]['Latitude'])+ ","
    InsertTimeGasStation += "'"+ df.iloc[i]['Location']+ "',"
    InsertTimeGasStation += str(df.iloc[i]['Longitude'])+ ","
    InsertTimeGasStation += "'"+ df.iloc[i]['Margin']+ "',"
    InsertTimeGasStation += "'"+ df.iloc[i]['Town']+ "',"
    InsertTimeGasStation += str(df.iloc[i]['PriceBiodiesel'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceBioetanol'])+","
    InsertTimeGasStation += str(df.iloc[i]['PriceCompressedNaturalGas'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceLiquefiedNaturalGas'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceLiquefiedPetroleumGases'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceDieselA'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceDieselB'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceDieselPremium'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceFuel95E10'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceFuel95E5'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceFuel95E5Premium'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceFuel98E10'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceFuel98E5'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PriceHydrogen'])+ ","
    InsertTimeGasStation += "'"+ df.iloc[i]['Province']+ "',"
    InsertTimeGasStation += "'"+ df.iloc[i]['Referral']+ "',"
    InsertTimeGasStation += "'"+ df.iloc[i]['Label']+ "',"
    InsertTimeGasStation += "'"+ df.iloc[i]['TypeSale']+ "',"
    InsertTimeGasStation += str(df.iloc[i]['PercentageBioetanol'])+ ","
    InsertTimeGasStation += str(df.iloc[i]['PercentageMethilEster'])+ ","
    InsertTimeGasStation += "'"+ df.iloc[i]['IdStation']+ "',"
    InsertTimeGasStation += "'"+ df.iloc[i]['IdTown']+ "',"
    InsertTimeGasStation += "'"+ df.iloc[i]['IdProvince']+ "',"
    InsertTimeGasStation += "'"+ df.iloc[i]['IdAutonomousCommunity']+ "',"
    InsertTimeGasStation += "'"+ Datetimeformated.strftime('%Y-%m-%d %H:%M:%S')+ "',"
    InsertTimeGasStation += "'"+ df.iloc[i]['Notes']+ "',"
    InsertTimeGasStation += "'"+ df.iloc[i]['QueryResult']+ "') ON CONFLICT DO NOTHING"

    InsertTimeGasStation=InsertTimeGasStation.replace('nan','null')
    #InsertTimeGasStation=InsertTimeGasStation.replace("'","·")
    #InsertTimeGasStation=InsertTimeGasStation.replace('"',"'")
    #InsertTimeGasStation=InsertTimeGasStation.replace('·','"')

    #print(InsertTimeGasStation)

    cur.execute(InsertTimeGasStation)
con.commit()  # to commit changes in the table
print ("Done")
cerrar_conexion_bbdd(cur, con) #closing db cursor
