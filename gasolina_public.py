# ####################################################################### #
# APP: Program to use with Siri to get top3 cheaper gas station close to  #
# you and guide you there.                                               #
# Year:2023                                                               #
#                                                                         #
# ####################################################################### #
# libraries 
from datetime import datetime
import psycopg2 # postgres lib . Install previously with  pip3 install psycopg2-binary
import sys

# libraries to perform geo calcultations. 
import geopy #Install previously with  pip3 install geopy
import geopy.distance as distance 
import plotly.graph_objects as go # Install previously with  pip3 install plotly
from haversine import haversine, Unit #Install previously with  pip3 install haversine

database_ip="xxx.xxx.xxx.xxx" # your db IP
database_port=5432 # your postgres port (by default 5432)
database_user='user' # your postgres user
database_password='pass' #your postgres pass
database_db='db' #your gas db name


# ####################################################################### #
# Data Base functions to avoid complex main program reading.              #
# ####################################################################### #

# La funciona de conexión a la base de datos para peticiones
def conectar_bd (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB, PS_QUERY):
    #"""Funcion para conectar con la base de datos, mandamos los datos de conexion y la consulta,
    #devolvemos un array con cursor y connector"""
    #realizamos la conexión
    try:
        # Conectarse a la base de datos
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB)
        conn = psycopg2.connect(connstr)

        # Abrir un cursor para realizar operaciones sobre la base de datos
        cur = conn.cursor()
        
        #Ejecutamos la peticion
        cur.execute(PS_QUERY)

        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if conn is not None:
            conn.close()
            print('Database connection closed.')   
    return cur, conn

def cerrar_conexion_bbdd (PS_CURSOR, PS_CONN):
    # Cerrar la conexión con la base de datos
    PS_CURSOR.close()
    PS_CONN.close()

def consulta_bbdd (sql_query):
    #conectamos a bbdd
    cur,con = conectar_bd (database_ip,database_port,database_user,database_password, database_db,"select 1" ) 

    cur.execute(sql_query)
    datos=cur.fetchall()
    return datos


#############################################
## MAIN
#############################################
lat_ini=40.03743056999999
long_ini= -3.30449227448612

lat_fin=40.91444720
long_fin=-3.9023060

GPS=[]
campos=len(sys.argv)
# print ("Recibidos:",campos,"argumentos")
# print ("Argumento1: ",sys.argv[0])
# print ("Argumento2: ",sys.argv[1])
i=0
for line in sys.stdin:
    GPS.append(line.rstrip())
    i+=1
    if i==2:
        break
# print ("LATITUD",GPS[0])
# print ("LONGITUD",GPS[1])
# print("Done")

km=sys.argv[1] # Take the distance in km
# print ("The km radious from the point is:", km)
# Each side is km appart
d = distance.distance(kilometers=float(km))
# if the distance from the point is 25km, so we will find a square of 50km of side 
# with the point in the middle

# print ("Distance in Km calculated in module distance:",d)

if len(GPS[0])>2: # >2 looks good, is a latitude, not an enter o other thing
   my_lat= float(GPS[0])
else:
   print ("using default latitude")
   my_lat = 40.73743056999999

if len(GPS[1])>2: # >2 looks good, is a latitude, not an enter o other thing
   my_lon= float(GPS[1])
else:
    print("Using default longitude")
    my_lon = -3.850449227448612

center_point = geopy.Point((my_lat,my_lon))
# Going clockwise, from lower-left to upper-left, upper-right...
p2 = d.destination(point=center_point, bearing=45)
p3 = d.destination(point=center_point, bearing=135)
p4 = d.destination(point=center_point, bearing=-135)
p5 = d.destination(point=center_point, bearing=-45)

#print("-Position: lat:",my_lat)
lat_max=p2.latitude
#print (">>lat_max:",lat_max)
lat_min=p3.latitude
#print (">>lat_min:",lat_min)

#print("-Position: lon:",my_lon)
lon_max=p2.longitude
#print ("||lon_max:",lon_max)
lon_min=p4.longitude
#print ("||lon_min:",lon_min)

#print('p2','-->',p2.latitude, p2.longitude)
#print('p3','-->',p3.latitude, p3.longitude)
#print('p4','-->',p4.latitude, p4.longitude)
#print('p5','-->',p5.latitude, p5.longitude)


# ###################################
# Lets access the data base for tyhe gas stations
# ###################################

query="select label, town, latitude, longitude, pricediesela, pricefuel95e5, address"
query=query+" from gasprices"
query=query+" where latitude < "+str(lat_max)+" and latitude > "+str(lat_min)
query=query+" and longitude < "+str(lon_max)+" and longitude > "+str(lon_min)
query=query+" and date_part('day',date-current_date) =0"
query=query+" order by pricediesela  asc limit 3"
#print ("SQL:",query)
res=consulta_bbdd (query)

print("TOP3 Gasolineas baratas:")
for gas_station in res:
   text=gas_station[0].strip()+" en "
   text+=gas_station[6].strip()+","+gas_station[1].strip()
   text+=". Precio Diesel:"+str(gas_station[4]).replace('.',',')+"."
   print(text)

