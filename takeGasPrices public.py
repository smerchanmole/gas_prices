# ####################################################################### #
# APP: program to take spain gas station data to a panda frame            #
# Data obtained via URL from MINETUR.GOB.ES
# Year:2022                                                               #
#                                                                         #
# ####################################################################### #
# importamos
import pandas as pd
import requests
import json
import time
from datetime import datetime


# -------------------------------------------------------------------------------------------------------
# -- Funtion:     takeGasPrices (url as string)                                                        --
# -------------------------------------------------------------------------------------------------------
# -- Description: Take the Gas Prices from Spanish open data and cast correctly the fields             --
# -------------------------------------------------------------------------------------------------------
# -- Return:      A pandas data frame                                                                  --
# -------------------------------------------------------------------------------------------------------

def takeGasPrices(url) :
    # set the URL
    if url=='':
        uri_total = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
    else:
        uri_total = url
    print ("Fetching data from")
    print(uri_total)
    print ("Please wait ...")
    # Make the https call and the reponse will be saved on a request object. if there is a certificate error, add ", verify=False")
    response = requests.get(uri_total)
    status=response.status_code
    print ("Done")
    if status != 200:
       print ("Bad response from the URL") 
    # if all goes well the message is a 200
    else:
       # We get the response text (json) to a string variable, and format it again to json properly (to normalize)
       json_string=response.text
       json_formated=json.loads(json_string)
       df = pd.read_json(json_string)

       #print("Info top brach:")
       #print(df.info())
       #print(df.head(5))

       #We normalized the json to have all columns in a flat view
       df2 = pd.json_normalize(json_formated,record_path=['ListaEESSPrecio'],meta=['Fecha','Nota','ResultadoConsulta'])
       #print(df2.head(5).to_string())
       #print(df2.describe(include='all').to_string())
       #print(df2.dtypes)
       # we need to add a schema with data typers correctly instead of allways 'object'
       # this is the actual schema
       # name = "Fecha" nillable = "true"
       # name = "ListaEESSPrecio" nillable = "true" type = "tns:ArrayOfEESSPrecio" / >
       # name = "Nota" nillable = "true" type = "xs:string"
       # name = "ResultadoConsulta" nillable = "true" type = "xs:string"
       # name = "C.P." nillable = "true" type = "xs:string"
       # name = "Dirección" nillable = "true" type = "xs:string"
       # name = "Horario" nillable = "true" type = "xs:string"
       # name = "Latitud" nillable = "true" type = "xs:string"
       # name = "Localidad" nillable = "true" type = "xs:string"
       # name = "Longitud_x0020__x0028_WGS84_x0029_" nillable = "true" type = "xs:string"
       # name = "Margen" nillable = "true" type = "xs:string"
       # name = "Municipio" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Biodiesel" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Bioetanol" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Gas_x0020_Natural_x0020_Comprimido" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Gas_x0020_Natural_x0020_Licuado" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Gases_x0020_licuados_x0020_del_x0020_petróleo" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Gasoleo_x0020_A" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Gasoleo_x0020_B" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Gasoleo_x0020_Premium" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Gasolina_x0020_95_x0020_E10" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Gasolina_x0020_95_x0020_E5" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Gasolina_x0020_95_x0020_E5_x0020_Premium" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Gasolina_x0020_98_x0020_E10" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Gasolina_x0020_98_x0020_E5" nillable = "true" type = "xs:string"
       # name = "Precio_x0020_Hidrogeno" nillable = "true" type = "xs:string"
       # name = "Provincia"nillable = "true" type = "xs:string"
       # name = "Remisión" nillable = "true" type = "xs:string"
       # name = "Rótulo" nillable = "true" type = "xs:string"
       # name = "Tipo_x0020_Venta" nillable = "true" type = "xs:string"
       # name = "_x0025__x0020_BioEtanol" nillable = "true" type = "xs:string"
       # name = "_x0025__x0020_Éster_x0020_metílico" nillable = "true" type = "xs:string"
       # name = "IDEESS" nillable = "true" type = "xs:string"
       # name = "IDMunicipio" nillable = "true" type = "xs:string"
       # name = "IDProvincia" nillable = "true" type = "xs:string"
       # name = "IDCCAA" nillable = "true" type = "xs:string"

       # here we change the column names to English names is PascalCase format
       columns=df2.columns
       #print ("las columnas son:",columns)
       columns2=['PostalCode','Address','Schedule','Latitude','Location','Longitude','Margin','Town','PriceBiodiesel', \
                 'PriceBioetanol','PriceCompressedNaturalGas','PriceLiquefiedNaturalGas','PriceLiquefiedPetroleumGases',\
                 'PriceDieselA','PriceDieselB','PriceDieselPremium','PriceFuel95E10','PriceFuel95E5','PriceFuel95E5Premium',\
                 'PriceFuel98E10','PriceFuel98E5','PriceHydrogen','Province','Referral','Label','TypeSale','PercentageBioetanol',\
                 'PercentageMethilEster','IdStation','IdTown','IdProvince','IdAutonomousCommunity','Date','Notes','QueryResult']
       df2.columns=columns2
       #print("The new columns are:")
       #print(df2.columns)
       #print(df2.describe(include='all').to_string())

       #now we change the "," as decimal separator to "." more international and set '' to Nan value.
       print ("Transforming data ... (replacing ',' with '.' in float fields, replacing ' with \" in texts, casting to float or string)")
       print ("Please wait ...")
       for i in range(len(df2)):

          Address = df2.iloc[i]['Address']
          Address = Address.replace("'","\"")
          df2.at[i, 'Address'] = Address

          Schedule = df2.iloc[i]['Schedule']
          Schedule = Schedule.replace("'","\"")
          df2.at[i, 'Schedule'] = Schedule

          Location = df2.iloc[i]['Location']
          Location = Location.replace("'", "\"")
          df2.at[i, 'Location'] = Location

          Town = df2.iloc[i]['Town']
          Town = Town.replace("'", "\"")
          df2.at[i, 'Town'] = Town

          Province = df2.iloc[i]['Province']
          Province = Province.replace("'", "\"")
          df2.at[i, 'Province'] = Province

          Label = df2.iloc[i]['Label']
          Label = Label.replace("'", "\"")
          df2.at[i, 'Label'] = Label

          Notes = df2.iloc[i]['Notes']
          Notes = Notes.replace("'", "\"")
          df2.at[i, 'Notes'] = Notes

          Latitude=df2.iloc[i]['Latitude']
          Latitude=Latitude.replace(",",".")
          if Latitude=='':
             latitude=float('nan')
          df2.at[i,'Latitude']=Latitude

          Longitude = df2.iloc[i]['Longitude']
          Longitude = Longitude.replace(",", ".")
          if Longitude=='':
             Longitude=float('nan')
          df2.at[i, 'Longitude'] = Longitude

          PriceBiodiesel = df2.iloc[i]['PriceBiodiesel']
          PriceBiodiesel = PriceBiodiesel.replace(",", ".")
          if PriceBiodiesel=='':
             PriceBiodiesel=float('nan')
          df2.at[i, 'PriceBiodiesel'] = PriceBiodiesel

          PriceBioetanol = df2.iloc[i]['PriceBioetanol']
          PriceBioetanol = PriceBioetanol.replace(",", ".")
          if PriceBioetanol=='':
             PriceBioetanol=float('nan')
          df2.at[i, 'PriceBioetanol'] = PriceBioetanol

          PriceCompressedNaturalGas = df2.iloc[i]['PriceCompressedNaturalGas']
          PriceCompressedNaturalGas = PriceCompressedNaturalGas.replace(",", ".")
          if PriceCompressedNaturalGas == '':
             PriceCompressedNaturalGas = float('nan')
          df2.at[i, 'PriceCompressedNaturalGas'] = PriceCompressedNaturalGas

          PriceLiquefiedNaturalGas = df2.iloc[i]['PriceLiquefiedNaturalGas']
          PriceLiquefiedNaturalGas = PriceLiquefiedNaturalGas.replace(",", ".")
          if PriceLiquefiedNaturalGas == '':
             PriceLiquefiedNaturalGas = float('nan')
          df2.at[i, 'PriceLiquefiedNaturalGas'] = PriceLiquefiedNaturalGas

          PriceLiquefiedPetroleumGases = df2.iloc[i]['PriceLiquefiedPetroleumGases']
          PriceLiquefiedPetroleumGases = PriceLiquefiedPetroleumGases.replace(",", ".")
          if PriceLiquefiedPetroleumGases == '':
             PriceLiquefiedPetroleumGases = float('nan')
          df2.at[i, 'PriceLiquefiedPetroleumGases'] = PriceLiquefiedPetroleumGases

          PriceDieselA = df2.iloc[i]['PriceDieselA']
          PriceDieselA = PriceDieselA.replace(",", ".")
          if PriceDieselA == '':
             PriceDieselA = float('nan')
          df2.at[i, 'PriceDieselA'] = PriceDieselA

          PriceDieselB = df2.iloc[i]['PriceDieselB']
          PriceDieselB = PriceDieselB.replace(",", ".")
          if PriceDieselB == '':
             PriceDieselB = float('nan')
          df2.at[i, 'PriceDieselB'] = PriceDieselB

          PriceDieselPremium = df2.iloc[i]['PriceDieselPremium']
          PriceDieselPremium = PriceDieselPremium.replace(",", ".")
          if PriceDieselPremium == '':
             PriceDieselPremium = float('nan')
          df2.at[i, 'PriceDieselPremium'] = PriceDieselPremium

          PriceFuel95E10 = df2.iloc[i]['PriceFuel95E10']
          PriceFuel95E10 = PriceFuel95E10.replace(",", ".")
          if PriceFuel95E10 == '':
             PriceFuel95E10 = float('nan')
          df2.at[i, 'PriceFuel95E10'] = PriceFuel95E10

          PriceFuel95E5 = df2.iloc[i]['PriceFuel95E5']
          PriceFuel95E5 = PriceFuel95E5.replace(",", ".")
          if PriceFuel95E5 == '':
             PriceFuel95E5 = float('nan')
          df2.at[i, 'PriceFuel95E5'] = PriceFuel95E5

          PriceFuel95E5Premium = df2.iloc[i]['PriceFuel95E5Premium']
          PriceFuel95E5Premium = PriceFuel95E5Premium.replace(",", ".")
          if PriceFuel95E5Premium == '':
             PriceFuel95E5Premium = float('nan')
          df2.at[i, 'PriceFuel95E5Premium'] = PriceFuel95E5Premium

          PriceFuel98E10 = df2.iloc[i]['PriceFuel98E10']
          PriceFuel98E10 = PriceFuel98E10.replace(",", ".")
          if PriceFuel98E10 == '':
             PriceFuel98E10 = float('nan')
          df2.at[i, 'PriceFuel98E10'] = PriceFuel98E10

          PriceFuel98E5 = df2.iloc[i]['PriceFuel98E5']
          PriceFuel98E5 = PriceFuel98E5.replace(",", ".")
          if PriceFuel98E5 == '':
             PriceFuel98E5 = float('nan')
          df2.at[i, 'PriceFuel98E5'] = PriceFuel98E5

          PriceHydrogen = df2.iloc[i]['PriceHydrogen']
          PriceHydrogen = PriceHydrogen.replace(",", ".")
          if PriceHydrogen == '':
             PriceHydrogen = float('nan')
          df2.at[i, 'PriceHydrogen'] = PriceHydrogen

          PercentageBioetanol = df2.iloc[i]['PercentageBioetanol']
          PercentageBioetanol = PercentageBioetanol.replace(",", ".")
          if PercentageBioetanol == '':
             PercentageBioetanol = float('nan')
          df2.at[i, 'PercentageBioetanol'] = PercentageBioetanol

          PercentageMethilEster = df2.iloc[i]['PercentageMethilEster']
          PercentageMethilEster = PercentageMethilEster.replace(",", ".")
          if PercentageMethilEster == '':
             PercentageMethilEster = float('nan')
          df2.at[i, 'PercentageMethilEster'] = PercentageBioetanol

       #we convert to date, string and to float now that it contains a . instedad of a ,
       convert_dict = {'PostalCode': str,
                       'Address': str,
                       'Schedule': str,
                       'Margin': str,
                       'Town': str,
                       'Location': str,
                       'Latitude': float,
                       'Longitude': float,
                       'PriceBiodiesel': float,
                       'PriceBioetanol':float,
                       'PriceCompressedNaturalGas': float,
                       'PriceLiquefiedNaturalGas': float,
                       'PriceLiquefiedPetroleumGases': float,
                       'PriceDieselA': float,
                       'PriceDieselB': float,
                       'PriceDieselPremium': float,
                       'PriceFuel95E10': float,
                       'PriceFuel95E5': float,
                       'PriceFuel95E5Premium':float,
                       'PriceFuel98E10': float,
                       'PriceFuel98E5': float,
                       'PriceHydrogen': float,
                       'Province': str,
                       'Referral': str,
                       'Label':str,
                       'TypeSale': str,
                       'PercentageBioetanol': float,
                       'PercentageMethilEster': float,
                       'IdStation': str,
                       'IdProvince': str,
                       'IdAutonomousCommunity': str,
                       'Date': str,
                       'Notes': str,
                       'QueryResult': str
                       }

       df2 = df2.astype(convert_dict)
    return (df2)

# -------------------------------------------------------------------------------------------------------
#dfGaPrices=takeGasPrices('')
#print("--- Data Types: ---")
#print(dfGaPrices.dtypes)
#print(dfGaPrices.describe(include='all').to_string())
#print(dfGaPrices.head().to_string())