import pandas as pd
import datetime
import json
import requests
import os
import dotenv
import time
import mqtt_db_service as service

def getDataloggerData(ip,table,queryStepTime,queryEndTime):
    try:
        url = f'http://{ip}/?command=dataquery&uri=dl:{table}&format=json&mode=date-range&p1={queryStepTime.strftime("%Y-%m-%dT%H:%M:%S")}&p2={queryEndTime.strftime("%Y-%m-%dT%H:%M:%S")}'
        print(url)
        data = {
            'report' : "Success",
            'loggerRequestBeginTime' : datetime.datetime.now().isoformat()
        }
        getData = requests.get(url,timeout=3) # Caso ocorra um timeout o que fazer? (fazer hoje)
        data['loggerRequestEndTime'] = datetime.datetime.now().isoformat()
        data_json = json.loads(getData.text)
        if data_json["data"] == []: #No data recevied continue search
            data['df_data'] = pd.DataFrame()
            data['report'] = "No data in packet"
            return data
        if "more" in data_json: # Datalogger did not return all asked data
            data['report'] = "missing query lines"
        df_data = pd.DataFrame(data_json['data'])
        cols = pd.DataFrame(data_json['head']['fields'])['name'].to_list()
        df_data[cols] = pd.DataFrame(df_data['vals'].to_list(), index=df_data.index)
        df_data = df_data.drop(columns=['vals'])
        df_data = df_data.rename(columns={'time': 'TIMESTAMP', 'no': 'RecNbr'})
        df_data['TIMESTAMP'] = pd.to_datetime(df_data['TIMESTAMP'])
        df_data = fixing(df_data)
        data['df_data'] = df_data
        data['loggerProcessingEndTime'] = datetime.datetime.now().isoformat()
        
    except requests.Timeout:
        print("Timeout occurred when trying to fetch data.")
        data['report'] = "Timeout occurred"
        return data

    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        data['report'] = "Request error"
        return data

    except json.JSONDecodeError:
        print("Error decoding JSON response.")
        data['report'] = "Invalid JSON response"
        return data

    return data

def fixing(d_frame):
    names = []
    mydict = {'Datetime': 'TIMESTAMP'}
    for current, column in enumerate(d_frame):
        names.append(str(column))
        if names[current][0] == 'b':
            names[current] = names[current][1:]
            names[current] = names[current].strip("''")
            mydict[column] = names[current]
    d_frame.rename(columns=mydict, inplace=True)
    d_frame = d_frame.fillna(-9999)

    return d_frame

def getLoggerTabeNames(ip):
    #get datalogger tables
    url = f'http://{ip}/?command=dataquery&uri=dl:DataTableInfo.DataTableName&format=json&mode=most-recent'
    request_data = requests.get(url,timeout=3)
    value = json.loads(request_data.text)
    tables = value['data'][0]['vals']
    return tables

def getLoggerCurrentTime(ip):
    url = f'http://{ip}/?command=dataquery&uri=dl:Status.MeasureTime&format=json&mode=most-recent'    
    request_data = requests.get(url,timeout=3)
    value = json.loads(request_data.text)
    date_str = value['data'][0]['time']
    date_str = date_str[:19] + date_str[19:].replace('.', ':', 1) + '0'  # Adiciona um dígito de milissegundo
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S:%f')
    queryEndTime = date_obj - datetime.timedelta(minutes=1)
    return queryEndTime

def healthCheck():
    # Código crítico
    with open('/tmp/heartbeat.txt', 'w') as f:
        f.write(str(time.time()))  # Escreve o timestamp

dotenv.load_dotenv()
brokers = os.getenv("MQTT_BROKER").split(',')
service.initDBService(user=os.getenv("USER"), server1=brokers[0], server2=brokers[1])
ips = os.getenv("IPS").split(',')

#fazer while de tempo
while  True:
    for ip in ips:
        datalogger = {"ip":ip,"tables":[]}

        tables = getLoggerTabeNames(ip)
        queryEndTime = getLoggerCurrentTime(ip)
        
        for table in tables:
            print(table)
            
            lastTime = service.getLastTimestamp(table=table) # está dando crash caso o servidor não tenha a tabela
            if lastTime == "mqtt timeout":
                print("getting table last timestamp timeout")
                continue
            if lastTime == None: # table dont exist
                print(f'This table: {table} dont exist on server please create table') # Criar automáticamente no futuro?
                continue
            print(lastTime)
            queryStartTime = lastTime + datetime.timedelta(seconds=1)
            #queryStartTime = datetime.datetime.strptime("2024-08-10T00:00:00", '%Y-%m-%dT%H:%M:%S') #Para recuperar dados
            queryStepTime = queryStartTime

            #df_data = pd.DataFrame()
            while queryStepTime < queryEndTime:
                queryStepEnd = queryStepTime + datetime.timedelta(minutes=5)
                
                if queryStepEnd>queryEndTime: # na chamada mais recente utilizar o valor final do logger
                    queryStepEnd=queryEndTime
                
                data = getDataloggerData(ip,table,queryStepTime,queryStepEnd)
                if data["df_data"].empty: # caso o logger nao tenha retornado data !!!(verificar essa implementação)!!!
                    queryStepTime = queryStepEnd + datetime.timedelta(seconds=1)
                    print("empty data")
                    healthCheck()
                    continue

                if data["report"] == "missing query lines": # caso o logger tenha enviado menos dados do que solicitado
                    #print(data)
                    lastLoggerTimestamp = data["df_data"].iloc[-1]["TIMESTAMP"].to_pydatetime()
                    response = service.sendDF(data, table=table)
                    print(response)
                    if response == "mqtt timeout":
                        print("Sending data to mqtt timeout")
                        continue
                    queryStepTime = lastLoggerTimestamp + datetime.timedelta(seconds=1) #if datalogger did not send all data make new query follow the last sended data
                    time.sleep(0.2)
                    healthCheck()
                    continue

                if data["report"] == "Request error":
                    continue

                if data["report"] == "Invalid JSON response":
                    continue

                if data["report"] == "Timeout occurred":
                    continue

                if data["report"] == "Success":
                    response = service.sendDF(data, table=table)
                    print(response)
                    if response == "mqtt timeout":
                        print("Sending data to mqtt timeout")
                        continue
                    queryStepTime = queryStepEnd + datetime.timedelta(seconds=1)
                    time.sleep(0.2)
                    healthCheck()
                    continue

                print(f'unknown error ocurred: {data["report"]}')

    print("waiting 15 min to get new measurements")
    time.sleep(900)


