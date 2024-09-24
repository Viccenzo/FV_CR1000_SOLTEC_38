import pandas as pd
import paho.mqtt.client as mqtt
import datetime
import time
import json

client1 = mqtt.Client()
client2 = mqtt.Client()
topicUser = ""
return_value = None

def dataframeExample():
    data = {
        "TIMESTAMP": ["2024-07-30 20:01:48","2024-07-30 20:01:44","2024-07-30 20:01:49"],
        "calories": [420, 380, 390],
        "duration": [50, 40, 45]
    }
    return pd.DataFrame(data)


def on_callback(client, userdata, msg):
    global return_value
    return_value = msg.payload

def on_connect(client, userdata, flags, rc):
    #print(f"Conectado com o código de resultado {rc}")
    client.subscribe(f'message/{userdata}/#')  # Subscrição ao tópico passado via userdata

def initDBService(user,server1,server2):
    global client1
    global client2
    global topicUser
    
    topicUser = user

    #client1
    client1 = mqtt.Client(userdata=user)
    mqtt_broker = server1 #"58296fae6ca74b90bda9fc67e3646310.s1.eu.hivemq.cloud"
    mqtt_port = 1883  #8883
    client1.connect(mqtt_broker, mqtt_port)
    client1.on_message = on_callback
    client1.on_connect = on_connect
    client1.loop_start()
    
    #client2
    #mqtt_broker = server2 #"58296fae6ca74b90bda9fc67e3646310.s1.eu.hivemq.cloud"
    #mqtt_port = 1883  #8883
    #client2.connect(mqtt_broker, mqtt_port)
    #client2.on_message = callback
    #client2.on_connect = on_connect
    #client2.loop_start()

def sendDF(data,table):
    global client1
    global client2
    global topicUser
    global return_value

    user = topicUser

    if not isinstance(data['df_data'],pd.DataFrame):
        return ("pandas dataframe is not correct")
    if not user:
        return ("missing user argument")
    if not isinstance(user, str):
        return ("user should be of type string")
    if not table:
        return ("missing user argument")
    if not isinstance(table, str):
        return ("user should be of type string")

    data['df_data'] = data['df_data'].to_json()
    #print(data)
    try:    
        client1.publish(f'DB_INSERT/{user}/{table}', json.dumps(data), qos=1)
    except Exception as e:
        print(e)
        #client2.publish(f'DB_INSERT/{user}/{table}', json.dumps(data), qos=1)
        #return "Your db insertion was send to backend through secondary gateway. please inform service provider"
    
    timeoutCount = 0
    while return_value is None:
        time.sleep(0.01)
        if timeoutCount > 300: # timeout de 3 segundos
            return ("mqtt timeout")
    response = return_value
    return_value = None
    return (response)

def getLastTimestamp(table):
    global client1
    global client2
    global topicUser
    global return_value

    user = topicUser
    
    if not user:
        return ("missing user argument")
    if not isinstance(user, str):
        return ("user should be of type string")
    if not table:
        return ("missing user argument")
    if not isinstance(table, str):
        return ("user should be of type string")
    
    try:
        client1.publish(f'DB_GERT_RECENT_ROW/{user}/{table}', "", qos=1)
    except:
        print("erro")
    timeoutCount = 0
    while return_value is None:
        time.sleep(0.01)
        timeoutCount += 1
        if timeoutCount > 300: # timeout de 3 segundos
            return ("mqtt timeout")
        
    response = return_value
    return_value = None
    response = response.decode('utf-8')
    response = response.strip('(),')
    response = response + ")"
    try: 
        response = eval(response, {"datetime": datetime})
        return (response)
    except:
        return (None)

    
