import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest
import struct

import mcprotocol
from mcprotocol import CpuType, Protocol
from mcprotocol.fxdevice import FxDevice, FxDataType

import mysql.connector

import time
import threading
import logging
import concurrent.futures

from datetime import datetime


# def thread_function(name):
#     mydb = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     passwd="Memak123",
#     database="mydb"
#     )

#     plc_id = name[0]
#     name_label = name[1]

#     while True:

#         time.sleep(name[2]/500)
#         mc_proc = mcprotocol.MCProtocol(cpu_type= CpuType.FX5UCPU)
#         address_value = mc_proc.get_device(name_label, FxDataType.Signed16)
#         print(name_label,"\t",address_value)

    # if address_value is not None:
    #     mycursor3 = mydb.cursor()

    #     mycursor3.execute("SELECT Value FROM Device_Log WHERE PLC_ID=" + str(plc_id) + " AND Label='" + str(name_label) + "'")

    #     eval_result = mycursor3.fetchall()
    #     last_value = eval_result[len(eval_result) - 1][0]

    #     if last_value != address_value[0]:
    #         now = datetime.now()
    #         formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
    #         mycursor4 = mydb.cursor()

    #         mycursor4.execute('INSERT INTO Device_Log (PLC_ID, Label, Value, Date_Time) VALUES(%s, %s, %s, %s)', (plc_id, name_label, address_value[0], formatted_date))
    #         mydb.commit()

    #         logging.info("Value %d inserted to PLC %d label %s at %s", address_value[0], plc_id, name_label, formatted_date)

def thread_function1(plc):

    plc_id=plc[0]
    plc_ip=plc[1]
    plc_port=plc[2]

    while True:
         
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="Memak123",
        database="mydb"
        )
        try:
            mcprotocol.config.DESTINATION_IP = plc_ip
            mcprotocol.config.DESTINATION_PORT = plc_port
            mcprotocol.config.PROTOCOL = Protocol.TCP_IP


            mycursor2 = mydb.cursor()

            mycursor2.execute("SELECT * FROM Device_Description WHERE PLC_ID=" + str(plc_id))

            address_array = mycursor2.fetchall()

        
        
            time.sleep(plc[4]/1000)
            
            for address in address_array:
                mc_proc = mcprotocol.MCProtocol(cpu_type= CpuType.FX5UCPU)
                address_value = mc_proc.get_device(address[1], FxDataType.Signed16)
                # print(address[0],"\t",address[1],"\t",address_value)

                mycursor3 = mydb.cursor()

                mycursor3.execute("SELECT Value FROM Device_Log WHERE PLC_ID=" + str(address[0]) + " AND Label='" + str(address[1]) + "'")

                eval_result = mycursor3.fetchall()
                last_value = eval_result[len(eval_result) - 1][0]

                if last_value != address_value[0]:
                    now = datetime.now()
                    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
                    mycursor4 = mydb.cursor()

                    mycursor4.execute('INSERT INTO Device_Log (PLC_ID, Label, Value, Date_Time) VALUES(%s, %s, %s, %s)', (address[0], address[1], address_value[0], formatted_date))
                    mydb.commit()

                    logging.info("Value %d inserted to PLC %d label %s at %s", address_value[0], address[0], address[1], formatted_date)
        except:
            print("unable to connect to plc ",plc_id)
            pass
            

    # while True:
    #     executor2 = concurrent.futures.ProcessPoolExecutor(20)
    #     futures2 = [executor2.submit(thread_function, item) for item in address_array]
    #     concurrent.futures.wait(futures2)

        

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
    
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="Memak123",
    database="mydb"
    )


    mycursor = mydb.cursor()

    mycursor.execute("SELECT * FROM PLCs")

    plcs_array = mycursor.fetchall()
    
    executor = concurrent.futures.ProcessPoolExecutor(10)
    futures = [executor.submit(thread_function1, item) for item in plcs_array]
    concurrent.futures.wait(futures)

