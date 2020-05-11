# Importing necessary libraies
import mcprotocol
from mcprotocol import CpuType, Protocol
from mcprotocol.fxdevice import FxDevice, FxDataType
import mysql.connector
import threading
import logging
import concurrent.futures
import time
from datetime import datetime

# Function called for each plc
def thread_function1(plc):

    plc_id=plc[0]
    plc_ip=plc[1]
    plc_port=plc[2]

    # Continue running until user/system terminates program (Ctrl+C)
    while True:

        # Connect to PLC to read/insert values from/into table Device_Description/Device_Log
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="Memak123",
        database="mydb"
        )

        # Tries the program if connection established and passes to loop otherwise
        try:

            # Runs program for each plc based on their specified interval
            time.sleep(plc[4]/1000)

            # Establishing connection to PLC
            mcprotocol.config.DESTINATION_IP = plc_ip
            mcprotocol.config.DESTINATION_PORT = plc_port
            mcprotocol.config.PROTOCOL = Protocol.TCP_IP

            # Query to read addresses from table Device_Description
            mycursor2 = mydb.cursor()
            mycursor2.execute("SELECT * FROM Device_Description WHERE PLC_ID=" + str(plc_id))
            address_array = mycursor2.fetchall()
            
            # Read value of each address from PLC
            for address in address_array:
                mc_proc = mcprotocol.MCProtocol(cpu_type= CpuType.FX5UCPU)
                address_value = mc_proc.get_device(address[1], FxDataType.Signed16)

                # Query to read values of current address from table Device_Log and select the last one
                mycursor3 = mydb.cursor()
                mycursor3.execute("SELECT Value FROM Device_Log WHERE PLC_ID=" + str(address[0]) + " AND Label='" + str(address[1]) + "'")

                eval_result = mycursor3.fetchall()
                last_value = eval_result[len(eval_result) - 1][0]

                # Check if last value and current value are different
                if last_value != address_value[0]:

                    # Record current date and time
                    now = datetime.now()
                    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')

                    # Query to insert current value into table Device_Log
                    mycursor4 = mydb.cursor()
                    mycursor4.execute('INSERT INTO Device_Log (PLC_ID, Label, Value, Date_Time) VALUES(%s, %s, %s, %s)', (address[0], address[1], address_value[0], formatted_date))
                    mydb.commit()

                    # Show message in terminal
                    logging.info("Value %d inserted to PLC %d label %s at %s", address_value[0], address[0], address[1], formatted_date)
        except:
            print("unable to connect to plc ",plc_id)
            pass

        # Close connection to mysql
        mydb.close()

if __name__ == "__main__":

    # Connect to PLC to read values from table PLCs
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="Memak123",
    database="mydb"
    )

    # Format the message shown in terminal
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    # Query to read values form table PLCs
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM PLCs")
    plcs_array = mycursor.fetchall()
    
    # Parallel thread for each plc in table PLCs
    executor = concurrent.futures.ProcessPoolExecutor(10)
    futures = [executor.submit(thread_function1, item) for item in plcs_array]
    concurrent.futures.wait(futures)