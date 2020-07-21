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
import json

# Function called for each plc
def thread_function1(plc):

    # Continue running until user/system terminates program (Ctrl+C)
    while True:

        # Connect to PLC to read/insert values from/into table Device_Description/Device_Log
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        database="mydb"
        )
        
        # Tries the program if connection established and passes to loop otherwise
        try:
            # Runs program for each plc based on their specified interval
            time.sleep(plc[4]/1000)

            # Establishing connection to PLC
            mcprotocol.config.DESTINATION_IP = plc[1]
            mcprotocol.config.DESTINATION_PORT = plc[2]
            mcprotocol.config.PROTOCOL = Protocol.TCP_IP
            mc_proc = mcprotocol.MCProtocol(cpu_type= CpuType.FX5UCPU)
	
            # Query to read addresses from table Device_Description
            mycursor2 = mydb.cursor()
            mycursor2.execute("SELECT * FROM Device_Description WHERE PLC_ID=" + str(plc[0]))
            address_array = mycursor2.fetchall()

            # Read value of each address from PLC
            for address in address_array:
                address_value = mc_proc.get_device(address[1], FxDataType.Signed16)
<<<<<<< HEAD
                # print(address[1], address_value)
                calc_col = address[4]
                if calc_col is not None:
                    replaced_calc_col = calc_col.replace("x",str(address_value[0]))
                    replaced_calc_col_result = eval(replaced_calc_col)
                    # print(replaced_calc_col_result)
                    address_value[0] = replaced_calc_col_result
=======
>>>>>>> 8fd66363b9aece904df8c90c3b34f3b66034f833

                if address_value is None:
                    print("unable to connect to plc ", plc[0])
                    break
                else:
                    # Query to read values of current address from table Device_Log and select the last one
                    mycursor3 = mydb.cursor()
                    mycursor3.execute("SELECT Value FROM Device_Log WHERE PLC_ID=" + str(address[0]) + " AND Label='" + str(address[1]) + "'")
                    eval_result = mycursor3.fetchall()

                    if len(eval_result) <= 0:
                        # Connect to PLC to read/insert values from/into table Device_Description/Device_Log
                        mydb3 = mysql.connector.connect(
                            host="localhost",
                            user="root",
                            database="mydb"
                            )

                        # Record current date and time
                        now = datetime.now()
                        formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')

                        # Query to insert current value into table Device_Log
                        mycursor4 = mydb3.cursor()
                        mycursor4.execute('INSERT INTO Device_Log (PLC_ID, Label, Value, Date_Time) VALUES(%s, %s, %s, %s)', (address[0], address[1], address_value[0], formatted_date))
                        mydb3.commit()

                        # Show message in terminal
<<<<<<< HEAD
                        logging.info("Value %.2f inserted to PLC %d label %s at %s", address_value[0], address[0], address[1], formatted_date)
=======
                        logging.info("Value %d inserted to PLC %d label %s at %s", address_value[0], address[0], address[1], formatted_date)
>>>>>>> 8fd66363b9aece904df8c90c3b34f3b66034f833
                    else:
                        last_value = eval_result[len(eval_result) - 1][0]

                        # Check if last value and current value are different
                        if last_value != address_value[0]:

                            # Connect to PLC to read/insert values from/into table Device_Description/Device_Log
                            mydb3 = mysql.connector.connect(
                                host="localhost",
                                user="root",
                                database="mydb"
                                )

                            # Record current date and time
                            now = datetime.now()
                            formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')

                            # Query to insert current value into table Device_Log
                            mycursor4 = mydb3.cursor()
                            mycursor4.execute('INSERT INTO Device_Log (PLC_ID, Label, Value, Date_Time) VALUES(%s, %s, %s, %s)', (address[0], address[1], address_value[0], formatted_date))
                            mydb3.commit()

                            # Show message in terminal
<<<<<<< HEAD
                            logging.info("Value %.2f inserted to PLC %d label %s at %s", address_value[0], address[0], address[1], formatted_date)
=======
                            logging.info("Value %d inserted to PLC %d label %s at %s", address_value[0], address[0], address[1], formatted_date)
>>>>>>> 8fd66363b9aece904df8c90c3b34f3b66034f833
                        else:
                            continue

            # Close connection to mysql
            mydb.close()
            
        except Exception as exc:
            print(exc)
            pass


if __name__ == "__main__":

    # Read database info from json file
    with open('databse_info.json') as json_file:
        database_info = json.load(json_file)

    # Connect to database
    mydb = mysql.connector.connect(
        host = database_info['host'],
        user = database_info['user'],
        database = database_info['database']
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
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=60)
    futures = [executor.submit(thread_function1, item) for item in plcs_array]
    concurrent.futures.wait(futures)

