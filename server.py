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
import re

# Check if string contains a special charater
def special_match(strg, search=re.compile(r'-').search):
    return bool(search(strg))

# Function called for each plc
def thread_function1(plc):

    # Continue running until user/system terminates program (Ctrl+C)
    while True:

        # Connect to PLC to read/insert values from/into table Device_Description/address_table_name
        mydb = mysql.connector.connect(
        host="localhost",
        user="user",
        password = "Zdopfut89.",
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
            mycursor2.execute("SELECT * FROM Device_Description WHERE PLC_ID = " + str(plc[0]))
            address_array = mycursor2.fetchall()

            # Read value of each address from PLC
            for address in address_array:
                time.sleep(0.004) 
                address_table_name = address[5]
                is_address_interval = special_match(address[1])
                # Write string
                if is_address_interval:
                    addresses = address[1].split('-')
                    address_start = addresses[0]
                    address_end = addresses[1]
                    address_char = 'D'
                    addresses_str = ''
                    for address_int in range(int(address_start[1:]), int(address_end[1:])+1):
                        joined_address = address_char + str(address_int)
                        address_value = mc_proc.get_device(joined_address, FxDataType.Signed16)
                        if address_value[0] >= 0:
                            first_byte_number = address_value[0] >> 8
                            second_byte_number = address_value[0] & 255
                            addresses_str += chr(second_byte_number) + chr(first_byte_number)
                    # print(addresses_str)
                else:
                    if address[1][0] == "Y" or address[1][0] == "X":
                        address_oct = address[1][1:]
                        address_dec = int(address_oct, 8)
                        if address[1][0] == "Y":
                            address_new = 'Y' + str(address_dec)
                        else:
                            address_new = 'X' + str(address_dec)
                        address_value = mc_proc.get_device(address_new, fx_data_type = FxDataType.Bit) 
                    elif address[1][0] == "M":
                        address_value = mc_proc.get_device(address[1], fx_data_type = FxDataType.Bit)
                    else:
                        address_value = mc_proc.get_device(address[1], FxDataType.Signed16)
                    #print(address[1], address_value)

                    calc_col = address[4]
                    if calc_col is not None:
                        replaced_calc_col = calc_col.replace("x",str(address_value[0]))
                        replaced_calc_col_result = eval(replaced_calc_col)
                        address_value[0] = int(replaced_calc_col_result)

                    if address_value is None:
                        print("unable to connect to plc ", plc[0])
                        break                        
                    if int(address_value[0]) < 0:
                        address_value[0] = 0    
                    elif address_table_name == "Device_Log":
                        # Query to read values of current address from table Device_Log and select the last one
                        mycursor3 = mydb.cursor()
                        mycursor3.execute("SELECT Value FROM Device_Log WHERE PLC_ID=" + str(address[0]) + " AND Label='" + str(address[1]) + "'")
                        eval_result = mycursor3.fetchall()

                        if len(eval_result) <= 0:
                            # Connect to PLC to read/insert values from/into table Device_Description/Device_Log
                            mydb3 = mysql.connector.connect(
                                host="localhost",
                                user="user",
                                password = "Zdopfut89.",
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
                            logging.info("Value %.2f inserted to PLC %d label %s at %s", float(address_value[0]), address[0], address[1], formatted_date)
                            # logging.info("Value %d inserted to PLC %d label %s at %s", address_value[0], address[0], address[1], formatted_date)

                        else:
                            last_value = eval_result[len(eval_result) - 1][0]

                            # Check if last value and current value are different
                            if last_value != float(address_value[0]):
                                # Connect to PLC to read/insert values from/into table Device_Description/Device_Log
                                mydb3 = mysql.connector.connect(
                                    host="localhost",
                                    user="user",
                                    password = "Zdopfut89.",
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
                                logging.info("Value %.2f inserted to PLC %d label %s at %s", float(address_value[0]), address[0], address[1], formatted_date)
                                # logging.info("Value %d inserted to PLC %d label %s at %s", address_value[0], address[0], address[1], formatted_date)

                                if address[3] and address_value[0]:
                                    mydb4 = mysql.connector.connect(
                                        host="localhost",
                                        user="user",
                                        password = "Zdopfut89.",
                                        database="mydb"
                                        )
                                    # Query to read addresses from table Device_Description
                                    mycursor5 = mydb4.cursor()
                                    mycursor5.execute("SELECT * FROM Alarms WHERE PLC_ID = " + str(plc[0]))
                                    alarms_address_array = mycursor5.fetchall()
                                    
                                    for alarm_address in alarms_address_array:
                                        alarm_address_value = mc_proc.get_device(alarm_address[1], fx_data_type = FxDataType.Bit)
                                        print(alarm_address_value)
                                        if alarm_address_value[0]:
                                            # Query to insert current value into table Device_Log
                                            mycursor6 = mydb4.cursor()
                                            mycursor6.execute('INSERT INTO Alarms_Log (PLC_ID, Label, Value, Date_Time) VALUES(%s, %s, %s, %s)', (alarm_address[0], alarm_address[1], alarm_address_value[0], formatted_date))
                                            mydb4.commit()
                                else:
                                    continue
                            else:
                                continue
                    elif address_table_name == "Raw":
                        # Need to write to table Recipes
                        # Query to read values of current address from table Raw and select the last one
                        # if address[1] == 'D2020':
                        #     st_year = address_value[0]
                        # print(st_year)
                        
                        
                        mycursor3 = mydb.cursor()
                        mycursor3.execute("SELECT Value FROM Raw WHERE PLC_ID=" + str(address[0]) + " AND Label='" + str(address[1]) + "'")
                        eval_result = mycursor3.fetchall()

                        if len(eval_result) <= 0:
                            # Connect to PLC to read/insert values from/into table Device_Description/Raw
                            mydb3 = mysql.connector.connect(
                                host="localhost",
                                user="user",
                                password = "Zdopfut89.",
                                database="mydb"
                                )

                            # Record current date and time
                            now = datetime.now()
                            formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')

                            
                            # Query to insert current value into table Device_Log
                            mycursor4 = mydb3.cursor()
                            mycursor4.execute('INSERT INTO Raw (PLC_ID, Label, Date_Time, Value) VALUES(%s, %s, %s, %s)', (address[0], address[1],  formatted_date, address_value[0]))
                            mydb3.commit()

                            # Show message in terminal
                            logging.info("Value %.2f inserted to table Raw PLC %d label %s at %s", float(address_value[0]), address[0], address[1], formatted_date)
                            # logging.info("Value %d inserted to PLC %d label %s at %s", address_value[0], address[0], address[1], formatted_date)
                            
                        else:
                            last_value = eval_result[len(eval_result) - 1][0]

                            # Check if last value and current value are different
                            if last_value != float(address_value[0]):
                                # Connect to PLC to read/insert values from/into table Device_Description/Device_Log
                                mydb3 = mysql.connector.connect(
                                    host="localhost",
                                    user="user",
                                    password = "Zdopfut89.",
                                    database="mydb"
                                    )

                                # Record current date and time
                                now = datetime.now()
                                formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')

                                    # Query to insert current value into table Device_Log
                                mycursor4 = mydb3.cursor()
                                mycursor4.execute('INSERT INTO Raw (PLC_ID, Label, Date_Time, Value) VALUES(%s, %s, %s, %s)', (address[0], address[1],  formatted_date, address_value[0]))
                                mydb3.commit()

                                # Show message in terminal
                                logging.info("Value %.2f inserted to table Raw PLC %d label %s at %s", float(address_value[0]), address[0], address[1], formatted_date)
                                # logging.info("Value %d inserted to PLC %d label %s at %s", address_value[0], address[0], address[1], formatted_date)
                            else:
                                continue
            
            # cursor5 = mydb.cursor()
            # cursor5.callproc('shiftoku')
            # # Close connection to mysql
            # mydb.close()
            
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
        password = database_info['password'],
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

