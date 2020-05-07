import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest
import struct

import mcprotocol
from mcprotocol import CpuType, Protocol
from mcprotocol.fxdevice import FxDevice, FxDataType
import time


# この様に設定する
mcprotocol.config.DESTINATION_IP = '192.168.3.248'
mcprotocol.config.DESTINATION_PORT = 1281
mcprotocol.config.PROTOCOL = Protocol.TCP_IP

# CPU 毎に通信プロトコルが異なるので、
# クラス生成時に CPU情報を入れる事でプロトコル判断を行う
mc_proc = mcprotocol.MCProtocol(cpu_type= CpuType.FX5UCPU)

# 単一デバイスの読み書き
while True:

    address_value = mc_proc.get_device('Y3', FxDataType.Signed16)
    address_value1 = mc_proc.get_device('Y4', FxDataType.Signed16)
    print (address_value)
    print (address_value1)
    time.sleep(5)
    # > [123]
