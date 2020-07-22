import mcprotocol
from mcprotocol.classes import CpuType, Protocol
from mcprotocol.fxdevice import FxDevice, FxDataType
import mcprotocol.tools as tools
from mcprotocol.classes import MCCommand, Protocol, EtherFrame, SerialFrameID, CPUSeries
import time


mcprotocol.config.DESTINATION_IP = '192.168.3.166'
mcprotocol.config.DESTINATION_PORT = 1281
mcprotocol.config.PROTOCOL = Protocol.TCP_IP
mcprotocol.config.ETHERNET_FRAME = EtherFrame.Ether_1E

mcprotocol.config.CPU_SERIES = CPUSeries.iQ_F
mc_proc = mcprotocol.MCProtocol(cpu_type= CpuType.FX5UCPU)


# ret_wr = mc_proc.set_device('D100', 5, FxDataType.Signed16)    
while True:
    
    
#    ret_wr = mc_proc.set_device('M70',0 , FxDataType.Bit)
#    ret_wr = mc_proc.set_device('M71', 1, FxDataType.Bit)
#    ret_rd = mc_proc.get_device('Y39', FxDataType.Bit)
#    ret_rd1 = mc_proc.get_device('Y40', FxDataType.Bit)
    ret_rd=mc_proc.get_device('M70', FxDataType.Bit)
    #ret_rd1= mc_proc.get_device('M57', FxDataType.Bit)

    print (ret_rd)
    #print (ret_rd1)

    print ("\n")
    time.sleep(3)



# unit_buff_rd = mc_proc.get_device('U1\\G70', FxDataType.Signed16)
