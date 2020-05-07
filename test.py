import mcprotocol
from mcprotocol.classes import CpuType, Protocol
from mcprotocol.fxdevice import FxDevice, FxDataType


mcprotocol.config.DESTINATION_IP = '192.168.3.248'
mcprotocol.config.DESTINATION_PORT = 1281
mcprotocol.config.PROTOCOL = Protocol.TCP_IP



mc_proc = mcprotocol.MCProtocol(cpu_type= CpuType.FX5UCPU)


ret_wr = mc_proc.set_device('D100', 5, FxDataType.Signed16)    
ret_rd = mc_proc.get_device('D100', FxDataType.Signed16)
print (ret_rd)



unit_buff_rd = mc_proc.get_device('U1\\G70', FxDataType.Signed16)
