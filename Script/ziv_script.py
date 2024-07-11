#LFEP Rev-1.0.0
#****************************
#COMPILE_TIME: 2024-05-08 09:31:32 UTC
#GIT BRANCH:
#GIT HASH: ee41ca6d9df29350653e2c0130d0e658dfeeb5b4
#****************************
#Licensing Required
#Fix for Same Alias across Multiple Schemes

#Update the Data header :     "period_call_function": "periodicFEP(OdataApi)"

import json
import time
from datetime import datetime
import random

#This imports are required to utilize DNP/ICCP/OData api
import PyEventType
import PyFepRole
import PyOdataCodes
import PyDnp3Codes
import PyIccpApi
import PyIccpTsState

# global array of list to cache data to be sent to Cimphony
g_iccp_client_events = {}
g_odata_tag_name = []
g_cim_sending_buffer = []
g_odata_api= []

g_ValidityMap = ["Valid", "Held", "Suspect", "Invalid"]
g_CurrentSourceMap = ["Telemetered", "Calculated", "Entered", "Estimated"]
g_NormalValueMap = ["Normal", "Abnormal"]
g_TimeStampQualityMap = ["Valid", "Invalid"]
g_iccp_api = None


def updateICCPResponse():
    Logger.pyLog("updateICCPResponse")
    if CurrentEvent.getEvent() == PyEventType.EVENT_ICCP_RSP_RECEIVED:
        Logger.pyLog("")
        Logger.pyLog("EVENT_ICCP_RSP_RECEIVED")
        Logger.pyLog("")
    elif CurrentEvent.getEvent() == PyEventType.EVENT_DB_RSP_RECEIVED:
        Logger.pyLog("")
        Logger.pyLog("EVENT_DB_RSP_RECEIVED")
        Logger.pyLog("")

    someString = CurrentEvent.getData()
    Logger.pyLog("Received Response: " + someString)
    #Here, the string may contain for EVENT_ICCP_RSP_RECEIVED
    #Received RSP: {"InvokeID":33,"OK":1}
    #For EVENT_DB_RSP_RECEIVED
    #Received RSP: ERROR / SUCCESS


def iccpClientSetVarResponse():
    Logger.pyLog("iccpClientSetVarResponse")
    if CurrentEvent.getEvent() == PyEventType.EVENT_ICCP_RSP_RECEIVED:
        Logger.pyLog("")
        Logger.pyLog("EVENT_ICCP_RSP_RECEIVED")
        Logger.pyLog("")
    elif CurrentEvent.getEvent() == PyEventType.EVENT_DB_RSP_RECEIVED:
        Logger.pyLog("")
        Logger.pyLog("EVENT_DB_RSP_RECEIVED")
        Logger.pyLog("")

    someString = CurrentEvent.getData()
    Logger.pyLog("Received Response: " + someString)


def getbufferindex(odata_tagname):
    i=0
    if g_odata_tag_name:  
        for tagname in g_odata_tag_name:
            if tagname==odata_tagname:
                return i
            i=i+1
    return -1
    
def periodicFEP():
    i=0
    if g_odata_tag_name:
        for odata_tagname in g_odata_tag_name:
            periodicFEPSpecific(odata_tagname, i)
            i=i+1
    
def periodicFEP(odataApi):
    i=0
    #Here construct single json array that will contain all tag updates for a given resource,
    #The json body that will be send to the Cimphony should be formatted like below, may contain the same and 
    #different __cid(s)
    #[
    #{
    #  "__cid":"_80fa8524-d449-4c32-8e4a-e29ce2c2b69e",
    #  "value":0,
    #  "timestamp":"2024-03-11T15:02:12.000+0000",
    #  "iccpValidity":"Valid",
    #  "iccpCurrentSource":"Telemetered",
    #  "iccpNormalValue":"Normal",
    #  "iccpTimeStamp":"Valid"
    # },
    # {
    #   "__cid":"_80fa8524-d449-4c32-8e4a-e29ce2c2b69e",
    #  "value":0,
    #  "timestamp":"2024-03-11T15:02:17.000+0000",
    #  "iccpValidity":"Valid",
    #  "iccpCurrentSource":"Telemetered",
    #  "iccpNormalValue":"Normal",
    #  "iccpTimeStamp":"Valid"
    #}
    #]

    #Map to gather resource as a key, and json payload with all the tag updates for cimphony as value
    odata_update_data = {}
    #Map to keep the reference between the resource_id and example odata tag that holds this resource_id, 
    #- will be later used by updateICCPNoBlock function
    odata_tag_name_to_obtain_resource = {}
    if g_odata_tag_name:
        for odata_tagname in g_odata_tag_name:
            #Obtain cimphony resource id
            resource_id = odataApi.getNetwork(odata_tagname)
            #Logger.pyLog("Resource_id: " + resource_id)
            #Check whether the resource is in the map, concatanate json payload or create new map entry for the new resource
            if resource_id in odata_update_data:
                odata_update_data[resource_id] += g_cim_sending_buffer[i]
           
            else: 
                odata_update_data[resource_id] = g_cim_sending_buffer[i] 
            
                #Fill the resource_id and example odata_tag name
                odata_tag_name_to_obtain_resource[resource_id] = odata_tagname
            g_cim_sending_buffer[i] = []
            i=i+1
        
        #Send cimphony update for each resource id
        for key in odata_update_data:
            #Logger.pyLog("odata_update_data: " + key)
            #Logger.pyLog("odata_update_data: " + value)
            sendAllTagDataToTheSameOdataResourceAtOnce(odata_tag_name_to_obtain_resource[key], odata_update_data[key], odataApi)

def sendAllTagDataToTheSameOdataResourceAtOnce(odata_tag_to_obtain_resource, odata_update, odataApi):
    Logger.pyLog("** sendAllTagDataToTheSameOdataResourceAtOnce **")
    #Logger.pyLog("sendAllTagDataToTheSameOdataResourceAtOnce for resource" + odataApi.getNetwork(odata_tag_to_obtain_resource))
    json_body = json.dumps(odata_update)
    Logger.pyLog("json body: " + str(json_body))
    updateIcppRes = odataApi.updateICCPNoBlock(odata_tag_to_obtain_resource, json.dumps(odata_update), 2000,"updateICCPResponse()")

def handleICCPClientEvents():
    Logger.pyLog("** handleICCPClientEvents **")
    global g_iccp_client_events
    global g_iccp_api
    if g_iccp_client_events:
        client_connected = False
        for tagKey, single_event in g_iccp_client_events.items():
            iccp_out_tag = single_event[0]
            clientname = single_event[1]
            rcvValue = single_event[2]
            iccpApi = g_iccp_api
            stringVect = iccpApi.getPeerList(clientname);
            Logger.pyLog(clientname + " " + str(stringVect))
            if len(stringVect) > 0:
                client_connected = True
                if type(rcvValue) is float:
                    peer = stringVect[0]
                    Logger.pyLog("periodicFEP iccp api control clientSetVar ")
                    realPod = PyIccpApi.CTASE2DeviceRealPOD()
                    realPod.value = rcvValue
                    Logger.pyLog("periodicFEP iccp api control clientSetVar " + iccp_out_tag.getName())
                    clientSetVarReply = iccpApi.clientSetVarNoBlock(clientname, peer, iccp_out_tag.getName(), realPod, 5, "iccpClientSetVarResponse()")
                    Logger.pyLog("periodicFEP iccp api control clientSetVar Reply: " + clientSetVarReply)
                    # empty strings are falsey
                    if clientSetVarReply :
                        dicRply = json.loads(clientSetVarReply)
                        dKeys = list(dicRply.keys())
                        # also write the value to the client as well
                        if dKeys.count("OK") > 0:
                            iccp_out_tag.setICCPObj(realPod)
                elif type(rcvValue) is int:
                    peer = stringVect[0]
                    Logger.pyLog("periodicFEP iccp api control clientSetVar ")
                    discPod = PyIccpApi.CTASE2DeviceDiscretePOD()
                    discPod.value = rcvValue
                    Logger.pyLog("periodicFEP iccp api control clientSetVar " + iccp_out_tag.getName())
                    clientSetVarReply = iccpApi.clientSetVarNoBlock(clientname, peer, iccp_out_tag.getName(), discPod, 5, "iccpClientSetVarResponse()")
                    Logger.pyLog("periodicFEP iccp api control clientSetVar Reply: " + clientSetVarReply)
                    # empty strings are falsey
                    if clientSetVarReply :
                        dicRply = json.loads(clientSetVarReply)
                        dKeys = list(dicRply.keys())
                        # also write the value to the client as well
                        if dKeys.count("OK") > 0:
                            iccp_out_tag.setICCPObj(discPod)
            else:
                # if any of elements are not connected then skip processing
                client_connected = False
                break
        if client_connected:
            g_iccp_client_events = {}

def doNothing():
    Logger.pyLog("**doNothing**")
    if CurrentEvent.getEvent() == PyEventType.EVENT_DB_INIT:
        Logger.pyLog("doNothing PyEventType.EVENT_DB_INIT")
    else: 
        Logger.pyLog("doNothing unhandled event")

def derHandle():
    Logger.pyLog("**derHandle**")

def handleNotification(clientname, odata_tag, iccp_out_tag, iccpApi):
    Logger.pyLog("**handleNotification client**")
    if CurrentEvent.getEvent() == PyEventType.EVENT_DB_NOTIFICATION:
        data = CurrentEvent.getData()
        parsed = json.loads(data.getJson())
        Logger.pyLog("notification: " + json.dumps(parsed, indent=4, sort_keys=True))
        if "iccpPoints" in parsed:
            tagCID = odata_tag.getCid()
            if tagCID in parsed["iccpPoints"]:
                rcvValue = parsed["iccpPoints"][tagCID]
                odata_tag.setValueDouble(float(rcvValue))
                stringVect = iccpApi.getPeerList(clientname);
                Logger.pyLog(clientname + " " + str(stringVect))
                if len(stringVect) > 0:
                    peer = stringVect[0]
                    Logger.pyLog("iccp api control clientSetVar ")
                    realPod = PyIccpApi.CTASE2DeviceRealPOD()
                    realPod.value = float(rcvValue)
                    Logger.pyLog("iccp api control clientSetVar " + iccp_out_tag.getName())
                    clientSetVarReply = iccpApi.clientSetVarNoBlock(clientname, peer, iccp_out_tag.getName(), realPod, 5, "iccpClientSetVarResponse()")
                    Logger.pyLog("iccp api control clientSetVar Reply: " + clientSetVarReply)
                    # empty strings are falsey
                    if str(clientSetVarReply).count("OK") > 0:
                        iccp_out_tag.setICCPObj(realPod)
                else:
                    # client is not connected cache the event for later
                    global g_iccp_client_events
                    global g_iccp_api
                    single_event = [iccp_out_tag, clientname, float(rcvValue)]
                    g_iccp_client_events.update({ iccp_out_tag.getName(): single_event})
                    if not g_iccp_api :
                        g_iccp_api = iccpApi
            else:
                Logger.pyLog("tagCID not in iccpPoints " + tagCID)
        else:
            Logger.pyLog("iccpPoints is not in parsed")

def handleDiscreteNotification(clientname, odata_tag, iccp_out_tag, iccpApi):
    Logger.pyLog("**handleDiscreteNotification client**")
    if CurrentEvent.getEvent() == PyEventType.EVENT_DB_NOTIFICATION:
        data = CurrentEvent.getData()
        parsed = json.loads(data.getJson())
        Logger.pyLog("notification: " + json.dumps(parsed, indent=4, sort_keys=True))
        if "iccpPoints" in parsed:
            tagCID = odata_tag.getCid()
            if tagCID in parsed["iccpPoints"]:
                rcvValue = parsed["iccpPoints"][tagCID]
                odata_tag.setValueDouble(float(rcvValue))
                stringVect = iccpApi.getPeerList(clientname);
                Logger.pyLog(clientname + " " + str(stringVect))
                if len(stringVect) > 0:
                    peer = stringVect[0]
                    Logger.pyLog("iccp api control clientSetVar ")
                    discPod = PyIccpApi.CTASE2DeviceDiscretePOD()
                    discPod.value = int(rcvValue)
                    Logger.pyLog("iccp api control clientSetVar " + iccp_out_tag.getName())
                    clientSetVarReply = iccpApi.clientSetVarNoBlock(clientname, peer, iccp_out_tag.getName(), discPod, 5, "iccpClientSetVarResponse()")
                    Logger.pyLog("iccp api control clientSetVar Reply: " + clientSetVarReply)
                    # empty strings are falsey
                    if str(clientSetVarReply).count("OK") > 0:
                        iccp_out_tag.setICCPObj(discPod)
                else:
                    # client is not connected cache the event for later
                    global g_iccp_client_events
                    global g_iccp_api
                    single_event = [iccp_out_tag, clientname, int(rcvValue)]
                    g_iccp_client_events.update({ iccp_out_tag.getName(): single_event})
                    if not g_iccp_api :
                        g_iccp_api = iccpApi
            else:
                Logger.pyLog("tagCID not in iccpPoints " + tagCID)
        else:
            Logger.pyLog("iccpPoints is not in parsed")

def ODataToICCPClient_Float(clientname, odata_tag, iccp_out_tag, scale, iccpApi):
    handleNotification(clientname, odata_tag, iccp_out_tag, iccpApi)

def handleRealNotificationS(clientname,odata_tag, iccp_out_tag, iccpApi):
    Logger.pyLog("**handleRealNotification server**")
    if CurrentEvent.getEvent() == PyEventType.EVENT_DB_NOTIFICATION:
        data = CurrentEvent.getData()
        parsed = json.loads(data.getJson())
        Logger.pyLog("notification: " + json.dumps(parsed, indent=4, sort_keys=True))
        if "iccpPoints" in parsed:
            tagCID = odata_tag.getCid()
            if tagCID in parsed["iccpPoints"]:
                rcvValue = parsed["iccpPoints"][tagCID]
                odata_tag.setValueDouble(float(rcvValue))
                Logger.pyLog("iccp api control serverSetVar ")
                serverGetVarReply = iccpApi.serverGetVar(clientname, iccp_out_tag.getName())
                Logger.pyLog("serverGetVarReply value: "+str(serverGetVarReply))
                if type(serverGetVarReply) is PyIccpApi.CTASE2StateQTimeTagPOD:
                    someICCPObject = PyIccpApi.CTASE2StateQTimeTagPOD()
                    someICCPObject.state = rcvValue
                    someICCPObject.timeStamp = int(time.time())
                    iccp_out_tag.setICCPObj(someICCPObject)
                    serverSetVarReply = iccpApi.serverSetVar(clientname, iccp_out_tag.getName(), someICCPObject)
                    Logger.pyLog("StateQTime serverSetVar s[" + str(someICCPObject.state) + "] t[" + str(someICCPObject.timeStamp) + "] Reply: " + str(serverSetVarReply))
                elif type(serverGetVarReply) is PyIccpApi.CTASE2RealQTimeTagPOD:
                    realPod = PyIccpApi.CTASE2RealQTimeTagPOD()
                    realPod.value = float(rcvValue)
                    realPod.state = 0
                    realPod.timeStamp = int(time.time())
                    iccp_out_tag.setICCPObj(realPod)
                    serverSetVarReply = iccpApi.serverSetVar(clientname, iccp_out_tag.getName(), realPod)
                    Logger.pyLog("RealQTime serverSetVar v[" + str(realPod.value) + "] s[" + str(realPod.state) + "] t[" + str(realPod.timeStamp) + "] Reply: " + str(serverSetVarReply))
                else:
                    discPod = PyIccpApi.CTASE2DeviceDiscretePOD()
                    discPod.value = int(rcvValue)
                    Logger.pyLog("iccp api control serverSetVar " + iccp_out_tag.getName())
                    serverSetVarReply = iccpApi.serverSetVar(clientname, iccp_out_tag.getName(), discPod)
                    Logger.pyLog("Discrete serverSetVar v[" + str(discPod.value) + "] Reply: " + str(serverSetVarReply))
                # empty strings are falsey
                #if serverSetVarReply :
                    #dicRply = json.loads(serverSetVarReply)
                    #dKeys = list(dicRply.keys())
                    # also write the value to the client as well
                    #if dKeys.count("OK") > 0:
                        #iccp_out_tag.setICCPObj(discPod)
            else:
                Logger.pyLog("tagCID not in iccpPoints " + tagCID)
        else:
            Logger.pyLog("iccpPoints is not in parsed")

def handleDiscreteNotificationS(clientname, odata_tag, iccp_out_tag, iccpApi):
    Logger.pyLog("**handleDiscreteNotificationS server**")
    if CurrentEvent.getEvent() == PyEventType.EVENT_DB_NOTIFICATION:
        data = CurrentEvent.getData()
        parsed = json.loads(data.getJson())
        Logger.pyLog("notification: " + json.dumps(parsed, indent=4, sort_keys=True))
        if "iccpPoints" in parsed:
            tagCID = odata_tag.getCid()
            if tagCID in parsed["iccpPoints"]:
                rcvValue = parsed["iccpPoints"][tagCID]
                if isinstance(rcvValue, str) :
                    if rcvValue == "true" :
                        rcvValue = 1
                    elif rcvValue == "false" :
                        rcvValue = 2
                    else :
                        rcvValue = int(rcvValue)
                odata_tag.setValueDouble(float(rcvValue))
                Logger.pyLog("iccp api control serverSetVar ")
                serverGetVarReply = iccpApi.serverGetVar(clientname, iccp_out_tag.getName())
                Logger.pyLog("serverGetVarReply value: "+str(serverGetVarReply))
                if type(serverGetVarReply) is PyIccpApi.CTASE2StateQTimeTagPOD:
                    someICCPObject = PyIccpApi.CTASE2StateQTimeTagPOD()
                    someICCPObject.state = rcvValue
                    someICCPObject.timeStamp = int(time.time())
                    iccp_out_tag.setICCPObj(someICCPObject)
                    serverSetVarReply = iccpApi.serverSetVar(clientname, iccp_out_tag.getName(), someICCPObject)
                    Logger.pyLog("iccp api control serverSetVar Reply: " + str(serverSetVarReply))
                elif type(serverGetVarReply) is PyIccpApi.CTASE2DeviceDiscretePOD:
                    discPod = PyIccpApi.CTASE2DeviceDiscretePOD()
                    discPod.value = int(rcvValue)
                    Logger.pyLog("iccp api control serverSetVar " + iccp_out_tag.getName())
                    serverSetVarReply = iccpApi.serverSetVar(clientname, iccp_out_tag.getName(), discPod)
                    Logger.pyLog("iccp api control serverSetVar Reply: " + str(serverSetVarReply))
                elif type(serverGetVarReply) is PyIccpApi.CTASE2RealQTimeTagPOD:
                    realPod = PyIccpApi.CTASE2RealQTimeTagPOD()
                    realPod.value = float(rcvValue)
                    realPod.state = 0
                    realPod.timeStamp = int(time.time())
                    Logger.pyLog("iccp api control serverSetVar " + iccp_out_tag.getName() + " v["+str(realPod.value)+"] s["+ str(realPod.state) + "] +t["+str(realPod.timeStamp)+"]")
                    serverSetVarReply = iccpApi.serverSetVar(clientname, iccp_out_tag.getName(), realPod)
                    Logger.pyLog("iccp api control serverSetVar Reply: " + str(serverSetVarReply))
                else:
                    Logger.pyLog("handleDiscreteNotificationS Error: type not expected " + str(type(serverGetVarReply)))
                # empty strings are falsey
                #if serverSetVarReply :
                    #dicRply = json.loads(serverSetVarReply)
                    #dKeys = list(dicRply.keys())
                    # also write the value to the client as well
                    #if dKeys.count("OK") > 0:
                        #iccp_out_tag.setICCPObj(discPod)
            else:
                Logger.pyLog("handleDiscreteNotificationS Error: tagCID not in iccpPoints " + tagCID)
        else:
            Logger.pyLog("handleDiscreteNotificationS Error: iccpPoints is not in parsed")

def ODataToICCPServer_Bool(clientname, odata_tag, iccp_out_tag, scale, iccpApi):
    handleDiscreteNotificationS(clientname, odata_tag, iccp_out_tag, iccpApi)

def ODataToICCPServer_Integer(clientname,odata_tag, iccp_out_tag, scale, iccpApi):
    handleDiscreteNotificationS(clientname, odata_tag, iccp_out_tag, iccpApi)

def ODataToICCPServer_Analog(clientname,odata_tag, iccp_out_tag, scale, iccpApi):
    handleRealNotificationS(clientname, odata_tag, iccp_out_tag, iccpApi)

def ODataToICCPServer_Float(clientname,odata_tag, iccp_out_tag, scale, iccpApi):
    handleRealNotificationS(clientname, odata_tag, iccp_out_tag, iccpApi)
    
    
def ICCPtoOData_Data_RealQTimeTag(iccp_tag, odata_tag, scale, odataApi):
    Logger.pyLog("*********ICCPtoOData_Data_RealQTimeTag client handle**********")
    if CurrentEvent.getEvent() == PyEventType.EVENT_ICCP_WRITE_REQUEST:
        Logger.pyLog("EVENT_ICCP_WRITE_REQUEST")
        return
    elif CurrentEvent.getEvent() == PyEventType.EVENT_ICCP_READ_REQUEST:
        Logger.pyLog("EVENT_ICCP_READ_REQUEST")
        return
    elif CurrentEvent.getEvent() == PyEventType.EVENT_ICCP_UNCONFIRMED_PDU:
        Logger.pyLog("EVENT_ICCP_UNCONFIRMED_PDU")
        someICCPObject = CurrentEvent.readICCPFromICCP()
        if type(someICCPObject) is PyIccpApi.CTASE2RealQTimeTagPOD:
            global g_cim_sending_buffer
            global g_odata_api
            global g_odata_tag_name
            global g_ValidityMap
            global g_CurrentSourceMap
            global g_NormalValueMap
            global g_TimeStampQualityMap
            Logger.pyLog("ICCPtoOData_Data_RealQTimeTag - received CTASE2RealQTimeTagPOD")
            Logger.pyLog("state: " + str(someICCPObject.state))
            Logger.pyLog("timeStamp: " + str(someICCPObject.timeStamp))
            date_time = datetime.fromtimestamp(someICCPObject.timeStamp)
            Logger.pyLog("value: " + str(someICCPObject.value))
            #Update tag in local FEP client database
            iccp_tag.setICCPObj(someICCPObject)
            #Update tag in odata database
            json_body_raw = {}
            val=float(someICCPObject.value)*float(scale)
            json_body_raw["__cid"] = odata_tag.getCid()
            json_body_raw["value"] = val
            json_body_raw["timestamp"] = date_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+"+0000"
            # validity are bit3 and bit4 mask = 0xC = 12
            json_body_raw["iccpValidity"] = g_ValidityMap[(someICCPObject.state & 12)>>2]
            # validity are bit5 and bit6 mask = 0x30 = 48
            json_body_raw["iccpCurrentSource"] = g_CurrentSourceMap[(someICCPObject.state & 48)>>4]
            # normalValue bit7 mask = 0x40 = 64
            json_body_raw["iccpNormalValue"] = g_NormalValueMap[(someICCPObject.state & 64)>>6]
            # timeStampQuality bit8 mask = 0x80 = 128
            json_body_raw["iccpTimeStamp"] = g_TimeStampQualityMap[(someICCPObject.state & 128)>>7]
            odata_tag.setValueDouble(float(val))
            json_body = json.dumps(json_body_raw)
            Logger.pyLog("updateICCP " + str(json_body))
            # This method sends json body request to gridim/derms/core/iccp/<resource>/<cid>
            #odataApi.updateICCPNoBlock(odata_tag.getName(), json_body, 1000,"updateICCPResponse()")
            index=getbufferindex(odata_tag.getName())
            if index < 0 :
                g_odata_tag_name.append(odata_tag.getName())    
                g_odata_api.append(odataApi)
                g_cim_sending_buffer.append([])
            index=getbufferindex(odata_tag.getName())
            g_odata_api[index] = odataApi
            if (CurrentEvent.getICCPTsState() == PyIccpTsState.TS_END or CurrentEvent.getICCPTsState() == PyIccpTsState.TS_SINGLE_DATA):
                g_cim_sending_buffer[index].append(json_body_raw)
                updateIcppRes=g_odata_api[index].updateICCPNoBlock(g_odata_tag_name[index], json.dumps(g_cim_sending_buffer[index]), 2000,"updateICCPResponse()")
                Logger.pyLog("updateICCPNoBlock Result " + str(updateIcppRes))
                g_cim_sending_buffer[index] = []
            else:
                g_cim_sending_buffer[index].append(json_body_raw)
        else:
            Logger.pyLog("ICCPtoOData_Data_RealQTimeTag - wrong type received")

def ICCPtoOData_Data_DiscreteQTimeTag(iccp_tag, odata_tag, scale, odataApi):
    Logger.pyLog("*********ICCPtoOData_Data_DiscreteQTimeTag client handle**********")
    if CurrentEvent.getEvent() == PyEventType.EVENT_ICCP_WRITE_REQUEST:
        Logger.pyLog("EVENT_ICCP_WRITE_REQUEST")
        return
    elif CurrentEvent.getEvent() == PyEventType.EVENT_ICCP_READ_REQUEST:
        Logger.pyLog("EVENT_ICCP_READ_REQUEST")
        return
    elif CurrentEvent.getEvent() == PyEventType.EVENT_ICCP_UNCONFIRMED_PDU:
        Logger.pyLog("EVENT_ICCP_UNCONFIRMED_PDU")
        someICCPObject = CurrentEvent.readICCPFromICCP()
        if type(someICCPObject) is PyIccpApi.CTASE2DiscreteQTimeTagPOD:
            global g_cim_sending_buffer
            global g_odata_api
            global g_odata_tag_name
            global g_ValidityMap
            global g_CurrentSourceMap
            global g_NormalValueMap
            global g_TimeStampQualityMap
            Logger.pyLog("ICCPtoOData_Data_DiscreteQTimeTag - received CTASE2RealQTimeTagPOD")
            Logger.pyLog("state: " + str(someICCPObject.state))
            Logger.pyLog("timeStamp: " + str(someICCPObject.timeStamp))
            date_time = datetime.fromtimestamp(someICCPObject.timeStamp)
            Logger.pyLog("value: " + str(someICCPObject.value))
            #Update tag in local FEP client database
            iccp_tag.setICCPObj(someICCPObject)
            #Update tag in odata database
            json_body_raw = {}
            val=float(someICCPObject.value)*float(scale)
            json_body_raw["__cid"] = odata_tag.getCid()
            json_body_raw["value"] = int(val)
            json_body_raw["timestamp"] = date_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+"+0000"
            # validity are bit3 and bit4 mask = 0xC = 12
            json_body_raw["iccpValidity"] = g_ValidityMap[(someICCPObject.state & 12)>>2]
            # validity are bit5 and bit6 mask = 0x30 = 48
            json_body_raw["iccpCurrentSource"] = g_CurrentSourceMap[(someICCPObject.state & 48)>>4]
            # normalValue bit7 mask = 0x40 = 64
            json_body_raw["iccpNormalValue"] = g_NormalValueMap[(someICCPObject.state & 64)>>6]
            # timeStampQuality bit8 mask = 0x80 = 128
            json_body_raw["iccpTimeStamp"] = g_TimeStampQualityMap[(someICCPObject.state & 128)>>7]
            odata_tag.setValueDouble(float(val))
            json_body = json.dumps(json_body_raw)
            Logger.pyLog("updateICCPNoBlock " + str(json_body))
            # This method sends json body request to gridim/derms/core/iccp/<resource>/<cid>
            #odataApi.updateICCPNoBlock(odata_tag.getName(), json_body, 1000,"updateICCPResponse()")
            index=getbufferindex(odata_tag.getName())
            if index < 0 :
                g_odata_tag_name.append(odata_tag.getName())    
                g_odata_api.append(odataApi)
                g_cim_sending_buffer.append([])
            index=getbufferindex(odata_tag.getName())
            g_odata_api[index] = odataApi
            if (CurrentEvent.getICCPTsState() == PyIccpTsState.TS_END or CurrentEvent.getICCPTsState() == PyIccpTsState.TS_SINGLE_DATA):
                g_cim_sending_buffer[index].append(json_body_raw)
                updateIcppRes=g_odata_api[index].updateICCPNoBlock(g_odata_tag_name[index], json.dumps(g_cim_sending_buffer[index]), 2000,"updateICCPResponse()")
                Logger.pyLog("updateICCPNoBlock Result " + str(updateIcppRes))
                g_cim_sending_buffer[index] = []
            else:
                g_cim_sending_buffer[index].append(json_body_raw)
        else:
            Logger.pyLog("ICCPtoOData_Data_DiscreteQTimeTag - wrong type received")

def ICCPtoOData_Data_StateQTimeTag(iccp_tag, odata_tag, scale, odataApi):
    Logger.pyLog("*********ICCPtoOData_Data_StateQTimeTag client handle**********")
    
    if CurrentEvent.getEvent() == PyEventType.EVENT_ICCP_WRITE_REQUEST:
        Logger.pyLog("EVENT_ICCP_WRITE_REQUEST")
        return
    elif CurrentEvent.getEvent() == PyEventType.EVENT_ICCP_READ_REQUEST:
        Logger.pyLog("EVENT_ICCP_READ_REQUEST")
        return
    elif CurrentEvent.getEvent() == PyEventType.EVENT_ICCP_UNCONFIRMED_PDU:
        Logger.pyLog("EVENT_ICCP_UNCONFIRMED_PDU")
        someICCPObject = CurrentEvent.readICCPFromICCP()
        if type(someICCPObject) is PyIccpApi.CTASE2StateQTimeTagPOD:
            global g_cim_sending_buffer
            global g_odata_api
            global g_odata_tag_name
            global g_ValidityMap
            global g_CurrentSourceMap
            global g_NormalValueMap
            global g_TimeStampQualityMap
            Logger.pyLog("ICCPtoOData_Data_StateQTimeTag - received CTASE2StateQTimeTagPOD")
            Logger.pyLog("state: " + str(someICCPObject.state))
            Logger.pyLog("timeStamp: " + str(someICCPObject.timeStamp))
            date_time = datetime.fromtimestamp(someICCPObject.timeStamp)
            #Update tag in local FEP client database
            iccp_tag.setICCPObj(someICCPObject)
            #Update tag in odata database
            json_body_raw = {}
            #Only use the first 2 bit for value
            if (someICCPObject.state & 0x3) == 1:
                val = 1
            else:
                val = 0
            json_body_raw["__cid"] = odata_tag.getCid()
            json_body_raw["value"] = val
            json_body_raw["timestamp"] = date_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+"+0000"
            # validity are bit3 and bit4 mask = 0xC = 12
            json_body_raw["iccpValidity"] = g_ValidityMap[(someICCPObject.state & 12)>>2]
            # validity are bit5 and bit6 mask = 0x30 = 48
            json_body_raw["iccpCurrentSource"] = g_CurrentSourceMap[(someICCPObject.state & 48)>>4]
            # normalValue bit7 mask = 0x40 = 64
            json_body_raw["iccpNormalValue"] = g_NormalValueMap[(someICCPObject.state & 64)>>6]
            # timeStampQuality bit8 mask = 0x80 = 128
            json_body_raw["iccpTimeStamp"] = g_TimeStampQualityMap[(someICCPObject.state & 128)>>7]
            odata_tag.setValueDouble(float(val))
            json_body = json.dumps(json_body_raw)
            Logger.pyLog("updateICCPNoBlock " + str(json_body))
            #This method sends json body request to gridim/derms/core/iccp/<resource>/<cid>
            #odataApi.updateICCPNoBlock(odata_tag.getName(), json_body, 1000,"updateICCPResponse()")
            index=getbufferindex(odata_tag.getName())
            if index < 0 :
                g_odata_tag_name.append(odata_tag.getName())    
                g_odata_api.append(odataApi)
                g_cim_sending_buffer.append([])
            index=getbufferindex(odata_tag.getName())
            g_odata_api[index] = odataApi
            if (CurrentEvent.getICCPTsState() == PyIccpTsState.TS_END or CurrentEvent.getICCPTsState() == PyIccpTsState.TS_SINGLE_DATA):
                g_cim_sending_buffer[index].append(json_body_raw)
                updateIcppRes=g_odata_api[index].updateICCPNoBlock(g_odata_tag_name[index], json.dumps(g_cim_sending_buffer[index]), 2000,"updateICCPResponse()")
                Logger.pyLog("updateICCPNoBlock Result " + str(updateIcppRes))
                g_cim_sending_buffer[index] = []
            else:
                g_cim_sending_buffer[index].append(json_body_raw)
        else:
            Logger.pyLog("ICCPtoOData_Data_StateQTimeTag - wrong type received")


def controlICCP(clientname, iccp_tag, iccpApi):
    Logger.pyLog("*********controlICCP client**********")
    crtEvent = CurrentEvent.getEvent()
    if crtEvent == PyEventType.EVENT_ICCP_WRITE_REQUEST:
        Logger.pyLog( "EVENT_ICCP_WRITE_REQUEST "+ iccp_tag.getName() + " " + str(iccp_tag.getICCPObj()) )
        someICCPObject = CurrentEvent.readICCPFromICCP()
        Logger.pyLog("someICCPObject" + str(someICCPObject))
        if type(someICCPObject) is PyIccpApi.CTASE2DeviceRealPOD:
            Logger.pyLog("controlICCP - received CTASE2DeviceRealPOD")
            Logger.pyLog("value: " + str(someICCPObject.value))
            # write command to server
            Logger.pyLog("Connected peer:")
            stringVect = iccpApi.getPeerList(clientname);
            Logger.pyLog(clientname + " "+str(stringVect))
            if len(stringVect) > 0:
                peer = stringVect[0]
                Logger.pyLog("iccp api control clientSetVar ")
                realpod = PyIccpApi.CTASE2DeviceRealPOD()
                realpod.value = someICCPObject.value
                Logger.pyLog("iccp api control clientSetVar " + iccp_tag.getName())
                clientSetVarReply = iccpApi.clientSetVarNoBlock(clientname, peer, iccp_tag.getName(), realpod, 5, "iccpClientSetVarResponse()")
                Logger.pyLog("iccp api control clientSetVar Reply: " + clientSetVarReply)
                # also write the value to the client as well
                if str(clientSetVarReply).count("OK") > 0:
                    iccp_tag.setICCPObj(someICCPObject)
        elif type(someICCPObject) is PyIccpApi.CTASE2DeviceDiscretePOD:
            Logger.pyLog("controlICCP - received CTASE2DeviceDiscretePOD")
            Logger.pyLog("value: " + str(someICCPObject.value))
            stringVect = iccpApi.getPeerList(clientname);
            Logger.pyLog(clientname + " "+str(stringVect))
            if len(stringVect) > 0:
                peer = stringVect[0]
                Logger.pyLog("iccp api control clientSetVar ")
                discretePod = PyIccpApi.CTASE2DeviceDiscretePOD()
                discretePod.value = someICCPObject.value
                Logger.pyLog("iccp api control clientSetVar " + iccp_tag.getName())
                clientSetVarReply = iccpApi.clientSetVarNoBlock(clientname, peer, iccp_tag.getName(), discretePod, 5, "iccpClientSetVarResponse()")
                if str(clientSetVarReply).count("OK") > 0:
                    iccp_tag.setICCPObj(someICCPObject)
        else:
            Logger.pyLog("controlICCP - wrong type received")
    elif crtEvent == PyEventType.EVENT_ICCP_READ_REQUEST:
        Logger.pyLog("EVENT_ICCP_READ_REQUEST")
    elif crtEvent == PyEventType.EVENT_ICCP_UNCONFIRMED_PDU:
        Logger.pyLog("EVENT_ICCP_UNCONFIRMED_PDU")

def computeStateQTime(iccp_tag_state_q_time_in, iccpApi):
    Logger.pyLog("*********computeStateQTime server**********")
    crtEvent = CurrentEvent.getEvent()
    someICCPObject = CurrentEvent.readICCPFromICCP()
    if type(someICCPObject) is PyIccpApi.CTASE2StateQTimeTagPOD:
        Logger.pyLog("computeStateQTime - received CTASE2StateQTimeTagPOD")
        Logger.pyLog("state: " + str(someICCPObject.state))
        Logger.pyLog("timeStamp: " + str(someICCPObject.timeStamp))
        if crtEvent == PyEventType.EVENT_ICCP_WRITE_REQUEST:
            Logger.pyLog("EVENT_ICCP_WRITE_REQUEST")
            #Update tag in local Fep database
            iccp_tag_state_q_time_in.setICCPObj(someICCPObject)
            #Update database to generate report to client
            if someICCPObject.state == 0 :
                someICCPObject.state = 1
            else:
                someICCPObject.state = 0
            CurrentEvent.writeICCPFromPy(someICCPObject)
            iccp_tag_state_q_time_in.setICCPObj(someICCPObject)
        elif crtEvent == PyEventType.EVENT_ICCP_UNCONFIRMED_PDU:
            Logger.pyLog("EVENT_ICCP_UNCONFIRMED_PDU")
            #Update tag in local Fep database
            #if someICCPObject.state == 0 :
            #    someICCPObject.state = 1
            #else:
            #    someICCPObject.state = 0
            CurrentEvent.writeICCPFromPy(someICCPObject)
            iccp_tag_state_q_time_in.setICCPObj(someICCPObject)
            errCode = iccpApi.serverSetVar("CHESTERFIELD", iccp_tag_state_q_time_in.getName(), someICCPObject)
            Logger.pyLog("iccpApi.serverSetVar return: " + str(errCode))
        if crtEvent == PyEventType.EVENT_ICCP_READ_REQUEST :
            Logger.pyLog("EVENT_ICCP_READ_REQUEST")
    else:
        Logger.pyLog("computeStateQTime - wrong type received" + str(type(someICCPObject)))

def computeRealQTime(iccp_tag_real_q_time_in, clientname, iccpApi):
    Logger.pyLog("*********computeRealQTime server**********")
    crtEvent = CurrentEvent.getEvent()
    someICCPObject = CurrentEvent.readICCPFromICCP()
    if type(someICCPObject) is PyIccpApi.CTASE2RealQTimeTagPOD:
        Logger.pyLog("computeStateQTime - received CTASE2RealQTimeTagPOD")
        Logger.pyLog("iccp api control serverSetVar ")
        serverGetVarReply = iccpApi.serverGetVar(clientname, iccp_tag_real_q_time_in.getName())
        Logger.pyLog("serverGetVarReply value: "+str(serverGetVarReply))
        if type(serverGetVarReply) is PyIccpApi.CTASE2RealQTimeTagPOD:
            iccp_tag_real_q_time_in.setICCPObj(someICCPObject)
            serverSetVarReply = iccpApi.serverSetVar(clientname, iccp_tag_real_q_time_in.getName(), someICCPObject)
            Logger.pyLog("RealQTime serverSetVar v[" + str(someICCPObject.value) + "] s[" + str(someICCPObject.state) + "] t[" + str(someICCPObject.timeStamp) + "] Reply: " + str(serverSetVarReply))


def ICCPtoTWOOData_Data_RealQTimeTag(iccp_tag, odata_tag1, odata_tag2, scale, odataApi):
    Logger.pyLog("*********ICCPtoTWOOData_Data_RealQTimeTag**********")
    ICCPtoOData_Data_RealQTimeTag(iccp_tag, odata_tag1, scale, odataApi)
    ICCPtoOData_Data_RealQTimeTag(iccp_tag, odata_tag2, scale, odataApi)
 
def ICCPtoTWOOData_Data_DiscreteQTimeTag(iccp_tag, odata_tag1, odata_tag2, scale, odataApi):
    Logger.pyLog("*********ICCPtoTWOOData_Data_DiscreteQTimeTag client handle**********")
    ICCPtoOData_Data_DiscreteQTimeTag(iccp_tag, odata_tag1, scale, odataApi)
    ICCPtoOData_Data_DiscreteQTimeTag(iccp_tag, odata_tag2, scale, odataApi)
 
def ICCPtoTWOOData_Data_StateQTimeTag(iccp_tag, odata_tag1, odata_tag2, scale, odataApi):
    Logger.pyLog("*********ICCPtoTWOOData_Data_StateQTimeTag client handle**********")
    ICCPtoOData_Data_StateQTimeTag(iccp_tag, odata_tag1, scale, odataApi)
    ICCPtoOData_Data_StateQTimeTag(iccp_tag, odata_tag2, scale, odataApi)
