from pymongo import MongoClient
from uuid import uuid4,UUID
from random import randint
import json
from urllib.request import urlopen
import urllib
import numpy as np
from time import sleep, localtime, strftime, time
import os
import numpy as np
from matplotlib import pyplot as plt
from collections import namedtuple
from ortools.constraint_solver import pywrapcp
from ortools.constraint_solver import routing_enums_pb2
from datetime import datetime, timedelta
import pandas as pd
import sys

class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)
  
IDs          = []
pickup_date  = []
Type         = []
company_id   = []
veh_cap      = []
early_time   = 10
EASY_WAY     = True
OSRM_URL          = 'http://127.0.0.1:5000'
OSM_READ_INTERVAL = 5

MONGO_URL         = 'mongodb://localhost:27017/'
DB_NAME           = 'autosol'
CLIENTS_LIMIT     = 100
# prepares the addr of the server to be contacted and returns the distances matrix 
def get_dist_mat(loc):
      print("Connecting to OSM db")
      #app_log.info("Connecting to OSM db")
      start_time = time()
      addr = OSRM_URL
      table = '/table/v1/driving/'
      addr += table
      for i in loc[:-1]:
          addr += str(i[1])+','+str(i[0])+';'
      Addr = addr + str(loc[-1][1])+','+str(loc[-1][0])
      addr += str(loc[-1][1])+','+str(loc[-1][0])+ '?annotations=distance'
      while True:
            try:
                  #print(addr)
                  a = urlopen(addr)
            except urllib.error.URLError as e:
                  print("Unable to connect to OSM db, URLError")
                  print(str(e.reason))
                  sleep(OSM_READ_INTERVAL)
            except urllib.error.HTTPError as e:
                  print("Unable to connect to OSM db, HTTPError")
                  print(str(e.reason))
                  sleep(OSM_READ_INTERVAL)
            else:
                  break
      for i in a:
          m = i
      l = json.loads(m.decode('utf-8'))
      while True:
            try:
                a = urlopen(Addr)
            except urllib.error.URLError as e:
                  print("Unable to connect to OSM db, URLError")
                  print(str(e.reason))
                  sleep(OSM_READ_INTERVAL)
                  #
            except urllib.error.HTTPError as e:
                  print("Unable to connect to OSM db, HTTPError")
                  print(str(e.reason))
                  sleep(OSM_READ_INTERVAL)
                  #
            else:
                  break
      for i in a:
          m = i
      m = json.loads(m.decode('utf-8'))
      m = np.asarray(m['durations'])
      m = m//60
      m = m.astype(int)
      m = np.ndarray.tolist(m)
      print('reading the distance matrix took',time()-start_time,' for ',len(loc),' locations')
      return l['distances'], m

def print_solution(manager, routing, plan, data):
    """
    Return a string displaying the output of the routing instance and
    assignment (plan).

    Args: routing (ortools.constraint_solver.pywrapcp.RoutingModel): routing.
    plan (ortools.constraint_solver.pywrapcp.Assignment): the assignment.

    Returns:
        (string) plan_output: describing each vehicle's plan.

        (List) dropped: list of dropped orders.

    """
    Json_output = {}
    print('found a solution')
    dropped = []
    for order in range(routing.Size()):
        if (plan.Value(routing.NextVar(order)) == order):
            dropped.append(str(order))

    #capacity_dimension = routing.GetDimensionOrDie('Capacity')
    
    #time_dimension = routing.GetDimensionOrDie('Time')
    tot_no = 0
    plan_output = ''
    js_Iter = 0
    for route_number in range(routing.vehicles()):
        order = routing.Start(route_number)
        plan_output += 'Route {0}:'.format(route_number)
        order = plan.Value(routing.NextVar(order))
        if routing.IsEnd(order):
            #print('empty')
            plan_output += ' Empty \n'
        else:
            js_Iter+=1
            ST = 'Shuttle '+str(js_Iter)
            Json_output[ST] = {}
            Json_output[ST]['trips'] = [[]]
            Json_output[ST]['_id']            = uuid4()
            Json_output[ST]['company_id']     = data['company_id'][order]
            Json_output[ST]['tracked']        = 0
            Json_output[ST]['rested']         = 0
            Json_output[ST]['active']         = 1
            Json_output[ST]['done']           = 1
            fir = manager.IndexToNode(order)
            Json_output[ST]['date_to_ride']   = data['Day'][fir]
            Json_output[ST]['start_datetime'] = data['pickup_date'][fir]
            trip = {}
            M_iter = 0
            while True:
                tot_no+=1
                load_var = capacity_dimension.CumulVar(order)
                time_var = time_dimension.CumulVar(order)
                node = manager.IndexToNode(order)
                plan_output += 'full_name({name})  pickup_date({date})  -> '.format(name=data['full_name'][node], date=data['pickup_date'][node])
                """
                plan_output +=\
                    ' {node} Load({load}) Time({tmin}, {tmax}) full_name({name})  pickup_date({date})  -> '.format(
                        node=node,
                        load=plan.Value(load_var),
                        tmin=str(timedelta(seconds=plan.Min(time_var))),
                          tmax=str(timedelta(seconds=plan.Max(time_var))),
                          name=data['full_name'][node],
                          date=data['pickup_date'][node])
                """
                
                Json_output[ST]['trips'][M_iter] = {}
                Json_output[ST]['trips'][M_iter]['full_name']        = data['full_name'][node]
                Json_output[ST]['trips'][M_iter]['pick_datetime']    = data['pickup_date'][node]
                Json_output[ST]['trips'][M_iter]['client_id']        = data['IDs'][node]
                Json_output[ST]['trips'][M_iter]['total_passengers'] = data['demands'][node]
                Json_output[ST]['trips'].append([])
                M_iter += 1
                prev  = order
                order = plan.Value(routing.NextVar(order))
                if routing.IsEnd(order):
                    plan_output += ' EndRoute {0}. \n'.format(route_number)
                    #Json_output[ST]['trips'] = trip.copy()
                    #Json_output[ST]['type'].append(data['trip_type'][prev])                
                    #Json_output[ST]['stop_datetime'].append(data['pick_datetime'][prev])
                    break
        plan_output += '\n'
    print(plan_output)
    #print(tot_no)
    with open('data.json', 'w') as outfile:
             json.dump(Json_output, outfile, cls = UUIDEncoder)
    return (plan_output, dropped)



size = []
size_c  = []
company_id = []
Day = []
Time_windows = []
locations = []
pickup_deliver = []
IDs = []
full_name = []
pickup_date = []
speedy_idx = []
veh_cap = []
veh_cap_c = []
start = []
end = []
trepz = []

#client = MongoClient('mongodb://localhost:27017/')
client = MongoClient(MONGO_URL)
trip_types = ['S','Z']
Final   = []
misfits = [[],[],[],[]]
passengers = client[DB_NAME]['passengers'].find()
for idx, passenger in enumerate(passengers):
      FLAT = False
      TLAT = False
      destinations = client[DB_NAME]['destinations'].find()
      for dest in destinations:
          if passenger['from'] == dest['slug']:
              try:
                  FLAT = float(dest['lat'])
                  FLNG = float(dest['long'])
                  if passenger['to'] == passenger['from']:
                        TLAT = float(dest['lat'])
                        TLNG = float(dest['long'])
              except ValueError:
                  #print('here',dest['lat'])
                  #misfits[0].append(passenger['from'])
                  #misfits[1].append(passenger['to'])
                  #misfits[2].append(dest['lat'])
                  #misfits[3].append(dest['long'])
                  FLAT = False
                  FLNG = False
                  TLAT = False
                  TLNG = False
                  continue
          elif passenger['to'] == dest['slug']:
              try:
                  TLAT = float(dest['lat'])
                  TLNG = float(dest['long'])
                  if passenger['to'] == passenger['from']:
                       FLAT = float(dest['lat'])
                       FLNG = float(dest['long'])                              
              except ValueError:
                  #print('there',dest['lat'])
                  #misfits[0].append(passenger['from'])
                  #misfits[1].append(passenger['to'])
                  #misfits[2].append(dest['lat'])
                  #misfits[3].append(dest['long'])
                  FLAT = False
                  FLNG = False
                  TLAT = False
                  TLNG = False
                  continue
          if FLAT != False and TLAT != False:
                Pass         = passenger.copy()
                Pass['from'] = [FLAT,FLNG]
                Pass['to']   = [TLAT,TLNG]
                Final.append(Pass.copy())
                break      
      if FLAT == False or TLAT == False:
                  misfits[0].append(passenger['from'])
                  misfits[1].append(passenger['to'])
                  misfits[2].append('*')
                  misfits[3].append('*')
      if len(Final) >= CLIENTS_LIMIT:
            break
misfits = pd.DataFrame({'from':misfits[0],'to':misfits[1],'lat':misfits[2],'lng':misfits[3]})
misfits.to_csv('misfits.csv')
config     = client[DB_NAME]['config_shuttles'].find()[0]
Iter       = 0
trepz = []
speedy_idx = []
size = []
size_c = []
company_id = []
Day = []
Hour = []
Time_windows = []
locations = []
pickup_deliver = []
IDs = []
full_name = []
pickup_date = []
desired_day = sys.argv[1][7:]
idx=0
for passenger in Final:
    if passenger['trip_id'] == '0':
        #passenger['trip_type'] = trip_types[randint(0,1)]
        if passenger['trip_type'] == 'P' or desired_day != passenger['pick_datetime'].strftime("%Y-%m-%d"):
            continue
        if passenger['trip_type'] == 'S':
            trepz.append('S')
            tolerance = config['wait_client']
            #size_SS.append(0)
        else:
            trepz.append('SS')
            tolerance = config['wait_client_speedy']
            speedy_idx.append(idx)
            #size_SS.append(1)
        #
        size.append(passenger['total_people'])
        size.append(-passenger['total_people'])
        size_c.append(1)
        size_c.append(-1)
        #
        no = randint(0,10)
        #company_cli = client[DB_NAME]
        company_id.append(no)#client['company_id'])
        company_id.append(no)#client['company_id'])
        
        Day.append(passenger['pick_datetime'].strftime("%Y-%m-%d"))
        Hour.append(passenger['pick_datetime'].strftime("%H:%M:%S"))

        Day.append(passenger['pick_datetime'].strftime("%Y-%m-%d"))
        Hour.append(passenger['pick_datetime'].strftime("%H:%M:%S"))
        #
        Time = passenger['pick_datetime'].strftime("%H:%M:%S")
        Time = int(Time[:2])*60 + int(Time[3:5]) - early_time
        Time_windows.append((Time, Time+tolerance))
        Time_windows.append((Time,    Time+tolerance))
        #
        locations.append(passenger['from'])
        locations.append(passenger['to'])
        #
        pickup_deliver.append([Iter,Iter+1])
        Iter+=2
        #
        IDs.append(passenger['client_id'])
        full_name.append(passenger['full_name'])
        pickup_date.append(passenger['pick_datetime'].strftime("%Y-%m-%d %H:%M:%S"))

        IDs.append(passenger['client_id'])
        full_name.append(passenger['full_name'])
        pickup_date.append(passenger['pick_datetime'].strftime("%Y-%m-%d %H:%M:%S"))

        idx+=1
        #if Iter >= CLIENTS_LIMIT:
        #      break
#print('number of clients found'+str(len(IDs)))
cars_start = Iter
cars       = client[DB_NAME]['cars'].find()
max_stops  = config['max_stop_speedy']
veh_cap    = []
veh_cap_c  = []
start      = []
end        = []
for car in cars:
    veh_cap.append(int(car['sits']))
    veh_cap_c.append(int(car['sits']))
    #veh_cap_c.append(max_stops)
    #veh_cap_ss.append(max_stops)
    #
    IDs.append(car['_id'])
    company_id.append(car['company_id'])
    # 
    start.append(Iter)
    end.append(Iter+1)
    Iter += 2

#print(company_id)
#Time_windows.extend([[0,1440]]*len(veh_cap))

data = {}
data['speedy_idx']   =  speedy_idx
data['capacity']     =  veh_cap
data['capacity_c']   =  veh_cap_c
data['demands']      =  size
data['demands_c']    =  size_c
data['Day']          =  Day
data['Hour']         =  Hour
data['time_windows'] =  Time_windows
data['num_vehicles'] =  len(veh_cap)
data['pickups_deliveries'] = pickup_deliver
data['company_id']   =  company_id
data['cars_start']   =  cars_start
data['starts']          =  start
data['ends']            =  end
data['trip_type']       = trepz
data['IDs']          =  IDs
data['full_name']    =  full_name
data['pickup_date']  =  pickup_date
#print('number of vehicles',len(veh_cap))
#print(max(Time_windows))
#------------ add the vehicles's start and end locations ---------------------
#print(len(locations))
offset = len(veh_cap)*2
data['distance_matrix'], data['durations'] = get_dist_mat(locations)

print('number of vehicles ',offset)
print('length of the distance matrix',len(data['distance_matrix']))
print('length of the durations matrix',len(data['durations'][0]))

for i in range(len(data['distance_matrix'])):
    data['distance_matrix'][i].extend([0]*offset)
    data['durations'][i].extend([0]*offset)
    
    
orig_len = len(data['durations'])
data['distance_matrix'].extend([[0]*(orig_len+offset)]*offset)
data['durations'].extend([[0]*(orig_len+offset)]*offset)

print('length of the distance matrix after padding',len(data['distance_matrix']))
print('length of the durations matrix after padding',len(data['durations'][0]))
print('length of starts and ends',len(data['starts']),len(data['ends']))

#------------------------------------------------------------------------------

manager = pywrapcp.RoutingIndexManager(len(data['distance_matrix']), data['num_vehicles'], data['starts'], data['ends'])
routing = pywrapcp.RoutingModel(manager)

print('initiated the route manager')
def distance_callback(from_index, to_index):
    #print(from_index, to_index,)#len(data['company_id']),data['cars_start'], data['distance_matrix'][from_node][to_node])
    from_node = manager.IndexToNode(from_index)
    to_node = manager.IndexToNode(to_index)
    if from_node >= data['cars_start'] or to_node >= data['cars_start']:
          return 0
    #if data['company_id'][from_node] != data['company_id'][to_node]:
    #         return 2147483647
    if data['Day'][from_node] != data['Day'][to_node]:
          return 2147483647
    return data['distance_matrix'][from_node][to_node]

distance_callback_index = routing.RegisterTransitCallback(distance_callback)
Max_wait_time = 2147483647
Max_drive_dist = 2147483647
Distance = 'Distance'
routing.AddDimension(
    distance_callback_index,
    Max_wait_time,   # allow waiting time
    Max_drive_dist,  # maximum time per vehicle
    True,            # Don't force start cumul to zero.
    Distance)
#"""
def time_callback(from_index, to_index):
    from_node = manager.IndexToNode(from_index)
    to_node = manager.IndexToNode(to_index)
    if from_node >= data['cars_start'] or to_node >= data['cars_start']:
          return 0
    #if data['company_id'][from_node] != data['company_id'][to_node]:
    #         return 2147483647
    if data['Day'][from_node] != data['Day'][to_node]:
          return 2147483647
    return data['durations'][from_node][to_node]

time_callback_index = routing.RegisterTransitCallback(time_callback)
Max_wait_time  = 2147483647
Max_drive_time = 2147483647
time = 'Time'
routing.AddDimension(
    time_callback_index,
    Max_wait_time,   # allow waiting time
    Max_drive_time,  # maximum time per vehicle
    True,            # Don't force start cumul to zero.
    time)
#"""
#"""
def demand_callback(from_index):
    #return 0
    from_node = manager.IndexToNode(from_index)
    if from_node >= data['cars_start']:
          return 0 
    return data['demands'][from_node]
  
demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
CaP = 'CApacity'
routing.AddDimensionWithVehicleCapacity(demand_callback_index,
                                        0,
                                        data['capacity'],
                                        True,
                                        CaP)

capacity_dimension = routing.GetDimensionOrDie(CaP)
#"""
#"""
def demand_c_callback(from_index):
    #return 0
    from_node = manager.IndexToNode(from_index)
    if from_node >= data['cars_start']:
          return 0
    return data['demands_c'][from_node]
  
demand_c_callback_index = routing.RegisterUnaryTransitCallback(demand_c_callback)

Stops_capacity = 'capacity_c'
routing.AddDimensionWithVehicleCapacity(demand_c_callback_index,
                                        0,
                                        data['capacity_c'],
                                        True,
                                        Stops_capacity)

#"""
distance_dimension = routing.GetDimensionOrDie(Distance)
#"""
for request in data['pickups_deliveries']:
    pickup_index = manager.NodeToIndex(request[0])
    delivery_index = manager.NodeToIndex(request[1])
    route_cost = data['distance_matrix'][pickup_index][delivery_index]
    
    #print(data['distance_matrix'][pickup_index][delivery_index],'---',data['durations'][pickup_index][delivery_index],'---',data['company_id'][pickup_index],'---',data['company_id'][delivery_index])
    routing.AddPickupAndDelivery(pickup_index, delivery_index)
    routing.solver().Add(
        routing.VehicleVar(pickup_index) == routing.VehicleVar(
            delivery_index))
    if EASY_WAY == True:
         routing.solver().Add(
               distance_dimension.CumulVar(pickup_index) <=
               distance_dimension.CumulVar(delivery_index))
    else:
        routing.solver().Add(
              distance_dimension.CumulVar(pickup_index) >=
              distance_dimension.CumulVar(delivery_index) - int(route_cost) - config['distance'])
    
    
#"""
time_dimension = routing.GetDimensionOrDie(time)
#"""
# Add time window constraints for each location except depot.
for location_idx, time_window in enumerate(data['time_windows']):
    if location_idx >= data['cars_start']:
        break
    index = manager.NodeToIndex(location_idx)
    if index % 2 == 0:
       time_dimension.CumulVar(index).SetRange(time_window[0], time_window[1])
    else:
      route_cost = data['durations'][index-1][index]
      #print(data['full_name'][index])
      #print(data['full_name'][index-1])
      route_perc = int(route_cost * config['distance']  / max(1,data['distance_matrix'][index-1][index]))
      time_dimension.CumulVar(index).SetRange(time_window[0], time_window[1]+route_cost+max(1,route_perc))
# Add time window constraints for each vehicle start node.
#"""
for vehicle_id in range(data['num_vehicles']):
    index = routing.Start(vehicle_id)
    time_dimension.CumulVar(index).SetRange(0,
                                            1400)
#"""
#"""
for i in range(data['num_vehicles']):
    routing.AddVariableMinimizedByFinalizer(
        time_dimension.CumulVar(routing.Start(i)))
    routing.AddVariableMinimizedByFinalizer(
        time_dimension.CumulVar(routing.End(i)))
    # configure speedy clients
    for idx in data['speedy_idx']:
        index = manager.NodeToIndex(idx)
        capacity_dimension.SlackVar(index).SetValue(0)
        for vehicle_index in range(len(data['ends'])):
              end_index = routing.End(i)
              routing.solver().Add(
                     capacity_dimension.CumulVar(end_index) <= 
                     capacity_dimension.CumulVar(index) + 3)

#"""
nodes = []
for i in range(cars_start):
      nodes.append(routing.AddDisjunction([manager.NodeToIndex(i)],2147483647999))
search_parameters = pywrapcp.DefaultRoutingSearchParameters()
search_parameters.first_solution_strategy = (routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION)
#search_parameters.time_limit.seconds = 4
routing.SetArcCostEvaluatorOfAllVehicles(time_callback_index)
assignment = routing.SolveWithParameters(search_parameters)
if assignment:
   print_solution(manager,routing,assignment,data)
else:
   print('no solution')

#with open('data.json', 'w', encoding='utf-8') as f:
#    json.dump(data, f, ensure_ascii=False, indent=4)
