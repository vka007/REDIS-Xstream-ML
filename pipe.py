import apache_beam as beam
import joblib
import pandas as pd
import numpy as np
import time
from datetime import datetime
import requests
import json
from requests.auth import HTTPBasicAuth
import os
import random
import redis
from apache_beam.options.pipeline_options import PipelineOptions,StandardOptions
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.io import WriteToMongoDB
# from flink.plan.Environment import get_environment


class simulate():
    def __init__(self):
        print("Initializing Simulator")
        self.dataset = pd.read_csv("GeneratedData.csv",index_col = 0)
        self.timeStamp = 1574063600
        print("Successfully Initialized Simulator")
    def data(self):
        stringTime = datetime.fromtimestamp(self.timeStamp).__str__()
        value = self.dataset.loc[stringTime].values[0]
        self.timeStamp += 5
        dataString = {"timestamp":stringTime,"sensorValue":value}
        return dataString


# class printData(beam.DoFn):
#     def process(self,data):
#         dateTime, sensorValue = data["timestamp"], data["sensorValue"]
#         print(dateTime)
#         print(sensorValue)
#         return data


class writetoRedisDS(beam.DoFn):
    def __init__(self):
        self.conn = redis.Redis(host='localhost', port=6379, db=0)

    def process(self,data):
        dateTime, sensorValue = data["timestamp"], data["sensorValue"]
        print(dateTime)
        print(sensorValue)
        self.conn.set('data',sensorValue)




class readData(beam.io.iobase.BoundedSource):
    def __init__(self,count):
        self.similator = simulate()
        self._count = count

    def estimate_size(self):
        return self._count

    def get_range_tracker(self, start_position, stop_position):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self._count
        return OffsetRangeTracker(start_position, stop_position)
    
    def read(self, range_tracker):
        for i in range(range_tracker.start_position(),range_tracker.stop_position()):
            if not range_tracker.try_claim(i):
                return self.records_read.inc()
            da = self.similator.data()
            print(da)
            yield da
            time.sleep(5)
            # yield ("data",'18,8,307,130,3504,12,70,1,chevrolet chevelle malibu')

    # def read(self):
    #     print("Working")
    #     return [1,4,5,7,8,0,5]

def run():

    p = beam.Pipeline('DirectRunner')
    # p = beam.Pipeline(runner = 'PortableRunner',options=options)
    # (p | 'ReadMessage' >>  beam.io.textio.ReadFromTextWithFilename('mpg.csv')
    (p | 'ReadMessage' >>  beam.io.Read(readData(5))
                        # | 'printer' >> beam.ParDo(printData())
                        | 'writeToRedisDS' >> beam.ParDo(writetoRedisDS())
                        # | 'Arima' >> beam.ParDo(ArimaDummy())
                        # | 'processDataForANN' >> beam.ParDo(processDataForANN())
                        # | 'ANN' >> beam.ParDo(ANN())
                        # | 'postProcess' >> beam.ParDo(postProcess())
                        # | 'PublishtoCumulocity' >> beam.ParDo(toCumulocity())
                        
                        
                        
                        
                        ) 
    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    run()