# -*- coding: utf-8 -*-
import json
import os
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from const import (
    SAMPLE_DATA_DIR
)

es = Elasticsearch()

#term = "ps" + " -eo user,pid,%mem,pcpu,command | awk -v OFS=, '{print $1, $2, $3, $4, $5}' | jq -R 'split(",") | {user: .[0], pid: .[1], mem: .[2], cpu: .[3], command: .[4] }'"
os.system("ps -eo user,pid,%mem,pcpu,command | awk -v OFS=, '{print $1, $2, $3, $4, $5}' > tmp.json")
#os.system("cat tmp.json | jq -cR '[split(",")] '")


arr = []
with open('tmp.json', 'r+') as f:
    for line in f:
      # split around semicolon and then strip spaces from the ends
      fields = (line.split(','))
      arr.append({
      	  "_index": "process",
          "user": fields[0],
          "pid": fields[1],
          "mem": fields[2],
          "cpu": fields[3],
          "command": fields[4],
      })
jsonString = (json.dumps(arr, indent=2))
jsonFile = open("tmp.json", "w")
jsonFile.write(jsonString)
jsonFile.close()

def load_jsondata():
    with open(SAMPLE_DATA_DIR) as f:
        return json.load(f)

def insert_data_by_bulk(data):
    res = helpers.bulk(es, data)
    print(res)


if __name__ == "__main__":
    demo_data_2 = load_jsondata()
    insert_data_by_bulk(demo_data_2)