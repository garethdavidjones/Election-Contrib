import sys
import pyspark
import csv
from StringIO import StringIO
from operator import add

AMT_COL = 3
OUTPUT_PATH = 'gs://cs123data/Output/'
INPUT_PATH = 'gs://cs123data/Data/contribDB_1984.csv'
CID_COL = 5
TOP_K = 20
CONTR_CF = 36


Outline:
    
    Problems:
        Avoid Unkowns CLustering too close to one another


Years Donated Binary: 
    Each Year [2002-2010]:
        Restricted because more features means more difficult comparison
        * 3 General Election Cycles
        Yes:  
        No:

Gender:

Contribution Ideology:
    


Political Scene:
    Party in Offic:
        Yes:
        Indifferent: 
        No:

Recipient Catogories:
    
    Pasts Candidates Ideology
        Very Liberal:
        Liberal:
        Moderate:
        Conservative:
        Very Conservative:
        Unkown

    Shared Potential Candidate Ideology:
       - Does the Candidate Map to the Same C
        Yes: 1
        No: 0
        Close: 2
        Unkown: 4


Transaction Information:     
        
        Size:   
            Need to Distinguish Clusters

        Type:
            See CodeBook:
                Should Be Able to Preform Reduced Bucketing


Contributer Information:
    Type:
        Committe "C": 1
        Individauls "T" : 2
        Unkown " ": 0
    
    Indv Last Known Ideology:
        Very Liberal: -2  [<-2]
        Liberal: -1   [-1, -0.3]
        Moderate: 0  [-0.3, 0.3]
        Conservative: 1 [0.3, 1]
        Very Conservative: 2 [1<]
        Unkown: 4  "Error"


Location:
    State Ideology:
        "Dictionary Created"
        Very Liberal: -2 
        Liberal: -1
        BattleGround: 0
        Conservative: 1
        Very Conservative: 2

        stateIdeo = {"WA": -2, "OR": -2, "CA":-2. "NV": -2, "ID": 2, "MT": 1, 
                     "WY": 2, "UT": 2, "AZ": 1, "ND": 1, "SD": 2, "NE": 2,
                     "CO": -1, "NM": -2, "KS": 2, "OK": 2, "TX": 2, "MN": -2,
                     "IA": -1, "MO": 0, "AR": 2, "LA":2, "WI":-2, "IL":-2,
                     "MI": -2, "IN": 0, "OH": 0, "KY": 2, "TN": 2, "MS": 2,
                     "AL":2, "WV":2, "VA": 0, "NC": 0, "SC": 1, "GA": 1, "FL":0,
                     "ME": -2, "VT": -2, "MA":-2, "RI": -2, "CT":-2, "NJ":-2, 
                     "DE": -2, "MD": -2, "DC": -2, "NY":-2, "PA": -2, "Ak": 2,
                     "HI": -2
                    }

def csv_parser(line):   

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        return rv
    except:
        rv = [1]  #This row will be removed in data cleaning
        return rv

def data_cleaning(file_in):

    lines = sc.textFile(file)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove header lines

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) == 51) 
    
    return data


def get_unique





