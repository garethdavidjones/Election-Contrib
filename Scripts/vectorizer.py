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

'''

Outline:
    
    Problems:
        Avoid Unkowns CLustering too close to one another


Years Donated Binary: 
    Each Year [2002-2010]:
        Restricted because more features means more difficult comparison
        * 3 General Election Cycles
        Yes: 1  
        No:  0

Political Scene:
    Party in Offic:
        Yes:
        Indifferent: 
        No:

Recipient Catogories:
    
    Pasts Candidates Ideology
        Same as Own:
        More Than Own:
        Less Than Own:

    Shared Potential Candidate Ideology:
       - Does the Candidate Map to the Same C
        Yes: 1
        No: 0
        Close: 2
        Unkown: 4


Transaction Information:     
        
        Amount:   
            <400: 1
            400 - 5,0000: 2
            5,000 - 50,0000: 3
            >50,000 : 4


        Type:
            See CodeBook:
                Should Be Able to Preform Reduced Bucketing


Contributer Information:
    Type:
        Committe "C": 1
        Individauls "T" : 2
        Unkown " ": 0
    
    Degree of Support:
        Weak: abs([0.5, 1.25]
        Strong: abs() > 1.25
        Moderate: 0  abs() < 0.5

    Indv Last Known Ideology:
        Liberal: "Negative"
        Conservative: "Positive"
        Unkown: 4  "Error"


Location:
    State Ideology:
        "Dictionary Created"
        Very Liberal: -2 
        Liberal: -1
        BattleGround: 0
        Conservativ
        Very Conservative: 2


Label:
        Donate Democratic: 0
        Donate Republican: 1
        Not Donate Democrat: 2
        Not Donate Republican: 3
'''
stateIdeo = {"WA": -2, "OR": -2, "CA": -2, "NV": -2, "ID": 2, "MT": 1, 
             "WY":  2, "UT":  2, "AZ":  1, "ND":  1, "SD": 2, "NE": 2,
             "CO": -1, "NM": -2, "KS":  2, "OK":  2, "TX": 2, "MN": -2,
             "IA": -1, "MO":  0, "AR":  2, "LA":  2, "WI": -2, "IL":-2,
             "MI": -2, "IN":  0, "OH":  0, "KY":  2, "TN":  2, "MS": 2,
             "AL":  2, "WV":  2, "VA":  0, "NC":  0, "SC":  1, "GA": 1, "FL":0,
             "ME": -2, "VT": -2, "MA": -2, "RI": -2, "CT": -2, "NJ":-2, 
             "DE": -2, "MD": -2, "DC": -2, "NY": -2, "PA": -2, "Ak": 2,
             "HI": -2
            }

Col_Nums = {"cycle":0,"transaction_id":1,"transaction_type":2,"amount":3,"date":4,"bonica_cid":5,"contributor_name":6,"contributor_lname":7,
            "contributor_fname":8,"contributor_mname":9,"contributor_suffiline":10,"contributor_title":11,"contributor_ffname":12,"contributor_type":13,
            "contributor_gender":14,"contributor_address":15,"contributor_city":16,"contributor_state":17,"contributor_zipcode":18,
            "contributor_occupation":19,"contributor_employer":20,"contributor_category":21,"contributor_category_order":22,"is_corp":23,
            "organization_name":24,"parent_organization_name":25,"recipient_name":26,"bonica_rid":27,"recipient_party":28,"recipient_type":29,
            "recipient_state":30,"recipient_category":31,"recipient_category_order":32,"recipient_district":33,"seat":34,"election_type":35,
            "contributor_cfscore":36,"candidate_cfscore":37,"latitude":38,"longitude":39,"gis_confidence":40,"contributor_district_90s":41,
            "contributor_district_00s":42,"contributor_district_10s":43,"lname_frequency":44,"efec_memo":45,"efec_memo2":46,"efec_transaction_id_orig":47,
            "efec_org_orig":48,"efec_comid_orig":49,"efec_form_type":50}



def csv_parser(line):   

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        rv[Col_Nums["amount"]] = float(rv[Col_Nums["amount"]])
        rv[Col_Nums["contributor_cfscore"]] = float(rv[Col_Nums["contributor_cfscore"]])
        rv[Col_Nums["candidate_cfscore"]] = float(rv[Col_Nums["candidate_cfscore"]])
        rv[Col_Nums["contributor_state"]] = int(rv[Col_Nums["contributor_state"]])
        return rv

    except:
        rv = [1]  #This row will be removed in data cleaning

        return rv

def data_cleaning(sc, file_in):

    lines = sc.textFile(file_in)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove header lines

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) == 51) 
    
    return data


def create_vectors(line):

    key = line[Col_Nums["bonica_cid"]]
    v_cycle = line[Col_Nums["cycle"]]  #Does not need to be a number
    v_amount = line[Col_Nums["amount"]]
    contr_counter = 1

    contr_type = line[Col_Nums["contributor_type"]]
    if contr_type == "C":
        v_contr_type = 0
    elif contr_type == "I":
        v_contr_type = 1
    else:
        v_contr_type = 2

    gender = line[Col_Nums["contributor_gender"]]
    if gender == "M":
        v_gender = 0
    elif gender == "F":
        v_gender = 1
    else:
        v_gender = 2

    rec_type = line[Col_Nums["recipient_type"]]
    if rec_type == "COMM":
        v_rec_type = 0
    elif rec_type == "CAND":
        v_rec_type = 1
    else:
        v_rec_type = 0 

    contr_cfscore = line[Col_Nums["contributor_cfscore"]]

    if (contr_cfscore > -1.8 and contr_cfscore <= -1.1):
        v_contr_cfscore = -2.0

    elif (contr_cfscore > -1.1 and contr_cfscore  < -0.6):
        v_contr_cfscore = -1.0

    elif (contr_cfscore >= -0.6 and contr_cfscore <= 0.4):
        v_contr_cfscore = 0

    elif (contr_cfscore <= 0.8 and contr_cfscore > 0.4):
        v_contr_cfscore = 1.0

    elif (contr_cfscore <= 1.2 and contr_cfscore > 0.8):
        v_contr_cfscore = 2.0

    elif contr_cfscore > 1.2:
        v_contr_cfscore = 3.0

    else: #contr_cfscore <= -1.9
        v_contr_cfscore = -3.0


    # Need to analyze whether the same distribution exists for candidates as it does for contributors
    candidate_cfscore = line[Col_Nums["candidate_cfscore"]]

    if (candidate_cfscore > -1.8 and candidate_cfscore <= -1.1):
        v_candidate_cfscore = -2.0

    elif (candidate_cfscore > -1.1 and candidate_cfscore  < -0.6):
        v_candidate_cfscore = -1.0

    elif (candidate_cfscore >= -0.6 and candidate_cfscore <= 0.4):
        v_candidate_cfscore = 0

    elif (candidate_cfscore <= 0.8 and candidate_cfscore > 0.4):
        v_candidate_cfscore = 1.0

    elif (candidate_cfscore <= 1.2 and candidate_cfscore > 0.8):
        v_candidate_cfscore = 2.0

    elif candidate_cfscore > 1.2:
        v_candidate_cfscore = 3.0

    else: # candidate_cfscore <= -1.9
        v_candidate_cfscore = -3.0

    if v_candidate_cfscore != v_contr_cfscore:
        ideoDifference = abs(v_candidate_cfscore) - abs(v_contr_cfscore)
        if ideoDifference == 1:
            v_id_diff = 1
        elif ideoDifference == 2:
            v_id_diff = 2
        else:
            v_id_diff = 3
    else:
        v_id_diff = 0

    party = line[Col_Nums["recipient_party"]] #100 is dem; 200 is rep; 328 is ind

    if party == "100":
        label = 1
    elif party == "200":
        label = 2
    else:  #Try to determine Later
        if v_candidate_cfscore < 0:
            label = 1
        if v_candidate_cfscore >= 0:
            label = 2

    v_state = line[Col_Nums["contributor_state"]]

    features = { "contributor_types": set(v_contr_type), "gender": v_gender, "state": v_state, "recipient_type": v_rec_type,
                 "cycles": {v_cycle: 
                                {"count": 1, "amount": v_amount, "contr_cfscore": v_contr_cfscore, 
                                 1 : 
                                        {"label": label, "candidate_cfscore": v_candidate_cfscore}
                                }
                           }
                }

    # contr_dummy = contributor_company_dummy[contr_type] #fed in from above
    # rec_type_cand_dummy = cand_dummy[rec_type]
    # rec_type_comm_dummy = comm_dummy[rec_type]
    # #Do we want to include state?UT
    # rec_cat = line[Col_Nums["recipient_category"]]
    # rec_order = line[Col_Nums["recipient_category_order"]]
    # elect_type = line[Col_Nums["election_type"]]
    

    # rec_cfscore = line[Col_Nums["candidate_cfscore"]]

    vector = (key, features)
    return vector


def get_individuals(a, b):

    b_year = list(b["cycles"][0]
    
    if all(i < b_year for i in list(a["cycles"].keys())):
        a["state"] = b["state"] # Get the most recent     
        a["gender"] = b["gender"]

    if b["cycles"] not in a["cycles"]:
        a["cycles"][b_year] = b["cycles"][b_year]
    else:
        a["cycles"]["amount"] += b["cycles"]["amount"]
        a["cycles"]["count"] +=  1
        trans_num = a["cycles"]["count"]
        a["cycles"][b_year][trans_num] = b["cycles"][b_year][1]

    return a
    
 
def improve_vectos(line):

    pass

def main(file):

    sc = pyspark.SparkContext() 
    data = data_cleaning(sc, file)
    contributions = data.map(create_vectors)

    unique_indv = contributions.reduceByKey(get_individuals)

    print(unique_indv.first())


if __name__ == '__main__':

    file = "gs://cs123data/Data/contribDB_2004.csv"
    
    main(file)







