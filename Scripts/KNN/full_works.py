import pyspark
import csv
from StringIO import StringIO
from gaussalgo.knn import compute_neighbors
import numpy as np

#PRIMARY_FILE_PATH = 'gs://cs123data/Data/full_data.csv'
PRIMARY_FILE_PATH = 'gs://cs123data/Data/contribDB_1980.csv'

Col_Nums = {"cycle":0,"transaction_id":1,"transaction_type":2,"amount":3,"date":4,"bonica_cid":5,"contributor_name":6,"contributor_lname":7,
            "contributor_fname":8,"contributor_mname":9,"contributor_suffix":10,"contributor_title":11,"contributor_ffname":12,"contributor_type":13,
            "contributor_gender":14,"contributor_address":15,"contributor_city":16,"contributor_state":17,"contributor_zipcode":18,
            "contributor_occupation":19,"contributor_employer":20,"contributor_category":21,"contributor_category_order":22,"is_corp":23,
            "organization_name":24,"parent_organization_name":25,"recipient_name":26,"bonica_rid":27,"recipient_party":28,"recipient_type":29,
            "recipient_state":30,"recipient_category":31,"recipient_category_order":32,"recipient_district":33,"seat":34,"election_type":35,
            "contributor_cfscore":36,"candidate_cfscore":37,"latitude":38,"longitude":39,"gis_confidence":40,"contributor_district_90s":41,
            "contributor_district_00s":42,"contributor_district_10s":43,"lname_frequency":44,"efec_memo":45,"efec_memo2":46,"efec_transaction_id_orig":47,
            "efec_org_orig":48,"efec_comid_orig":49,"efec_form_type":50}
'''

Important!!
    We can only use cateogrical or numeric values, not both


Resources:
    KNN Lecture: http://cis.poly.edu/~mleung/FRE7851/f07/k-NearestNeighbor.pdf
    Categorical Paper: https://www.cis.upenn.edu/~sudipto/mypapers/categorical.pdf

    Important Questions:
        How are we defining distance:

        Determing Optimal K:
            Data shouldn't prevent an iterative approach
                Consdier cross validation methods


'''



def csv_parser(line):

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        return rv
    except:
        rv = [1]  #This row will be removed in data cleaning
        return rv

def primary_data_cleaning():

    lines = sc.textFile(PRIMARY_FILE_PATH)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove first line

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) == 51) 

    return data

def formKeyValues(x):

    #title_dummies = 

    contributor_company_dummy = {'I':0, 'C':1, '':0}
    
    female_dummy = {'M':0,'F':1, 'U':0, '':0}
    male_dummy = {'M':1,'F':0, 'U':0, '':0}

    cand_dummy = {"CAND": 1, 'COMM':0, '':0}
    comm_dummy = {"CAND": 0, 'COMM':1, '':0}

    #Donor Data

    cycle = float(x[Col_Nums["cycle"]])
    amount = x[Col_Nums["amount"]]
    cid = x[Col_Nums["bonica_cid"]]
    contr_type = x[Col_Nums["contributor_type"]]
    contr_dummy = contributor_company_dummy[contr_type] #fed in from above
    gender = x[Col_Nums["contributor_gender"]]
    contr_female_dummy = female_dummy[gender] #fed in from above
    contr_male_dummy = male_dummy[gender] #fed in from above
    #zip_code = x[Col_Nums["contributor_zipcode"]] #clsoer the zip closer the donor?; should we just do long/lat?
    industry = x[Col_Nums["contributor_category"]] 
    industry_order = x[Col_Nums["contributor_category_order"]]
    is_corp = x[Col_Nums["is_corp"]]
    contr_cfscore = x[Col_Nums["contributor_cfscore"]]
    lat = x[Col_Nums["latitude"]]
    lng = x[Col_Nums["longitude"]]
    #Recipient Data
    #rid = x[Col_Nums["bonica_rid"]]
    party = x[Col_Nums["recipient_party"]] #100 is dem; 200 is rep; 328 is ind
    rec_type = x[Col_Nums["recipient_type"]]
    rec_type_cand_dummy = cand_dummy[rec_type]
    rec_type_comm_dummy = comm_dummy[rec_type]
    #Do we want to include state?
    rec_cat = x[Col_Nums["recipient_category"]]
    rec_order = x[Col_Nums["recipient_category_order"]]
    elect_type = x[Col_Nums["election_type"]]
    rec_cfscore = x[Col_Nums["candidate_cfscore"]]

    return (cid, (set(cycle), float(amount), contr_dummy, contr_female_dummy, contr_male_dummy, industry, industry_order, is_corp,
                 contr_cfscore, lat, lng, [party], [rec_type_cand_dummy], [rec_type_comm_dummy], [rec_cat], [rec_order], [election_type], [rec_cfscore], 1))

def reduce_key_vals(a,b):
    #total amount sufficient? I'd like to look into amount trajectory as well

    return (a[0].union(b[0]), a[1] + b[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10], a[11] + b[11], a[12] + b[12], a[13] + b[13],
            a[14] + b[14], a[15] + b[15], a[16] + b[16], a[17] + b[17], a[18] + b[18]) #the issue could be theat we don't know which are floats and which are just floats


def main(RDD):

    key_vals = RDD.map(formKeyValues)
    reduced_form = key_vals.reduceByKey(reduce_key_vals)

    return reduced_form

if __name__ == '__main__':

    sc = pyspark.SparkContext()
    data = primary_data_cleaning()
    keyVals = main(data)
    keyVals.saveAsTextFile("gs://cs123data/Output/first_attempt_2.txt")

#what to run: 
#gs://cs123data/Scripts/full_works.py