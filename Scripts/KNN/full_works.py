import sys
import pyspark
import csv
from StringIO import StringIO
import numpy as np

Col_Nums = {"cycle":0,"transaction_id":1,"transaction_type":2,"amount":3,"date":4,"bonica_cid":5,"contributor_name":6,"contributor_lname":7,
            "contributor_fname":8,"contributor_mname":9,"contributor_suffix":10,"contributor_title":11,"contributor_ffname":12,"contributor_type":13,
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
        return rv
    except:
        rv = [1]  #This row will be removed in data cleaning
        return rv

def primary_data_cleaning(file_path):

    lines = sc.textFile(file_path)
    header = lines.first()
    rm = lines.filter(lambda x: x != header) # remove first line

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) != 1) 

    return data

def formKeyValues(x):

    #key
    cid = x[Col_Nums["bonica_cid"]]
    
    #values

    rv = []

    #contributor data
    #amount donated
    donation_hist_amt = [0] * 17
    donation_hist_cnt - [0] * 17
    cycle = int(x[Col_Nums["cycle"]])
    amount = x[Col_Nums["amount"]]
    donation_hist_cnt[((2012 - cycle)/2)] = 1
    donation_hist_amt[((2012 - cycle)/2)] = amount

    rv = rv + donation_hist_cnt + donation_hist_amt

    #ind or committee?
    contr_type = x[Col_Nums["contributor_type"]]

    #gender
    gender = x[Col_Nums["contributor_gender"]]
    gender_dict = {"M":0, "F":1, "U":2, "":4}
    gender = gender_dict[gender]

    #industry involved
    industry = x[Col_Nums["contributor_category"]] 
    industry_order = x[Col_Nums["contributor_category_order"]]
    
    #is corporation if 1
    is_corp = x[Col_Nums["is_corp"]]

    #where does contributor fall on the spectrum?
    contr_cfscore = x[Col_Nums["contributor_cfscore"]]
    if contributor_cfscore < -1.9:
        contributor_cfscore = -2
    elif contributor_cfscore < -1.1:
        contributor_cfscore = -1
    elif contributor_cfscore < -0.6:
        contributor_cfscore = 0
    elif contributor_cfscore < 0.4:
        contributor_cfscore = 1
    elif contributor_cfscore < 0.8:
        contributor_cfscore = 2
    elif contributor_cfscore < 1.2:
        contributor_cfscore = 3
    else:
        contributor_cfscore = 4

    #zip
    zip_code = x[Col_Nums["contributor_zipcode"]]

    rv = rv + [cycle, zip_code, contributor_cfscore, is_corp, industry_order, industry, gender, contr_type]

    #Recipient Data

    #party
    party = x[Col_Nums["recipient_party"]] #100 is dem; 200 is rep; 328 is ind

    elect_type = x[Col_Nums["election_type"]]

    rec_cfscore = x[Col_Nums["candidate_cfscore"]]

    rel_score = abs(rec_cfscore - contr_cfscore)

    rv = rv + [1, [rec_cfscore], [rel_score], set([party]), set([elect_type])]

    return (cid, rv)

def reduce_key_vals(a,b):

    rv = []

    #sum all donation history

    for i in range(34):
        rv[i] = a[i] + b[i]
    
        #get latest individual info (though, it should
        #all be the same)

    cycle_old = a[34]
    cycle_new = b[34]

    if cycle_new > cycle_old:
        for i in range(34,42):
            rv[i] = b[i]
    else:
        for i in range(34,42):
            rv[i] = a[i]

    #Recipient data

    for i in range(42, 45):
        rv[i] = a[i] + b[i]

    for i in range(45, 47):
        rv[i] = a[i].union(b[i])

    rv[47] = len(rv[45])
    rv[48] = len(rv[46])

    #can add more if we want

    return rv


def get_similarity(RDD):

    x = RDD[0]
    y = RDD[1]

    factors = len(x)

    sim = 0

    #skip 1 because that's 2012 donation amount 
    for i in range(1, factors):
        if x[i] == y[i]:
            sim += 1

    return (x, (y, sim))
    
def get_trend(x):

    #trend is based purely on percent of current year of all previous years 
    
    sims = len(x)
    avg_percent = 0

    for i in range(sims):
        train = x[i][1][0]
        tot_don_2008 = sum(train[2:18])
        don_2010 = train[1]
        percent = don_2010 / tot_don_2008
        avg_percent += percent / sims

    test = x[0][0]

    return (test, avg_percent)


def similarity_algo(test, train, k):
    
    pairs = test.cartesian(train)
    sim_pairs = pairs.map(get_similarity) #second map? #One of these methods should work, but which?
    top_sim = sim_pairs.filter(lambda x: x[1][1] >= k)
    sim_hist = top_sim.groupByKey()
    trends = sim_hist.map(get_trend)
    predictions = trends.map(lambda x: (sum(x[0][1:17]) * x[1]))
    return predictions

def main(RDD):

    key_vals = RDD.map(formKeyValues)
    reduced_form = key_vals.reduceByKey(reduce_key_vals)

    return reduced_form

if __name__ == '__main__':

    k = sys.argv[1]
    cycles_back = int(sys.argv[2])

    sc = pyspark.SparkContext()

    main_file_path = "gs://cs123data/Data/full_data.csv"
    
    year_min = 2012 - cycles_back * 2

    full_data = primary_data_cleaning(main_file_path)
    train_data = full_data.filter(lambda x: x[0] <= 2010 and  x[0] >= year_min)
    keyVals_train = main(train_data)

    test_data = full_data.filter(lambda x: x[0] <= 2012 and x[0] >= year_min)
    keyVals_test = main(test_data)

    zip_path = "gs://cs123data/Auxillary/updated_merger_4.csv"
    zips = primary_data_cleaning(zip_path)
    keyVals_train = keyVals_train.leftOuterJoin(zips)
    keyVals_test = keyVals_test.leftOuterJoin(zips)

    results = similarity_algo(test_data, train_data, k)
    #still should get error
    results.saveAsTextFile("gs://cs123data/Output/heres2hope.txt")

#what to run: 
#gs://cs123data/Scripts/full_works.py