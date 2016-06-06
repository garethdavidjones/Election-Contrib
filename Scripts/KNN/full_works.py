import sys
import pyspark
import csv
from StringIO import StringIO
import numpy as np

#dictionary to link column numbers with column names from DIME dataset
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
    #function that reads a single line of a csv file 
    try:
        #return the line of the csv as a list if there are no errors
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        return rv
    except:
        #else just return a list with 1 as the only element
        #This row will be removed in data cleaning
        rv = [1]  
        return rv

def primary_data_cleaning(file_path):
    #function that handles the reading of the entire 
    #csv file, given a filepath to csv
    lines = sc.textFile(file_path)
    header = lines.first()
    #remove first line
    rm = lines.filter(lambda x: x != header) 
    #for each line, read it with the csv_parser function
    data = rm.map(csv_parser)
    #filter out lines that are of length 1
    data = data.filter(lambda x: len(x) != 1) 

    return data

def formKeyValues(x):
    #This is the mapping phase of working with the csv
    #Takes in a line from the read-in csv file and converts it into
    #key-value form with the key being each donor's unique id
    #and the value being all the data we wish ultimately to use in
    #categorizing donors

    #key (unique id for donor)
    cid = x[Col_Nums["bonica_cid"]]
    
    #values
    rv = []

    #contributor data
    #amount donated
    #this list will track donation history amounts, with 
    #the first indice being amount donated in 2012, the
    #second being 2010, 2008, etc.
    donation_hist_amt = [0] * 17
    #this part of the list will track number of donations history
    donation_hist_cnt - [0] * 17
    cycle = int(x[Col_Nums["cycle"]])
    amount = x[Col_Nums["amount"]]
    donation_hist_cnt[((2012 - cycle)/2)] = 1
    donation_hist_amt[((2012 - cycle)/2)] = amount

    #append to the value
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
    #bucketed in order to transfer from continuous to
    #categorical variable
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

    #append donor data to the value-list
    rv = rv + [cycle, zip_code, contributor_cfscore, is_corp, industry_order, industry, gender, contr_type]

    #Recipient Data

    #party
    party = x[Col_Nums["recipient_party"]] #100 is dem; 200 is rep; 328 is ind
    #general or primary election
    elect_type = x[Col_Nums["election_type"]]
    #candidate donated to cfscore
    rec_cfscore = x[Col_Nums["candidate_cfscore"]]
    #relative cfscore between candidate and donor
    rel_score = abs(rec_cfscore - contr_cfscore)    
    #append recipient info to value list
    #add a one to make total count easier
    #we have to store these variables as lists/sets within the value-list
    #since there can be multiple recipients to the same donor
    rv = rv + [1, [rec_cfscore], [rel_score], set([party]), set([elect_type])]

    #finally, return the key-value pair
    return (cid, rv)

def reduce_key_vals(a,b):
    #reduction step of reading in the DIME dataset
    #it should be noted that combining ought to be
    #handled internally with Spark
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
    #sum/append the count and lists together
    for i in range(42, 45):
        rv[i] = a[i] + b[i]
    #union the two sets together
    for i in range(45, 47):
        rv[i] = a[i].union(b[i])
    #make an additional variable to count the number of parties
    #the individual donated to and the number of elections (primary 
    #and general) that they donated to
    rv[47] = len(rv[45])
    rv[48] = len(rv[46])

    return rv


def get_similarity(RDD):
    #Calculates the number of similar traits between individual x and y

    #break the cartesian-pair into two
    x = RDD[0]
    y = RDD[1]

    factors = len(x)
    sim = 0

    #skip 1 because that's 2012 donation amount 
    for i in range(1, factors):
        if x[i] == y[i]:
            sim += 1
    #return a tuple with number of similar factors included
    return (x, (y, sim))
    
def get_trend(x):
    #after figuring out all of the similar donor's in the dataset, we predict
    #2012 donations based on the average ratio of 2010 donations with all previous donations 
    #amongst all similar donors
    
    sims = len(x)
    avg_percent = 0
    #for all similar donors
    for i in range(sims):
        #get similar donor data
        train = x[i][1][0]
        #sum of donation history
        #note, certain cycles are certainly 0 based on the number of cycles
        #we're looking back
        tot_don_2008 = sum(train[2:18])
        #amount donated in 2010
        don_2010 = train[1]
        percent = don_2010 / tot_don_2008
        avg_percent += percent / sims

    #return just a tuple of test's data and the average percent
    test = x[0][0]
    return (test, avg_percent)


def similarity_algo(test, train, k):
    #institutes the KNN algorithm over the read-in dataset
    #pair together the test dataset with the training dataset via cartesian product
    pairs = test.cartesian(train)
    #get similar count on every one of the pairs
    sim_pairs = pairs.map(get_similarity) 
    #filter out all pairs that have less than k similar features
    top_sim = sim_pairs.filter(lambda x: x[1][1] >= k)
    #reduce all pairs by the unique id key value
    #puts all data-pairs into a list
    sim_hist = top_sim.map(lambda x: [x]).reduceByKey(lambda a, b: a + b)
    #get average trend for every list of similar pairings
    trends = sim_hist.map(get_trend)
    #make predictions by multiplying the sum of all previous
    #donations of test case with the average percent
    predictions = trends.map(lambda x: (sum(x[0][1:17]) * x[1]))
    return predictions

def main(RDD):
    #takes in read-in csv data and returns it as reduced form

    key_vals = RDD.map(formKeyValues)
    reduced_form = key_vals.reduceByKey(reduce_key_vals)
    return reduced_form

if __name__ == '__main__':
    #k is the number of similar traits necessary for two neighbors to
    #be considered similar
    k = int(sys.argv[1])
    #cycles_back is the number of cyles to trace donation history for
    cycles_back = int(sys.argv[2])
    #start spark
    sc = pyspark.SparkContext()

    main_file_path = "gs://cs123data/Data/full_data.csv"

    #get training data (donor behavior up to 2010)
    year_min = 2012 - cycles_back * 2
    full_data = primary_data_cleaning(main_file_path)
    train_data = full_data.filter(lambda x: x[0] <= 2010 and  x[0] >= year_min)
    #tried taking a 10% sample, and that did not work
    keyVals_train = main(train_data).sample(False, 0.10, 23)
    print("training data done")

    #get testing data (donor behavior up to 2012)
    test_data = full_data.filter(lambda x: x[0] <= 2012 and x[0] >= year_min)
    keyVals_test = main(test_data).sample(False, 0.10, 23)
    print("testing data done")

    #conduct KNN over test versus training data
    results = similarity_algo(keyVals_test, keyVals_train, k)
    #still should get error
    results.saveAsTextFile("gs://cs123data/Output/heres2hope.txt")

    #ultimately would have calculated MSE with actual 2012 donations but program 
    #failed to run efficiently, so opted for random forest
