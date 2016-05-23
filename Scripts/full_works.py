import pyspark
import csv
from StringIO import StringIO
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

    #Bucketing Dictinaries


	#Donor Data

	cycle = x[Col_Nums["cycle"]]
	amount = x[Col_Nums["amount"]]
	cid = x[Col_Nums["bonica_cid"]]
	contr_type = x[Col_Nums["contributor_type"]]

	gender = x[Col_Nums["contributor_gender"]]
    gender_dict = {"M":0, "F":1, "":4}
    gender = gender_dict[gender]

	industry = x[Col_Nums["contributor_category"]] 
	industry_order = x[Col_Nums["contributor_category_order"]]
	is_corp = x[Col_Nums["is_corp"]]

	contr_cfscore = x[Col_Nums["contributor_cfscore"]]

    if contributor_cfscore < -2:
        contributor_cfscore = -2
    else if contributor_cfscore <= -0.3:
        contributor_cfscore = -1
    else if contributor_cfscore <= 0.3:
        contributor_cfscore = 0
    else if contributor_cfscore <= 1:
        contributor_cfscore = 1
    else if contributor_cfscore > 1:
        contributor_cfscore = 2
    else:
        contributor_cfscore = 4

    state = x[Col_Nums["contributor_state"]]
    stateIdeo = {"WA": -2, "OR": -2, "CA":-2. "NV": -2, "ID": 2, "MT": 1, 
                     "WY": 2, "UT": 2, "AZ": 1, "ND": 1, "SD": 2, "NE": 2,
                     "CO": -1, "NM": -2, "KS": 2, "OK": 2, "TX": 2, "MN": -2,
                     "IA": -1, "MO": 0, "AR": 2, "LA":2, "WI":-2, "IL":-2,
                     "MI": -2, "IN": 0, "OH": 0, "KY": 2, "TN": 2, "MS": 2,
                     "AL":2, "WV":2, "VA": 0, "NC": 0, "SC": 1, "GA": 1, "FL":0,
                     "ME": -2, "VT": -2, "MA":-2, "RI": -2, "CT":-2, "NJ":-2, 
                     "DE": -2, "MD": -2, "DC": -2, "NY":-2, "PA": -2, "AK": 2,
                     "HI": -2, "":4
                    }
    State_Ideo = stateIdeo[state]

	#Recipient Data

	party = x[Col_Nums["recipient_party"]] #100 is dem; 200 is rep; 328 is ind

	rec_type = x[Col_Nums["recipient_type"]] #ind or party/pac
    rec_type_dict = {"CAND":0, "COMM":1, "":4}
    rec_type = rec_type_dict[rec_type]

	#Do we want to include state?
	rec_cat = x[Col_Nums["recipient_category"]]
	rec_order = x[Col_Nums["recipient_category_order"]]

	elect_type = x[Col_Nums["election_type"]]
    elect_type_dict = {"P":0 ,"G":1, "":4}
    elect_type = elect_type_dict[elect_type]

	rec_cfscore = x[Col_Nums["candidate_cfscore"]]

    rel_score = abs(rec_cfscore - contr_cfscore)

    #just suggested bucketing
    if rel_score < 0.2:
        rel = 1
    else if rel_score < 1:
        rel = 2
    else if rel_score >= 1:
        rel = 0
    else:
        rel = 4

	return (cid, (set(float(cycle)), float(amount), contr_type, gender, industry, industry_order, is_corp,
				 contributor_cfscore, State_Ideo, [party], [rec_type], [rec_cat], [rec_order], [elect_type], [rel_score], 1, {}))

def reduce_key_vals(a,b):
	#total amount sufficient? I'd like to look into amount trajectory as well

    cycle = list(b[0])[0]

    if cycle not in a[17].keys():
        a[17][cycle] = 0

	return (a[0].union(b[0]), a[1] + b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], a[10] + b[10], a[11] + b[11], a[12] + b[12], a[13] + b[13],
			a[14] + b[14], a[15] + b[15], a[16] + b[16], a[17][cycle] += b[1]) 

def get_similarity(x, y):
    #x and y are both RDD's

    factors = len(x)

    sim = 0

    for i in range(factors):
        if x[i] == y[i]:
            sim += 1
            #I don't think we'll need to use Jaccard since vectors are of the same length
    return (x, (y, sim))
    
def get_trend(x):

    #trend is based purely on percent of current year of all previous years 

    percent_sum = 0
    count = len(x)

    for i in range(count):
        donations_dict = x[i][17]
        current_year = donations_dict.pop(2010) #assuming we're doing 2012
        prev_years = sum(donations_dict.values())
        percent_sum += (current_year / prev_years) 

    avg_percent = percent_sum / count

    return avg_percent


def similarity_algo(RDD, k):
    
    pairs = RDD.cartesian(RDD)
    sim_pairs = pairs.map(get_similarity)
    top_sim = sim_pairs.map(lambda y: y.filter(lambda x: x[1][1] >= k)).collect() #Wow, I don't think this will work
    trends = top_sim.map(get_trend)
    get_predictors = trends.union(RDD)
    predictions = get_predictors.map(lambda x: x[0] * x[1][1])

    return predictions



def main(RDD):

	key_vals = RDD.map(formKeyValues)
	reduced_form = key_vals.reduceByKey(reduce_key_vals)

	return reduced_form

if __name__ == '__main__':

    k = sys.argv[1]
    #years_back = sys.argv[2]
    #can yse the foreachfunction
    sc = pyspark.SparkContext()
    data = primary_data_cleaning()
    keyVals = main(data)
    


    keyVals.saveAsTextFile("gs://cs123data/Output/first_attempt_2.txt")

#what to run: 
#gs://cs123data/Scripts/full_works.py
