import pyspark
import csv
from StringIO import StringIO
from operator import add
from pyspark.mllib.regression import LabeledPoint
import pysark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel

# http://www.270towin.com/2008_Election/
stateIdeo = {"WA": -2, "OR": -2, "CA": -2, "NV": -2, "ID": 2, "MT": 1,
             "WY":  2, "UT":  2, "AZ":  1, "ND":  1, "SD": 2, "NE": 2,
             "CO": -1, "NM": -2, "KS":  2, "OK":  2, "TX": 2, "MN": -2,
             "IA": -1, "MO":  0, "AR":  2, "LA":  2, "WI": -2, "IL":-2,
             "MI": -2, "IN":  0, "OH":  0, "KY":  2, "TN": 2, "MS": 2,
             "AL":  2, "WV":  2, "VA":  0, "NC":  0, "SC": 1, "GA": 1, "FL": 0,
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



# http://www.forbes.com/sites/betsyschiffman/2015/11/10/full-list-most-expensive-zip-codes-in-2015/7/#5a128f712907
top20zips = set([94027, 11962, 10012, 81656, 10013, 33109, 94062, 91302, 81611, 94010, 94022, 
             07620, 94920, 90210, 10065, 89413, 90402, 11976, 80111, 94957])

next100zips = set([11975, 94123, 93108, 02108, 90077, 11932, 10011, 33156, 90265, 090274, 10014, 
               10006, 94028, 10024, 33143, 94301, 92662, 10007, 94920, 95030, 90272, 92067, 
               92657, 11568, 94133, 91108, 95070, 10021, 81654, 06831, 95030, 92661, 90266, 
               93920, 92625, 10023, 94024, 90049, 90069, 11024, 92651, 92091, 11930, 10001, 
               94904, 10022, 94022, 10069, 06870, 11959, 91008, 10004, 98039, 02554, 34102, 
               07976, 92014, 10580, 10003, 10577, 80113, 06870, 33149, 11231, 94306, 93953, 
               02493, 06830, 90212, 94506, 90274, 06840, 11217, 32461, 83014, 81655, 94303, 
               10075, 60043, 80121, 92118, 92660, 11765, 89402, 07078, 02574, 02116, 93066, 
               90254, 06878, 92037, 11968, 90401, 02210, 93460, 96714, 91302, 02467, 94507, 
               94025])

GENERALS = set(range(1980, 2016, 4))
ALL_YEAR = set(range(1980, 2014, 2))
RECENT = set(range(2000, 2014, 2))


def csv_parser(line):

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        rv[Col_Nums["amount"]] = int(abs(float(rv[Col_Nums["amount"]])))  # Conver to Integer to Reduce Memory
        rv[Col_Nums["contributor_cfscore"]] = float(rv[Col_Nums["contributor_cfscore"]])  # Consider changing to in fro ^ reason
        rv[Col_Nums["candidate_cfscore"]] = float(rv[Col_Nums["candidate_cfscore"]])
        rv[Col_Nums["cycle"]] = int(rv[Col_Nums["cycle"]])
        return rv

    except:
        rv = [1]  # This row will be removed in data cleaning

        return rv


def data_cleaning(sc, file_in):

    lines = sc.textFile(file_in)
    header = lines.first()
    rm = lines.filter(lambda x: x != header)  # Remove header lines

    data = rm.map(csv_parser)
    data = data.filter(lambda x: len(x) == 51)

    return data


def build_features(line, testing=False):

    key = line[Col_Nums["bonica_cid"]]
    v_cycle = line[Col_Nums["cycle"]]  # Does not need to be a number
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

    elif (contr_cfscore > -1.1 and contr_cfscore < -0.6):
        v_contr_cfscore = -1.0

    elif (contr_cfscore >= -0.6 and contr_cfscore <= 0.4):
        v_contr_cfscore = 0

    elif (contr_cfscore <= 0.8 and contr_cfscore > 0.4):
        v_contr_cfscore = 1.0

    elif (contr_cfscore <= 1.2 and contr_cfscore > 0.8):
        v_contr_cfscore = 2.0

    elif contr_cfscore > 1.2:
        v_contr_cfscore = 3.0

    else:  # Contr_cfscore <= -1.9
        v_contr_cfscore = -3.0


    # Need to analyze whether the same distribution exists for candidates as it does for contributors
    candidate_cfscore = line[Col_Nums["candidate_cfscore"]]

    if (candidate_cfscore > -1.8 and candidate_cfscore <= -1.1):
        v_candidate_cfscore = -2.0

    elif (candidate_cfscore > -1.1 and candidate_cfscore < -0.6):
        v_candidate_cfscore = -1.0

    elif (candidate_cfscore >= -0.6 and candidate_cfscore <= 0.4):
        v_candidate_cfscore = 0

    elif (candidate_cfscore <= 0.8 and candidate_cfscore > 0.4):
        v_candidate_cfscore = 1.0

    elif (candidate_cfscore <= 1.2 and candidate_cfscore > 0.8):
        v_candidate_cfscore = 2.0

    elif candidate_cfscore > 1.2:
        v_candidate_cfscore = 3.0

    else:  # candidate_cfscore <= -1.9
        v_candidate_cfscore = -3.0

    if v_candidate_cfscore != v_contr_cfscore:
        ideoDifference = abs(v_candidate_cfscore - v_contr_cfscore)
        if ideoDifference == 1:
            v_id_diff = 1
        elif ideoDifference == 2:
            v_id_diff = 2
        else:
            v_id_diff = 3
    else:
        v_id_diff = 0

    party = line[Col_Nums["recipient_party"]]  # 100 is dem; 200 is rep; 328 is ind

    if party == "100":
        label = 1
    elif party == "200":
        label = 2
    else:   # Try to determine Later
        if v_candidate_cfscore < 0:
            label = 1
        if v_candidate_cfscore >= 0:
            label = 2

    v_state = line[Col_Nums["contributor_state"]]

    features = {"contributor_types": v_contr_type, "gender": v_gender, "state": v_state, "recipient_type": v_rec_type, "total_amount": v_amount,  "contr_cfscore": v_contr_cfscore,
                 "cycles": {v_cycle:
                        {"count": 1, "amount": v_amount,
                            1: {"label": label, "candidate_cfscore": v_candidate_cfscore}}}}

    vector = (key, features)
    return vector

def reduce_individuals(a, b):

    b_year = list(b["cycles"])[0]

    a["total_amount"] += b["total_amount"]

    # Get the most recent characteristics for the individual
    if all(i < b_year for i in list(a["cycles"].keys())):
        a["state"] = b["state"]
        a["contr_cfscore"] = b["contr_cfscore"]

    if b_year not in a["cycles"]:
        a["cycles"][b_year] = b["cycles"][b_year]

    else:
        a["cycles"][b_year]["amount"] += b["cycles"][b_year]["amount"]
        a["cycles"][b_year]["count"] +=  1
        trans_num = a["cycles"][b_year]["count"]
        a["cycles"][b_year][trans_num] = b["cycles"][b_year][1]

    return a


def create_vectors(line):

    # gender, contribution_type, state, recipient_type
    values = line[1]
    cycles = set(values["cycles"].keys())
    num_recent = len(RECENT - cycles)
    num_general = len(GENERALS - cycles)

    gender = values["gender"]
    cf_score = values["contr_cfscore"]

    # Need to bucket
    avg_contributed = values["total_amount"] / len(cycles)

    if avg_contributed <= 500:
        v_avg = 0
    elif (avg_contributed > 500) and (avg_contributed < 5000):
        v_avg = 1
    elif (avg_contributed >= 5000) and (avg_contributed < 50000):
        v_avg = 2
    else:
        v_avg = 3

    return (line[0], [num_recent, num_general, gender, cf_score, v_avg])


def evaluate_transactions(line):

    key = line[Col_Nums["bonica_cid"]]
    party = line[Col_Nums["recipient_party"]]  # 100 is dem; 200 is rep; 328 is ind

    candidate_cfscore = line[Col_Nums["candidate_cfscore"]]

    if (candidate_cfscore > -1.8 and candidate_cfscore <= -1.1):
        v_candidate_cfscore = -2.0

    elif (candidate_cfscore > -1.1 and candidate_cfscore < -0.6):
        v_candidate_cfscore = -1.0

    elif (candidate_cfscore >= -0.6 and candidate_cfscore <= 0.4):
        v_candidate_cfscore = 0

    elif (candidate_cfscore <= 0.8 and candidate_cfscore > 0.4):
        v_candidate_cfscore = 1.0

    elif (candidate_cfscore <= 1.2 and candidate_cfscore > 0.8):
        v_candidate_cfscore = 2.0

    elif candidate_cfscore > 1.2:
        v_candidate_cfscore = 3.0

    else:  # candidate_cfscore <= -1.9
        v_candidate_cfscore = -3.0

    if party == "100":
        label = 1.0
    elif party == "200":
        label = 2.0
    else:  # Try to determine Later
        if v_candidate_cfscore < 0:
            label = 1.0
        if v_candidate_cfscore >= 0:
            label = 2.0

    return (key, label)


def transfomation(main_file, sc):

    full_data = data_cleaning(sc, main_file)
    # Evaluate 2012 Testing Data
    data_2012 = full_data.filter(lambda x: x[0] == 1984)  # Should probably filter out other transaction types
    evaluated_data = data_2012.map(evaluate_transactions)  # Determine the label for each transaction
    evaluations = evaluated_data.reduceByKey(lambda x, y: x)  # An RDD of Unique Keys

    # Create Vectors
    transactions = full_data.map(build_features)  # Collect Information On Every Transaction
    individuals = transactions.reduceByKey(reduce_individuals)  # Turn Each person into a vector based on their contributions
    vectorized = individuals.map(create_vectors)

    # Finalize Ouput Data
    non_contributors = vectorized.subtractByKey(evaluations)
    contributors = vectorized.join(evaluations)

    labeled_non_contributors = non_contributors.map(lambda x: LabeledPoint(0.0, x[1]))
    labeled_contributors = contributors.map(lambda x: LabeledPoint(x[1], x[0]))
    combined = labeled_non_contributors.union(labeled_contributors)

    return combined


def build_model(trainingData):

    pass


def main(main_file, sc):

    data = transfomation(main_file, sc)
    trainingData, testData, CVdata = data.randomSplit([0.70, 0.15, 0.15])

    traniningData.cache()
    testData.cache()
    CVdata.cache()

    predictions = model.predict(testData.map(lambda x: x.features))
    model = GradientBoostedTrees.trainClassifier(, {}, numIterations=100)



if __name__ == '__main__':

    main_file = "gs://cs123data/Data/practice.csv"
    sc = pyspark.SparkContext(appName="LabelMaker")

    main(main_file, sc)
