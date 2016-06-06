# from processingTools import */
import pyspark
from pyspark.mllib.regression import LabeledPoint
import sys
import csv
from StringIO import StringIO
from operator import add

Col_Nums = {"cycle":0, "transaction_id":1, "transaction_type":2,"amount":3,"date":4,
            "bonica_cid":5,"contributor_name":6,"contributor_lname":7, "contributor_fname":8,
            "contributor_mname":9,"contributor_suffiline":10,"contributor_title":11,
            "contributor_ffname":12,"contributor_type":13, "contributor_gender":14,
            "contributor_address":15,"contributor_city":16,"contributor_state":17,
            "contributor_zipcode":18,"contributor_occupation":19,"contributor_employer":20,
            "contributor_category":21,"contributor_category_order":22,"is_corp":23,
            "organization_name":24,"parent_organization_name":25,"recipient_name":26,
            "bonica_rid":27,"recipient_party":28,"recipient_type":29,"recipient_state":30,
            "recipient_category":31,"recipient_category_order":32,"recipient_district":33,
            "seat":34,"election_type":35,"contributor_cfscore":36,"candidate_cfscore":37,
            "latitude":38,"longitude":39,"gis_confidence":40,"contributor_district_90s":41,
            "contributor_district_00s":42,"contributor_district_10s":43, "lname_frequency":44,
            "efec_memo":45,"efec_memo2":46,"efec_transaction_id_orig":47, "efec_org_orig":48,
            "efec_comid_orig":49,"efec_form_type":50 }

def evaluate_transactions(line):

    key = line[Col_Nums["bonica_cid"]]
    party = line[Col_Nums["recipient_party"]]  # 100 is dem; 200 is rep; 328 is ind

    candidate_cfscore = line[Col_Nums["candidate_cfscore"]]

    if party == "100":
        label = 1
    elif party == "200":
        label = 2
    else:  # Try to determine Later
        if candidate_cfscore < 0:
            label = 1
        if candidate_cfscore >= 0:
            label = 2

    return (key, label)

def evaluation_determine(a,b):

    if a != b:
        return 3
    else:
        return a

def finalize_vectors(line):

    # Discard ZipCode
    left = line[1][0]   # Contributor Information
    right = line[1][1]  # Demographic Data 
    key = left.pop(0)  # Get CID key
    return (key, left + right)

def build_labels(line):

    key = line[0]
    label = line[1][1]
    features = line[0]
    return (label, features)

def parse_zipcodes(line):

    csv_line = list(csv.reader(StringIO(line), delimiter=","))[0]
    zipCode = csv_line[1]
    values = csv_line[2:]
    return (zipCode, values)

def main(main_file, output_directory, year, sc):

    from processingTools import data_cleaning, build_features, create_vectors, reduce_individuals

    # Convert from CSV to RDD and make type changes 
    full_data = data_cleaning(sc, main_file)
    # print "full_data", full_data.take(2)
    zipData = sc.textFile("gs://cs123data/Auxillary/updated_merger_4.csv")
    zipData = zipData.map(parse_zipcodes)

    zipData.cache()  # Look into doing this efficiently
    
    # Evaluate 2012 Testing Data
    data_2012 = full_data.filter(lambda x: x[0] == year)  # Should probably filter out other transaction types in datacleaning
    evaluated_data = data_2012.map(evaluate_transactions)  # Determine the label for each transaction
    evaluations = evaluated_data.reduceByKey(lambda x, y: x)  # An RDD of Unique Keys
    # print "evals", evaluations.take(2)

    # Create Vectors
    transactions = full_data.map(build_features)  # Collect Information On Every Transaction
    individuals = transactions.reduceByKey(reduce_individuals)  # Turn Each person into a vector based on their contributions

    transformed = individuals.map(create_vectors)
    merged = transformed.leftOuterJoin(zipData)
    bypass = merged.filter(lambda x: x[1]   [1] != None)
    vectorized = bypass.map(finalize_vectors)   

    non_contributors = vectorized.subtractByKey(evaluations)  # Non-Contributors Are Individuals Who Don't Appear in 
    contributors = vectorized.join(evaluations)
    labeled_non_contributors = non_contributors.map(lambda x: LabeledPoint(0.0, x[1])) 
    labeled_contributors = contributors.map(lambda x: LabeledPoint(x[1][1], x[1][0]))
    # Make final combination
    combined = labeled_non_contributors.union(labeled_contributors)
    combined.saveAsTextFile(output_directory)


if __name__ == '__main__':

    sc = pyspark.SparkContext(appName="partyVectorizer")
    args = sys.argv
    if len(args) < 3:
        print "Not enoughs passed: "
        print "Number Passed {}".format(len(args))
        print "Args Passed {}".format(args)
        sys.exit()
    else:
        if args[1] == "practice":
            input_file = "gs://cs123data/Data/practice.csv"
            year = "1984"
        elif args[1] == "full":
            input_file = "gs://cs123data/Data/full_data.csv"
            year = "2012"
        else:
            print args
            print "Options are full or practice"
            sys.exit()

        output_directory = "gs://cs123data/Output/" + args[2]
        main(input_file, output_directory, year, sc)
