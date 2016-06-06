# This file containts the helper functions used for creating vectors
import pyspark
import csv
from StringIO import StringIO
from operator import add

ROM_CF = 1.175483
OBA_CF = -1.648152 

GENERALS = set(range(1980, 2016, 4))
ALL_YEAR = set(range(1980, 2014, 2))
RECENT = set(range(2000, 2014, 2))

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

# Zip codes that do not have demographic information available; military bases and territories
BAD_ZIPS = set(['00801', '00802', '00803', '00804', '00805', '00820', '00821', '00822', '00823', '00824', '00830', 
                '00831', '00840', '00841', '00850', '00851', '09002', '09003', '09004', '09005', '09006', '09007', 
                '09008', '09009', '09010', '09011', '09012', '09013', '09014', '09020', '09021', '09028', '09033', 
                '09034', '09038', '09042', '09046', '09049', '09051', '09053', '09054', '09055', '09058', '09059', 
                '09060', '09063', '09067', '09068', '09069', '09075', '09079', '09081', '09086', '09088', '09090', 
                '09092', '09094', '09095', '09096', '09099', '09100', '09102', '09103', '09104', '09107', '09112', 
                '09114', '09123', '09126', '09128', '09131', '09136', '09137', '09138', '09139', '09140', '09142', 
                '09143', '09154', '09172', '09173', '09177', '09180', '09186', '09201', '09211', '09213', '09214', 
                '09226', '09227', '09229', '09237', '09245', '09250', '09261', '09263', '09264', '09265', '09267', 
                '09301', '09302', '09304', '09305', '09306', '09307', '09308', '09309', '09310', '09311', '09312', 
                '09313', '09314', '09315', '09316', '09317', '09320', '09321', '09327', '09328', '09330', '09331', 
                '09332', '09333', '09334', '09336', '09337', '09338', '09339', '09340', '09342', '09343', '09344', 
                '09347', '09348', '09350', '09351', '09352', '09353', '09354', '09355', '09356', '09357', '09359', 
                '09360', '09363', '09364', '09365', '09366', '09367', '09368', '09369', '09370', '09371', '09372', 
                '09373', '09374', '09375', '09376', '09377', '09378', '09380', '09382', '09383', '09384', '09387', 
                '09391', '09393', '09394', '09396', '09397', '09399', '09403', '09421', '09447', '09454', '09459', 
                '09461', '09463', '09464', '09468', '09469', '09470', '09494', '09496', '09498', '09501', '09502', 
                '09503', '09504', '09505', '09506', '09507', '09508', '09509', '09510', '09511', '09513', '09517', 
                '09524', '09532', '09534', '09543', '09545', '09549', '09550', '09554', '09556', '09557', '09564', 
                '09565', '09566', '09567', '09568', '09569', '09570', '09573', '09574', '09575', '09576', '09577', 
                '09578', '09579', '09581', '09582', '09586', '09587', '09588', '09589', '09590', '09591', '09593', 
                '09594', '09596', '09599', '09602', '09603', '09604', '09605', '09606', '09607', '09608', '09609', 
                '09610', '09611', '09613', '09617', '09618', '09620', '09621', '09622', '09623', '09624', '09625', 
                '09626', '09627', '09630', '09631', '09633', '09636', '09642', '09643', '09645', '09647', '09648', 
                '09649', '09701', '09702', '09703', '09704', '09705', '09706', '09707', '09708', '09709', '09710', 
                '09711', '09713', '09714', '09715', '09716', '09717', '09718', '09719', '09720', '09721', '09722', 
                '09723', '09724', '09726', '09727', '09728', '09729', '09730', '09731', '09732', '09733', '09734', 
                '09735', '09736', '09737', '09738', '09739', '09741', '09742', '09743', '09744', '09745', '09747', 
                '09748', '09749', '09750', '09751', '09752', '09754', '09755', '09756', '09757', '09758', '09759', 
                '09762', '09769', '09771', '09777', '09780', '09798', '09801', '09803', '09804', '09805', '09806', 
                '09807', '09808', '09809', '09810', '09811', '09812', '09813', '09814', '09815', '09816', '09817', 
                '09818', '09820', '09821', '09822', '09823', '09824', '09825', '09826', '09827', '09828', '09829', 
                '09830', '09831', '09832', '09833', '09834', '09835', '09836', '09837', '09838', '09839', '09840', 
                '09841', '09842', '09844', '09845', '09846', '09852', '09853', '09855', '09858', '09859', '09862', 
                '09865', '09868', '09870', '09880', '09890', '09892', '09898', '34002', '34004', '34006', '34007', 
                '34008', '34011', '34020', '34021', '34022', '34023', '34024', '34025', '34030', '34031', '34032', 
                '34033', '34034', '34035', '34036', '34037', '34038', '34039', '34041', '34042', '34050', '34055', 
                '34058', '34060', '34078', '34090', '34091', '34092', '34093', '34095', '34098', '34099', '87115', 
                '96201', '96202', '96203', '96204', '96205', '96206', '96207', '96209', '96213', '96214', '96218', 
                '96220', '96224', '96257', '96258', '96259', '96260', '96262', '96264', '96266', '96267', '96269', 
                '96271', '96275', '96276', '96278', '96283', '96284', '96297', '96303', '96306', '96309', '96310', 
                '96319', '96321', '96322', '96323', '96326', '96328', '96330', '96336', '96337', '96338', '96339', 
                '96343', '96346', '96347', '96348', '96349', '96350', '96351', '96362', '96365', '96367', '96368', 
                '96370', '96372', '96373', '96374', '96375', '96376', '96377', '96378', '96379', '96384', '96386', 
                '96387', '96388', '96401', '96426', '96427', '96444', '96447', '96501', '96502', '96503', '96507', 
                '96510', '96511', '96515', '96516', '96517', '96518', '96520', '96521', '96522', '96530', '96531', 
                '96532', '96534', '96535', '96537', '96538', '96540', '96541', '96542', '96543', '96544', '96546',
                '96548', '96549', '96550', '96551', '96552', '96553', '96554', '96555', '96557', '96562', '96577', 
                '96595', '96598', '96599', '96601', '96602', '96603', '96604', '96605', '96606', '96607', '96608', 
                '96609', '96610', '96611', '96612', '96613', '96614', '96615', '96616', '96617', '96619', '96620', 
                '96621', '96622', '96624', '96628', '96629', '96643', '96650', '96657', '96660', '96661', '96662', 
                '96663', '96664', '96665', '96666', '96667', '96668', '96669', '96670', '96671', '96672', '96673', 
                '96674', '96675', '96677', '96678', '96679', '96681', '96682', '96683', '96686', '96687', '96698', 
                '96799', '96910', '96912', '96913', '96915', '96916', '96917', '96919', '96921', '96923', '96928', 
                '96929', '96931', '96932', '96939', '96940', '96941', '96942', '96943', '96944', '96950', '96951', 
                '96952', '96960', '96970'])

def csv_parser(line):

    try:
        rv = list(csv.reader(StringIO(line), delimiter=","))[0]
        zipCode = rv[Col_Nums["contributor_zipcode"]]
        if len(zipCode) == 1:
            return [1]
        if len(zipCode) < 5:
            num_zeros = 5 - len(zipCode)
            zipCode = str(0) * num_zeros + zipCode 
        else:
            zipCode = zipCode[:5]
        if zipCode in BAD_ZIPS:
            return [1]
        rv[Col_Nums["contributor_zipcode"]] = zipCode
        rv[Col_Nums["amount"]] = int(abs(float(rv[Col_Nums["amount"]])))  # Conver to Integer to Reduce Memory
        rv[Col_Nums["contributor_cfscore"]] = float(rv[Col_Nums["contributor_cfscore"]])  # Consider changing to in fro ^ reason
        rv[Col_Nums["candidate_cfscore"]] = float(rv[Col_Nums["candidate_cfscore"]])
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
    v_cycle = line[Col_Nums["cycle"]]
    v_amount = line[Col_Nums["amount"]]

    if v_cycle != "2012":
        v_total_amount = v_amount
    else:
        v_total_amount = 0

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
        v_rec_type = 2

    contr_cfscore = line[Col_Nums["contributor_cfscore"]]

    if (contr_cfscore > -1.8 and contr_cfscore <= -1.1):
        v_contr_cfscore = -2

    elif (contr_cfscore > -1.1 and contr_cfscore < -0.6):
        v_contr_cfscore = -1

    elif (contr_cfscore >= -0.6 and contr_cfscore <= 0.4):
        v_contr_cfscore = 0

    elif (contr_cfscore <= 0.8 and contr_cfscore > 0.4):
        v_contr_cfscore = 1

    elif (contr_cfscore <= 1.2 and contr_cfscore > 0.8):
        v_contr_cfscore = 2

    elif contr_cfscore > 1.2:
        v_contr_cfscore = 3

    else:  # Contr_cfscore <= -1.9
        v_contr_cfscore = -3


    # Need to analyze whether the same distribution exists for candidates as it does for contributors
    candidate_cfscore = line[Col_Nums["candidate_cfscore"]]

    if (candidate_cfscore > -1.8 and candidate_cfscore <= -1.1):
        v_candidate_cfscore = -2

    elif (candidate_cfscore > -1.1 and candidate_cfscore < -0.6):
        v_candidate_cfscore = -1
    elif (candidate_cfscore >= -0.6 and candidate_cfscore <= 0.4):
        v_candidate_cfscore = 0

    elif (candidate_cfscore <= 0.8 and candidate_cfscore > 0.4):
        v_candidate_cfscore = 1

    elif (candidate_cfscore <= 1.2 and candidate_cfscore > 0.8):
        v_candidate_cfscore = 2

    elif candidate_cfscore > 1.2:
        v_candidate_cfscore = 3

    else:  # i.e candidate_cfscore <= -1.9
        v_candidate_cfscore = -3
    
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
    v_zip = line[Col_Nums["contributor_zipcode"]]

    features = {"contributor_types": v_contr_type, 
                "gender": v_gender, 
                "state": v_state, 
                "ideo_dif": v_id_diff, 
                "zip": v_zip, 
                "recipient_type": v_rec_type,
                "total_counts": 1, 
                "total_amount": v_total_amount,
                "contr_cfscore": v_contr_cfscore, 
                "cycles": {
                        v_cycle:
                        {"count": 1, "amount": v_amount,
                            1: {"label": label, "candidate_cfscore": v_candidate_cfscore}}}}

    vector = (key, features)
    return vector

def reduce_individuals(a, b):

    b_year = list(b["cycles"])[0]

    a["total_amount"] += b["total_amount"]
    a["total_counts"] += 1

    # Get the most recent characteristics for the individual
    if all(i < b_year for i in list(a["cycles"].keys())):
        a["state"] = b["state"]
        a["contr_cfscore"] = b["contr_cfscore"]
        a["zip"] = b["zip"]
        a["contributor_types"] = b["contributor_types"]
        a["recipient_type"] = b["recipient_type"]


    if b_year not in a["cycles"]:
        a["cycles"][b_year] = b["cycles"][b_year]


    else:
        a["cycles"][b_year]["amount"] += b["cycles"][b_year]["amount"]
        a["cycles"][b_year]["count"] +=  1
        trans_num = a["cycles"][b_year]["count"]
        a["cycles"][b_year][trans_num] = b["cycles"][b_year][1]

    return a

def create_vectors(line):

    values = line[1]
    cid = line[0]
    cycles = set(values["cycles"].keys())
    num_contributions = values["total_counts"]
    num_recent = len(RECENT - cycles)
    num_general = len(GENERALS - cycles)
    zipCode = values["zip"]

    gender = values["gender"]
    cf_score = values["contr_cfscore"]
    recip_type = values["recipient_type"]
    contr_type = values["contributor_types"]

    if contr_type == "C":
        v_contrb_type = 0
    elif contr_type == "I":
        v_contrb_type = 1
    else:
        v_contrb_type = 2


    # Recipient Type
    if values['recipient_type'] == "COMM":
        v_recip = 0
    elif values['recipient_type'] == "CAND":
        v_recip = 1
    else:
        v_recip = 2

    previous_amt = 0
    for year in cycles:
        if year != "2012":
            previous_amt += values["cycles"][year]["amount"]

    ob_dif = abs(OBA_CF - cf_score)
    rom_dif = abs(ROM_CF - cf_score)
    if ob_dif < rom_dif: # Ideology more similar to Obama than Romney
        v_nearer = 0
    else:
        v_nearer = 1

    # Need to bucket
    avg_contributed = values["total_amount"] / len(cycles)

    if avg_contributed <= 500:
        v_avg = 0
    elif (avg_contributed > 500) and (avg_contributed <= 5000):
        v_avg = 1
    elif (avg_contributed > 5000) and (avg_contributed < 50000):
        v_avg = 2
    else:
        v_avg = 3
                        #      1            2          3        4       5       6            
    return (zipCode, [cid, num_recent, num_general, gender, cf_score, v_avg, v_nearer, 
                      v_contrb_type, v_recip, previous_amt, num_contributions])   
