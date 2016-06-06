def get_individuals(a, b):

    b_year = list(b["cycles"])[0]
    
    if all(i < b_year for i in list(a["cycles"].keys())):
        a["state"] = b["state"] # Get the most recent     
        a["gender"] = b["gender"]

    if b_year not in a["cycles"]:
        a["cycles"][b_year] = b["cycles"][b_year]
    else:
        a["cycles"][b_year]["amount"] += b["cycles"][b_year]["amount"]
        a["cycles"][b_year]["count"] +=  1
        trans_num = a["cycles"][b_year]["count"]
        a["cycles"][b_year][trans_num] = b["cycles"][b_year][1]
    return a
    