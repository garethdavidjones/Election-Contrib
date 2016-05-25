import matplotlib.pylab as plt
import seaborn as sns
import pandas as pd

output = [('', 3882045), ('32506', 4), ('21043', 2), ('VA', 2), ('B12', 10833), ('B11', 1749), ('Z09', 16507), ('PA', 3), ('Z08', 123), ('Z02', 20269), ('Z04', 2206), ('Z07', 1660), ('82718', 1), ('P03', 9496), ('P02', 6251), ('P01', 7151), ('P05', 9993), ('P04', 11337), ('Y0000', 481), ('K2000', 3), ('Y1000', 1), ('FL', 2), ('Y2000', 1), ('TX', 2), ('E11', 641), ('E10', 1175), ('E08', 11705), ('E09', 613), ('E07', 2649), ('E04', 3343), ('E01', 16884), ('rincon', 1), ('Y3000', 2), ('85259', 3), ('N08', 3292), ('N05', 17348), ('N04', 4008), ('N07', 2885), ('N06', 1338), ('N01', 7845), ('N00', 3441), ('N03', 8503), ('N02', 6550), ('G6400', 1), ('N12', 18650), ('N13', 5409), ('N16', 1314), ('N14', 1484), ('N15', 13672), ('MD', 5), ('77354', 1), ('C01', 12804), ('C02', 5209), ('C03', 5042), ('C04', 8639), ('C05', 5802), ('A11', 322), ('A10', 3549), ('A4200', 2), ('77339', 10), ('NA', 61670), ('NC', 1), ('X1200', 11), ('A09', 6857), ('A02', 3609), ('Q15', 2789), ('G6500', 1), ('Q14', 3030), ('Q16', 10500), ('A01', 13403), ('A06', 3953), ('Q11', 3769), ('CT', 1), ('Q10', 3539), ('A07', 6289), ('A04', 2905), ('Q13', 2353), ('B5500', 1), ('A05', 1117), ('Q12', 185), ('H03', 4615), ('H02', 12537), ('H01', 53384), ('H05', 2393), ('H04', 10807), ('T1100', 1), ('C1100', 2), ('G5000', 1), ('Q08', 15758), ('Q09', 4399), ('Q02', 20638), ('Q03', 13074), ('Q01', 17251), ('Q04', 3452), ('Q05', 8582), ('Y02', 57592), ('Y03', 346), ('Y00', 1286), ('Y01', 26178), ('Y04', 129375), ('F05', 2108), ('H5000', 1), ('F04', 2449), ('F07', 29490), ('F06', 2662), ('F03', 20233), ('48768968', 1), ('F09', 26501), ('UT', 1), ('Z9020', 2), ('F13', 13712), ('F10', 35322), ('F11', 11988), ('61837590', 4), ('Z9010', 4), ('H5100', 5), ('W04', 18873), ('W05', 1007), ('W06', 115150), ('W07', 3614), ('15717', 1), ('W02', 2604), ('W03', 12249), ('K01', 83206), ('K02', 15246), ('D03', 2288), ('D02', 3715), ('D01', 5250), ('07945', 1), ('M06', 1640), ('M04', 4116), ('M05', 3024), ('M02', 12016), ('M03', 3573), ('M01', 8735), ('AL', 1), ('H1130', 5), ('B01', 6721), ('B00', 123), ('B02', 12977), ('ActBlue', 5), ('B09', 3008), ('B08', 7157)]


df = pd.DataFrame(output, columns=["Type", "Count"])
df.head()

sns.barplot(df.Type, df.Count)
plt.title("Contributer Type")
plt.show()