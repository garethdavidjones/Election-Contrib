library(stringr)
library(dplyr)
library(classInt)
library(plyr)

Data_ZipZACTmerged <- read.csv("D:/Data_ZipZACTmerged.csv")
ACS_11_5YR_DP03_ann <- read.csv("C:/Users/Brandon/Downloads/ACS_11_5YR_DP03/ACS_11_5YR_DP03_with_ann.csv", header = F)
updated_merger_4 = read.csv("D:/updated_merger_4.csv")

ACS_11_5YR_DP03_ann = ACS_11_5YR_DP03_ann[-1,]

colnames(ACS_11_5YR_DP03_ann) =  as.character(unlist(ACS_11_5YR_DP03_ann[1,]))
ACS_11_5YR_DP03_ann = ACS_11_5YR_DP03_ann[-1,]

ACS_11_5YR_DP03_ann$ZCTA = as.numeric(as.character(substr(ACS_11_5YR_DP03_ann$Geography, 7, 11)))

new_data = full_join(Data_ZipZACTmerged, ACS_11_5YR_DP03_ann, by = "ZCTA")

new_data = new_data[-which(is.na(new_data$ZCTA)),]
write.csv(new_data, file = "D:/updated_merger_cities.csv")
rm(new_data)

new_data = read.csv("D:/updated_merger_cities.csv")
#delete repeated cities

uniques = unique(new_data$City.Name)

unique_rows = c()
for(row in uniques){
  row_got = which(new_data$City.Name == row) #just take first appearance?
  unique_rows = append(unique_rows, row_got)
}

new_data = new_data[unique_rows,]
end_2 = ncol(updated_merger_4)
cols = str_sub(colnames(updated_merger_4[3:end_2]), 1, -5)
cols = append(cols, "City.Name")
new_data = new_data[,colnames(new_data) %in% cols]

end = ncol(new_data)
for(col in colnames(new_data)[2:end]){
  print(col)
  ints = classIntervals(as.numeric(new_data[,col]), n = 10, style = "kmeans")
  new_data[,paste(col, "cat", sep = "_")] = findCols(ints)
}

new_data = new_data[-c(2:end)]

write.csv(new_data, file = "D:/updated_merger_cities.csv")
