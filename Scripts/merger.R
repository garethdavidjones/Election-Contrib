library(stringr)
library(dplyr)
library(classInt)

Data_ZipZACTmerged <- read.csv("D:/Data_ZipZACTmerged.csv")
ACS_11_5YR_DP03_ann <- read.csv("C:/Users/Brandon/Downloads/ACS_11_5YR_DP03/ACS_11_5YR_DP03_with_ann.csv", header = F)

ACS_11_5YR_DP03_ann = ACS_11_5YR_DP03_ann[-1,]

colnames(ACS_11_5YR_DP03_ann) =  as.character(unlist(ACS_11_5YR_DP03_ann[1,]))
ACS_11_5YR_DP03_ann = ACS_11_5YR_DP03_ann[-1,]

ACS_11_5YR_DP03_ann$ZCTA = as.numeric(as.character(substr(ACS_11_5YR_DP03_ann$Geography, 7, 11)))

new_data = full_join(Data_ZipZACTmerged, ACS_11_5YR_DP03_ann, by = "ZCTA")

#delete repeated rows

uniques = unique(new_data$ZCTA)

unique_rows = c()
for(row in uniques){
  row_got = which(new_data$ZCTA == row)[1]
  unique_rows = append(unique_rows, row_got)
}

new_data = new_data[unique_rows,]

#delete columns without info

comp = matrix(" ( X ) ", nrow(new_data), 1)

drops = c()
for(col in colnames(new_data)){
  if(is.na(all(new_data[,col] == comp))){
    drops = append(drops, col)
  }
}

new_data = new_data[,!colnames(new_data) %in% drops]

comp = matrix("(X)", nrow(new_data), 1)

drops = c()
for(col in colnames(new_data)){
  if(is.na(all(new_data[,col] == comp))){
    drops = append(drops, col)
  }
}

new_data = new_data[,!colnames(new_data) %in% drops]

drops = c()

for(x in colnames(new_data)){
  if(grepl("Margin of Error", x)){
    drops = append(drops, x)
  }
}

new_data = new_data[,!colnames(new_data) %in% drops]

#get rid of estimates/totals

drop.na = c()
for(col in colnames(new_data[,c(6:559)])){
  comp = as.numeric(as.character((new_data[,col])))
  comp[is.na(comp)] = 0
  if(any(comp > 100)){
    drop.na = append(drop.na, col)
  }
}

new_data = new_data[,!colnames(new_data) %in% drop.na]

new_data = new_data[,-c(190,191)]

drops = c()
for(x in colnames(new_data)){
  if(grepl("Number", x)|grepl("Estimate", x)){
    drops = append(drops, x)
  }
}

new_data = new_data[,!colnames(new_data) %in% drops]

new_data = new_data[,-c(2,3,4)]

for(col in colnames(new_data)[3:262]){
  ints = classIntervals(as.numeric(new_data[,col]), n = 10, style = "kmeans")
  new_data[,paste(col, "cat", sep = "_")] = findCols(ints)
}

new_data$ZIP = str_pad(new_data$ZIP, 5, pad = "0")
new_data$ZCTA = str_pad(new_data$ZCTA, 5, pad = "0")

write.csv(new_data, file = "D:/updated_merger_3.csv")
