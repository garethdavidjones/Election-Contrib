library(stringr)
library(dplyr)
library(classInt)

#file merges demographic and economic data found in two separate files
#provided by the US Census. It then filters out any unnecessary columns
#(for example, margin of error columns). Finally, it clusters the remaining
#columns using k-means with a k set at 10.

#read in both csv's of census data
Data_ZipZACTmerged <- read.csv("D:/Data_ZipZACTmerged.csv")
ACS_11_5YR_DP03_ann <- read.csv("C:/Users/Brandon/Downloads/ACS_11_5YR_DP03/ACS_11_5YR_DP03_with_ann.csv", header = F)

#set up a ZCTA column (original file has ZCTA's written as a part of a larger
#string in its own column)
ACS_11_5YR_DP03_ann = ACS_11_5YR_DP03_ann[-1,]
colnames(ACS_11_5YR_DP03_ann) =  as.character(unlist(ACS_11_5YR_DP03_ann[1,]))
ACS_11_5YR_DP03_ann = ACS_11_5YR_DP03_ann[-1,]
ACS_11_5YR_DP03_ann$ZCTA = as.numeric(as.character(substr(ACS_11_5YR_DP03_ann$Geography, 7, 11)))

#perform a full join of the two datasets on the ZCTA column
#Census economic estimates are only on ZCTA's, not zip codes
new_data = full_join(Data_ZipZACTmerged, ACS_11_5YR_DP03_ann, by = "ZCTA")

#delete repeated zipcode rows
uniques = unique(new_data$ZIP)
unique_rows = c()
for(row in uniques){
  row_got = which(new_data$ZIP == row)
  unique_rows = append(unique_rows, row_got)
}
#keep only unique rows
new_data = new_data[unique_rows,]

#delete columns without info
#some columns are only filled with ( X ) or (X)
comp = matrix(" ( X ) ", nrow(new_data), 1)
drops = c()
for(col in colnames(new_data)){
  #for some reason, returns na if true, so have to 
  #add additional is.na step
  if(is.na(all(new_data[,col] == comp))){
    drops = append(drops, col)
  }
}

#keep all columns that should not be dropped
new_data = new_data[,!colnames(new_data) %in% drops]

#perform same procedure for (x) columns
comp = matrix("(X)", nrow(new_data), 1)
drops = c()
for(col in colnames(new_data)){
  if(is.na(all(new_data[,col] == comp))){
    drops = append(drops, col)
  }
}

new_data = new_data[,!colnames(new_data) %in% drops]

#drop columns that contain the word "Margin of Error", "Number" or "Estimate"
#in their title. To control for our predictions, we only care about percents,
#not raw counts or margins of error
drops = c()
for(x in colnames(new_data)){
  if(grepl("Margin of Error", x)|grepl("Number", x)|grepl("Estimate", x)){
    drops = append(drops, x)
  }
}
#keep all columns not to be dropped
new_data = new_data[,!colnames(new_data) %in% drops]

#Go through once again to make sure we only have columns that
#represent percents. Although this isn't a perfect test (some columns
#like median number of children in a family for example will 
#likely have less than 100 in every row), this does a fair job 
#at completing what we need
end = ncol(new_data)
drop = c()
#make sure we don't remove ZCTA's or zipcodes in process, so
#start at column 6
for(col in colnames(new_data[,c(6:end)])){
  comp = as.numeric(as.character((new_data[,col])))
  #treat na's as 0's
  comp[is.na(comp)] = 0
  #if any row is above 100, drop
  if(any(comp > 100)){
    drop = append(drop, col)
  }
}
#keep all columns not in drop
new_data = new_data[,!colnames(new_data) %in% drop]

#finally, clean up by removing ID and Geography column
#as well as columns 2,3,4 (both of the ZCTA and one Zip column)
#Note, this was done with a later version of the data and 
#might not have been implemented in certain or any of the 
#model building programs
new_data = new_data[,-which(colnames(new_data) == "Id")]
new_data = new_data[,-which(colnames(new_data) == "Geography")]
new_data = new_data[,-c(2,3,4)]

#apply kmeans to the cleaned data
end = ncol(new_data)
for(col in colnames(new_data)[3:end]){
  ints = classIntervals(as.numeric(new_data[,col]), n = 10, style = "kmeans")
  #make a new column with the name of the original column name + _cat
  new_data[,paste(col, "cat", sep = "_")] = findCols(ints)
}

#add leading zeroes to zip code and convert into string
#this is done for the merger performed in vectorizer
new_data$ZIP = str_pad(new_data$ZIP, 5, pad = "0")

#delete all the original column data such that we are
#left with only categories
new_data = new_data[,-(2:end)]

#save csv
write.csv(new_data, file = "D:/updated_merger_6.csv")
