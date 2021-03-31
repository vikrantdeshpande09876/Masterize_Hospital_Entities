##############################################################################################################
###########################################     PROJECT: SITE MASTER     #####################################
##############################################################################################################
#   PURPOSE: 
#     - Read the cleaned-dataframe generated from Python and try to invoke the RecordLinkage library source-code

#   INPUTS:
#     - BINARIES_NAME
#     - BINARIES_EXTENSION
#     - THRESHOLD_FOR_INDIVIDUAL
#     - THRESHOLD_FOR_ADDRESS_COMBINED
#     - SCALING_FACTOR
#     - curr_country
#     - RAW_SCORES_DIRECTORY
#     - TOTAL_MATCHES_THRESHOLD

#   OUTPUTS:
#     - CSV file with each candidate-pair having match-score>=TOTAL_MATCHES_THRESHOLD

# VERSION FOR BUILD:
#     - R version 3.4.4 (2018-03-15)

# SCRIPT VERSION:
#     - 1.0

# CREATED ON:
#     - 2021-03-17

# CREATOR:
#     - Vikrant Deshpande

# LAST UPDATED ON:
#     - 2021-03-28

# LAST UPDATED BY:
#     - Vikrant Deshpande

# REQUIRES:
#   - tools: R package
########################################################################################################################################################




## Load Required Libraries and Scripts
#require(RODBC)
#require(dplyr)
#require(RODBCDBI)
#install.packages("RecordLinkage")
#install.packages(c('DBI', 'RSQLite', 'ff', 'ffbase', 'e1071', 'ada', 'ipred', 'evd', 'data.table', 'xtable'))
#install.packages("https://cran.r-project.org/src/contrib/Archive/RecordLinkage/RecordLinkage_0.4-11.2.tar.gz", repos = NULL, type="source")
#require(RecordLinkage)
require(tools)

## Create DB connections
#print(paste("DB details", sql_dsn, sql_id , sql_pwd ))
#db_conn <- odbcConnect(sql_dsn , uid=sql_id, pwd=sql_pwd , DBMSencoding="latin1")
#print(db_conn)

## Read data from DB
#output <- sqlQuery(db_conn, "")



# Remove all objects in current R Workspace
rm(list = ls(all.names = TRUE))
args=commandArgs(trailingOnly = TRUE)
#args="levenshtein .dll 0.85 0.75 3 United_States Raw_Scores 4 Linkage Master_Data\\United_States_2_Master.csv Master_Data\\United_States_3_Master.csv"
print(args)
args=as.list(strsplit(args, " ")[[1]])

start=Sys.time()

# Initialize all the parameters for the R computations
BINARIES_NAME=args[1][[1]]# "levenshtein"
BINARIES_EXTENSION=args[2][[1]]# ".dll"
THRESHOLD_FOR_INDIVIDUAL=as.numeric(args[3][[1]])# 0.85
THRESHOLD_FOR_ADDRESS_COMBINED=as.numeric(args[4][[1]])# 0.75
SCALING_FACTOR=as.numeric(args[5][[1]])# 3
curr_country=args[6][[1]]# 'United_States'
TARGET_DIRECTORY=args[7][[1]]# 'Raw_Scores'
TARGET_CSV_NAMES=c("SR_NUM_1", "SR_NUM_2", "SITE_NAME_COMPARISON_SCORE",
                   "STATE_COMPARISON_SCORE", "CITY_COMPARISON_SCORE", "POSTAL_CODE_COMPARISON_SCORE",
                   "CONCAT_ADDRESS_COMPARISON_SCORE", "NUM_OF_MATCHES_FOUND")

# Thresholds as scaling factors for each candidate-pairs' match-scores calculated
THRESHOLDS_COLUMNS= c("SITE_NAME", "STATE", "CITY",
                      "POSTAL_CODE", "CONCAT_ADDRESS")
THRESHOLDS_VALUES=  c(THRESHOLD_FOR_INDIVIDUAL, THRESHOLD_FOR_INDIVIDUAL, THRESHOLD_FOR_INDIVIDUAL,
                      THRESHOLD_FOR_INDIVIDUAL, THRESHOLD_FOR_ADDRESS_COMBINED)
SCALING_FACTORS=    c(1, 1, 1, 
                      1, SCALING_FACTOR)

TOTAL_MATCHES_THRESHOLD=as.numeric(args[8][[1]])# 4
METHOD=args[9][[1]]# Dedup or Linkage

# Load the utility functions
source("SourceCode_Record_Linkage.R")

# Experimental: If the levenshtein C function is not loaded already, load the pre-compiled binaries to which it belongs.
if (!is.loaded(BINARIES_NAME)){
  print(paste0("Loading ",BINARIES_NAME,BINARIES_EXTENSION," !"))
  dyn.load(paste0(BINARIES_NAME, BINARIES_EXTENSION))
}else {
  print(paste0(BINARIES_NAME,BINARIES_EXTENSION," is already loaded !"))
}


# Experimental: To increase allocated RAM size and invoke garbage-collector
# if (memory.limit()!=4000){
#   memory.limit(size=4000)
# }
gc()

# Deduplicate the incoming dataset, and create an output /Raw_Scores/country_Score_Features.csv file
if (METHOD=="Dedup"){
  
  # Replaced this character in notepad
  # Read the country-specific batch with columns for only relevant match-score
  country_df=read.csv(paste0(curr_country,'_country_df.csv'), encoding="UTF-8")
  country_df[is.na(country_df)]=""
  n_rows=nrow(country_df)
  n_candidates=n_rows*(n_rows-1)/2
  print(paste("NRows=",n_rows,", Candidate-pairs=",n_candidates,", Columns are "))
  print(names(country_df))
  COLUMNS_TO_KEEP_IN_CSV=c("id2","id1","SITE_NAME","STATE","CITY","POSTAL_CODE","CONCAT_ADDRESS","NUM_OF_MATCHES_FOUND")
  
  candidate_pairs=processDedupBatch(country_df)
  if (nrow(candidate_pairs)>0){
    for (i in 1:nrow(candidate_pairs)){
      candidate_pairs[i,'SR_NUM_1']=country_df[candidate_pairs[i,'SR_NUM_1'],1]
      candidate_pairs[i,'SR_NUM_2']=country_df[candidate_pairs[i,'SR_NUM_2'],1]
    }
    write_df_to_csv(df=candidate_pairs, root_dir = TARGET_DIRECTORY, curr_country = curr_country, file_suffix = "_Score_Features.csv", index_flag = FALSE)
  }else{
    dummy.names=names(candidate_pairs)
    candidate_pairs=rbind(candidate_pairs, c(0, 0, 0, 0, 0, 0, 0, 0))
    names(candidate_pairs)=dummy.names
    print("No potential matches found in the incoming dataset! Creating a dummy csv...")
    write_df_to_csv(df=candidate_pairs, root_dir = TARGET_DIRECTORY, curr_country = curr_country, file_suffix = "_Score_Features.csv", index_flag = FALSE)
  }
  
}else if (METHOD=="Linkage"){
  
  FIRST_FILE=args[10][[1]]# First_dataset csv
  SECOND_FILE=args[11][[1]]# Second_dataset csv
  # Replaced this character in notepad
  # Read the country-specific batch with columns for only relevant match-score
  country_df=read.csv(FIRST_FILE, encoding="UTF-8")
  country_df[is.na(country_df)]=""
  n_rows=nrow(country_df)
  print(paste("NRows=",n_rows,", Columns are "))
  print(names(country_df))
  
  
  country_df2=read.csv(SECOND_FILE, encoding="UTF-8")
  country_df2[is.na(country_df2)]=""
  n_rows=nrow(country_df2)
  print(paste("NRows=",n_rows,", Columns are "))
  print(names(country_df2))
  COLUMNS_TO_KEEP_IN_CSV=c("id1","id2","SITE_NAME","STATE","CITY","POSTAL_CODE","CONCAT_ADDRESS","NUM_OF_MATCHES_FOUND")
  
  candidate_pairs=processLinkageBatch(country_df, country_df2)
  if (nrow(candidate_pairs)>0){
    for (i in 1:nrow(candidate_pairs)){
      candidate_pairs[i,'SR_NUM_1']=country_df[candidate_pairs[i,'SR_NUM_1'],1]
      candidate_pairs[i,'SR_NUM_2']=country_df2[candidate_pairs[i,'SR_NUM_2'],1]
    }
    write_df_to_csv(df=candidate_pairs, root_dir = TARGET_DIRECTORY, curr_country = curr_country, file_suffix = "_Score_Features.csv", index_flag = FALSE)
  }else{
    dummy.names=names(candidate_pairs)
    candidate_pairs=rbind(candidate_pairs, c(0, 0, 0, 0, 0, 0, 0, 0))
    names(candidate_pairs)=dummy.names
    print("No potential matches found in the incoming 2 datasets! Creating a dummy csv...")
    write_df_to_csv(df=candidate_pairs, root_dir = TARGET_DIRECTORY, curr_country = curr_country, file_suffix = "_Score_Features.csv", index_flag = FALSE)
  }
  
}

#View(candidate_pairs)
end=Sys.time()
print(end-start)