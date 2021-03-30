# R CMD SHLIB levenshtein.c
# dyn.load("levenshtein.dll")


# DOCSTRING: Deduplicates an incoming dataset, and creates an output /Raw_Scores/country_Score_Features.csv

processBatch <- function(df){
  tryCatch({
    # Generate the match-scores for n(n-1)/2 candidate-pairs using the levenshtein stringComparison algorithm. Returns a list of pair-wise scores, input dataframes, and average-frequencies of unique values.
    data_candidate_pairs_freqs=compare_dedup(df, strcmp = TRUE, strcmpfun = levenshteinSim, exclude=c("SR_NUM"))
    
    # Extract the pair-wise scores and delete the entire output-list to free up RAM
    candidate_pairs=data_candidate_pairs_freqs$pairs
    rm(data_candidate_pairs_freqs)
    
    # Impute scores wherever NA with 0
    candidate_pairs[is.na(candidate_pairs)]=0
    print(paste("NRows=",nrow(candidate_pairs),", Columns are "))
    print(names(candidate_pairs))
    
    print("Scaling up column scores if threshold crossed")
    # Convert to scaled binary value: if column-comparison score is greater than required threshold, then scale-up this binary value by a factor
    for (i in 1:length(THRESHOLDS_COLUMNS)){
      colname=THRESHOLDS_COLUMNS[[i]]
      col_threshold=THRESHOLDS_VALUES[[i]]
      print(paste(colname," : ",col_threshold))
      candidate_pairs[colname]=ifelse( candidate_pairs[colname]>=col_threshold, 1*SCALING_FACTORS[[i]], 0)
    }
    
    # Create a separate column to indicate total-score of a candidate-pair
    candidate_pairs["NUM_OF_MATCHES_FOUND"]=rowSums(candidate_pairs[,THRESHOLDS_COLUMNS])
    
    # Wrangle the dataframe to generate CSV file with acceptable format
    candidate_pairs=candidate_pairs[COLUMNS_TO_KEEP_IN_CSV]
    names(candidate_pairs)=TARGET_CSV_NAMES
    
    # Filter only those candidate-pairs which have total-score greater than or equal to overall-threshold-of-matches
    candidate_pairs=subset(candidate_pairs, NUM_OF_MATCHES_FOUND>=TOTAL_MATCHES_THRESHOLD)
    # View(candidate_pairs)
    
    end=Sys.time()
    print(end-start)
    
    write_df_to_csv(df=candidate_pairs, root_dir = TARGET_DIRECTORY, curr_country = curr_country, file_suffix = "_Score_Features.csv", index_flag = FALSE)
    
  },
  error=function(e){
    print("Please check the size of incoming dataframe. We've seen issues with batch-size > 2000, since n(computations) will be ncols*[n(n-1)/2] !")
    print(e)
  })
}








# DOCSTRING: Returns TRUE if input is FALSE
isFALSE <- function (x) {
  identical(x, FALSE)
}








# DOCSTRING: Create an array of n(n-1)/2 length to identify all the candidate-pair combinations

unorderedPairs <- function (x)
{
  if (length(x) == 1) {
    if (!is.numeric(x) || x < 2)
      stop("x must be a vector or a number >= 2")
    return(array(unlist(lapply(1:(x - 1), function(k) rbind(k, (k + 1):x))), dim = c(2, x * (x - 1)/2)))
  }
  if (!is.vector(x))
    stop("x must be a vector or a number >= 2")
  n = length(x)
  return(array(unlist(lapply(1:(n - 1), function(k) rbind(x[k], x[(k + 1):n]))), dim = c(2, n * (n - 1)/2)))
}








# DOCSTRING: Write the dataframe to a csv file and throw error if it fails

write_df_to_csv <- function(df, root_dir='', curr_country='', file_suffix='_temp.csv', index_flag=FALSE){
  
  abs_path=paste0(root_dir, '//', curr_country, file_suffix)
  print(abs_path)
  tryCatch({
    write.csv(df, abs_path, row.names=index_flag)
    print(paste0('Successfully created //',abs_path,' !'))
  },
  error=function(e){
    print('Something went wrong while writing the file. Please check if it is currently in use.')
    print(e)
  })
}








# DOCSTRING: Calculate the Levenshtein-Distance between 2 strings by invoking the pre-compiled C-function

levenshteinDist <- function(str1, str2)
{
  # edge-cases
  if (typeof(str1) != "character" && class(str1) != "data.frame")
    stop(sprintf("Illegal data type: %s", typeof(str1)))
  
  if (typeof(str2) != "character" && class(str2) != "data.frame")
    stop(sprintf("Illegal data type: %s", typeof(str2)))
  
  if ((is.array(str1) || is.array(str2)) && !identical(dim(str1), dim(str2)))
    stop ("non-conformable arrays")
  
  if(length(str1)==0 || length(str2)==0) return(integer(0))
  
  
  l1 <- length(str1)
  l2 <- length(str2)
  out <- .C("levenshtein", as.character(str1), as.character(str2), l1, l2, ans=integer(max(l1,l2)))
  
  if (any(is.na(str1),is.na(str2)))
    out$ans[is.na(str1) | is.na(str2)]=NA
  
  if (is.array(str1))
    return(array(out$ans,dim(str1)))
  
  if (is.array(str2))
    return(array(out$ans,dim(str2)))
  
  return(out$ans)
}








# DOCSTRING: Calculate the Levenshtein-Distance between 2 strings and normalize it against max-length of the 2 strings

levenshteinSim <- function(str1, str2)
{
  return (1-(levenshteinDist(str1,str2)/pmax(nchar(str1),nchar(str2))))
}








# DOCSTRING: Version for single data set. Requires that both have the same format

compare_dedup <- function (dataset, strcmp = FALSE, strcmpfun = levenshteinSim, exclude = FALSE, identity = NA, n_match = NA, n_non_match = NA) 
{
  # edge-cases
  if (!is.data.frame(dataset) && !is.matrix(dataset)) 
    stop("Illegal format of dataset")
  ndata = nrow(dataset)
  nfields = ncol(dataset)
  if (ndata < 2) 
    stop("dataset must contain at least two records")
  if (is.character(strcmp)) 
    strcmp <- match(strcmp, colnames(dataset))
  if (!is.numeric(strcmp) && !is.logical(strcmp)) 
    stop("strcmp must be numeric or a single logical value")
  if (is.character(exclude)) 
    exclude <- match(exclude, colnames(dataset))
  if (!is.numeric(exclude) && !isFALSE(exclude)) 
    stop("exclude must be numeric or FALSE")
  if (!isFALSE(strcmp) && any(is.na(strcmp) | strcmp <= 0 | strcmp > nfields)) 
    stop("phonetic contains out of bounds index")
  if (!isFALSE(exclude) && any(is.na(exclude) | exclude <= 
                               0 | exclude > nfields)) 
    stop("phonetic contains out of bounds index")
  if (!is.na(n_match) && !is.numeric(n_match)) 
    stop("Illegal type for n_match!")
  if (!is.na(n_non_match) && !is.numeric(n_non_match)) 
    stop("Illegal type for n_match!")
  if (!identical(identity, NA)) {
    if (length(identity) != nrow(dataset)) {
      stop("Length of identity vector does not match number of records!")
    }
  }
  dataset = as.matrix(dataset, rownames.force = FALSE)
  dataset[dataset == ""] = NA
  dataset = as.data.frame(dataset)
  ret = list()
  ret$data = dataset
  full_data = as.matrix(dataset)
  if (is.numeric(exclude)) {
    if (is.numeric(strcmp)) {
      strcmp = setdiff(strcmp, exclude)
      strcmp = sapply(strcmp, function(x) return(x - length(which(exclude < x))))
    }
  }
  if (!is.function(strcmpfun)) {
    stop("strcmpfun is not a function!")
  }
  pair_ids = matrix(as.integer(0), nrow = 0, ncol = 2)
  if (is.na(n_match) || is.na(n_non_match)) {
    pair_ids = t(unorderedPairs(ndata))
  }
  else {
    tempdat = data.frame(id = 1:ndata, identity = identity)
    pairs = merge(x = tempdat, y = tempdat, by = 2)
    match_ids = as.matrix(pairs[as.integer(pairs[, 2]) < as.integer(pairs[, 3]), 2:3], rownames.force = FALSE)
    n_available_matches = nrow(match_ids)
    n_available_non_matches = ndata * (ndata - 1)/2 - n_available_matches
    if (n_available_matches < n_match && n_available_non_matches < n_non_match) {
      warning(sprintf("Only %d matches and %d non-matches!", n_available_matches, n_available_non_matches))
      pair_ids = t(unorderedPairs(ndata))
    }
    else {
      if (n_match > n_available_matches) {
        warning(sprintf("Only %d matches!", n_available_matches))
      }
      else {
        s = sample(nrow(match_ids), n_match)
        match_ids = match_ids[s, ]
      }
      if (n_non_match > n_available_non_matches) {
        warning(sprintf("Only %d non-matches!", n_available_non_matches))
        all_pairs = t(unorderedPairs(ndata))
        is_match = (identity[all_pairs[, 1]] == identity[all_pairs[,2]])
        non_match_ids = all_pairs[!is_match, ]
        pair_ids = rbind(match_ids, non_match_ids)
      }
      else {
        A = list()
        for (i in 1:n_non_match) {
          repeat {
            d1 = sample(ndata, 1)
            d2 = sample(ndata, 1)
            if (identical(identity[d1], identity[d2])) {
              next
            }
            if (d1 == d2) 
              next
            if (d1 > d2) {
              sorted_id = c(d2, d1)
            }
            else {
              sorted_id = c(d1, d2)
            }
            if (!is.null(A[[paste(sorted_id, collapse = " ")]])) {
              next
            }
            A[[paste(sorted_id, collapse = " ")]] = sorted_id
            break
          }
        }
        non_match_ids = matrix(unlist(A), ncol = 2, nrow = n_non_match, byrow = TRUE)
        pair_ids = rbind(match_ids, non_match_ids)
        rm(match_ids, non_match_ids, A)
      }
    }
  }
  if (is.numeric(exclude)) {
    dataset = dataset[, -exclude, drop = FALSE]
  }
  
  left <- dataset[pair_ids[, 1], , drop = FALSE]
  right <- dataset[pair_ids[, 2], , drop = FALSE]
  patterns = matrix(0, ncol = ncol(left), nrow = nrow(left))
  if (isTRUE(strcmp)) {
    patterns = strcmpfun(as.matrix(left, rownames.force = FALSE), as.matrix(right, rownames.force = FALSE))
  }
  else if (is.numeric(strcmp)) {
    patterns[, -strcmp] = (as.matrix(left[, -strcmp], rownames.force = FALSE) == as.matrix(right[, -strcmp], rownames.force = FALSE)) * 1
    patterns[, strcmp] = strcmpfun(as.matrix(left[, strcmp], rownames.force = FALSE), as.matrix(right[, strcmp], rownames.force = FALSE))
  }
  else {
    patterns = (left == right) * 1
  }
  rm(left)
  rm(right)
  is_match = identity[pair_ids[, 1]] == identity[pair_ids[, 2]]
  ret$pairs = as.data.frame(cbind(pair_ids, patterns, is_match))
  if (is.numeric(exclude)) {
    colnames(ret$pairs) = c("id1", "id2", colnames(ret$data)[-exclude], "is_match")
  }
  else {
    colnames(ret$pairs) = c("id1", "id2", colnames(ret$data), "is_match")
  }
  rownames(ret$pairs) = NULL
  ret$frequencies = apply(dataset, 2, function(x) 1/length(unique(x)))
  ret$type = "deduplication"
  class(ret) = "RecLinkData"
  return(ret)
}








# DOCSTRING: Version for two data sets. Requires that both have the same format

compare_linkage <- function(dataset1, dataset2, strcmp=FALSE, strcmpfun=jarowinkler, exclude=FALSE, identity1=NA, identity2=NA, n_match=NA, n_non_match=NA)
{
  # edge-cases
  if (!is.data.frame(dataset1) && !is.matrix(dataset1))
    stop ("Illegal format of dataset1")
  if (!is.data.frame(dataset2) && !is.matrix(dataset2))
    stop ("Illegal format of dataset2")
  if (ncol(dataset1) != ncol(dataset2))
    stop ("Data sets have different format")
  ndata1=nrow(dataset1) # number of records
  ndata2=nrow(dataset2)
  nfields=ncol(dataset1)
  if (ndata1<1 || ndata2<1) 
    stop ("empty data set")
  
  if (is.character(strcmp))
    strcmp <- match(strcmp, colnames(dataset1))
  if (!is.numeric(strcmp) && !is.logical(strcmp))
    stop ("strcmp must be numeric, character or a single logical value")
  if (!isFALSE(strcmp) && any(is.na(strcmp) | strcmp <= 0 | strcmp > nfields))
    stop ("strcmp contains out of bounds index")
  
  if (is.character(exclude))
    exclude <- match(exclude, colnames(dataset1))
  if (!is.numeric(exclude) && !is.logical(exclude))
    stop ("exclude must be numeric, character or a single logical value")
  if (!isFALSE(exclude) && any(is.na(exclude) | exclude <= 0 | exclude > nfields))
    stop ("exclude contains out of bounds index")
  if (!is.na(n_match) && !is.numeric(n_match))
    stop ("Illegal type for n_match!")
  if (!is.na(n_non_match) && !is.numeric(n_non_match))
    stop ("Illegal type for n_match!")
  
  if(!identical(identity1,NA))
  {
    if(length(identity1)!=nrow(dataset1))
    {
      stop("Length of identity1 does not match number of records!")
    }
  }
  
  if(!identical(identity2,NA))
  {
    if(length(identity2)!=nrow(dataset2))
    {
      stop("Length of identity2 does not match number of records!")
    }
  }
  
  
  dataset1=as.data.frame(dataset1)
  dataset2=as.data.frame(dataset2)
  ret=list()  # return object
  ret$data1=dataset1
  ret$data2=dataset2
  full_data1=as.matrix(dataset1, rownames.force=FALSE)
  full_data2=as.matrix(dataset2, rownames.force=FALSE)
  
  if (is.numeric(exclude))
  {        
    dataset1=dataset1[,-exclude, drop = FALSE]  # remove excluded columns
    dataset2=dataset2[,-exclude, drop = FALSE]  # remove excluded columns
    # adjust indices to list of included fields
    if (is.numeric(strcmp))
    {
      strcmp=setdiff(strcmp,exclude)
      strcmp=sapply(strcmp,function(x) return (x-length(which(exclude<x))))       
    }
  }
  dataset1[dataset1==""]=NA # label missing values
  dataset2[dataset2==""]=NA # label missing values
  full_data1[full_data1==""]=NA # label missing values
  full_data2[full_data2==""]=NA # label missing values
  dataset1=as.matrix(dataset1, rownames.force=FALSE)
  dataset2=as.matrix(dataset2, rownames.force=FALSE)
  
  if (!is.function(strcmpfun))
  {
    stop("strcmpfun is not a function!")
  }
  
  # Pair_ids collects ids of record pairs. It is a matrix because the following
  # rbind() calls are much faster than with a data.frame
  pair_ids=matrix(as.integer(0),nrow=0,ncol=2) # each row holds indices of one record pair
    if (is.na(n_match) || is.na(n_non_match))
    {
      # full outer join
      pair_ids=merge(1:nrow(dataset1),1:nrow(dataset2),all=TRUE)
      # sort to enforce particular order
      pair_ids=pair_ids[order(pair_ids[,1],pair_ids[,2]),]
    }   else
    {
      tempdat1=data.frame(id=1:ndata1,identity=identity1)
      tempdat2=data.frame(id=1:ndata2,identity=identity2)
      
      # Determine matches by join on identity vector 
      pairs=merge(x=tempdat1,y=tempdat2,by=2)
      match_ids=as.matrix(pairs[,2:3], rownames.force=FALSE)
      n_available_matches=nrow(match_ids)
      n_available_non_matches=ndata1*ndata2 - n_available_matches
      if (n_available_matches < n_match && n_available_non_matches < n_non_match)
      {
        warning(sprintf("Only %d matches and %d non-matches!",n_available_matches, n_available_non_matches))
        # return all pairs
        pair_ids=merge(1:nrow(dataset1),1:nrow(dataset2),all=TRUE)
      } else 
      {
        # draw required number
        if (n_match>n_available_matches)
        {
          warning(sprintf("Only %d matches!",n_available_matches))
          # in this case no sampling from match_ids
        } else
        {
          s=sample(nrow(match_ids),n_match)
          match_ids=match_ids[s,]
        }
        
        if (n_non_match > n_available_non_matches)
        {
          warning(sprintf("Only %d non-matches!",n_available_non_matches))
          all_pairs=merge(1:nrow(dataset1),1:nrow(dataset2),all=TRUE)
          is_match=(identity1[all_pairs[,1]]==identity2[all_pairs[,2]])
          non_match_ids=all_pairs[!is_match,]
          pair_ids=rbind(match_ids,non_match_ids)
        } else
        {
          # mark pairs already drawn in A
          A=list()
          for (i in 1:n_non_match)
          {
            repeat
            {
              d1=sample(ndata1,1)
              d2=sample(ndata2,1)
              # If a match has been drawn, try again
              if (identical(identity1[d1],identity2[d2]))
              {
                next
              }
              # If the pairs has been drawn already, try again
              if (!is.null(A[[paste(d1,d2)]]))
              {
                next
              }
              # Mark the pair as drawn
              A[[paste(d1,d2)]]=c(d1,d2)
              break
            }
          }
          non_match_ids=matrix(unlist(A),ncol=2,nrow=n_non_match,byrow=TRUE)
          pair_ids=rbind(match_ids,non_match_ids)
          rm(match_ids,non_match_ids,A)
        }
      }
    }
  
  rm(full_data1,full_data2)
  left=dataset1[pair_ids[,1],,drop=FALSE]
  right=dataset2[pair_ids[,2],,drop=FALSE]
  # matrix to hold comparison patterns
  patterns=matrix(0,ncol=ncol(left),nrow=nrow(left)) 
  if (isTRUE(strcmp))
  {
    patterns=strcmpfun(as.matrix(left, rownames.force=FALSE),as.matrix(right, rownames.force=FALSE))
  } else if (is.numeric(strcmp)) 
  {
    patterns[,-strcmp]=(as.matrix(left[,-strcmp], rownames.force=FALSE)==as.matrix(right[,-strcmp], rownames.force=FALSE))*1
    patterns[,strcmp]=strcmpfun(as.matrix(left[,strcmp], rownames.force=FALSE), as.matrix(right[,strcmp], rownames.force=FALSE)) #*1
  } else
  {
    patterns=(left==right)*1
  }
  rm(left)
  rm(right)
  
  is_match=as.numeric(identity1[pair_ids[,1]]==identity2[pair_ids[,2]]) # match status of pairs
  ret$pairs=as.data.frame(cbind(pair_ids, patterns, is_match)) # Matches
  
  colnames(ret$pairs)=c("id1","id2",colnames(dataset1),"is_match")
  rownames(ret$pairs)=NULL
  
  ret$frequencies=apply(rbind(dataset1,dataset2),2, function(x) 1/length(unique(x)))
  ret$type="linkage"
  class(ret)="RecLinkData"
  return(ret)
}

















# Original Version for two data sets
# Requires that both have the same format

compare.linkage <- function(dataset1, dataset2, blockfld=FALSE, phonetic=FALSE,
                            phonfun=soundex, strcmp=FALSE,strcmpfun=jarowinkler, exclude=FALSE, 
                            identity1=NA, identity2=NA, n_match=NA, n_non_match=NA)
{
  # edge-cases
  if (!is.data.frame(dataset1) && !is.matrix(dataset1))
    stop ("Illegal format of dataset1")
  if (!is.data.frame(dataset2) && !is.matrix(dataset2))
    stop ("Illegal format of dataset2")
  if (ncol(dataset1) != ncol(dataset2))
    stop ("Data sets have different format")
  ndata1=nrow(dataset1) # number of records
  ndata2=nrow(dataset2)
  nfields=ncol(dataset1)
  if (ndata1<1 || ndata2<1) 
    stop ("empty data set")
  
  if (is.character(strcmp))
    strcmp <- match(strcmp, colnames(dataset1))
  if (!is.numeric(strcmp) && !is.logical(strcmp))
    stop ("strcmp must be numeric, character or a single logical value")
  if (!isFALSE(strcmp) && any(is.na(strcmp) | strcmp <= 0 | strcmp > nfields))
    stop ("strcmp contains out of bounds index")
  
  if (is.character(phonetic))
    phonetic <- match(phonetic, colnames(dataset1))
  if (!is.numeric(phonetic) && !is.logical(phonetic))
    stop ("phonetic must be numeric, character or a single logical value")
  if (!isFALSE(phonetic) && any(is.na(phonetic) | phonetic <= 0 | phonetic > nfields))
    stop ("phonetic contains out of bounds index")
  
  if (is.character(exclude))
    exclude <- match(exclude, colnames(dataset1))
  if (!is.numeric(exclude) && !is.logical(exclude))
    stop ("exclude must be numeric, character or a single logical value")
  if (!isFALSE(exclude) && any(is.na(exclude) | exclude <= 0 | exclude > nfields))
    stop ("exclude contains out of bounds index")
  if (!is.na(n_match) && !is.numeric(n_match))
    stop ("Illegal type for n_match!")
  if (!is.na(n_non_match) && !is.numeric(n_non_match))
    stop ("Illegal type for n_match!")
  
  if(!identical(blockfld, FALSE))
  {
    if (!is.list(blockfld) && !is.null(blockfld)) blockfld <- list(blockfld)
    if (!all(sapply(blockfld, function(x) class(x) %in% c("character", "integer", "numeric"))))
      stop("blockfld has wrong format!")
    blockfld <- lapply(blockfld, 
                       function(x) {if (is.character(x)) match(x, colnames(dataset1)) else (x)})
    if(any(unlist(blockfld) <= 0 | unlist(blockfld) > nfields))
      stop("blockfld countains out-of-bounds value!")
  }
  
  if(!identical(identity1,NA))
  {
    if(length(identity1)!=nrow(dataset1))
    {
      stop("Length of identity1 does not match number of records!")
    }
  }
  
  if(!identical(identity2,NA))
  {
    if(length(identity2)!=nrow(dataset2))
    {
      stop("Length of identity2 does not match number of records!")
    }
  }
  
  dataset1=as.data.frame(dataset1)
  dataset2=as.data.frame(dataset2)
  ret=list()  # return object
  ret$data1=dataset1
  ret$data2=dataset2
  full_data1=as.matrix(dataset1, rownames.force=FALSE)
  full_data2=as.matrix(dataset2, rownames.force=FALSE)
  
  # keep phonetics for blocking fields
  if (is.numeric(phonetic))
  {
    phonetic_block=intersect(phonetic,unlist(blockfld))
  }
  if (is.numeric(exclude))
  {        
    dataset1=dataset1[,-exclude, drop = FALSE]  # remove excluded columns
    dataset2=dataset2[,-exclude, drop = FALSE]  # remove excluded columns
    # adjust indices to list of included fields
    if (is.numeric(phonetic)) 
    {
      phonetic=setdiff(phonetic,exclude)
      phonetic=sapply(phonetic,function(x) return (x-length(which(exclude<x))))
    }
    if (is.numeric(strcmp))
    {
      strcmp=setdiff(strcmp,exclude)
      strcmp=sapply(strcmp,function(x) return (x-length(which(exclude<x))))       
    }
  }
  # issue a warning if both phonetics and string metric are used on one field
  if ((length(intersect(phonetic,strcmp))>0 && !isFALSE(strcmp) && !isFALSE(phonetic)) ||
      (isTRUE(strcmp) && !isFALSE(phonetic)) ||
      (isTRUE(phonetic) && !isFALSE(strcmp)))
  {
    warning(sprintf("Both phonetics and string metric are used on some fields",length(intersect(phonetic,strcmp))))
  }
  dataset1[dataset1==""]=NA # label missing values
  dataset2[dataset2==""]=NA # label missing values
  full_data1[full_data1==""]=NA # label missing values
  full_data2[full_data2==""]=NA # label missing values
  dataset1=as.matrix(dataset1, rownames.force=FALSE)
  dataset2=as.matrix(dataset2, rownames.force=FALSE)
  
  if (!is.function(phonfun))
  {
    stop("phonfun is not a function!")
  }
  
  if (!isFALSE(phonetic)) # true, if phonetic is TRUE or not a logical value
  {
    if (isTRUE(phonetic)) # true, if phonetic is a logical value and TRUE
    {    
      dataset1=soundex(dataset1)
      dataset2=soundex(dataset2)
    } else # phonetic is not a logical value
      dataset1[,phonetic]=soundex(dataset1[,phonetic])
    dataset2[,phonetic]=soundex(dataset2[,phonetic])
  }
  
  if (!is.function(strcmpfun))
  {
    stop("strcmpfun is not a function!")
  }
  
  # Pair_ids collects ids of record pairs. It is a matrix because the following
  # rbind() calls are much faster than with a data.frame
  pair_ids=matrix(as.integer(0),nrow=0,ncol=2) # each row holds indices of one record pair
  if (isFALSE(blockfld))
  { 
    if (is.na(n_match) || is.na(n_non_match))
    {
      # full outer join
      pair_ids=merge(1:nrow(dataset1),1:nrow(dataset2),all=TRUE)
      # sort to enforce particular order
      pair_ids=pair_ids[order(pair_ids[,1],pair_ids[,2]),]
    }   else
    {
      tempdat1=data.frame(id=1:ndata1,identity=identity1)
      tempdat2=data.frame(id=1:ndata2,identity=identity2)
      
      # Determine matches by join on identity vector 
      pairs=merge(x=tempdat1,y=tempdat2,by=2)
      match_ids=as.matrix(pairs[,2:3], rownames.force=FALSE)
      n_available_matches=nrow(match_ids)
      n_available_non_matches=ndata1*ndata2 - n_available_matches
      if (n_available_matches < n_match && n_available_non_matches < n_non_match)
      {
        warning(sprintf("Only %d matches and %d non-matches!",
                        n_available_matches, n_available_non_matches))
        # return all pairs
        pair_ids=merge(1:nrow(dataset1),1:nrow(dataset2),all=TRUE)
      } else 
      {
        # draw required number
        if (n_match>n_available_matches)
        {
          warning(sprintf("Only %d matches!",n_available_matches))
          # in this case no sampling from match_ids
        } else
        {
          s=sample(nrow(match_ids),n_match)
          match_ids=match_ids[s,]
        }
        
        if (n_non_match > n_available_non_matches)
        {
          warning(sprintf("Only %d non-matches!",n_available_non_matches))
          all_pairs=merge(1:nrow(dataset1),1:nrow(dataset2),all=TRUE)
          is_match=(identity1[all_pairs[,1]]==identity2[all_pairs[,2]])
          non_match_ids=all_pairs[!is_match,]
          pair_ids=rbind(match_ids,non_match_ids)
        } else
        {
          # mark pairs already drawn in A
          A=list()
          for (i in 1:n_non_match)
          {
            repeat
            {
              d1=sample(ndata1,1)
              d2=sample(ndata2,1)
              # If a match has been drawn, try again
              if (identical(identity1[d1],identity2[d2]))
              {
                next
              }
              # If the pairs has been drawn already, try again
              if (!is.null(A[[paste(d1,d2)]]))
              {
                next
              }
              # Mark the pair as drawn
              A[[paste(d1,d2)]]=c(d1,d2)
              break
            }
          }
          non_match_ids=matrix(unlist(A),ncol=2,nrow=n_non_match,byrow=TRUE)
          pair_ids=rbind(match_ids,non_match_ids)
          rm(match_ids,non_match_ids,A)
        }
      }
    }
  } else  # branch for blocking
  {
    if (!is.list(blockfld)) blockfld=list(blockfld)
    for (blockelem in blockfld) # loop over blocking definitions
    {
      if (isTRUE(phonetic))
      {
        block_data1=phonfun(full_data1)
        block_data2=phonfun(full_data2)
      } else if (is.numeric(phonetic))
      {
        block_data1=full_data1
        block_data1[,phonetic_block]=phonfun(full_data1[,phonetic_block])
        block_data2=full_data2
        block_data2[,phonetic_block]=phonfun(full_data2[,phonetic_block])
      } else
      {
        block_data1=full_data1
        block_data2=full_data2
      }
      # for each record, concatenate values in blocking fields
      # do.call is faster than a former apply solution
      blockstr1 <- do.call(paste, as.data.frame(block_data1[,blockelem]))
      blockstr2 <- do.call(paste, as.data.frame(block_data2[,blockelem]))
      # exclude pairs with NA in blocking variable
      for (i in blockelem)
      {
        is.na(blockstr1)=is.na(block_data1[,i])
        is.na(blockstr2)=is.na(block_data2[,i])
      }
      rm(block_data1)
      rm(block_data2)
      
      id_vec=merge(data.frame(id1=1:ndata1,blockstr=blockstr1),
                   data.frame(id2=1:ndata2,blockstr=blockstr2),
                   incomparables=NA)[,-1]
      
      rm(blockstr1)
      rm(blockstr2)
      # reshape vector and attach to matrix of record pairs
      if (nrow(id_vec)>0)
        pair_ids=rbind(pair_ids,id_vec)
      rm(id_vec)
    }
    if (length(pair_ids)==0)
    {
      stop("No pairs generated. Check blocking criteria.")
    }
    
    pair_ids=unique(as.data.frame(pair_ids))  # runs faster with data frame
  } # end else
  
  rm(full_data1,full_data2)
  left=dataset1[pair_ids[,1],,drop=FALSE]
  right=dataset2[pair_ids[,2],,drop=FALSE]
  # matrix to hold comparison patterns
  patterns=matrix(0,ncol=ncol(left),nrow=nrow(left)) 
  if (isTRUE(strcmp))
  {
    patterns=strcmpfun(as.matrix(left, rownames.force=FALSE),as.matrix(right, rownames.force=FALSE))
  } else if (is.numeric(strcmp)) 
  {
    patterns[,-strcmp]=(as.matrix(left[,-strcmp], rownames.force=FALSE)==as.matrix(right[,-strcmp], rownames.force=FALSE))*1
    patterns[,strcmp]=strcmpfun(as.matrix(left[,strcmp], rownames.force=FALSE),
                                as.matrix(right[,strcmp], rownames.force=FALSE)) #*1
  } else
  {
    patterns=(left==right)*1
  }
  rm(left)
  rm(right)
  
  is_match=as.numeric(identity1[pair_ids[,1]]==identity2[pair_ids[,2]]) # match status of pairs
  ret$pairs=as.data.frame(cbind(pair_ids, patterns, is_match)) # Matches
  
  colnames(ret$pairs)=c("id1","id2",colnames(dataset1),"is_match")
  rownames(ret$pairs)=NULL
  
  ret$frequencies=apply(rbind(dataset1,dataset2),2,
                        function(x) 1/length(unique(x)))
  ret$type="linkage"
  class(ret)="RecLinkData"
  return(ret)
}
