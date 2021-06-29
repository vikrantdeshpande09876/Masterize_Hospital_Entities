import pandas as pd, numpy as np, config as conf
from util_functions import *

preformat_input_using_sparksql()
print('\nFormatted the {} file into {} using PySpark successfully.'.format(conf._RAW_STATIC_FILE_NAME, conf._STATIC_FILE_NAME))

site_master_df=pd.read_csv(conf._STATIC_FILE_NAME, index_col=0)
print('\nRead the Source-file {}'.format(conf._STATIC_FILE_NAME))

site_master_df=preprocess_dataframe(df=site_master_df)
print('\nColumns: {}\n'.format(site_master_df.columns.values))

countries=list(site_master_df['COUNTRY'].unique())
print('\nCountries identified are: {}'.format(countries))



for c in range(len(countries)):
    curr_country=countries[c]
    entire_country_df=site_master_df[site_master_df['COUNTRY']==curr_country]
    entire_country_df=clean_dataframe(entire_country_df, columns_to_clean=conf._COLUMNS_TO_CLEAN, fields_to_concat=conf._FIELDS_TO_CONCAT, replace_punctuations=True)
    entire_country_df_copy=site_master_df[site_master_df['COUNTRY']==curr_country]
    entire_country_df_copy=clean_dataframe(entire_country_df_copy, columns_to_clean=conf._COLUMNS_TO_CLEAN, fields_to_concat=conf._FIELDS_TO_CONCAT, replace_punctuations=False)
    nrows=entire_country_df.shape[0]
    m=int(np.ceil(np.divide(nrows, conf._MAXSIZE)))
    print('\nThere will be {} batches since incoming dataset-size={} and minibatch-size={}'.format(m, entire_country_df.shape[0], conf._MAXSIZE))
    entire_country_cross_ref_df=pd.DataFrame()
    queue_of_csvs=list()
    
    for i in range(m):
        print('\n\nStarting Batch[{}]...'.format(i))
        country_df=entire_country_df.iloc[i*conf._MAXSIZE : (i+1)*conf._MAXSIZE]
        country_df_copy=entire_country_df_copy.iloc[i*conf._MAXSIZE : (i+1)*conf._MAXSIZE]
        write_df_to_csv(df=country_df[conf._THRESHOLDS_DICT.keys()], curr_country=curr_country, file_suffix='_country_df.csv', index_flag=True)
        _CREATE_MASTER_MINIBATCHES = (country_df.shape[0]>1)
        
        if not _CREATE_MASTER_MINIBATCHES:
    
            print('\n\nGet the unique set of all record-ids, since Layer-zero cannot create mastered mini-batches.\n')
            # Get the unique set of master-record-ids
            master_record_ids = country_df.index.values.astype(list)
            # Create the country-master-df
            country_master_df = generate_deduplicated_master(country_df=country_df, master_record_ids=master_record_ids, curr_country=curr_country, target_dir=conf._STAGING_AREA_DIRECTORY, write_csv=False)
            # Create a dummy set of cross-refs for masters
            cross_ref_df = generate_dummy_cross_refs_for_masters(master_record_ids=master_record_ids, curr_country=curr_country)
    
            # Write the current master dataset to a csv, add the filename to the queue of csvs, and append currently generated cross-ref to existing cross-ref
            new_file_name='_{}_Master.csv'.format(i)
            write_df_to_csv(df=country_master_df, root_dir=conf._STAGING_AREA_DIRECTORY, curr_country=curr_country, file_suffix=new_file_name, index_flag=True)
            new_file_name=curr_country+new_file_name
            queue_of_csvs.append(new_file_name)
            entire_country_cross_ref_df = entire_country_cross_ref_df.append(cross_ref_df)
    
    
        elif _CREATE_MASTER_MINIBATCHES:
    
            # Invoke the Rscript and generate the Raw_score_features csv file for each minibatch
            args='{} {} {} {} {} {} {} {} {} NA NA'.format(conf._BINARIES_NAME, conf._BINARIES_EXTENSION, conf._THRESHOLD_FOR_INDIVIDUAL, conf._THRESHOLD_FOR_ADDRESS_COMBINED, conf._SCALING_FACTOR, curr_country, conf._RAW_SCORES_DIRECTORY, conf._TOTAL_MATCHES_THRESHOLD, conf._DEDUP_METHOD)
            print('\n{}_{} has {} records.\n\nInvoking the Rscript now...'.format(curr_country, i, country_df.shape[0]))
            deduplicate_dataset_R( rscript_command=conf._RSCRIPT_CMD,  script_name=conf._SCRIPT_NAME, args=args )
    
            normalized_duplicates=pd.DataFrame()
            # Clean and normalize the score features
            normalized_duplicates = clean_score_features(curr_country=curr_country, country_df=country_df, source_dir=conf._RAW_SCORES_DIRECTORY, target_dir=conf._CLEANED_SCORES_DIRECTORY, verbose=False)
    
            if normalized_duplicates.shape[0]!=0:
                
                print('\n\nFound potential duplicates. Processing their master and cross-reference...\n')
                # Get the unique set of master-record-ids
                master_record_ids = get_deduplicated_master_records(normalized_duplicates=normalized_duplicates, country_df=country_df)
                # Get the country-master-df
                country_master_df = generate_deduplicated_master(country_df=country_df, master_record_ids=master_record_ids, curr_country=curr_country, target_dir=conf._STAGING_AREA_DIRECTORY, write_csv=False)
                # Create a dummy set of cross-refs for masters
                cross_ref_df = generate_dummy_cross_refs_for_masters(master_record_ids=master_record_ids, curr_country=curr_country)
                # Create full set of cross-refs for country-df
                cross_ref_df = generate_final_cross_refs(cross_ref_df=cross_ref_df, normalized_duplicates=normalized_duplicates, curr_country=curr_country, target_dir=conf._STAGING_AREA_DIRECTORY, write_csv=False)
                # Create the csv for the cross-ref report
                generate_cross_ref_report(cross_ref_df=cross_ref_df, curr_country=curr_country, country_df=country_df_copy, target_dir=conf._STAGING_AREA_DIRECTORY)
    
    
            else:
                print('\n\nGet the unique set of all record-ids since there aren\'t any potential duplicates.\n')
                # Get the unique set of all-record-ids since there aren't any potential duplicates
                master_record_ids = country_df.index.values.astype(list)
                # Get the country-master-df
                country_master_df = generate_deduplicated_master(country_df=country_df, master_record_ids=master_record_ids, curr_country=curr_country, target_dir=conf._STAGING_AREA_DIRECTORY, write_csv=False)
                # Create a dummy set of cross-refs for masters
                cross_ref_df = generate_dummy_cross_refs_for_masters(master_record_ids=master_record_ids, curr_country=curr_country)
                    
            # Write the current master dataset to a csv, add the filename to the queue of csvs, and append currently generated cross-ref to existing cross-ref
            new_file_name='_{}_Master.csv'.format(i)
            write_df_to_csv(df=country_master_df, root_dir=conf._STAGING_AREA_DIRECTORY, curr_country=curr_country, file_suffix=new_file_name, index_flag=True)
            new_file_name=curr_country+new_file_name
            queue_of_csvs.append(new_file_name)
            entire_country_cross_ref_df = entire_country_cross_ref_df.append(cross_ref_df)
    
            del country_df, country_df_copy, master_record_ids
            if _CREATE_MASTER_MINIBATCHES:
                del normalized_duplicates
    
    print('{} csvs generated are: {}'.format(len(queue_of_csvs), queue_of_csvs))
    
    
    
    # Number of levels for the recursive computations
    d=(m+1)//2
    print('\nMax-depth for {} will be {}'.format(curr_country, d))
    
    
    for j in range(1,d+1):
        combined_crossref_at_depth=pd.DataFrame()
        n_csvs_to_read=len(queue_of_csvs)
        length=n_csvs_to_read if n_csvs_to_read%2==0 else n_csvs_to_read+1
        print('{} csvs need to be processed: {} , length={}'.format(n_csvs_to_read, queue_of_csvs, length))
        for i in range(0, length, 2):
            master_csv_1=os.path.join(conf._STAGING_AREA_DIRECTORY, queue_of_csvs[i])
            master_df_1=pd.read_csv(master_csv_1, index_col=0)
            
            if i+1<n_csvs_to_read:
                master_csv_2=os.path.join(conf._STAGING_AREA_DIRECTORY, queue_of_csvs[i+1])
                master_df_2=pd.read_csv(master_csv_2, index_col=0)
                
                # Invoke the Rscript and generate the Raw_score_features csv file
                print('\n{} has {} records, and {} has {} records.\n\nInvoking the Rscript now...\n'.format(master_csv_1, master_df_1.shape[0],master_csv_2, master_df_2.shape[0]))
                args='{} {} {} {} {} {} {} {} {} {} {}'.format(conf._BINARIES_NAME, conf._BINARIES_EXTENSION, conf._THRESHOLD_FOR_INDIVIDUAL, conf._THRESHOLD_FOR_ADDRESS_COMBINED, conf._SCALING_FACTOR, curr_country, conf._RAW_SCORES_DIRECTORY, conf._TOTAL_MATCHES_THRESHOLD, conf._LINKAGE_METHOD, master_csv_1, master_csv_2)
                deduplicate_dataset_R( rscript_command=conf._RSCRIPT_CMD,  script_name=conf._SCRIPT_NAME, args=args )
                
                
                normalized_duplicates=pd.DataFrame()
                # Clean and normalize the score features
                normalized_duplicates = clean_score_features(curr_country=curr_country, country_df=master_df_1.append(master_df_2), source_dir=conf._RAW_SCORES_DIRECTORY, target_dir=conf._CLEANED_SCORES_DIRECTORY, verbose=False)
                
                if normalized_duplicates.shape[0]!=0:
                    
                    print('\n\nFound potential duplicates. Processing their master and cross-reference...\n')
                    # Get the unique set of master-record-ids
                    master_record_ids = get_deduplicated_master_records(normalized_duplicates=normalized_duplicates, country_df=master_df_1.append(master_df_2))
                    # Get the country-master-df
                    country_master_df = generate_deduplicated_master(country_df=master_df_1.append(master_df_2), master_record_ids=list(master_record_ids), curr_country=curr_country, target_dir=conf._STAGING_AREA_DIRECTORY, write_csv=False)
                    # Create a dummy set of cross-refs for masters
                    cross_ref_df = generate_dummy_cross_refs_for_masters(master_record_ids=master_record_ids, curr_country=curr_country)
                    # Create full set of cross-refs for country-df
                    cross_ref_df = generate_final_cross_refs(cross_ref_df=cross_ref_df, normalized_duplicates=normalized_duplicates, curr_country=curr_country, target_dir=conf._STAGING_AREA_DIRECTORY, write_csv=False)
                    # Create the csv for the cross-ref report
                    generate_cross_ref_report(cross_ref_df=cross_ref_df, country_df=master_df_1.append(master_df_2), curr_country=curr_country, target_dir=conf._STAGING_AREA_DIRECTORY)
                    
                else:
                    
                    print('\n\nGet the unique set of all record-ids since there aren\'t any potential duplicates.\n')
                    # Get the unique set of all-record-ids since there aren't any potential duplicates
                    master_record_ids = master_df_1.append(master_df_2).index.values.astype(list)
                    # Get the country-master-df
                    country_master_df = generate_deduplicated_master(country_df=master_df_1.append(master_df_2), master_record_ids=master_record_ids, curr_country=curr_country, target_dir=conf._STAGING_AREA_DIRECTORY, write_csv=False)
                    # Create a dummy set of cross-refs for masters
                    cross_ref_df = generate_dummy_cross_refs_for_masters(master_record_ids=master_record_ids, curr_country=curr_country)
                    
            else:
                
                print('\n\nGet the unique set of all record-ids since there isn\'t a second file to compare.\n')
                # Get the unique set of master-record-ids
                master_record_ids = master_df_1.index.values.astype(list)
                # Get the country-master-df
                country_master_df = generate_deduplicated_master(country_df=master_df_1, master_record_ids=master_record_ids, curr_country=curr_country, target_dir=conf._STAGING_AREA_DIRECTORY, write_csv=False)
                # Create a dummy set of cross-refs for masters
                cross_ref_df = generate_dummy_cross_refs_for_masters(master_record_ids=master_record_ids, curr_country=curr_country)
                
            
            # Write the current master dataset to a csv, add the filename to the queue of csvs, and append currently generated cross-ref to existing cross-ref
            combined_crossref_at_depth = combined_crossref_at_depth.append(cross_ref_df)
            new_file_name='_d{}_{}_Master.csv'.format(j,i)
            write_df_to_csv(df=country_master_df, root_dir=conf._STAGING_AREA_DIRECTORY, curr_country=curr_country, file_suffix=new_file_name, index_flag=True)
            new_file_name=curr_country+new_file_name
            queue_of_csvs.append(new_file_name)
            del master_record_ids, master_df_1
            if i+1<n_csvs_to_read:
                del normalized_duplicates, master_df_2
        
        write_df_to_csv(df=combined_crossref_at_depth, root_dir=conf._STAGING_AREA_DIRECTORY, curr_country=curr_country, file_suffix='_d{}_Raw_Cross_Ref.csv'.format(j), index_flag=False)
        print('\n\nDepth[{}] processed successfully.'.format(j))
        update_entire_country_cross_ref(new_depth_cross_ref_df=combined_crossref_at_depth, entire_country_cross_ref_df=entire_country_cross_ref_df)
        queue_of_csvs=queue_of_csvs[n_csvs_to_read:]
    
    
    
    if len(queue_of_csvs)==1:
        print('\n\n\n\nProcessed all {} levels. Generating the master and cross-reference at the final-layer...'.format(d))
        master_csv_1=os.path.join(conf._STAGING_AREA_DIRECTORY, queue_of_csvs[i])
        master_df_1=pd.read_csv(master_csv_1, index_col=0)
        # Get the unique set of master-record-ids
        master_record_ids = master_df_1.index.values.astype(list)
        # Get the country-master-df
        country_master_df = generate_deduplicated_master(country_df=entire_country_df_copy, master_record_ids=master_record_ids, curr_country=curr_country, target_dir=conf._MASTER_DATA_DIRECTORY, write_csv=True)
        # Write the final raw-cross-ref to a csv
        write_df_to_csv(df=entire_country_cross_ref_df, root_dir=conf._MASTER_DATA_DIRECTORY, curr_country=curr_country, file_suffix='_Raw_Cross_Ref.csv', index_flag=False)
        # Create the csv for the cross-ref report
        generate_cross_ref_report(cross_ref_df=entire_country_cross_ref_df, country_df=entire_country_df_copy, curr_country=curr_country, target_dir=conf._MASTER_DATA_DIRECTORY)
        
print('\n\n\nPipeline completed execution...')