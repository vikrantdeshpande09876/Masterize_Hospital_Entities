import time, numpy as np, pandas as pd, re, string, subprocess, os
from subprocess import Popen, PIPE
from config import Config

    
conf=Config()

def write_df_to_csv(df, root_dir='', curr_country='', file_suffix='_temp.csv', index_flag=False):
    """
        DOCSTRING:  Writes the dataframe to a csv file and throw error if it fails.
        INPUT:      Dataframe, Target-Directory, Country-name, Suffix-of-csv-file, Index-Flag
        OUTPUT:     Dataframe csv at target-directory, or error.
    """
    try:
        abs_path=os.path.join(root_dir, curr_country+file_suffix)
        df.to_csv(abs_path, index=index_flag)
        print('\nSuccessfully created \{}!'.format(abs_path))
    except:
        print('\nSomething went wrong while writing the file. Please check if it is currently in use.')


def preprocess_dataframe(df):
    """
        DOCSTRING:  Imputes blank cells with '', replaces whitespace with underscore in country-name, and strips whitespace in cells.
        INPUT:      Dataframe
        OUTPUT:     Imputed and cleaned dataframe.
    """
    df_copy=df.copy(deep=True)
    df_copy.replace(np.nan, '', inplace=True)
    for colname in df_copy.columns.values:
        if colname=='COUNTRY':
            df_copy[colname]=df_copy[colname].apply(lambda x: x.replace(' ','_'))            
        df_copy[colname]=df_copy[colname].astype(str).apply(lambda x: x.strip())
    return df_copy


def clean_dataframe(df, columns_to_clean=conf._COLUMNS_TO_CLEAN, fields_to_concat=conf._FIELDS_TO_CONCAT, replace_punctuations=True):
    """
        DOCSTRING:  Replaces special-chars in lowercase-converted cells if replace_punctuation==True, for the columns relevant to computing match-scores.
                    Generates the concatenated address fields, and drops the individual ones.
                    Overall will be left with alphanumeric chars in UTF-8 encoding.
        INPUT:      Dataframe, columns-to-clean, address-fields-to-concat, flag-to-replace-punctuations
        OUTPUT:     Imputed and cleaned dataframe.
    """
    copy_df=df.copy(deep=True)
    # Added another special character which was causing Italy CSV file read to fail in R
    if replace_punctuations:
        special_chars=re.escape(string.punctuation)+''
        print('\nSpecial Character that will be replaced are:  {}'.format(special_chars))
    for colname in copy_df.columns.values:
        if colname in columns_to_clean and replace_punctuations:
            copy_df[colname]=copy_df[colname].replace(r'['+special_chars+']', '', regex=True).str.lower()
    for colname, cols_to_concat in fields_to_concat.items():
        copy_df[colname]=copy_df[cols_to_concat].apply(lambda single_row: ''.join(single_row.values), axis=1)
    copy_df.drop(labels=fields_to_concat['CONCAT_ADDRESS'], axis=1, inplace=True)
    return copy_df



def deduplicate_dataset_R(rscript_command, script_name, args):
    """
        DOCSTRING:  Invokes the R-code from Python using 32-bit Rscript 3.4.4 command.
                    Uses the Python subprocess module to create a new Pipe.
        INPUT:      Abs-path-of-32bit-Rscript-command, Script-to-invoke, Args-for-script
        OUTPUT:     Prints R-console output based on return-code. Rscript command generates a csv of the score_features, or errors out.
    """
    cmd = [rscript_command, script_name, args]
    pipe = Popen( cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE )
    output, error = pipe.communicate()

    if pipe.returncode==0:
        print('R OUTPUT:\n')
        print(output)
    else:
        print('R OUTPUT:\n')
        print(output.decode())
        print('R ERROR:\n')
        print(error.decode())



def scale_up_comparison_score(df, colname='SITE_NAME_COMPARISON_SCORE', scaling_factor=conf._SCALING_FACTOR):
    """
        DOCSTRING:  Scale-up a column's binary-valued score by a factor
        INPUT:      Dataframe, score-colname, scaling-factor
        OUTPUT:     Scaled up dataframe.
    """
    df[colname]=df[colname].apply(lambda x: x*scaling_factor)



def return_top_match(df, child_column, score_key_column):
    """
        DOCSTRING:  Input Dataframe has SR_NUM_1 (child-col) matching against multiple SR_NUM_2.
                    Orders by child-col asc, score-col desc, and chooses the first possible entry of child-col.
        INPUT:      Dataframe-of-score-features-above-a-total-threshold, index-column (SR_NUM_1), total-score-column (NUM_OF_MATCHES_FOUND)
        OUTPUT:     Dataframe of normalized-score-features.
    """
    normalized_duplicates=df.sort_values(by=[child_column, score_key_column],ascending=[True,False])
    normalized_duplicates=normalized_duplicates.groupby(child_column).head(1).sort_values(by=[child_column])
    return normalized_duplicates



def replace_cyclic_dependencies_backup(df, child_indicator, master_indicator, verbose=True):
    """
        DOCSTRING:  Input Dataframe has cases like-     Record45 matches with Record44, and Record67 matches with Record45.
                    In this case we should maintain-    Record67 matches with Record44.
                    Applies a for-loop and replaces values in master-column whenever such a cyclic-occurence observed.
        INPUT:      Dataframe-of-score-features-with-cyclic-indexes, child-column, master-column
        OUTPUT:     Dataframe of normalized-score-features.
    """
    arr=set(df[child_indicator].array)
    for val in df[master_indicator]:
        if val in arr:
            replace_val=df[df[child_indicator]==val][master_indicator].values[0]
            if verbose: print('{} found in normalized_duplicates[{}]. Replacement: {}'.format(val, child_indicator, replace_val))
            df[master_indicator].replace(val, replace_val, inplace=True)
    return df



def replace_cyclic_dependencies(df, country_df, child_indicator='SR_NUM_1', master_indicator='SR_NUM_2', verbose=True):
    """
        DOCSTRING:  Input Dataframe has cases like-     Record45 matches with Record44, and Record67 matches with Record45.
                    In this case we should maintain-    Record67 matches with Record44.
                    Applies a for-loop and replaces values in master-column whenever such a cyclic-occurence observed.
        INPUT:      Dataframe-of-score-features-with-cyclic-indexes, child-column, master-column
        OUTPUT:     Dataframe of normalized-score-features.
    """
    df.sort_values(by=[master_indicator, 'NUM_OF_MATCHES_FOUND'], ascending=[True, False], inplace=True)
    arr1=set(df[child_indicator].array)
    arr2=set(df[master_indicator].array)
    for val in arr2:
        if val in arr1:
            replace_val=df.loc[df[child_indicator]==val, master_indicator].values[0]
            score_for_original=df.loc[df[master_indicator]==val, 'NUM_OF_MATCHES_FOUND'].values[0]
            score_for_replacement=df.loc[df[child_indicator]==val, 'NUM_OF_MATCHES_FOUND'].values[0]
            original_sitename=country_df.loc[val, 'SITE_NAME']
            replacement_sitename=country_df.loc[replace_val, 'SITE_NAME']
            if score_for_original>score_for_replacement and original_sitename!=replacement_sitename:
                df.loc[df[child_indicator]==val, 'COUNTRY']='To be deleted'
                if verbose: print("Delete from df where {}={} and {}}={}".format(child_indicator,val,master_indicator,replace_val))
            else:
                df[master_indicator].replace(val, replace_val, inplace=True)
                if verbose: print("{} [{}] will be replaced with {} [{}]".format(val,score_for_original,replace_val,score_for_replacement))

    indexes_to_delete=df[df['COUNTRY']=='To be deleted'].index
    print("\n\n{} raw-score pairs will be deleted off as their cyclic dependecies have lower score than existing.".format(len(indexes_to_delete)))
    df.drop(indexes_to_delete, inplace=True)
    df.sort_values(by=child_indicator)
    return df





def replace_cyclic_dependencies_bkp(df, child_indicator='SR_NUM_1', master_indicator='SR_NUM_2', verbose=True):
    """
        DOCSTRING:  Input Dataframe has cases like-     Record45 matches with Record44, and Record67 matches with Record45.
                    In this case we should maintain-    Record67 matches with Record44.
                    Applies a for-loop and replaces values in master-column whenever such a cyclic-occurence observed.
        INPUT:      Dataframe-of-score-features-with-cyclic-indexes, child-column, master-column
        OUTPUT:     Dataframe of normalized-score-features.
    """
    df.sort_values(by=['SR_NUM_2', 'NUM_OF_MATCHES_FOUND'], ascending=[True, False], inplace=True)
    arr1=set(df['SR_NUM_1'].array)
    arr2=set(df['SR_NUM_2'].array)
    for val in arr2:
        if val in arr1:
            replace_val=df.loc[df['SR_NUM_1']==val,'SR_NUM_2'].values[0]
            score_for_original=df.loc[df['SR_NUM_2']==val, 'NUM_OF_MATCHES_FOUND'].values[0]
            score_for_replacement=df.loc[df['SR_NUM_1']==val, 'NUM_OF_MATCHES_FOUND'].values[0]
            if score_for_original>score_for_replacement:
                df.loc[df['SR_NUM_1']==val, 'COUNTRY']='To be deleted'
                if verbose: print("Delete from df where SR_NUM_1={} and SR_NUM_2={}".format(val,replace_val))
            else:
                df['SR_NUM_2'].replace(val, replace_val, inplace=True)
                if verbose: print("{} [{}] will be replaced with {} [{}]".format(val,score_for_original,replace_val,score_for_replacement))

    indexes_to_delete=df[df['COUNTRY']=='To be deleted'].index
    print("\n\n{} raw-score pairs will be deleted off as their cyclic dependecies have lower score than existing.".format(len(indexes_to_delete)))
    df.drop(indexes_to_delete, inplace=True)
    df.sort_values(by='SR_NUM_1')
    return df


def clean_score_features(curr_country, country_df, source_dir=conf._RAW_SCORES_DIRECTORY, target_dir=conf._CLEANED_SCORES_DIRECTORY, verbose=True):
    """
        DOCSTRING:  Reads the output of the Rscript command that is a csv of score_features having total-score greater than a total-threshold.
                    Invokes the top-match function, and the replace-cyclic-occurences function to get a set of clean-score-features.
                    Writes the dataframe in the Cleaned-Scores directory.
        INPUT:      country-name
        OUTPUT:     Dataframe of cleaned-normalized-score-features.
    """
    duplicates=pd.read_csv(os.path.join(source_dir, curr_country+'_Score_Features.csv'))
    # if no potential duplicates found, return an empty df
    if duplicates.shape[0]==1 and duplicates['SR_NUM_1'][0]==0 and duplicates['SR_NUM_2'][0]==0:
        return duplicates.head(0)
    
    duplicates['COUNTRY']=curr_country
    duplicates=return_top_match(df=duplicates, child_column='SR_NUM_1', score_key_column='NUM_OF_MATCHES_FOUND')
    duplicates=replace_cyclic_dependencies(df=duplicates, country_df=country_df, child_indicator='SR_NUM_1', master_indicator='SR_NUM_2', verbose=verbose)
    write_df_to_csv(df=duplicates, root_dir=target_dir, curr_country=curr_country, file_suffix='_Cleaned_Feature_Scores.csv')
    print('\n"SR_NUM_2" will be the master record')
    return duplicates


def get_deduplicated_master_records(normalized_duplicates, country_df):
    """
        DOCSTRING:  From the list of cleaned-normalized-score-features, use set-theory to find the unique list of masters.
                        a.  Think of 'SR_NUM_1' as the list of incoming Primary-keys, and 'SR_NUM_2' as the value to which it should be mapped based on match-score.
                        b.  Hence, union of 'SR_NUM_1' & 'SR_NUM_2' will be entire set of duplicates.
                        c.  Stand-alone records in the current country_batch_dataframe will not fall in this entire set of duplicates.
                        d.  Master-records set wil be the sets of 'SR_NUM_2' & #c above.
                        >   Universe                            = {SR_NUM}
                        >   a1                                  = {SR_NUM_1}
                        >   a2                                  = {SR_NUM_2}
                        >   Falls into any duplication-scenario = anymatch  = {a1 U a2}
                        >   Falls into no duplication-scenario  = nomatch   = {Universe - anymatch}
                        >   Total masters                       = {nomatch U a2}
        INPUT:      Dataframe-of-cleaned-normalized-score_features
        OUTPUT:     Unique set of master-record-ids (SR_NUM)
    """
    a1=set(normalized_duplicates['SR_NUM_1'].values.tolist())
    a2=set(normalized_duplicates['SR_NUM_2'].values.tolist())
    country_set=set(country_df.index.values.tolist())
    entire_duplicates_set=a1.union(a2)
    no_match_set=country_set.difference(entire_duplicates_set)
    master_record_ids=no_match_set.union(a2)
    return master_record_ids


def generate_deduplicated_master(country_df, master_record_ids, curr_country, target_dir=conf._MASTER_DATA_DIRECTORY, write_csv=True):
    """
        DOCSTRING:  Use the original df to extract columns-info and generate the country-specific Master file.
        INPUT:      Original-country-Dataframe, Unique set of master-record-ids (SR_NUM)
        OUTPUT:     Dataframe-for-country-with-original-info, Master-Dataframe
    """
    country_master_df=country_df.loc[master_record_ids]
    if write_csv:
        write_df_to_csv(df=country_master_df, root_dir=target_dir, curr_country=curr_country, index_flag=True, file_suffix='_Master.csv')
    print('{} records get merged into {}'.format(country_df.shape[0],len(master_record_ids)))
    return country_master_df
    
    
    
def generate_dummy_cross_refs_for_masters(master_record_ids, curr_country):
    """
        DOCSTRING:  Create a dummy cross-reference dataframe for master-records; Record45 matches with Record45 having a total match-score of maximum.
        INPUT:      Unique set of master-record-ids (SR_NUM)
        OUTPUT:     Dataframe-of-dummy-entries-for-master-cross-references.
    """
    master_record_score_array=[1.0]*len(master_record_ids)
    master_record_df_dict={
        'SR_NUM_1': list(master_record_ids),
        'SR_NUM_2': list(master_record_ids),
        'SITE_NAME_COMPARISON_SCORE': master_record_score_array,
        'STATE_COMPARISON_SCORE': master_record_score_array,
        'CITY_COMPARISON_SCORE': master_record_score_array,
        'CONCAT_ADDRESS_COMPARISON_SCORE': master_record_score_array,
        'POSTAL_CODE_COMPARISON_SCORE': master_record_score_array }

    cross_ref_df=pd.DataFrame(master_record_df_dict)
    cross_ref_df['COUNTRY']=curr_country
    scale_up_comparison_score(cross_ref_df,'CONCAT_ADDRESS_COMPARISON_SCORE',conf._SCALING_FACTOR)
    cross_ref_df['NUM_OF_MATCHES_FOUND']=cross_ref_df[conf._COLS_FOR_TOTAL_MATCH_CALC].sum(axis=1)
    return cross_ref_df


def generate_final_cross_refs(cross_ref_df, normalized_duplicates, curr_country, target_dir=conf._MASTER_DATA_DIRECTORY, write_csv=True):
    """
        DOCSTRING:  Merges the dummy cross-reference of masters, with the cleaned-normalized-feature-scores.
        INPUT:      Dataframe-of-dummy-entries-for-master-cross-references, Dataframe-of-cleaned-normalized-score_features
        OUTPUT:     Dataframe-of-cross-references.
    """
    cross_ref_df=cross_ref_df.append(normalized_duplicates)
    cross_ref_df.sort_values(by=['SR_NUM_1'], axis=0, inplace=True)
    if write_csv:
        write_df_to_csv(df=cross_ref_df, root_dir=target_dir, curr_country=curr_country, file_suffix='_Raw_Cross_Ref.csv')
    return cross_ref_df



def update_entire_country_cross_ref(new_depth_cross_ref_df, entire_country_cross_ref_df):
    """
        DOCSTRING:  --Specific to recursively processing a huge country-batch--
                    Updates the entire cross-reference for a country at depth=0, with the merges that are observed for new depth=d.
                    The update function performs left-join on the indexes, hence we set-index before the operation, and reset-it later.
        INPUT:      Dataframe-of-cross-references-at-new-depth, Dataframe-of-existing-cross-references
        OUTPUT:     Dataframe-of-updated-cross-references.
    """
    cross_refs_with_merges=new_depth_cross_ref_df[new_depth_cross_ref_df['SR_NUM_1'] != new_depth_cross_ref_df['SR_NUM_2']]
    entire_country_cross_ref_df.set_index('SR_NUM_1', inplace=True)
    cross_refs_with_merges.set_index('SR_NUM_1', inplace=True)

    entire_country_cross_ref_df.update(cross_refs_with_merges, join='left', overwrite=True)
    entire_country_cross_ref_df['SR_NUM_2']=entire_country_cross_ref_df['SR_NUM_2'].apply(pd.to_numeric)
    entire_country_cross_ref_df.reset_index(inplace=True)




def generate_cross_ref_report(cross_ref_df, country_df, curr_country, target_dir=conf._MASTER_DATA_DIRECTORY):
    """
        DOCSTRING:  Creates cross-reference report by performing left-join of cross-reference-dataframe with the original-info in country-df.
                        a. Merge the master_cross_reference_df with the country_batch_dataframe as a left-outer-join on Primary-key='SR_NUM_1'
                        b. Merge this master_cross_reference_df with the country_batch_dataframe as a left-outer-join on Primary-key='SR_NUM_2'
                    Writes the dataframe in the Master-Data directory.
        INPUT:      Dataframe-of-cross-references, Dataframe-for-country-with-original-info
        OUTPUT:     Dataframe-of-cross-references-with-original-info.
    """
    country_df.reset_index(inplace=True)
    country_df_colnames=country_df.columns.values

    country_df.columns=[colname+'_1' for colname in country_df_colnames]
    cross_ref_df=cross_ref_df.merge(country_df, how='left', on='SR_NUM_1')

    country_df.columns=[colname+'_2' for colname in country_df_colnames]
    cross_ref_df=cross_ref_df.merge(country_df, how='left', on='SR_NUM_2')

    columns_in_report_format=['SR_NUM_1', 'SR_NUM_2', 'SITE_NAME_1','SITE_NAME_2','SITE_NAME_COMPARISON_SCORE','STATE_1','STATE_2','STATE_COMPARISON_SCORE', 'CITY_1', 'CITY_2','CITY_COMPARISON_SCORE','CONCAT_ADDRESS_1','CONCAT_ADDRESS_2','CONCAT_ADDRESS_COMPARISON_SCORE', 'POSTAL_CODE_1','POSTAL_CODE_2',   'POSTAL_CODE_COMPARISON_SCORE','NUM_OF_MATCHES_FOUND']
    cross_ref_df=cross_ref_df[columns_in_report_format]
    write_df_to_csv(df=cross_ref_df, root_dir=target_dir, curr_country=curr_country, file_suffix='_Cross_Ref_Full_Report.csv')
