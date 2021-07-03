import os

''' Environment Directory Config '''
_RAW_STATIC_FILE_NAME = 'hospital_account_info_raw.csv'
_STATIC_FILE_NAME = 'hospital_account_info.csv'
#_STATIC_FILE_NAME = 'Site_Master_Extract.xlsx'
_RAW_SCORES_DIRECTORY = 'Raw_Scores'
_CLEANED_SCORES_DIRECTORY = 'Cleaned_Scores'
_MASTER_DATA_DIRECTORY = 'Master_Data'
_STAGING_AREA_DIRECTORY=os.path.join(_MASTER_DATA_DIRECTORY, 'Recursive_Staging_Area')



''' Data_Fields Config '''
_RAW_COUNTRY = 'USA' # Parameterized this for now. For central csv with Country-column available remove this hardcoding from preformat_input_using_sparksql()
_RAW_TO_STD_COLS = {
            'Account_Num' : 'ACCOUNT_NUM',
            'Facility Name' : 'SITE_NAME',
            'Address1' : 'ADDRESS_LINE_1',
            'Address2' : 'ADDRESS_LINE_2',
            'Address3' : 'ADDRESS_LINE_3',
            'City' : 'CITY',
            'State' : 'STATE',
            'ZIP Code' : 'POSTAL_CODE',
            'County Name' : 'COUNTY_NAME',
            'Phone Number' : 'PHONE_NUM',
            'Hospital Type' : 'SITE_TYPE',
            'Hospital Ownership' : 'SITE_OWNERSHIP'
}
_STD_COLS_ORDER = [
            'POSTAL_CODE', 'SITE_NAME', 'STATE', 'CITY', 
            'ADDRESS_LINE_1', 'ADDRESS_LINE_2', 'ADDRESS_LINE_3', 
            'COUNTY_NAME', 'PHONE_NUM', 'SITE_TYPE', 'SITE_OWNERSHIP', 'ACCOUNT_NUM'
            ]
_FIELDS_TO_CONCAT = { 
            'CONCAT_ADDRESS':   ['ADDRESS_LINE_1','ADDRESS_LINE_2','ADDRESS_LINE_3']
            }
_COLUMNS_TO_CLEAN = [
            'ADDRESS_LINE_1','ADDRESS_LINE_2','ADDRESS_LINE_3',
            'SITE_NAME','STATE','CITY','POSTAL_CODE'
            ]
_MAXSIZE = 2000



''' R_Code Config '''
_RSCRIPT_CMD = 'C:/Program Files/R/R-4.1.0/bin/i386/Rscript'
_SCRIPT_NAME = 'Site_Master_Record_Linkage.R'
_DEDUP_METHOD = 'Dedup'
_LINKAGE_METHOD = 'Linkage'



''' Match_Score_Computation '''
_BINARIES_NAME = 'levenshtein'
_BINARIES_EXTENSION = '.dll'
#_BINARIES_EXTENSION = '.so'
_THRESHOLD_FOR_INDIVIDUAL=0.75
_THRESHOLD_FOR_ADDRESS_COMBINED=0.60
_THRESHOLDS_DICT = {
        'CONCAT_ADDRESS': _THRESHOLD_FOR_ADDRESS_COMBINED,
        'SITE_NAME': _THRESHOLD_FOR_INDIVIDUAL,
        'STATE': _THRESHOLD_FOR_INDIVIDUAL,
        'CITY': _THRESHOLD_FOR_INDIVIDUAL,
        'POSTAL_CODE': _THRESHOLD_FOR_INDIVIDUAL
        }
_COLS_FOR_TOTAL_MATCH_CALC = [colname+'_COMPARISON_SCORE' for colname in _THRESHOLDS_DICT]
_SCALING_FACTOR=3
_TOTAL_MATCHES_THRESHOLD=4