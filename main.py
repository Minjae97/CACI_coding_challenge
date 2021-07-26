import sqlalchemy
import pandas as pd 
import sqlite3
from zipfile import ZipFile
import itertools
import sys

# Credit Cards Vendor lists
maestro = ['5018', '5020', '5038', '56']
mastercard = ['51', '52', '54', '55', '222']
visa = ['4']
amex = ['34', '37']
discover = ['6011', '65']
diners = ['300', '301', '304', '305', '36', '38']
jcb16 = ['35']
jcb15 = ['2131', '1800']

# Concatenate the vendor lists for convinience
valid_digits = list(itertools.chain(maestro, mastercard, visa, amex, discover, diners, jcb16, jcb15))
valid_digits = list(map(str,valid_digits))


# Constants
DATABASE_LOCATION = "sqlite:///coding_challenge.sqlite"
FRAUD_PATH = 'fraud'

# Part 2
fraud_zip = ZipFile('fraud.zip')
fraud_zip.extractall()
fraud_txt = open('fraud', 'r')
contents = fraud_txt.readlines()

# add state column in the file for convinience when using pd.read_csv function
contents[0] = 'credit_card_number,ipv4,state\n'

fraud_txt = open('fraud', 'w')
fraud_txt.writelines(contents)
fraud_txt.close()

# Open the file and convert the card number into str
fraud_df = pd.read_csv('fraud', sep='[\r,]', usecols=range(3),lineterminator='\n', engine='python')
convert_dict = {'credit_card_number': str}
fraud_df = fraud_df.astype(convert_dict)

engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('coding_challenge.sqlite')
cursor = conn.cursor()

sql_query = """
    CREATE TABLE IF NOT EXISTS frauds(
        credit_card_number VARCHAR(100),
        ipv4 VARCHAR(100),
        country_code VARCHAR(50),
        CONSTRAINT primary_key_constraint PRIMARY KEY (credit_card_number)
    )
    """
cursor.execute(sql_query)
print("Opened database successfully")

try:
    fraud_df.to_sql("frauds", engine, index=False, if_exists='append')
except:
    print("Data already exists in the database")

conn.close()
print("Close database successfully")

# Part 3
transaction1_zip = ZipFile('transaction-001.zip')
transaction1_df = pd.read_csv(transaction1_zip.open('transaction-001'))
convert_dict = {'credit_card_number': str}
transaction1_df = transaction1_df.astype(convert_dict)

transaction2_zip = ZipFile('transaction-002.zip')
transaction2_df = pd.read_csv(transaction2_zip.open('transaction-002'))
convert_dict = {'credit_card_number': str}
transaction2_df = transaction2_df.astype(convert_dict)

# Find the indexes of the rows that should be removed for transaction-001 (list comprehension was used for an efficent computation)
drop_index_tran1 = [idx for idx, x in enumerate(transaction1_df['credit_card_number']) if not x.startswith(tuple(valid_digits))]
transaction1_df.drop(drop_index_tran1, inplace=True)

# Find the indexes of the rows that should be removed for transaction-002 (list comprehension was used for an efficent computation)
drop_index_tran2 = [idx for idx, x in enumerate(transaction2_df['credit_card_number']) if not x.startswith(tuple(valid_digits))]
transaction2_df.drop(drop_index_tran2, inplace=True)

# Part 4

# Combine transaction-001 and transaction-002 for convinience
transaction_combined = transaction1_df.append(transaction2_df, ignore_index=True)

# Convert the list of fraud transaction number into set for faster 'in' computation
fraud_card_set = set(fraud_df.credit_card_number)
fraud_trans = [x for x in transaction_combined['credit_card_number'] if x in fraud_card_set]

# Recorded the fraudulent transactions in a txt file
with open("fraudulent_transactions.txt", "w") as f:
    for card_num in fraud_trans:
        f.write("%s\n" % card_num)

# Find the index of the fraudulent transactions
fraud_trans_index = [idx for idx, x in enumerate(transaction_combined['credit_card_number']) if x in fraud_card_set]

# Retrieve the rows that correspond to the fraud_trans_index
fraud_trans_df = transaction_combined.iloc[fraud_trans_index]

# Get the number of fraudulent transactions per state
groupby_state_trans_count = fraud_trans_df['state'].value_counts()

# Recorded the number of fraudulent transactions per state in a txt file
with open("fraudulent_transactions_per_state.txt", "w") as f:
    for state, freq in groupby_state_trans_count.items():
        f.write("%s => %d\n" % (state, freq))

# Create a dictionary that contains the list of each vendor numbers
vecdor_dict = {'maestro':maestro, 'mastercard':mastercard, 'visa':visa, 'amex':amex, 'discover':discover, 'diners':diners, 'jcb16': jcb16, 'jcb15':jcb15}

# Count the number fraudulent transactions per vendor and save it in a dictionary
trans_per_vendor_dict = {}
for vendor, valid_digits in vecdor_dict.items():
    trans_per_vendor_list = [x for x in fraud_card_set if x.startswith(tuple(valid_digits))]
    trans_per_vendor_dict[vendor] = len(trans_per_vendor_list)

# Recorded the number of fraudulent transactions per vendor in a txt file
with open("fraudulent_transactions_per_vendor.txt", "w") as f:
    for vendor, freq in trans_per_vendor_dict.items():
        f.write("%s => %d\n" % (vendor, freq))

# Mask the last 9 digits
trans_num_encoded = [x[:-9] + '*********' for x in transaction_combined['credit_card_number']]
transaction_combined['credit_card_number'] = trans_num_encoded

# sum of bytes
sum_of_bytes = [sys.getsizeof(x[0]) + sys.getsizeof(x[1]) + sys.getsizeof(x[2]) for x in transaction_combined[['credit_card_number', 'ipv4', 'state']].to_numpy()]
transaction_combined['sum_of_bytes'] = sum_of_bytes

# save in json; the following json format is what I believe it to be idea for BI analysis because this format explicitly shows columns, index, and data
transaction_combined.to_json(r'final_dataset.json', orient='split')

# save in binary file
transaction_combined.to_pickle('final_dataset.pkl')