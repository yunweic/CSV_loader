from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, Sequence, String, DateTime, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from faker import Faker
import random
import pdb
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import csv
from config import ES_URL

Base = declarative_base()

# create the transactions index
def create_index(es):

    res = es.indices.create(index="transactions")
    print(" response: '%s'" % (res))

# load CSV into ElasticSearch
def load_ES():

    # set the url of the ElasticSearch here
    es = Elasticsearch([ES_URL])

    # delete the index
    if es.indices.exists(index="transactions"):
        res = es.indices.delete(index="transactions")
        print(" response: '%s'" % (res))

    create_index(es)

    if es.indices.exists(index="transactions"):
        print("\n[Info: ]Successfully create the transactions index \n")

    # read beneficiaries from csv
    with open("csvs/transaction.csv") as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index="transactions")

# Load CSV into MySQL
def csv_data_loader(engine):

    Session = sessionmaker(bind=engine)
    session = Session()

    transaction_list = []

    df = pd.read_csv("csvs/transaction.csv")
    df1 = df.where(pd.notnull(df), None)

    for index, row in df1.iterrows():
        transaction_list.append(Transaction(**row.to_dict()))
        print(row.to_dict())

    session.add_all(transaction_list)
    session.commit()

# Model for Transaction table
class Transaction(Base):
    __tablename__ = "transactions"

    tx_id = Column(String(16), primary_key=True)
    tx_direction = Column(String(8))
    tx_purpose = Column(String(16))

    app_id = Column(String(16))
    app_name = Column(String(32))

    policy_no = Column(String(32))
    policy_name = Column(String(32))

    loan_no = Column(String(32))

    Cathay_acct = Column(String(32))

    tx_amt = Column(Float)
    tx_currency = Column(String(16))
    tx_time = Column(DateTime)
    tx_location = Column(String(16))
    tx_platform = Column(String(16))

    tx_type = Column(String(16))

    claim_no = Column(String(16))
    apply_amt = Column(Float)

    payer_status = Column(String(8))
    payer_insurant_relationship = Column(String(8))
    payer_id = Column(String(16))
    payer_name = Column(String(32))
    payer_acct = Column(String(32))
    payer_bank = Column(String(32))
    payer_type = Column(String(16))

    payee_status = Column(String(16))
    payee_insurant_relationship = Column(String(8))
    payee_id = Column(String(16))
    payee_name = Column(String(32))
    payee_acct = Column(String(32))
    payee_bank = Column(String(32))
    payee_type = Column(String(16))

    tx_status = Column(String(16))
    tx_status_note = Column(String(64))
    hits = Column(Integer)
    ip_address = Column(String(128))
    device_type = Column(String(32))

    app_type = Column(String(16))
    app_version = Column(String(16))

    atm_id = Column(String(16))
    tx_bank_id = Column(String(16))

    representative = Column(Boolean)
    other_location_id = Column(String(16))
    loan_amt = Column(Float)
    loan_term = Column(String(8))

    collat_value = Column(Float)
    is_sar = Column(Boolean)
    data_source = Column(String(16))

    def __init__(
        self,
        tx_id,
        tx_direction,
        tx_purpose,
        app_id,
        app_name,
        policy_no,
        policy_name,
        loan_no,
        Cathay_acct,
        tx_amt,
        tx_currency,
        tx_time,
        tx_location,
        tx_platform,
        tx_type,
        claim_no,
        apply_amt,
        payer_status,
        payer_insurant_relationship,
        payer_id,
        payer_name,
        payer_acct,
        payer_bank,
        payer_type,
        payee_status,
        payee_insurant_relationship,
        payee_id,
        payee_name,
        payee_acct,
        payee_bank,
        payee_type,
        tx_status,
        tx_status_note,
        hits,
        ip_address,
        device_type,
        app_type,
        app_version,
        atm_id,
        tx_bank_id,
        representative,
        other_location_id,
        loan_amt,
        loan_term,
        collat_value,
        is_sar,
        data_source,
    ):

        self.tx_id = tx_id
        self.tx_direction = tx_direction
        self.tx_purpose = tx_purpose
        self.app_id = app_id
        self.app_name = app_name
        self.policy_no = policy_no
        self.policy_name = policy_name
        self.loan_no = loan_no
        self.Cathay_acct = Cathay_acct
        self.tx_amt = tx_amt
        self.tx_currency = tx_currency
        self.tx_time = tx_time
        self.tx_location = tx_location
        self.tx_platform = tx_platform
        self.tx_type = tx_type
        self.claim_no = claim_no
        self.apply_amt = apply_amt
        self.payer_status = payer_status
        self.payer_insurant_relationship = payer_insurant_relationship
        self.payer_id = payer_id
        self.payer_name = payer_name
        self.payer_acct = payer_acct
        self.payer_bank = payer_bank
        self.payer_type = payer_type
        self.payee_status = payee_status
        self.payee_insurant_relationship = payee_insurant_relationship
        self.payee_id = payee_id
        self.payee_name = payee_name
        self.payee_acct = payee_acct
        self.payee_bank = payee_bank
        self.payee_type = payee_type
        self.tx_status = tx_status
        self.tx_status_note = tx_status_note
        self.hits = hits
        self.ip_address = ip_address
        self.device_type = device_type
        self.app_type = app_type
        self.app_version = app_version
        self.atm_id = atm_id
        self.tx_bank_id = tx_bank_id
        self.representative = representative
        self.other_location_id = other_location_id
        self.loan_amt = loan_amt
        self.loan_term = loan_term
        self.collat_value = collat_value
        self.is_sar = is_sar
        self.data_source = data_source
