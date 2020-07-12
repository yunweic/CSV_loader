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

# create the claim_results index
def create_index(es):

    res = es.indices.create(index="claim_results")
    print(" response: '%s'" % (res))

# Load CSV into ElasticSearch
def load_ES():

    # set the url of the ElasticSearch here
    es = Elasticsearch([ES_URL])

    # delete the index
    if es.indices.exists(index="claim_results"):
        res = es.indices.delete(index="claim_results")
        print(" response: '%s'" % (res))

    create_index(es)

    if es.indices.exists(index="claim_results"):
        print("\n[Info: ]Successfully create the claim_results index \n")

    # read claim result from csv
    with open("csvs/claim_result.csv") as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index="claim_results")

# Load CSV into MySQL
def csv_data_loader(engine):

    Session = sessionmaker(bind=engine)
    session = Session()

    claim_result_list = []

    df = pd.read_csv("csvs/claim_result.csv")
    df1 = df.where(pd.notnull(df), None)

    for index, row in df1.iterrows():
        claim_result_list.append(ClaimResult(**row.to_dict()))
        print(row.to_dict())

    session.add_all(claim_result_list)
    session.commit()

# Claim Result model class
class ClaimResult(Base):
    __tablename__ = "claim_results"

    claim_no = Column(String(32), primary_key=True)

    policy_no = Column(String(32))

    insured_id = Column(String(16))
    insured_name = Column(String(32))

    ben_id = Column(String(16))
    ben_name = Column(String(32))

    occurence_class = Column(String(16))

    claim_category = Column(String(16))

    Organizer = Column(String(16))
    Implementer = Column(String(16))

    is_broker = Column(Boolean)

    bank_id = Column(String(16))
    bank_name = Column(String(32))

    claims_paymentby = Column(String(16))
    claims_amount = Column(Float)

    claim_result = Column(String(16))
    claims_actual_pay_amount = Column(Float)

    disapprove_comments = Column(String(128))
    claims_deny_amount = Column(Float)
    tx_id = Column(String(16))

    pay_premium_type = Column(String(16))
    pay_ratio = Column(Float)
    duration = Column(Integer)

    def __init__(
        self,
        claim_no,
        policy_no,
        insured_id,
        insured_name,
        ben_id,
        ben_name,
        occurrence_class,
        claim_category,
        Organizer,
        Implementer,
        is_broker,
        bank_id,
        bank_name,
        claims_paymentby,
        claims_amount,
        claim_result,
        claims_actual_pay_amount,
        disapprove_comments,
        claims_deny_amount,
        tx_id,
        pay_premium_type,
        pay_ratio,
        duration,
    ):

        self.claim_no = claim_no
        self.policy_no = policy_no
        self.insured_id = insured_id
        self.insured_name = insured_name
        self.ben_id = ben_id
        self.ben_name = ben_name
        self.occurrence_class = occurrence_class
        self.claim_category = claim_category
        self.Organizer = Organizer
        self.Implementer = Implementer
        self.is_broker = is_broker
        self.bank_id = bank_id
        self.bank_name = bank_name
        self.claims_paymentby = claims_paymentby
        self.claims_amount = claims_amount
        self.claim_result = claim_result
        self.claims_actual_pay_amount = claims_actual_pay_amount
        self.disapprove_comments = disapprove_comments
        self.claims_deny_amount = claims_deny_amount
        self.tx_id = tx_id
        self.pay_premium_type = pay_premium_type
        self.pay_ratio = pay_ratio
        self.duration = duration
