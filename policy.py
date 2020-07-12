from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, Sequence, String, DateTime, Boolean, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from faker import Faker
import random
import pdb
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import csv
from config import ES_URL

Base = declarative_base()


# create the policies index
def create_index(es):

    res = es.indices.create(index="policies")
    print(" response: '%s'" % (res))

# Load CSV into ElasticSearch
def load_ES():

    # set the url of the ElasticSearch here
    es = Elasticsearch([ES_URL])

    # delete the index
    if es.indices.exists(index="policies"):
        res = es.indices.delete(index="policies")
        print(" response: '%s'" % (res))

    create_index(es)

    if es.indices.exists(index="policies"):
        print("\n[Info: ]Successfully create the policies index \n")

    # read policy from csv
    with open("csvs/policy.csv") as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index="policies")

# Load CSV into MySQL
def csv_data_loader(engine):

    Session = sessionmaker(bind=engine)
    session = Session()

    policy_list = []

    df = pd.read_csv("csvs/policy.csv")
    df1 = df.where(pd.notnull(df), None)

    for index, row in df1.iterrows():
        policy_list.append(Policy(**row.to_dict()))
        print(row.to_dict())

    session.add_all(policy_list)
    session.commit()

# Policy model class
class Policy(Base):
    __tablename__ = "policies"

    change_id = Column(String(16))
    change_effective_date = Column(Date)
    action = Column(String(6))

    sales_id = Column(String(16))
    sales_name = Column(String(32))

    policy_no = Column(String(32), primary_key=True)

    applicant_id = Column(String(16))
    insurant_id = Column(String(16))

    ben_id_arr = Column(String(256))

    is_lapse = Column(Boolean)
    date_lapse = Column(Date)

    policy_category = Column(String(32))
    policy_name = Column(String(64))

    cover = Column(Float)
    unit = Column(Float)

    paymenet_period = Column(String(16))
    payment_method = Column(String(16))

    tx_id = Column(String(16))
    effective_date = Column(DateTime)
    mature_date = Column(DateTime)

    payperiod = Column(String(16))
    apl = Column(Boolean)

    rider_policy = Column(String(16))
    rider_insurant_id = Column(String(16))
    rider_cover = Column(Float)

    rider_dailycover = Column(Float)
    rider_unit = Column(Float)

    rider_effective_date = Column(DateTime)
    rider_mature_date = Column(DateTime)

    rider_is_lapse = Column(Boolean)
    rider_date_lapse = Column(Date)
    rider_payperiod = Column(String(16))

    def __init__(
        self,
        change_id,
        change_effective_date,
        action,
        sales_id,
        sales_name,
        policy_no,
        applicant_id,
        insurant_id,
        ben_id_arr,
        is_lapse,
        date_lapse,
        policy_category,
        policy_name,
        cover,
        unit,
        payment_period,
        payment_method,
        tx_id,
        effective_date,
        mature_date,
        payperiod,
        apl,
        rider_policy,
        rider_insurant_id,
        rider_cover,
        rider_dailycover,
        rider_unit,
        rider_effective_date,
        rider_mature_date,
        rider_is_lapse,
        rider_date_lapse,
        rider_payperiod,
    ):

        self.change_id = change_id
        self.change_effective_date = change_effective_date
        self.action = action
        self.sales_id = sales_id
        self.sales_name = sales_name
        self.policy_no = policy_no
        self.applicant_id = applicant_id
        self.insurant_id = insurant_id
        self.ben_id_arr = ben_id_arr
        self.is_lapse = is_lapse
        self.date_lapse = date_lapse
        self.policy_category = policy_category
        self.policy_name = policy_name
        self.cover = cover
        self.unit = unit
        self.payment_period = payment_period
        self.payment_method = payment_method
        self.tx_id = tx_id
        self.effective_date = effective_date
        self.mature_date = mature_date
        self.payperiod = payperiod
        self.apl = apl
        self.rider_policy = rider_policy
        self.rider_insurant_id = rider_insurant_id
        self.rider_cover = rider_cover
        self.rider_dailycover = rider_dailycover
        self.rider_unit = rider_unit
        self.rider_effective_date = rider_effective_date
        self.rider_mature_date = rider_mature_date
        self.rider_is_lapse = rider_is_lapse
        self.rider_date_lapse = rider_date_lapse
        self.rider_payperiod = rider_payperiod
