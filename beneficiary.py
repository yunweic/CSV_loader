from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, Sequence, String, DateTime, Boolean, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from faker import Faker
import random
import pdb
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import csv
from config import ES_URL

FAKER_LOCALE = "en_US"

Base = declarative_base()

# Load test data into MySQL
def test_data_loader(engine, size=10):

    Session = sessionmaker(bind=engine)
    session = Session()
    # initialize the faker and random seed
    fake = Faker(FAKER_LOCALE)

    beneficiary_list = []

    for i in range(1, size + 1):

        # pdb.set_trace()

        # load the data
        beneficiary_list.append(
            Beneficiary(
                policy_no=i,
                id="A" + str(random.randrange(1, 1000000000)),
                name=fake.name(),
                customer_since="2007-09-30",
                nationality="TW",
                gender=random.choice(["M", "F"]),
                date_of_birth="2001-03-12",
                age=random.randrange(1, 90),
            )
        )

    session.add_all(beneficiary_list)
    session.commit()


# create the beneficiaries index
def create_index(es):

    res = es.indices.create(index="beneficiaries")
    print(" response: '%s'" % (res))

# Load CSV into ElasticSearch
def load_ES():

    # set the url of the ElasticSearch here
    print(ES_URL)
    es = Elasticsearch([ES_URL])

    # delete the index
    if es.indices.exists(index="beneficiaries"):
        res = es.indices.delete(index="beneficiaries")
        print(" response: '%s'" % (res))

    create_index(es)

    if es.indices.exists(index="beneficiaries"):
        print("\n[Info: ]Successfully create the beneficiaries index \n")

    # read beneficiaries from csv
    with open("csvs/beneficiary.csv") as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index="beneficiaries")

# Load CSV into MySQL
def csv_data_loader(engine):

    Session = sessionmaker(bind=engine)
    session = Session()

    beneficiary_list = []

    df = pd.read_csv("csvs/beneficiary.csv")

    df1 = df.where(pd.notnull(df), None)

    for index, row in df1.iterrows():
        beneficiary_list.append(Beneficiary(**row.to_dict()))
        print(row.to_dict())

    session.add_all(beneficiary_list)
    session.commit()

# Beneficiary model class
class Beneficiary(Base):
    __tablename__ = "beneficiaries"
    change_id = Column(String(16))
    change_effective_date = Column(Date)
    action = Column(String(6))

    policy_no = Column(String(32))
    lapsed_policy_no = Column(String(256))

    id = Column(String(16), primary_key=True)
    name = Column(String(16))
    customer_since = Column(DateTime)
    nationality = Column(String(32))
    gender = Column(String(8))
    date_of_birth = Column(Date)
    age = Column(Integer)

    mailing_address = Column(String(128))
    mobile = Column(String(32))
    email = Column(String(64))
    main_phone = Column(String(32))

    relationship = Column(String(16))
    ben_type = Column(String(16))
    bank = Column(String(16))

    branch = Column(String(16))
    account_no = Column(String(32))
    Ratio = Column(Float)
    rank = Column(Integer)

    pay_premium_type = Column(String(16))
    pay_ratio = Column(Float)

    duration = Column(Integer)

    in_watchlist = Column(Boolean)
    is_watchlist_now = Column(Boolean)

    rule_alerts = Column(Integer)
    is_other_alert = Column(Boolean)

    rules_broke = Column(String(16))
    break_rule_dates_arr = Column(String(128))

    is_fraud = Column(Boolean)
    is_adv_media = Column(Boolean)
    is_pep = Column(Boolean)
    is_sar = Column(Boolean)

    def __init__(
        self,
        change_id,
        change_effective_date,
        action,
        policy_no,
        lapsed_policy_no,
        id,
        name,
        customer_since,
        nationality,
        gender,
        date_of_birth,
        age,
        mailing_address,
        mobile,
        email,
        main_phone,
        relationship,
        ben_type,
        bank,
        branch,
        account_no,
        Ratio,
        rank,
        pay_premium_type,
        pay_ratio,
        duration,
        in_watchlist,
        is_watchlist_now,
        rule_alerts,
        is_other_alert,
        rules_broke,
        break_rule_dates_arr,
        is_fraud,
        is_adv_media,
        is_pep,
        is_sar,
    ):

        self.change_id = change_id
        self.change_effective_date = change_effective_date
        self.action = action
        self.policy_no = policy_no
        self.lapsed_policy_no = lapsed_policy_no

        self.id = id
        self.name = name
        self.customer_since = customer_since
        self.nationality = nationality
        self.gender = gender
        self.date_of_birth = date_of_birth
        self.age = age
        self.mailing_address = mailing_address
        self.mobile = mobile
        self.email = email
        self.main_phone = main_phone
        self.relationship = relationship
        self.ben_type = ben_type
        self.bank = bank
        self.branch = branch
        self.account_no = account_no
        self.Ratio = Ratio
        self.rank = rank
        self.pay_premium_type = pay_premium_type
        self.pay_ratio = pay_ratio
        self.duration = duration
        self.in_watchlist = in_watchlist
        self.is_watchlist_now = is_watchlist_now
        self.rule_alerts = rule_alerts
        self.is_other_alert = is_other_alert
        self.rules_broke = rules_broke
        self.break_rule_dates_arr = break_rule_dates_arr
        self.is_fraud = is_fraud
        self.is_adv_media = is_adv_media
        self.is_pep = is_pep
        self.is_sar = is_sar
