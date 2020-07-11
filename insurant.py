from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, Sequence, String, DateTime, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base
from faker import Faker
import random
import pdb
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import csv
from config import ES_URL

FAKER_LOCALE = "zh_TW"

Base = declarative_base()


def test_data_loader(engine, size=10):

    Session = sessionmaker(bind=engine)
    session = Session()
    # initialize the faker and random seed
    fake = Faker(FAKER_LOCALE)

    insurant_list = []

    for i in range(1, size + 1):

        # load the data
        insurant_list.append(
            Insurant(
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

    session.add_all(insurant_list)
    session.commit()


# create the insurants index
def create_index(es):

    res = es.indices.create(index="insurants")
    print(" response: '%s'" % (res))


def load_ES():

    # set the url of the ElasticSearch here
    es = Elasticsearch([ES_URL])

    # delete the index
    if es.indices.exists(index="insurants"):
        res = es.indices.delete(index="insurants")
        print(" response: '%s'" % (res))

    create_index(es)

    if es.indices.exists(index="insurants"):
        print("\n[Info: ]Successfully create the insurants index \n")

    # read beneficiaries from csv
    with open("csvs/insurant.csv") as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index="insurants")


def csv_data_loader(engine):

    Session = sessionmaker(bind=engine)
    session = Session()

    insurant_list = []

    df = pd.read_csv("csvs/insurant.csv")

    df1 = df.where(pd.notnull(df), None)

    for index, row in df1.iterrows():
        insurant_list.append(Insurant(**row.to_dict()))
        print(row.to_dict())

    session.add_all(insurant_list)
    session.commit()


class Insurant(Base):
    __tablename__ = "insurants"

    change_id = Column(String(16))
    change_effective_date = Column(Date)
    action = Column(String(6))
    policy_no = Column(String(32))
    lapsed_policy_no = Column(String(256))

    id = Column(String(16), primary_key=True)
    name = Column(String(32))
    customer_since = Column(DateTime)
    nationality = Column(String(32))
    gender = Column(String(16))
    date_of_birth = Column(DateTime)
    age = Column(Integer)

    is_married = Column(Boolean)

    home_address = Column(String(128))
    mailing_address = Column(String(128))
    mobile = Column(String(32))

    home_phone = Column(String(32))
    office_phone = Column(String(32))

    email = Column(String(128))

    job_title = Column(String(32))
    industry = Column(String(64))
    industry_code = Column(String(16))
    job_level = Column(String(16))
    employer = Column(String(32))

    handicap = Column(Boolean)
    under_supervise = Column(Boolean)

    pay_as_you_go_injury_medical_insurance = Column(Boolean)
    pay_as_you_go_medical_insurance = Column(Boolean)

    in_watchlist = Column(Boolean)
    is_watchlist_now = Column(Boolean)

    rule_alerts = Column(Integer)
    is_other_alert = Column(Boolean)
    rules_broke_arr = Column(String(128))
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
        is_married,
        home_address,
        mailing_address,
        mobile,
        home_phone,
        office_phone,
        email,
        job_title,
        industry,
        industry_code,
        job_level,
        employer,
        handicap,
        under_supervise,
        pay_as_you_go_injury_medical_insurance,
        pay_as_you_go_medical_insurance,
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
        self.is_married = is_married
        self.home_address = home_address
        self.mailing_address = mailing_address
        self.mobile = mobile
        self.home_phone = home_phone
        self.office_phone = office_phone
        self.email = email
        self.job_title = job_title
        self.industry = industry
        self.industry_code = industry_code
        self.job_level = job_level
        self.employer = employer
        self.handicap = handicap
        self.under_supervise = under_supervise
        self.pay_as_you_go_injury_medical_insurance = (
            pay_as_you_go_injury_medical_insurance
        )
        self.pay_as_you_go_medical_insurance = pay_as_you_go_medical_insurance
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
