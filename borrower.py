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

# create the borrowers index
def create_index(es):

    res = es.indices.create(index="borrowers")
    print(" response: '%s'" % (res))


def load_ES():

    # set the url of the ElasticSearch here
    print(ES_URL)
    es = Elasticsearch([ES_URL])

    # delete the index
    if es.indices.exists(index="borrowers"):
        res = es.indices.delete(index="borrowers")
        print(" response: '%s'" % (res))

    create_index(es)

    if es.indices.exists(index="borrowers"):
        print("\n[Info: ]Successfully create the borrowers index \n")

    # read beneficiaries from csv
    with open("csvs/borrower.csv") as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index="borrowers")


def test_data_loader(engine, size=10):

    Session = sessionmaker(bind=engine)
    session = Session()
    # initialize the faker and random seed
    fake = Faker(FAKER_LOCALE)

    borrower_list = []

    for i in range(1, size + 1):

        # load the data
        borrower_list.append(
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

    session.add_all(borrower_list)
    session.commit()


def csv_data_loader(engine):

    Session = sessionmaker(bind=engine)
    session = Session()

    borrower_list = []

    df = pd.read_csv("csvs/borrower.csv")
    df1 = df.where(pd.notnull(df), None)

    for index, row in df1.iterrows():
        borrower_list.append(Borrower(**row.to_dict()))
        print(row.to_dict())

    session.add_all(borrower_list)
    session.commit()


class Borrower(Base):
    __tablename__ = "borrowers"

    change_id = Column(String(16))
    change_effective_date = Column(Date)
    action = Column(String(6))

    loan_no = Column(String(32))

    id = Column(String(16), primary_key=True)
    name = Column(String(16))

    nationality = Column(String(32))
    gender = Column(String(8))
    date_of_birth = Column(Date)
    age = Column(Integer)
    is_married = Column(Boolean)
    diploma = Column(String(16))

    home_address = Column(String(128))
    mailing_address = Column(String(128))
    mobile = Column(String(32))
    home_phone = Column(String(32))

    email = Column(String(64))
    main_phone = Column(String(32))

    policy_no_arr = Column(String(128))
    status_in_policy_arr = Column(String(128))

    borrower_in_watchlist = Column(Boolean)
    borrower_is_watchlist_now = Column(Boolean)

    borrower_rule_alerts = Column(Integer)
    borrower_is_other_alert = Column(Boolean)

    borrower_rules_broke = Column(String(16))
    borrower_break_rule_dates_arr = Column(String(128))

    borrower_is_fraud = Column(Boolean)
    borrower_is_adv_media = Column(Boolean)
    borrower_is_pep = Column(Boolean)
    borrower_is_sar = Column(Boolean)

    def __init__(
        self,
        change_id,
        change_effective_date,
        action,
        loan_no,
        id,
        name,
        nationality,
        gender,
        date_of_birth,
        age,
        is_married,
        diploma,
        home_address,
        mailing_address,
        mobile,
        home_phone,
        email,
        main_phone,
        policy_no_arr,
        status_in_policy_arr,
        borrower_in_watchlist,
        borrower_is_watchlist_now,
        borrower_rule_alerts,
        borrower_is_other_alert,
        borrower_rules_broke_arr,
        borrower_break_rule_dates_arr,
        borrower_is_fraud,
        borrower_is_adv_media,
        borrower_is_sar,
    ):

        self.change_id = change_id
        self.change_effective_date = change_effective_date
        self.action = action
        self.loan_no = loan_no
        self.id = id
        self.name = name
        self.nationality = nationality
        self.gender = gender
        self.date_of_birth = date_of_birth
        self.age = age
        self.is_married = is_married
        self.diploma = diploma
        self.home_address = home_address
        self.mailing_address = mailing_address
        self.mobile = mobile
        self.home_phone = home_phone
        self.email = email
        self.main_phone = main_phone
        self.policy_no_arr = policy_no_arr
        self.status_in_policy_arr = status_in_policy_arr
        self.borrower_in_watchlist = borrower_in_watchlist
        self.borrower_is_watchlist_now = borrower_is_watchlist_now
        self.borrower_rule_alerts = borrower_rule_alerts
        self.borrower_is_other_alert = borrower_is_other_alert
        self.borrower_rules_broke_arr = borrower_rules_broke_arr
        self.borrower_break_rule_dates_arr = borrower_break_rule_dates_arr
        self.borrower_is_fraud = borrower_is_fraud
        self.borrower_is_adv_media = borrower_is_adv_media
        self.borrower_is_sar = borrower_is_sar
