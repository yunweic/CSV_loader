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

# create the loan_applications index
def create_index(es):

    res = es.indices.create(index="loan_applications")
    print(" response: '%s'" % (res))


def load_ES():

    # set the url of the ElasticSearch here
    es = Elasticsearch([ES_URL])

    # delete the index
    if es.indices.exists(index="loan_applications"):
        res = es.indices.delete(index="loan_applications")
        print(" response: '%s'" % (res))

    create_index(es)

    if es.indices.exists(index="loan_applications"):
        print("\n[Info: ]Successfully create the loan_applications index \n")

    # read beneficiaries from csv
    with open("csvs/loan_application.csv") as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index="loan_applications")


def csv_data_loader(engine):

    Session = sessionmaker(bind=engine)
    session = Session()

    loan_application_list = []

    df = pd.read_csv("csvs/loan_application.csv")

    df1 = df.where(pd.notnull(df), None)

    for index, row in df1.iterrows():
        loan_application_list.append(LoanApplication(**row.to_dict()))
        print(row.to_dict())

    session.add_all(loan_application_list)
    session.commit()


class LoanApplication(Base):
    __tablename__ = "loan_application"

    change_id = Column(String(16))
    change_effective_date = Column(Date)
    action = Column(String(6))

    loan_no = Column(String(32), primary_key=True)

    borrower_id = Column(String(16))
    borrower_name = Column(String(16))

    date_apply = Column(Date)

    loan_type = Column(String(16))
    loan_class = Column(String(16))

    with_housing_mortgage = Column(Boolean)
    is_cleaned = Column(Boolean)
    date_cleaned = Column(Date)

    loan_amt = Column(Float)
    loan_purpose = Column(String(16))

    wealth_mgt_apply_type = Column(String(16))
    wealth_mgt_cycle_amt = Column(Float)

    return_method = Column(String(16))
    years_by_stages = Column(Integer)

    fixed_years = Column(Integer)
    previous_load_account = Column(String(16))

    pledge_situation = Column(String(16))
    arr_pledge_providers_id = Column(String(128))
    arr_pledge_providers_name = Column(String(128))
    arr_pledge_address = Column(String(128))

    case_type = Column(String(16))

    name_1st_agent = Column(String(16))
    id_1st_agent = Column(String(16))
    phone_1st_agent = Column(String(16))
    mobile_1st_agent = Column(String(16))
    name_2nd_agent = Column(String(16))
    id_2nd_agent = Column(String(16))
    phone_2nd_agent = Column(String(16))
    mobile_2nd_agent = Column(String(16))

    result = Column(Boolean)
    mortgage_approved = Column(Float)
    disapprove_comments = Column(String(128))
    tx_id = Column(String(16))

    actual_return_method = Column(String(16))
    actual_years_by_stages = Column(Integer)
    actual_fixed_years = Column(Integer)
    actual_tx_id = Column(String(16))

    def __init__(
        self,
        change_id,
        change_effective_date,
        action,
        loan_no,
        borrower_id,
        borrower_name,
        date_apply,
        loan_type,
        loan_class,
        with_housing_mortgage,
        is_cleaned,
        date_cleaned,
        loan_amt,
        loan_purpose,
        wealth_mgt_apply_type,
        wealth_mgt_cycle_amt,
        return_method,
        years_by_stages,
        fixed_years,
        previous_load_account,
        pledge_situation,
        arr_pledge_providers_id,
        arr_pledge_providers_name,
        arr_pledge_address,
        case_type,
        name_1st_agent,
        id_1st_agent,
        phone_1st_agent,
        mobile_1st_agent,
        id_2nd_agent,
        phone_2nd_agent,
        mobile_2nd_agent,
        result,
        mortgage_approved,
        disapprove_comments,
        tx_id,
        actual_return_method,
        actual_years_by_stages,
        actual_fixed_years,
        actual_tx_id,
    ):

        self.change_id = change_id
        self.change_effective_date = change_effective_date
        self.action = action
        self.loan_no = loan_no
        self.borrower_id = borrower_id
        self.borrower_name = borrower_name
        self.date_apply = date_apply
        self.loan_type = loan_type
        self.loan_class = loan_class
        self.with_housing_mortgage = with_housing_mortgage
        self.is_cleaned = is_cleaned
        self.date_cleaned = date_cleaned
        self.loan_amt = loan_amt
        self.loan_purpose = loan_purpose
        self.wealth_mgt_apply_type = wealth_mgt_apply_type
        self.wealth_mgt_cycle_amt = wealth_mgt_cycle_amt
        self.return_method = return_method
        self.years_by_stages = years_by_stages
        self.fixed_years = fixed_years
        self.previous_load_account = previous_load_account
        self.pledge_situation = pledge_situation
        self.arr_pledge_providers_id = arr_pledge_providers_id
        self.arr_pledge_providers_name = arr_pledge_providers_name
        self.arr_pledge_address = arr_pledge_address
        self.case_type = case_type
        self.name_1st_agent = name_1st_agent
        self.id_1st_agent = id_1st_agent
        self.phone_1st_agent = phone_1st_agent
        self.mobile_1st_agent = mobile_1st_agent
        self.id_2nd_agent = id_2nd_agent
        self.phone_2nd_agent = phone_2nd_agent
        self.mobile_2nd_agent = mobile_2nd_agent
        self.result = result
        self.mortgage_approved = mortgage_approved
        self.disapprove_comments = disapprove_comments
        self.tx_id = tx_id
        self.actual_return_method = actual_return_method
        self.actual_years_by_stages = actual_years_by_stages
        self.actual_fixed_years = actual_fixed_years
        self.actual_tx_id = actual_tx_id
