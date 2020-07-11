from sqlalchemy.orm import sessionmaker
from sqlalchemy import (
    Column,
    Integer,
    Sequence,
    String,
    DateTime,
    Boolean,
    Date,
    Float,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base
from faker import Faker
import random
import pdb
import pandas as pd
import applicant
from elasticsearch import Elasticsearch, helpers
import csv
from config import ES_URL

Base = declarative_base()

# create the borrower_financial_status index
def create_index(es):

    res = es.indices.create(index="borrower_financial_status")
    print(" response: '%s'" % (res))


def load_ES():

    # set the url of the ElasticSearch here
    print(ES_URL)
    es = Elasticsearch([ES_URL])

    # delete the index
    if es.indices.exists(index="borrower_financial_status"):
        res = es.indices.delete(index="borrower_financial_status")
        print(" response: '%s'" % (res))

    create_index(es)

    if es.indices.exists(index="borrower_financial_status"):
        print("\n[Info: ]Successfully create the borrower_financial_status index \n")

    # read beneficiaries from csv
    with open("csvs/borrower_financial_status.csv") as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index="borrower_financial_status")


def csv_data_loader(engine):

    Session = sessionmaker(bind=engine)
    session = Session()

    borrower_financial_status_list = []

    df = pd.read_csv("csvs/borrower_financial_status.csv")
    df1 = df.where(pd.notnull(df), None)

    for index, row in df1.iterrows():
        borrower_financial_status_list.append(BorrowerFinancialStatus(**row.to_dict()))
        print(row.to_dict())

    session.add_all(borrower_financial_status_list)
    session.commit()


class BorrowerFinancialStatus(Base):
    __tablename__ = "borrower_financial_status"

    change_id = Column(String(16))
    change_effective_date = Column(Date)
    action = Column(String(6))

    loan_no = Column(String(32))

    borrower_id = Column(String(16), primary_key=True)
    borrower_name = Column(String(16))

    employer = Column(String(32))

    borrower_title = Column(String(32))
    tax_id = Column(String(16))

    industry = Column(String(64))
    industry_code = Column(String(16))

    job_level = Column(String(16))

    office_phone = Column(String(32))

    company_email = Column(String(128))

    years_served = Column(Integer)

    monthly_salary = Column(Float)

    annual_income = Column(Float)

    company_own = Column(String(32))

    company_own_tax_id = Column(String(16))

    company_phone = Column(String(32))

    revenue = Column(Float)

    annual_net_profit = Column(Float)

    other_incomes = Column(Float)

    def __init__(
        self,
        change_id,
        change_effective_date,
        action,
        loan_no,
        borrower_id,
        borrower_name,
        employer,
        borrower_title,
        tax_id,
        industry,
        industry_code,
        job_level,
        office_phone,
        company_email,
        years_served,
        monthly_salary,
        annual_income,
        company_own,
        company_own_tax_id,
        company_phone,
        revenue,
        annual_net_profit,
        other_incomes,
    ):

        self.change_id = change_id
        self.change_effective_date = change_effective_date
        self.action = action
        self.loan_no = loan_no
        self.borrower_id = borrower_id
        self.borrower_name = borrower_name
        self.employer = employer
        self.borrower_title = borrower_title
        self.tax_id = tax_id
        self.industry = industry
        self.industry_code = industry_code
        self.job_level = job_level
        self.office_phone = office_phone
        self.company_email = company_email
        self.years_served = years_served
        self.monthly_salary = monthly_salary
        self.annual_income = annual_income
        self.company_own = company_own
        self.company_own_tax_id = company_own_tax_id
        self.company_phone = company_phone
        self.revenue = revenue
        self.annual_net_profit = annual_net_profit
        self.other_incomes = other_incomes
