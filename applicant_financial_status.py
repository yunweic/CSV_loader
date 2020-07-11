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
import pdb
import pandas as pd
import applicant
from elasticsearch import Elasticsearch, helpers
import csv
from config import ES_URL

Base = declarative_base()

# create the applicant_financial_status index
def create_index(es):

    res = es.indices.create(index="applicant_financial_status")
    print(" response: '%s'" % (res))


def load_ES():

    # set the url of the ElasticSearch here
    print(ES_URL)
    es = Elasticsearch([ES_URL])

    # delete the index
    if es.indices.exists(index="applicant_financial_status"):
        res = es.indices.delete(index="applicant_financial_status")
        print(" response: '%s'" % (res))

    create_index(es)

    if es.indices.exists(index="applicant_financial_status"):
        print("\n[Info: ]Successfully create the applicant_financial_status index \n")

    # read applicant_financial_status from csv
    with open("csvs/applicant_financial_status.csv") as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index="applicant_financial_status")


def csv_data_loader(engine):

    Session = sessionmaker(bind=engine)
    session = Session()

    applicant_financial_status_list = []

    df = pd.read_csv("csvs/applicant_financial_status.csv")

    df1 = df.where(pd.notnull(df), None)

    for index, row in df1.iterrows():
        applicant_financial_status_list.append(
            ApplicantFinancialStatus(**row.to_dict())
        )
        print(row.to_dict())

    session.add_all(applicant_financial_status_list)
    session.commit()


class ApplicantFinancialStatus(Base):
    __tablename__ = "applicant_financial_status"

    change_id = Column(String(16))
    change_effective_date = Column(Date)
    action = Column(String(6))

    policy_no = Column(String(32))

    applicant_id = Column(String(16), primary_key=True)
    insurant_id = Column(String(16))

    purpose = Column(String(16))

    applicant_title = Column(String(16))
    applicant_company = Column(String(32))
    applicant_tax_id = Column(String(16))

    insurant_title = Column(String(16))
    insurant_company = Column(String(32))
    insurant_tax_id = Column(String(16))

    applicant_annual_income = Column(Float)
    applicant_other_income = Column(Float)
    applicant_family_income = Column(Float)

    insurant_annual_income = Column(Float)
    insurant_other_income = Column(Float)
    insurant_family_income = Column(Float)

    def __init__(
        self,
        change_id,
        change_effective_date,
        action,
        policy_no,
        applicant_id,
        insurant_id,
        purpose,
        applicant_title,
        applicant_company,
        applicant_tax_id,
        insurant_title,
        insurant_company,
        insurant_tax_id,
        applicant_annual_income,
        applicant_other_income,
        applicant_family_income,
        insurant_annual_income,
        insurant_other_income,
        insurant_family_income,
    ):

        self.change_id = change_id
        self.change_effective_date = change_effective_date
        self.action = action
        self.policy_no = policy_no
        self.applicant_id = applicant_id
        self.insurant_id = insurant_id
        self.purpose = purpose
        self.applicant_title = applicant_title
        self.applicant_company = applicant_company
        self.applicant_tax_id = applicant_tax_id
        self.insurant_title = insurant_title
        self.insurant_company = insurant_company
        self.insurant_tax_id = insurant_tax_id
        self.applicant_annual_income = applicant_annual_income
        self.applicant_other_income = applicant_other_income
        self.applicant_family_income = applicant_family_income
        self.insurant_annual_income = insurant_annual_income
        self.insurant_other_income = insurant_other_income
        self.insurant_family_income = insurant_family_income
