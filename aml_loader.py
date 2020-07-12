from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import applicant
import applicant_financial_status
import insurant
import beneficiary
import borrower
import borrower_financial_status
import transaction
import claim_result
import policy
import loan_application
from config import MYSQL_IP, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

# Read data from applicant table
def read_from_table(table_name=applicant.Applicant):
    Session = sessionmaker(bind=engine)
    session = Session()
    result = session.query(table_name).all()

    for row in result:
        print("Name: ", row.name, "Age:", row.age, "ID:", row.id)


if __name__ == "__main__":
    engine = create_engine(
        "mysql+mysqlconnector://"
        + MYSQL_USER
        + ":"
        + MYSQL_PASSWORD
        + "@"
        + MYSQL_IP
        + ":"
        + MYSQL_PORT
        + "/"
        + MYSQL_DATABASE,
        echo=True,
    )

    # delete the table
    applicant_financial_status.ApplicantFinancialStatus.__table__.drop(engine)
    applicant.Applicant.__table__.drop(engine)

    insurant.Insurant.__table__.drop(engine)
    beneficiary.Beneficiary.__table__.drop(engine)

    borrower.Borrower.__table__.drop(engine)
    borrower_financial_status.BorrowerFinancialStatus.__table__.drop(engine)

    transaction.Transaction.__table__.drop(engine)
    claim_result.ClaimResult.__table__.drop(engine)

    policy.Policy.__table__.drop(engine)
    loan_application.LoanApplication.__table__.drop(engine)

    # create the tables
    applicant.Base.metadata.create_all(engine, checkfirst=True)
    applicant_financial_status.Base.metadata.create_all(engine, checkfirst=True)

    insurant.Base.metadata.create_all(engine, checkfirst=True)
    beneficiary.Base.metadata.create_all(engine, checkfirst=True)

    borrower.Base.metadata.create_all(engine, checkfirst=True)
    borrower_financial_status.Base.metadata.create_all(engine, checkfirst=True)

    transaction.Base.metadata.create_all(engine, checkfirst=True)
    claim_result.Base.metadata.create_all(engine, checkfirst=True)

    policy.Base.metadata.create_all(engine, checkfirst=True)
    loan_application.Base.metadata.create_all(engine, checkfirst=True)

    # load the applicant table
    applicant.csv_data_loader(engine)
    applicant_financial_status.csv_data_loader(engine)

    insurant.csv_data_loader(engine)
    beneficiary.csv_data_loader(engine)

    borrower.csv_data_loader(engine)
    borrower_financial_status.csv_data_loader(engine)

    transaction.csv_data_loader(engine)
    claim_result.csv_data_loader(engine)

    policy.csv_data_loader(engine)
    loan_application.csv_data_loader(engine)

    # load into ES
    applicant.load_ES()
    applicant_financial_status.load_ES()
    beneficiary.load_ES()
    borrower_financial_status.load_ES()
    borrower.load_ES()
    claim_result.load_ES()
    insurant.load_ES()
    loan_application.load_ES()
    policy.load_ES()
    transaction.load_ES()

    # check the result
    # read_from_table()
    # read_from_table(insurant.Insurant)
