from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from psycopg2.extras import RealDictCursor
from gainy.utils import get_logger

logger = get_logger(__name__)


class KycGetFormConfig(HasuraAction):

    def __init__(self, action_name="kyc_get_form_config"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]

        with context_container.db_conn.cursor(
                cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "select * from app.profiles where id = %(profile_id)s", {
                    "profile_id": profile_id,
                })
            profile = cursor.fetchone()

            cursor.execute("select * from raw_data.gainy_countries")
            countries = cursor.fetchall()

        country_choices = [{
            "value": country["alpha-3"],
            "name": country["name"],
        } for country in countries]

        gender_placeholder = "Male" if profile[
            'gender'] == 1 else "Female" if profile['gender'] == 0 else None

        data = {
            "first_name": {
                "required": True,
                "placeholder": profile["first_name"]
            },
            "last_name": {
                "required": True,
                "placeholder": profile["last_name"]
            },
            "country": {
                "required": True,
                "placeholder": "USA",
                "choices": country_choices
            },
            "email_address": {
                "required": True,
                "placeholder": profile["email"]
            },
            "language": {
                "required":
                True,
                "placeholder":
                "en_US",
                "choices": [
                    {
                        "value": "en_US",
                        "name": "English",
                    },
                    {
                        "value": "zh_CN",
                        "name": "Chinese",
                    },
                    {
                        "value": "es_ES",
                        "name": "Spanish",
                    },
                    {
                        "value": "pt_BR",
                        "name": "Portuguese",
                    },
                ]
            },
            "employment_status": {
                "required":
                True,
                "choices": [
                    {
                        "value": "EMPLOYED",
                        "name": "Employed",
                    },
                    {
                        "value": "RETIRED",
                        "name": "Retired",
                    },
                    {
                        "value": "STUDENT",
                        "name": "Student",
                    },
                    {
                        "value": "UNEMPLOYED",
                        "name": "Not Employed",
                    },
                    {
                        "value": "SELF_EMPLOYED",
                        "name": "Self Employed / Business Owner",
                    },
                ]
            },
            "employment_type": {
                "choices": [
                    {
                        "value": "AGRICULTURE",
                        "name": "Agriculture, Forestry, Fishing and Hunting",
                    },
                    {
                        "value": "MINING",
                        "name":
                        "Mining, Quarrying, and Oil and Gas Extraction",
                    },
                    {
                        "value": "UTILITIES",
                        "name": "Utilities",
                    },
                    {
                        "value": "CONSTRUCTION",
                        "name": "Construction",
                    },
                    {
                        "value": "MANUFACTURING",
                        "name": "Manufacturing",
                    },
                    {
                        "value": "WHOLESALE",
                        "name": "Wholesale Trade",
                    },
                    {
                        "value": "RETAIL",
                        "name": "Retail Trade",
                    },
                    {
                        "value": "TRANSPORT",
                        "name": "Transportation and Warehousing",
                    },
                    {
                        "value": "INFORMATION",
                        "name": "Information",
                    },
                    {
                        "value": "FINANCE",
                        "name": "Finance and Insurance",
                    },
                    {
                        "value": "REAL_ESTATE",
                        "name": "Real Estate and Rental and Leasing",
                    },
                    {
                        "value": "PROFESSIONAL",
                        "name":
                        "Professional, Scientific, and Technical Services",
                    },
                    {
                        "value": "MANAGEMENT",
                        "name": "Management of Companies and Enterprises",
                    },
                    {
                        "value": "EDUCATION",
                        "name": "Educational Services",
                    },
                    {
                        "value": "HEALTH",
                        "name": "Health Care and Social Assistance",
                    },
                    {
                        "value": "ART",
                        "name": "Arts, Entertainment, and Recreation",
                    },
                    {
                        "value": "FOOD",
                        "name": "Accommodation and Food Services",
                    },
                    {
                        "value": "PUBLIC",
                        "name": "Public Administration",
                    },
                    {
                        "value": "WASTE",
                        "name": "Waste Management and Remediation Services",
                    },
                ]
            },
            "employment_position": {
                "choices": [
                    {
                        "value": "ACCOUNTANT",
                        "name": "Accountant/CPA/Bookkeeper/Controller",
                    },
                    {
                        "value": "ACTUARY",
                        "name": "Actuary",
                    },
                    {
                        "value": "ADJUSTER",
                        "name": "Adjuster",
                    },
                    {
                        "value": "ADMINISTRATOR",
                        "name": "Administrator",
                    },
                    {
                        "value": "ADVERTISER",
                        "name": "Advertiser/Marketer/PR Professional",
                    },
                    {
                        "value": "AGENT",
                        "name": "Agent",
                    },
                    {
                        "value": "ATC",
                        "name": "Air Traffic Controller",
                    },
                    {
                        "value": "AMBASSADOR",
                        "name": "Ambassador/Consulate Professional",
                    },
                    {
                        "value": "ANALYST",
                        "name": "Analyst",
                    },
                    {
                        "value": "APPRAISER",
                        "name": "Appraiser",
                    },
                    {
                        "value": "ARCHITECT",
                        "name": "Architect/Designer",
                    },
                    {
                        "value": "ARTIST",
                        "name": "Artist/Performer/Actor/Dancer",
                    },
                    {
                        "value": "ASSISTANT",
                        "name": "Assistant",
                    },
                    {
                        "value": "ATHLETE",
                        "name": "Athlete",
                    },
                    {
                        "value": "ATTENDANT",
                        "name": "Attendant",
                    },
                    {
                        "value": "ATTORNEY",
                        "name": "Attorney/Judge/Legal Professional",
                    },
                    {
                        "value": "AUCTIONEER",
                        "name": "Auctioneer",
                    },
                    {
                        "value": "AUDITOR",
                        "name": "Auditor",
                    },
                    {
                        "value": "BARBER",
                        "name": "Barber/Beautician/Hairstylist",
                    },
                    {
                        "value": "BROKER",
                        "name": "Broker",
                    },
                    {
                        "value": "BUSINESS_EXEC",
                        "name": "Business Executive (VP, Director, etc.)",
                    },
                    {
                        "value": "BUSINESS_OWNER",
                        "name": "Business Owner",
                    },
                    {
                        "value": "CAREGIVER",
                        "name": "Caregiver",
                    },
                    {
                        "value": "CARPENTER",
                        "name": "Carpenter/Construction Worker",
                    },
                    {
                        "value": "CASHIER",
                        "name": "Cashier",
                    },
                    {
                        "value": "CHEF",
                        "name": "Chef/Cook",
                    },
                    {
                        "value": "CHIROPRACTOR",
                        "name": "Chiropractor",
                    },
                    {
                        "value": "CIVIL",
                        "name": "Civil Servant",
                    },
                    {
                        "value": "CLERGY",
                        "name": "Clergy",
                    },
                    {
                        "value": "CLERK",
                        "name": "Clerk",
                    },
                    {
                        "value": "COMPLIANCE",
                        "name": "Compliance/Regulatory Professional",
                    },
                    {
                        "value": "CONSULTANT",
                        "name": "Consultant",
                    },
                    {
                        "value": "CONTRACTOR",
                        "name": "Contractor",
                    },
                    {
                        "value": "COUNSELOR",
                        "name": "Counselor/Therapist",
                    },
                    {
                        "value": "CUSTOMER_SERVICE",
                        "name": "Customer Service Representative",
                    },
                    {
                        "value": "DEALER",
                        "name": "Dealer",
                    },
                    {
                        "value": "DEVELOPER",
                        "name": "Developer",
                    },
                    {
                        "value": "DISTRIBUTOR",
                        "name": "Distributor",
                    },
                    {
                        "value": "DOCTOR",
                        "name": "Doctor/Dentist/Veterinarian/Surgeon",
                    },
                    {
                        "value": "DRIVER",
                        "name": "Driver",
                    },
                    {
                        "value": "ENGINEER",
                        "name": "Engineer",
                    },
                    {
                        "value": "EXAMINER",
                        "name": "Examiner",
                    },
                    {
                        "value": "EXTERMINATOR",
                        "name": "Exterminator",
                    },
                    {
                        "value": "FACTORY",
                        "name": "Factory/Warehouse Worker",
                    },
                    {
                        "value": "FARMER",
                        "name": "Farmer/Rancher",
                    },
                    {
                        "value": "FINANCIAL",
                        "name": "Financial Planner",
                    },
                    {
                        "value": "FISHERMAN",
                        "name": "Fisherman",
                    },
                    {
                        "value": "FLIGHT",
                        "name": "Flight Attendant",
                    },
                    {
                        "value": "HR",
                        "name": "Human Resources Professional",
                    },
                    {
                        "value": "IMPEX",
                        "name": "Importer/Exporter",
                    },
                    {
                        "value": "INSPECTOR",
                        "name": "Inspector/Investigator",
                    },
                    {
                        "value": "INTERN",
                        "name": "Intern",
                    },
                    {
                        "value": "INVESTMENT",
                        "name": "Investment Advisor/Investment Manager",
                    },
                    {
                        "value": "INVESTOR",
                        "name": "Investor",
                    },
                    {
                        "value": "IT",
                        "name": "IT Professional/IT Associate",
                    },
                    {
                        "value": "JANITOR",
                        "name": "Janitor",
                    },
                    {
                        "value": "JEWELER",
                        "name": "Jeweler",
                    },
                    {
                        "value": "LABORER",
                        "name": "Laborer",
                    },
                    {
                        "value": "LANDSCAPER",
                        "name": "Landscaper",
                    },
                    {
                        "value": "LENDING",
                        "name": "Lending Professional",
                    },
                    {
                        "value": "MANAGER",
                        "name": "Manager",
                    },
                    {
                        "value": "MECHANIC",
                        "name": "Mechanic",
                    },
                    {
                        "value": "MILITARY",
                        "name": "Military, Officer or Associated",
                    },
                    {
                        "value": "MORTICIAN",
                        "name": "Mortician/Funeral Director",
                    },
                    {
                        "value": "NURSE",
                        "name": "Nurse",
                    },
                    {
                        "value": "NUTRITIONIST",
                        "name": "Nutritionist",
                    },
                    {
                        "value": "OFFICE",
                        "name": "Office Associate",
                    },
                    {
                        "value": "PHARMACIST",
                        "name": "Pharmacist",
                    },
                    {
                        "value": "PHYSICAL",
                        "name": "Physical Therapist",
                    },
                    {
                        "value": "PILOT",
                        "name": "Pilot",
                    },
                    {
                        "value":
                        "POLICE",
                        "name":
                        "Police Officer/Firefighter/Law Enforcement Professional",
                    },
                    {
                        "value": "POLITICIAN",
                        "name": "Politician",
                    },
                    {
                        "value": "PM",
                        "name": "Project Manager",
                    },
                    {
                        "value": "REP",
                        "name": "Registered Rep",
                    },
                    {
                        "value": "RESEARCHER",
                        "name": "Researcher",
                    },
                    {
                        "value": "SAILOR",
                        "name": "Sailor/Seaman",
                    },
                    {
                        "value": "SALES",
                        "name": "Salesperson",
                    },
                    {
                        "value": "SCIENTIST",
                        "name": "Scientist",
                    },
                    {
                        "value": "SEAMSTRESS",
                        "name": "Seamstress/Tailor",
                    },
                    {
                        "value": "SECURITY",
                        "name": "Security Guard",
                    },
                    {
                        "value": "SOCIAL",
                        "name": "Social Worker",
                    },
                    {
                        "value": "TEACHER",
                        "name": "Teacher/Professor",
                    },
                    {
                        "value": "TECHNICIAN",
                        "name": "Technician",
                    },
                    {
                        "value": "TELLER",
                        "name": "Teller",
                    },
                    {
                        "value": "TRADESPERSON",
                        "name": "Tradesperson/Craftsperson",
                    },
                    {
                        "value": "TRAINER",
                        "name": "Trainer/Instructor",
                    },
                    {
                        "value": "TRANSPORTER",
                        "name": "Transporter",
                    },
                    {
                        "value": "UNDERWRITER",
                        "name": "Underwriter",
                    },
                    {
                        "value": "WRITER",
                        "name": "Writer/Journalist/Editor",
                    },
                ]
            },
            "investor_profile_experience": {
                "required":
                True,
                "choices": [
                    {
                        "value": "NONE",
                        "name": "None",
                    },
                    {
                        "value": "YRS_1_2",
                        "name": "1–2 years",
                    },
                    {
                        "value": "YRS_3_5",
                        "name": "3–5 years",
                    },
                    {
                        "value": "YRS_5_10",
                        "name": "5–10 years",
                    },
                    {
                        "value": "YRS_10_",
                        "name": "10+ years",
                    },
                ]
            },
            "investor_profile_annual_income": {
                "required": True,
            },
            "investor_profile_net_worth_total": {
                "required": True,
            },
            "investor_profile_net_worth_liquid": {
                "required": True,
            },
            "investor_profile_risk_tolerance": {
                "required":
                True,
                "choices": [
                    {
                        "value": "LOW",
                        "name": "Low Risk",
                    },
                    {
                        "value": "MODERATE",
                        "name": "Moderate Risk",
                    },
                    {
                        "value": "SPECULATION",
                        "name": "Speculative Risk",
                    },
                    {
                        "value": "HIGH",
                        "name": "High Risk",
                    },
                ]
            },
            "investor_profile_objectives": {
                "required":
                True,
                "choices": [
                    {
                        "value": "LONG_TERM",
                        "name": "Long–term buy & hold investing",
                    },
                    {
                        "value": "INFREQUENT",
                        "name":
                        "Trading infrequently when I see an opportunity",
                    },
                    {
                        "value": "FREQUENT",
                        "name": "Frequent trader, depending on the market",
                    },
                    {
                        "value": "ACTIVE_DAILY",
                        "name": "Active trader, daily trader",
                    },
                    {
                        "value": "NEW",
                        "name": "New to investing",
                    },
                ]
            },
            "disclosures_drivewealth_terms_of_use": {
                "required": True,
            },
            "disclosures_drivewealth_customer_agreement": {
                "required": True,
            },
            "disclosures_drivewealth_market_data_agreement": {
                "required": True,
            },
            "disclosures_rule14b": {
                "required": True,
            },
            "disclosures_drivewealth_privacy_policy": {
                "required": True,
            },
            "disclosures_signed_by": {
                "required": True,
            },
            "tax_id_value": {
                "required": True,
            },
            "tax_id_type": {
                "required":
                True,
                "choices": [
                    {
                        "value": "SSN",
                        "name": "Social Security Number",
                    },
                    {
                        "value": "TIN",
                        "name": "Tax Identification Number",
                    },
                    {
                        "value": "other",
                        "name": "Other",
                    },
                ]
            },
            "citizenship": {
                "required": True,
                "placeholder": "USA",
                "choices": country_choices
            },
            "gender": {
                "placeholder":
                gender_placeholder,
                "required":
                False,
                "choices": [
                    {
                        "value": "Male",
                        "name": "Male",
                    },
                    {
                        "value": "Female",
                        "name": "Female",
                    },
                ]
            },
            "marital_status": {
                "required":
                True,
                "choices": [
                    {
                        "value": "SINGLE",
                        "name": "Single",
                    },
                    {
                        "value": "DIVORCED",
                        "name": "Divorced",
                    },
                    {
                        "value": "MARRIED",
                        "name": "Married",
                    },
                    {
                        "value": "WIDOWED",
                        "name": "Widowed",
                    },
                    {
                        "value": "PARTNER",
                        "name": "Domestic Partner",
                    },
                ]
            },
            "birthdate": {
                "required": True
            },
            "address_street1": {
                "required": True
            },
            "address_city": {
                "required": True
            },
            "address_postal_code": {
                "required": True
            },
            "address_country": {
                "required": True,
                "placeholder": "USA",
                "choices": country_choices
            }
        }

        data = {
            k: {
                "required": None,
                "placeholder": None,
                "choices": None,
                **i,
            }
            for k, i in data.items()
        }

        return data
