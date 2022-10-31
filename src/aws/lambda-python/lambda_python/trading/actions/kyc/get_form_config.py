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

        country_choices = {
            country["alpha-3"]: country["name"]
            for country in countries
        }

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
                "required": True,
                "placeholder": "en_US",
                "choices": {
                    "en_US": "English",
                    "zh_CN": "Chinese",
                    "es_ES": "Spanish",
                    "pt_BR": "Portuguese"
                }
            },
            "employment_status": {
                "required": True,
                "choices": {
                    "EMPLOYED": "Employed",
                    "RETIRED": "Retired",
                    "STUDENT": "Student",
                    "UNEMPLOYED": "Not Employed",
                    "SELF_EMPLOYED": "Self Employed / Business Owner",
                }
            },
            "employment_type": {
                "choices": {
                    "AGRICULTURE":
                    "Agriculture, Forestry, Fishing and Hunting",
                    "MINING": "Mining, Quarrying, and Oil and Gas Extraction",
                    "UTILITIES": "Utilities",
                    "CONSTRUCTION": "Construction",
                    "MANUFACTURING": "Manufacturing",
                    "WHOLESALE": "Wholesale Trade",
                    "RETAIL": "Retail Trade",
                    "TRANSPORT": "Transportation and Warehousing",
                    "INFORMATION": "Information",
                    "FINANCE": "Finance and Insurance",
                    "REAL_ESTATE": "Real Estate and Rental and Leasing",
                    "PROFESSIONAL":
                    "Professional, Scientific, and Technical Services",
                    "MANAGEMENT": "Management of Companies and Enterprises",
                    "EDUCATION": "Educational Services",
                    "HEALTH": "Health Care and Social Assistance",
                    "ART": "Arts, Entertainment, and Recreation",
                    "FOOD": "Accommodation and Food Services",
                    "PUBLIC": "Public Administration",
                    "WASTE": "Waste Management and Remediation Services"
                }
            },
            "employment_position": {
                "choices": {
                    "ACCOUNTANT": "Accountant/CPA/Bookkeeper/Controller",
                    "ACTUARY": "Actuary",
                    "ADJUSTER": "Adjuster",
                    "ADMINISTRATOR": "Administrator",
                    "ADVERTISER": "Advertiser/Marketer/PR Professional",
                    "AGENT": "Agent",
                    "ATC": "Air Traffic Controller",
                    "AMBASSADOR": "Ambassador/Consulate Professional",
                    "ANALYST": "Analyst",
                    "APPRAISER": "Appraiser",
                    "ARCHITECT": "Architect/Designer",
                    "ARTIST": "Artist/Performer/Actor/Dancer",
                    "ASSISTANT": "Assistant",
                    "ATHLETE": "Athlete",
                    "ATTENDANT": "Attendant",
                    "ATTORNEY": "Attorney/Judge/Legal Professional",
                    "AUCTIONEER": "Auctioneer",
                    "AUDITOR": "Auditor",
                    "BARBER": "Barber/Beautician/Hairstylist",
                    "BROKER": "Broker",
                    "BUSINESS_EXEC": "Business Executive (VP, Director, etc.)",
                    "BUSINESS_OWNER": "Business Owner",
                    "CAREGIVER": "Caregiver",
                    "CARPENTER": "Carpenter/Construction Worker",
                    "CASHIER": "Cashier",
                    "CHEF": "Chef/Cook",
                    "CHIROPRACTOR": "Chiropractor",
                    "CIVIL": "Civil Servant",
                    "CLERGY": "Clergy",
                    "CLERK": "Clerk",
                    "COMPLIANCE": "Compliance/Regulatory Professional",
                    "CONSULTANT": "Consultant",
                    "CONTRACTOR": "Contractor",
                    "COUNSELOR": "Counselor/Therapist",
                    "CUSTOMER_SERVICE": "Customer Service Representative",
                    "DEALER": "Dealer",
                    "DEVELOPER": "Developer",
                    "DISTRIBUTOR": "Distributor",
                    "DOCTOR": "Doctor/Dentist/Veterinarian/Surgeon",
                    "DRIVER": "Driver",
                    "ENGINEER": "Engineer",
                    "EXAMINER": "Examiner",
                    "EXTERMINATOR": "Exterminator",
                    "FACTORY": "Factory/Warehouse Worker",
                    "FARMER": "Farmer/Rancher",
                    "FINANCIAL": "Financial Planner",
                    "FISHERMAN": "Fisherman",
                    "FLIGHT": "Flight Attendant",
                    "HR": "Human Resources Professional",
                    "IMPEX": "Importer/Exporter",
                    "INSPECTOR": "Inspector/Investigator",
                    "INTERN": "Intern",
                    "INVESTMENT": "Investment Advisor/Investment Manager",
                    "INVESTOR": "Investor",
                    "IT": "IT Professional/IT Associate",
                    "JANITOR": "Janitor",
                    "JEWELER": "Jeweler",
                    "LABORER": "Laborer",
                    "LANDSCAPER": "Landscaper",
                    "LENDING": "Lending Professional",
                    "MANAGER": "Manager",
                    "MECHANIC": "Mechanic",
                    "MILITARY": "Military, Officer or Associated",
                    "MORTICIAN": "Mortician/Funeral Director",
                    "NURSE": "Nurse",
                    "NUTRITIONIST": "Nutritionist",
                    "OFFICE": "Office Associate",
                    "PHARMACIST": "Pharmacist",
                    "PHYSICAL": "Physical Therapist",
                    "PILOT": "Pilot",
                    "POLICE":
                    "Police Officer/Firefighter/Law Enforcement Professional",
                    "POLITICIAN": "Politician",
                    "PM": "Project Manager",
                    "REP": "Registered Rep",
                    "RESEARCHER": "Researcher",
                    "SAILOR": "Sailor/Seaman",
                    "SALES": "Salesperson",
                    "SCIENTIST": "Scientist",
                    "SEAMSTRESS": "Seamstress/Tailor",
                    "SECURITY": "Security Guard",
                    "SOCIAL": "Social Worker",
                    "TEACHER": "Teacher/Professor",
                    "TECHNICIAN": "Technician",
                    "TELLER": "Teller",
                    "TRADESPERSON": "Tradesperson/Craftsperson",
                    "TRAINER": "Trainer/Instructor",
                    "TRANSPORTER": "Transporter",
                    "UNDERWRITER": "Underwriter",
                    "WRITER": "Writer/Journalist/Editor"
                }
            },
            "investor_profile_experience": {
                "required": True,
                "choices": {
                    "NONE": "None",
                    "YRS_1_2": "1–2 years",
                    "YRS_3_5": "3–5 years",
                    "YRS_5_10": "5–10 years",
                    "YRS_10_": "10+ years"
                }
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
                "required": True,
                "choices": {
                    "LOW": "Low Risk",
                    "MODERATE": "Moderate Risk",
                    "SPECULATION": "Speculative Risk",
                    "HIGH": "High Risk"
                }
            },
            "investor_profile_objectives": {
                "required": True,
                "choices": {
                    "LONG_TERM": "Long–term buy & hold investing",
                    "INFREQUENT":
                    "Trading infrequently when I see an opportunity",
                    "FREQUENT": "Frequent trader, depending on the market",
                    "ACTIVE_DAILY": "Active trader, daily trader",
                    "NEW": "New to investing"
                }
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
                "required": True,
                "choices": {
                    "SSN": "Social Security Number",
                    "TIN": "Tax Identification Number",
                    "other": "Other"
                }
            },
            "citizenship": {
                "required": True,
                "placeholder": "USA",
                "choices": country_choices
            },
            "gender": {
                "placeholder": gender_placeholder,
                "required": False,
                "choices": {
                    "Male": "Male",
                    "Female": "Female"
                }
            },
            "marital_status": {
                "required": True,
                "choices": {
                    "SINGLE": "Single",
                    "DIVORCED": "Divorced",
                    "MARRIED": "Married",
                    "WIDOWED": "Widowed",
                    "PARTNER": "Domestic Partner"
                }
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
