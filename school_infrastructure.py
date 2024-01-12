import streamlit as st
import pandas as pd
from streamlit_option_menu import option_menu

### Set page width to occupy the whole screen
st.set_page_config(layout="wide") 

##### initialize connection to snowflake #####
connection = st.connection("snowflake")

##### load infrastructure dimension / fact to pandas
@st.cache_data
def load_data():
    session = connection.session()
    dim_table = session.table("integration.dim_location").to_pandas()
    dim_fact = session.table("integration.fact_school_infrastructure").to_pandas()
    dim_fact_teacher = session.table("integration.fact_school_teachers").to_pandas()
    dim_qualification = session.table("integration.dim_qualification").to_pandas()
    src_drouput_rate = session.table("staging.src_school_dropout_rate").to_pandas()
    infra_report = session.table("presentation.infrastructure_overall").to_pandas()
    infra_cat = session.table("presentation.infrastructure_category").to_pandas()
    infra_locality = session.table("presentation.infrastructure_locality").to_pandas()
    enrolment_overall = session.table("presentation.enrolment_overall").to_pandas()
    enrolment_caste = session.table("presentation.enrolment_caste").to_pandas()
    teacher_student_ratio = session.table("presentation.student_teacher_ratio").to_pandas()
    return dim_table,dim_fact,dim_fact_teacher,dim_qualification,src_drouput_rate,infra_report,infra_cat,infra_locality,enrolment_overall,enrolment_caste,teacher_student_ratio

df_dim_location,df_fact_infra,df_fact_teacher,df_qualification,df_drouput_rate,df_infra_report,df_infra_cat,df_infra_locality,df_enrolment_overall,df_enrolment_caste,df_teacher_student_ratio = load_data()


####join fact and dimension tables
df_merge_infra = df_fact_infra.set_index('LOCATION_ID').join(df_dim_location.set_index('LOCATION_ID'),lsuffix='_fct',rsuffix='_dim')
df_merge_teacher = df_fact_teacher.set_index('LOCATION_ID').join(df_dim_location.set_index('LOCATION_ID'),lsuffix='_fct',rsuffix='_dim')
df_merge_teacher_qualific = df_fact_teacher.set_index('QUALIFICATION_ID').join(df_qualification.set_index('QUALIFICATION_ID'),lsuffix='_fct',rsuffix='_dim')

#### Options tab io the landing page
with st.sidebar:
    selected = option_menu(
    menu_title = "Main Menu",
    options = ["Overall","Infrastructure","Enrolment","Dropout","Teacher","Developer Note"],
    icons = ["1-circle","2-circle","3-circle","4-circle","5-circle","6-circle"],
    menu_icon = "bar-chart-line",
    default_index = 0,
    styles="blue"
    )

#### Calculate the fact aggregations


#### State selector for infrastructure tab
selected_state = st.sidebar.selectbox('Select a State', df_merge_infra['STATE_NAME'].drop_duplicates().sort_values(),help="Only works with infrastructure tab")
df_selected = df_merge_infra[df_merge_infra['STATE_NAME']==selected_state]


#### Aggregation for Infrastructure across India
df_agg_medical =  df_selected[['STATE_NAME','TOTAL_NUMBER_OF_SCHOOLS','COMPLETE_MEDICAL_CHECKUP']].groupby("STATE_NAME").sum()
df_agg_computer =  df_selected[['STATE_NAME','TOTAL_NUMBER_OF_SCHOOLS','COMPUTER_AVAILABLE']].groupby("STATE_NAME").sum()
df_agg_toilets =  df_selected[['STATE_NAME','TOTAL_NUMBER_OF_SCHOOLS','FUNCTIONAL_TOILET_FACILITY']].groupby("STATE_NAME").sum()
df_agg_water =  df_selected[['STATE_NAME','TOTAL_NUMBER_OF_SCHOOLS','FUNCTIONAL_DRINKING_WATER']].groupby("STATE_NAME").sum()
df_agg_ramps =  df_selected[['STATE_NAME','TOTAL_NUMBER_OF_SCHOOLS','RAMPS']].groupby("STATE_NAME").sum()
df_agg_ground =  df_selected[['STATE_NAME','TOTAL_NUMBER_OF_SCHOOLS','PLAYGROUND']].groupby("STATE_NAME").sum()
df_agg_internet =  df_selected[['STATE_NAME','TOTAL_NUMBER_OF_SCHOOLS','INTERNET']].groupby("STATE_NAME").sum()
df_agg_library =  df_selected[['STATE_NAME','TOTAL_NUMBER_OF_SCHOOLS','LIBRARY_OR_READING_CORNER_OR_BOOK_BANK']].groupby("STATE_NAME").sum()
df_agg_handwash =  df_selected[['STATE_NAME','TOTAL_NUMBER_OF_SCHOOLS','HANDWASH']].groupby("STATE_NAME").sum()
df_agg_waterpurifier =  df_selected[['STATE_NAME','TOTAL_NUMBER_OF_SCHOOLS','WATER_PURIFIER']].groupby("STATE_NAME").sum()


### Aggregation for Teache data aceoss India
df_agg_teacher =  df_merge_teacher[['STATE_NAME','TOTAL_TEACHER','TOTAL_MALE','TOTAL_FEMALE']].groupby("STATE_NAME").sum()
df_agg_teacher_all =  df_merge_teacher[['TOTAL_TEACHER','TOTAL_MALE','TOTAL_FEMALE']].sum()
df_agg_teacher_qualification =  df_merge_teacher_qualific[['PROFESSIONAL_QUALIFICATION_NAME','TOTAL_TEACHER']].groupby("PROFESSIONAL_QUALIFICATION_NAME").sum()


#### Dropout Rate Aggregations
df_agg_dropout_rate =  df_drouput_rate[['STATE_UTS','TOTAL_2019_20','TOTAL_2018_19','TOTAL_2017_18','TOTAL_2016_17','TOTAL_2015_16']].groupby("STATE_UTS").sum()


### Overall tab Infrastructure aggregations
df_agg_infra_report = df_infra_report[['STATE_NAME','ALL_FACILITIES_AVG']].groupby("STATE_NAME").sum().sort_values(by='ALL_FACILITIES_AVG')
df_agg_infra_cat_report = df_infra_cat[['CATEGORY','ALL_FACILITIES_AVG']].groupby("CATEGORY").sum().sort_values(by='ALL_FACILITIES_AVG')
df_agg_infra_loc =  df_infra_locality[['LOCALITY','Toilets','Water','Electricity','Medical Checkup','Playground','Computer','Internet']].groupby("LOCALITY").sum()


## Enrolment Aggregations across India
df_aff_enrol_loc = df_enrolment_overall[['STATE','TOTAL_BOYS','TOTAL_GIRLS']].groupby('STATE').sum()
df_aff_enrol_caste = df_enrolment_caste[['CASTE','TOTAL_BOYS','TOTAL_GIRLS']].groupby('CASTE').sum()

## Overall page, teacher student ratio
df_agg_teacher_student_ratio =  df_teacher_student_ratio[['STATE','TOTAL_ENROLMENT','TOTAL_TEACHER']].groupby("STATE").sum()


### Dashboard Layout

if selected == "Infrastructure":
    st.header(selected_state + "'s Infrastructure details 2019 ",divider='rainbow')
    # Create a row layout
    c1, c2,c3,c4,c5= st.columns(5)
    c6, c7,c8,c9,c10= st.columns(5)
    with c1:
        st.bar_chart(df_agg_medical)
           
    with c2:
        st.bar_chart(df_agg_computer)

    with c3:
        st.bar_chart(df_agg_toilets)

    with c4:
        st.bar_chart(df_agg_water)

    with c5:
        st.bar_chart(df_agg_ramps)

    with c6:
        st.bar_chart(df_agg_ground)
           
    with c7:
        st.bar_chart(df_agg_internet)

    with c8:
        st.bar_chart(df_agg_library)

    with c9:
        st.bar_chart(df_agg_handwash)

    with c10:
        st.bar_chart(df_agg_waterpurifier)
    st.write("Using the above metrics, we can identify the facilities which needs immediate attention for the selected state."
             +" Example Tripura has 726 Schools but it has only 7 water purifiers installed which needs immediate attention")


if selected == "Teacher":
    st.header("Teachers Details Across India 2019",divider='rainbow')
    c1, c2 = st.columns(2)
    with c1:
        st.bar_chart(df_agg_teacher)
    with c2:
        st.write("__Teacher Gender Insights__")
        st.write("1. Male to Female ratio is uniform across India")
        st.write("2. Maharastra has the highest number of teachers in 2019")
        st.write("3. North eastern states has less number of teachers to rest")
    
    c3,c4 =  st.columns(2)
    with c3:
        st.bar_chart(df_agg_teacher_qualification)
    with c4:
        st.write("__Teacher Qualification Insights__")
        st.write("1. B.E.D is the most common profession in teaching")
        st.write("2. 2nd most common qualification is Diploma in teaching")
        st.write("2. M.E.D qualified teachers are very less when compared to B.E.D")


if selected == "Dropout":
    st.header("Droupout Rate Year-on-Year across India",divider='rainbow')
    st.area_chart(df_agg_dropout_rate)
    st.write("As per the above chart, we could see that, in few states there is no significant action taken and the dropout from previous year to current year is constant"
             +". Example: Arunachal Pradesh, Tripura, Bihar")

if selected == "Overall":
    st.header("Education Insights Across India 2019",divider='rainbow')
    c1,c2 = st.columns(2)
    with c1:
        
        st.bar_chart(df_agg_infra_report)
    with c2:
        st.write("__State-wise School Infrastructure Insights__")
        st.write("1. Most of north-eastern states lack basic facilities!" )
        st.write("2. Puducherry UT has 100% of most of the basic facilities" )
        st.write("3. Meghalaya is at the bottom with only 37% basic facilities")
        st.write("** _Please visit Infrastructure Tab for state wise insights_ **")

    c3,c4 = st.columns(2)
    with c3:
        st.bar_chart(df_agg_infra_cat_report)
    with c4:
        st.write("__Category-wise School Infrastructure Insights__")
        st.write("1. Ministry of Labor schools has only 27% facilities" )
        st.write("2. Jawahar Navodaya Vidyala (School for talented students from rural areas) has 90% facilities" )
        st.write("3. Private Schools has 25% more facilites then government schools ")

    c5,c6 = st.columns(2)
    with c5:
        st.line_chart(df_agg_infra_loc)
    with c6:
        st.write("__Locality-wise School Infrastructure Insights__")
        st.write("1. Rural School has less access to computer & internet" )
        st.write("2. Urban schools has only 35% of medical checkup facilities where as rural schools has 54%" )
        st.write("3. Rest all facilities are equally distributed across localities ")

    c7,c8 = st.columns(2)
    with c7:
        st.scatter_chart(df_agg_teacher_student_ratio)
    with c8:
        st.write("__Teacher Student Ratio Insights__")
        st.write("1. Across India only few states have atleast 1 teacher per 100 students" )
        st.write("2. Meghalaya tops the table with 6 teachers per 100 students" )
        st.write("*** _This is a indicative figure due to incomplete data_ ***")

if selected == "Enrolment":
    st.header("Enrolment Insights Across India 2019",divider='rainbow')
    c1,c2 = st.columns(2)
    with c1:
        
        st.line_chart(df_aff_enrol_loc)
    with c2:
        st.write("__State-wise School Enrolment Insights__")
        st.write("1. Boys VS Girls Enrolment is uniform across India" )
        st.write("2. UP has highest number of enrolment in 2019 year" )
        st.write("3. North eastern states has less enrolment which is < 25K ")
    
    c3,c4 = st.columns(2)
    with c3:
        
        st.bar_chart(df_aff_enrol_caste)
    with c4:
        st.write("__Caste-wise School Enrolment Insights__")
        st.write("1. SC & ST enrolment is less when compared to rest")
        st.write("2. OBC has the highest number of enrolment in 2019" )
        st.write("3. Boys VS Girls enrolment is uniform across caste ")
    
if selected == "Developer Note":
        st.snow()
        st.header("Thank you")
        st.write("Name: Srinivas Sukka")
        st.write("Organization: UST Global")
        st.write("Linkedin: https://www.linkedin.com/in/srinivas-sukka-60a83a4b")