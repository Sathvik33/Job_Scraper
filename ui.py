import streamlit as st
import pandas as pd
import os
from datetime import datetime
import plotly.express as px

from scraper import run_scraper_with_config, OUTPUT_FILE

# --- NEW: JOB_TITLES dictionary now lives here to populate the UI ---
JOB_TITLES = {
    'Full-time': [
        'Software Engineer', 'Data Scientist', 'Project Manager', 'Cloud Engineer', 'Web Developer',
        'Systems Analyst', 'Cybersecurity Specialist', 'IT Security Specialist', 'Network Administrator',
        'Backend Developer', 'Frontend Developer', 'Full Stack Developer',
        'Java Developer', 'Python Developer', 'JavaScript Developer', 'React Developer',
        'Node.js Developer', 'DevOps Engineer', 'AI Engineer', 'Mobile Developer',
        'Mobile App Developer', 'Quality Assurance Engineer', 'QA Analyst',
        'Site Reliability Engineer', 'Technical Support Engineer',
        'Database Administrator', 'Database Architect','Cloud Architect'
    ],
    'Internship': [
        'Software Engineer Intern', 'Software Development Intern', 'Backend Developer Intern',
        'Frontend Developer Intern', 'Data Science Intern', 'Machine Learning Intern',
        'Junior Software Developer', 'Entry-Level Software Engineer', 'Trainee Software Developer',
        'Junior Programmer', 'Software Engineer Summer Intern','Data Analyst Intern','AI Intern'
    ],
    'Contract': [
        'Freelance Developer', 'Contract Analyst', 'IT Consultant', 'Technical Writer',
        'Contract Software Developer', 'Freelance Web Developer', 'Temporary IT Support'
    ],
}

# Page configuration
st.set_page_config(
    page_title="Job Scraper Dashboard",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern styling
st.markdown("""
    <style>
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
    }
    .stApp {
        background: transparent;
    }
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
        color: #1f2937;
        font-weight: 700;
    }
    div[data-testid="stMetricLabel"] {
        font-size: 1rem;
        color: #6b7280;
        font-weight: 500;
    }
    .css-1d391kg {
        background-color: white;
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    h1 {
        color: white;
        font-weight: 800;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
    }
    h2, h3 {
        color: white;
        font-weight: 700;
    }
    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 0.75rem 2rem;
        font-size: 1.1rem;
        font-weight: 600;
        border-radius: 8px;
        transition: all 0.3s ease;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15);
    }
    .sidebar .sidebar-content {
        background: rgba(255, 255, 255, 0.95);
        border-radius: 10px;
    }
    div[data-baseweb="select"] {
        background-color: white;
        border-radius: 8px;
    }
    .stMarkdown h3 { color: #1f2937 !important; }
    .white-title { color: white !important; }
    .stMarkdown table { color: #1f2937; width: 100%; border-collapse: collapse; }
    .stMarkdown th { background-color: #f2f2f2; font-weight: bold; text-align: left; padding: 10px; border-bottom: 2px solid #ddd; }
    .stMarkdown td { text-align: left; padding: 10px; border-bottom: 1px solid #eee; }
    .stMarkdown tr:hover { background-color: #f9f9f9; }
    .stMarkdown a { color: #667eea !important; font-weight: 500; text-decoration: none; }
    .stMarkdown a:hover { text-decoration: underline; }
    </style>
    """, unsafe_allow_html=True)

# Initialize session state
if 'jobs_data' not in st.session_state: st.session_state.jobs_data = None
if 'scraping_complete' not in st.session_state: st.session_state.scraping_complete = False
if 'last_scrape_time' not in st.session_state: st.session_state.last_scrape_time = None

# Header
st.title("💼 Smart Job Scraper Dashboard")
st.markdown("<h3 class='white-title'>Discover your next opportunity with AI-powered job extraction</h3>", unsafe_allow_html=True)

# Sidebar - Configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # --- MODIFIED: Changed from text_input to multiselect ---
    all_job_titles = sorted(list(set(title for sublist in JOB_TITLES.values() for title in sublist)))
    
    job_titles_selected = st.multiselect(
        "Job Titles to Scrape",
        options=all_job_titles,
        default=["Software Engineer", "Data Scientist"],
        help="Select one or more job titles to search for."
    )
    
    st.markdown("---")

    st.subheader("💼 Job Types")
    job_types_options = ["Full-time", "Part-time", "Internship", "Contract", "Freelance", "Remote", "Hybrid", "Onsite"]
    job_types_selected = st.multiselect(
        "Select job types (optional)",
        options=job_types_options,
        default=["Full-time", "Remote"]
    )
    
    st.markdown("---")
    
    st.subheader("📍 Locations")
    locations = st.multiselect("Select locations", ["India", "United States", "United Kingdom", "Canada", "Singapore", "Australia"], default=["India"])
    
    st.markdown("---")
    
    st.subheader("🌐 Platforms")
    platforms = st.multiselect("Select job platforms", ["linkedin", "indeed", "naukri"], default=["linkedin", "indeed"])
    
    st.markdown("---")

    max_jobs = st.number_input("Max Number of Jobs", min_value=10, max_value=2000, value=50, step=10)

    st.markdown("---")

    if st.button("🚀 Start Scraping", use_container_width=True):
        if not job_titles_selected:
            st.warning("Please select at least one Job Title to start scraping.")
        else:
            with st.spinner("🔄 Scraping jobs... This may take a few minutes"):
                user_config = {
                    'job_titles': job_titles_selected,
                    'job_types': job_types_selected,
                    'locations': locations if locations else ['India'],
                    'platforms': platforms if platforms else ['linkedin'],
                    'max_jobs': max_jobs
                }

                try:
                    run_scraper_with_config(user_config)
                    st.session_state.scraping_complete = True
                    st.session_state.last_scrape_time = datetime.now()

                    if os.path.exists(OUTPUT_FILE):
                        st.session_state.jobs_data = pd.read_csv(OUTPUT_FILE)
                        st.success(f"✅ Successfully scraped {len(st.session_state.jobs_data)} jobs!")
                        st.rerun()
                    else:
                        st.error("❌ No data file found after scraping")
                except Exception as e:
                    st.error(f"❌ Error during scraping: {str(e)}")

# Main content area
if st.session_state.scraping_complete and st.session_state.last_scrape_time:
    st.info(f"🕒 Last scraped: {st.session_state.last_scrape_time.strftime('%Y-%m-%d %H:%M:%S')}")

if st.session_state.jobs_data is None and os.path.exists(OUTPUT_FILE):
    try:
        st.session_state.jobs_data = pd.read_csv(OUTPUT_FILE)
    except pd.errors.EmptyDataError:
        st.session_state.jobs_data = pd.DataFrame() 
    except Exception: pass

# --- METRICS and RESULTS DISPLAY ---
if st.session_state.jobs_data is not None and not st.session_state.jobs_data.empty:
    df = st.session_state.jobs_data.copy()
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Jobs Found", len(df))
    with col2:
        st.metric("Unique Companies", df[df['Company'] != 'Not Specified']['Company'].nunique())
    with col3:
        st.metric("Locations Found", df[df['Location'] != 'Not Specified']['Location'].nunique())
    with col4:
        st.metric("Full-time Jobs", len(df[df['Job Type'].str.contains('Full-time', case=False, na=False)]))
    
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("""
        <div style='background: white; padding: 1.5rem; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 2rem;'>
            <h3 style='color: #1f2937 !important; margin-top: 0;'>🔍 Filter Results</h3>
        </div>
    """, unsafe_allow_html=True)

    filter_col1, filter_col2 = st.columns(2)
    with filter_col1:
        min_experience = st.number_input("Min Experience (Years)", min_value=0, max_value=50, value=0)
    with filter_col2:
        max_experience = st.number_input("Max Experience (Years)", min_value=0, max_value=50, value=50)

    # Apply experience filter
    filtered_df = df.copy()
    filtered_df['Experience_Min'] = filtered_df['Experience'].apply(
        lambda x: int(str(x).split('-')[0]) if '-' in str(x) else int(str(x)) if str(x).isdigit() else 0
    )
    if min_experience > max_experience:
        st.warning("Min experience cannot be greater than max experience.")
        min_experience, max_experience = max_experience, min_experience 
    
    filtered_df = filtered_df[
        (filtered_df['Experience_Min'] >= min_experience) &
        (filtered_df['Experience_Min'] <= max_experience)
    ]

    # Visualizations (now using filtered_df)
    if not filtered_df.empty:
        viz_col1, viz_col2 = st.columns(2)
        with viz_col1:
            job_type_counts = filtered_df['Job Type'].value_counts()
            fig1 = px.pie(values=job_type_counts.values, names=job_type_counts.index, title="Job Type Distribution", color_discrete_sequence=px.colors.qualitative.Set3)
            fig1.update_layout(plot_bgcolor='rgba(255,255,255,0.9)', paper_bgcolor='rgba(255,255,255,0.9)', font=dict(size=12), title_font=dict(size=16, color='#1f2937', family='Arial Black'))
            st.plotly_chart(fig1, use_container_width=True)
        with viz_col2:
            top_companies = filtered_df[filtered_df['Company'] != 'Not Specified']['Company'].value_counts().head(10)
            fig2 = px.bar(x=top_companies.values, y=top_companies.index, orientation='h', title="Top 10 Companies Hiring", labels={'x': 'Number of Jobs', 'y': 'Company'}, color=top_companies.values, color_continuous_scale='Viridis')
            fig2.update_layout(plot_bgcolor='rgba(255,255,255,0.9)', paper_bgcolor='rgba(255,255,255,0.9)', showlegend=False, font=dict(size=12), title_font=dict(size=16, color='#1f2937', family='Arial Black'))
            st.plotly_chart(fig2, use_container_width=True)

        # Job listings table (now using filtered_df)
        display_df = filtered_df[['Job Title', 'Company', 'Location', 'Job Type', 'Experience', 'Job Link']].copy()
        display_df['Job Link'] = display_df['Job Link'].apply(lambda x: f'<a href="{x}" target="_blank">View Job</a>' if pd.notna(x) else '')
        table_html = display_df.to_html(escape=False, index=False)
        st.markdown(f"""
            <div style='background: white; padding: 1.5rem; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-top: 2rem;'>
                <h3 style='color: #1f2937 !important; margin-top: 0;'>📋 Job Listings ({len(filtered_df)} shown)</h3>
                {table_html}
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        csv = filtered_df.to_csv(index=False)
        st.download_button(label="⬇️ Download Filtered Data as CSV", data=csv, file_name=f"filtered_jobs_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv", use_container_width=True)
    else:
        st.warning("No jobs match the current filter criteria.")

else:
    # Welcome screen
    st.markdown("""
        <div style='background: rgba(255, 255, 255, 0.95); padding: 3rem; border-radius: 15px;
                    box-shadow: 0 10px 25px rgba(0,0,0,0.1); text-align: center; margin-top: 2rem;'>
            <h2 style='color: #1f2937; margin-bottom: 1rem;'>👋 Welcome to the Job Scraper Dashboard!</h2>
            <p style='color: #6b7280; font-size: 1.2rem; line-height: 1.8;'>
                Configure your targeted job search in the sidebar.<br>
                Select job titles, add preferences, and click <strong>"Start Scraping"</strong>!
            </p>
            <div style='margin-top: 2rem;'>
                <span style='font-size: 3rem;'>🎯</span>
                <span style='font-size: 3rem; margin: 0 1rem;'>💼</span>
                <span style='font-size: 3rem;'>🚀</span>
            </div>
        </div>
    """, unsafe_allow_html=True)