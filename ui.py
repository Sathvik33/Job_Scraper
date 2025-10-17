import streamlit as st
import pandas as pd
import os
from datetime import datetime
import plotly.express as px

from scraper import run_scraper_with_config, OUTPUT_FILE

# --- JOB_TITLES dictionary now lives here to populate the UI ---
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
if 'jobs_data' not in st.session_state:
    st.session_state.jobs_data = None
if 'scraping_complete' not in st.session_state:
    st.session_state.scraping_complete = False
if 'last_scrape_time' not in st.session_state:
    st.session_state.last_scrape_time = None
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False

# Header
st.title("💼 Smart Job Scraper Dashboard")
st.markdown("<h3 class='white-title'>Discover your next opportunity with AI-powered job extraction</h3>", unsafe_allow_html=True)

# Sidebar - Configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Job Titles Selection with category grouping
    st.subheader("🎯 Job Titles")
    
    # Create expandable sections for each job category
    selected_titles = []
    
    with st.expander("💼 Full-time Roles", expanded=True):
        full_time_selected = st.multiselect(
            "Select full-time roles",
            options=JOB_TITLES['Full-time'],
            default=["Software Engineer", "Data Scientist", "Full Stack Developer"],
            key="full_time"
        )
        selected_titles.extend(full_time_selected)
    
    with st.expander("🎓 Internship Roles", expanded=True):
        internship_selected = st.multiselect(
            "Select internship roles", 
            options=JOB_TITLES['Internship'],
            default=["Software Engineer Intern", "Data Science Intern"],
            key="internship"
        )
        selected_titles.extend(internship_selected)
    
    with st.expander("📝 Contract Roles", expanded=False):
        contract_selected = st.multiselect(
            "Select contract roles",
            options=JOB_TITLES['Contract'],
            default=[],
            key="contract"
        )
        selected_titles.extend(contract_selected)

    # Show selected count
    st.success(f"✅ {len(selected_titles)} job titles selected")

    st.markdown("---")

    st.subheader("💼 Job Types")
    job_types_options = ["Full-time", "Part-time", "Internship", "Contract", "Freelance", "Remote", "Hybrid", "Onsite"]
    job_types_selected = st.multiselect(
        "Select job types (optional)",
        options=job_types_options,
        default=["Full-time", "Remote", "Hybrid"]
    )

    st.markdown("---")

    st.subheader("📍 Locations")
    locations = st.multiselect(
        "Select locations", 
        ["India", "United States", "United Kingdom", "Canada", "Singapore", "Australia", "Germany", "France"],
        default=["India", "United States"]
    )

    st.markdown("---")

    st.subheader("🌐 Platforms")
    platforms = st.multiselect(
        "Select job platforms", 
        ["linkedin", "indeed", "naukri", "glassdoor"],
        default=["linkedin", "indeed", "naukri"]
    )

    st.markdown("---")

    # Experience Level Filter
    st.subheader("🎚️ Experience Level")
    experience_filter = st.multiselect(
        "Filter by experience (optional)",
        options=["Entry Level (0-2 years)", "Mid Level (3-5 years)", "Senior Level (5+ years)", "All Levels"],
        default=["All Levels"]
    )

    st.markdown("---")
    
    # Start Scraping Button
    if st.button("🚀 Start Scraping", use_container_width=True, type="primary"):
        if not selected_titles:
            st.warning("⚠️ Please select at least one Job Title to start scraping.")
        else:
            with st.spinner("🔄 Scraping jobs... This may take a few minutes."):
                st.session_state.jobs_data = None
                st.session_state.data_loaded = False

                user_config = {
                    'job_titles': selected_titles,
                    'job_types': job_types_selected,
                    'locations': locations if locations else ['India'],
                    'platforms': platforms if platforms else ['linkedin'],
                    'experience_level': experience_filter
                    # 'scraping_intensity' key is now removed
                }

                try:
                    # Show progress
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    status_text.text("🔄 Initializing scraper...")
                    progress_bar.progress(10)
                    
                    # Run the scraper
                    result = run_scraper_with_config(user_config)
                    
                    status_text.text("✅ Scraping completed! Loading data...")
                    progress_bar.progress(90)
                    
                    st.session_state.scraping_complete = True
                    st.session_state.last_scrape_time = datetime.now()

                    if os.path.exists(OUTPUT_FILE):
                        st.session_state.jobs_data = pd.read_csv(OUTPUT_FILE)
                        st.session_state.data_loaded = True
                        
                        progress_bar.progress(100)
                        status_text.text("")
                        
                        st.success(f"✅ Successfully scraped {len(st.session_state.jobs_data)} jobs!")
                        st.rerun()
                    else:
                        st.error("❌ No data file found after scraping")
                except Exception as e:
                    st.error(f"❌ Error during scraping: {str(e)}")
                    import traceback
                    st.error(traceback.format_exc())

# Main content area
if st.session_state.scraping_complete and st.session_state.last_scrape_time:
    st.info(f"🕒 Last scraped: {st.session_state.last_scrape_time.strftime('%Y-%m-%d %H:%M:%S')}")

if st.session_state.jobs_data is not None and not st.session_state.jobs_data.empty:
    df = st.session_state.jobs_data.copy()

    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 Total Jobs Found", len(df))
    with col2:
        st.metric("🏢 Unique Companies", df[df['Company'] != 'Not Specified']['Company'].nunique())
    with col3:
        st.metric("📍 Locations Found", df[df['Location'] != 'Not Specified']['Location'].nunique())
    with col4:
        full_time_count = len(df[df['Job Type'].str.contains('Full-time', case=False, na=False)])
        st.metric("💼 Full-time Jobs", full_time_count)

    st.markdown("<br>", unsafe_allow_html=True)

    # Data Quality Overview
    st.markdown("""
        <div style='background: white; padding: 1.5rem; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 2rem;'>
            <h3 style='color: #1f2937 !important; margin-top: 0;'>📈 Data Quality Overview</h3>
        </div>
    """, unsafe_allow_html=True)

    # Quality metrics
    quality_col1, quality_col2, quality_col3, quality_col4 = st.columns(4)
    
    with quality_col1:
        title_completeness = (df[df['Job Title'] != 'Not Specified']['Job Title'].count() / len(df)) * 100
        st.metric("Job Title Quality", f"{title_completeness:.1f}%")
    
    with quality_col2:
        company_completeness = (df[df['Company'] != 'Not Specified']['Company'].count() / len(df)) * 100
        st.metric("Company Quality", f"{company_completeness:.1f}%")
    
    with quality_col3:
        exp_completeness = (df[df['Experience'] != '0']['Experience'].count() / len(df)) * 100
        st.metric("Experience Extracted", f"{exp_completeness:.1f}%")
    
    with quality_col4:
        location_completeness = (df[df['Location'] != 'Not Specified']['Location'].count() / len(df)) * 100
        st.metric("Location Quality", f"{location_completeness:.1f}%")

    st.markdown("""
        <div style='background: white; padding: 1.5rem; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 2rem;'>
            <h3 style='color: #1f2937 !important; margin-top: 0;'>🔍 Filter Results</h3>
        </div>
    """, unsafe_allow_html=True)

    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        min_experience = st.number_input("Min Experience (Years)", min_value=0, max_value=50, value=0)
    with filter_col2:
        max_experience = st.number_input("Max Experience (Years)", min_value=0, max_value=50, value=50)
    with filter_col3:
        # Company filter
        companies = ['All Companies'] + sorted(df[df['Company'] != 'Not Specified']['Company'].unique().tolist())
        selected_company = st.selectbox("Filter by Company", companies)

    filtered_df = df.copy()

    def parse_experience(exp_str):
        if pd.isna(exp_str):
            return 0
        exp_str = str(exp_str).strip()
        if '-' in exp_str:
            try: return int(exp_str.split('-')[0])
            except: return 0
        if '+' in exp_str:
            try: return int(exp_str.replace('+', ''))
            except: return 0
        try: return int(float(exp_str))
        except: return 0

    filtered_df['Experience_Min'] = filtered_df['Experience'].apply(parse_experience)

    if min_experience > max_experience:
        st.warning("⚠️ Min experience cannot be greater than max experience.")
        min_experience, max_experience = max_experience, min_experience

    filtered_df = filtered_df[
        (filtered_df['Experience_Min'] >= min_experience) &
        (filtered_df['Experience_Min'] <= max_experience)
    ]
    
    # Apply company filter
    if selected_company != 'All Companies':
        filtered_df = filtered_df[filtered_df['Company'] == selected_company]

    if not filtered_df.empty:
        # Visualizations
        viz_col1, viz_col2 = st.columns(2)
        with viz_col1:
            job_type_counts = filtered_df['Job Type'].value_counts()
            fig1 = px.pie(values=job_type_counts.values, names=job_type_counts.index,
                          title="Job Type Distribution",
                          color_discrete_sequence=px.colors.qualitative.Set3)
            fig1.update_layout(plot_bgcolor='rgba(255,255,255,0.9)',
                               paper_bgcolor='rgba(255,255,255,0.9)',
                               font=dict(size=12),
                               title_font=dict(size=16, color='#1f2937', family='Arial Black'))
            st.plotly_chart(fig1, use_container_width=True)

        with viz_col2:
            top_companies = filtered_df[filtered_df['Company'] != 'Not Specified']['Company'].value_counts().head(10)
            fig2 = px.bar(x=top_companies.values, y=top_companies.index,
                          orientation='h',
                          title="Top 10 Companies Hiring",
                          labels={'x': 'Number of Jobs', 'y': 'Company'},
                          color=top_companies.values,
                          color_continuous_scale='Viridis')
            fig2.update_layout(plot_bgcolor='rgba(255,255,255,0.9)',
                               paper_bgcolor='rgba(255,255,255,0.9)',
                               showlegend=False,
                               font=dict(size=12),
                               title_font=dict(size=16, color='#1f2937', family='Arial Black'))
            st.plotly_chart(fig2, use_container_width=True)

        st.markdown("<br>", unsafe_allow_html=True)
        exp_col1, exp_col2 = st.columns(2)

        with exp_col1:
            exp_counts = filtered_df['Experience'].value_counts().head(10)
            fig3 = px.bar(x=exp_counts.index, y=exp_counts.values,
                          title="Experience Level Distribution",
                          labels={'x': 'Experience', 'y': 'Number of Jobs'},
                          color=exp_counts.values,
                          color_continuous_scale='Blues')
            fig3.update_layout(plot_bgcolor='rgba(255,255,255,0.9)',
                               paper_bgcolor='rgba(255,255,255,0.9)',
                               showlegend=False,
                               font=dict(size=12),
                               title_font=dict(size=16, color='#1f2937', family='Arial Black'))
            st.plotly_chart(fig3, use_container_width=True)

        with exp_col2:
            location_counts = filtered_df[filtered_df['Location'] != 'Not Specified']['Location'].value_counts().head(10)
            fig4 = px.bar(x=location_counts.values, y=location_counts.index,
                          orientation='h',
                          title="Top 10 Locations",
                          labels={'x': 'Number of Jobs', 'y': 'Location'},
                          color=location_counts.values,
                          color_continuous_scale='Greens')
            fig4.update_layout(plot_bgcolor='rgba(255,255,255,0.9)',
                               paper_bgcolor='rgba(255,255,255,0.9)',
                               showlegend=False,
                               font=dict(size=12),
                               title_font=dict(size=16, color='#1f2937', family='Arial Black'))
            st.plotly_chart(fig4, use_container_width=True)

        # Job Listings Table
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
        
        # Download buttons
        col1, col2 = st.columns(2)
        with col1:
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="⬇️ Download Filtered Data as CSV",
                data=csv,
                file_name=f"filtered_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        with col2:
            full_csv = df.to_csv(index=False)
            st.download_button(
                label="⬇️ Download Full Dataset as CSV",
                data=full_csv,
                file_name=f"all_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
    else:
        st.warning("⚠️ No jobs match the current filter criteria.")

else:
    st.markdown("""
        <div style='background: rgba(255, 255, 255, 0.95); padding: 3rem; border-radius: 15px;
                     box-shadow: 0 10px 25px rgba(0,0,0,0.1); text-align: center; margin-top: 2rem;'>
            <h2 style='color: #1f2937; margin-bottom: 1rem;'>👋 Welcome to the Job Scraper Dashboard!</h2>
            <p style='color: #6b7280; font-size: 1.2rem; line-height: 1.8;'>
                Configure your targeted job search in the sidebar.<br>
                Select job titles, set your preferences, and click <strong>"Start Scraping"</strong> to begin!<br><br>
                <strong>✨ Features:</strong><br>
                • Unlimited job title selection<br>
                • Multiple platforms (LinkedIn, Indeed, Naukri)<br>
                • Smart experience extraction<br>
                • Interactive filters and visualizations<br>
                • Export to CSV
            </p>
        </div>
    """, unsafe_allow_html=True)