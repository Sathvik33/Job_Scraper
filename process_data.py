import pandas as pd
import numpy as np
import os
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import DBSCAN, KMeans
from sklearn.pipeline import make_pipeline
from sklearn.decomposition import PCA
from collections import Counter

# Define paths
BASE_DIR = r'C:\Sathvik-py\Talrn\job_scraper'
DATA_DIR = os.path.join(BASE_DIR, 'Data')
INPUT_FILE = os.path.join(DATA_DIR, 'jobs_raw_data.csv')
OUTPUT_FILE = os.path.join(DATA_DIR, 'jobs_advanced.csv')

os.makedirs(DATA_DIR, exist_ok=True)

# --- ML Components ---

class ExperienceExtractor:
    @staticmethod
    def extract_experience(text):
        if not text or pd.isna(text):
            return 0
        
        text_lower = str(text).lower()
        
        # Experience patterns
        patterns = [
            (r'(\d+)\s*[-–to]+\s*(\d+)\s*(?:years?|yrs?)', 'range'),
            (r'(\d+)\s*\+\s*(?:years?|yrs?)', 'plus'),
            (r'(?:minimum|min|at least)\s*(\d+)\s*(?:years?|yrs?)', 'minimum'),
            (r'\b(\d+)\s*(?:years?|yrs?)\s*(?:of)?\s*(?:experience|exp)?', 'single'),
        ]
        
        for pattern, pattern_type in patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                if pattern_type == 'range':
                    nums = [int(n) for n in matches[0]]
                    return max(nums)  # Use max of range
                elif pattern_type == 'plus':
                    return int(matches[0])
                elif pattern_type == 'minimum':
                    return int(matches[0])
                else:
                    nums = [int(m) for m in matches if 0 < int(m) <= 20]
                    if nums:
                        return max(nums)
        
        # Check for fresher/entry level
        fresher_keywords = ['fresher', 'entry level', '0-1 year', 'graduate', 'no experience', 'junior']
        if any(word in text_lower for word in fresher_keywords):
            return 0
        
        # Check for senior level (estimate)
        senior_keywords = ['senior', 'lead', 'principal', 'staff', 'architect']
        if any(word in text_lower for word in senior_keywords):
            return 5  # Estimate 5+ years for senior roles
        
        return 0  # Default


class DynamicRoleExpander:
    """ML-powered dynamic role title expansion and classification"""
    
    def __init__(self):
        self.role_embeddings = {}
        self.discovered_roles = {}
        
    def expand_roles_with_ml(self, job_titles):
        """Use ML clustering to discover new role patterns"""
        print("  → Discovering role patterns with ML clustering...")
        
        if len(job_titles) < 2:
            return {}
        
        # Filter out None/NaN values
        job_titles = [str(title) for title in job_titles if pd.notna(title)]
        
        if len(job_titles) < 2:
            return {}
            
        vectorizer = TfidfVectorizer(max_features=100, stop_words='english')
        try:
            title_vectors = vectorizer.fit_transform(job_titles)
                
            if len(job_titles) > 10:
                n_components = min(10, len(job_titles)-1)
                pca = PCA(n_components=n_components)
                reduced_embeddings = pca.fit_transform(title_vectors.toarray())
            else:
                reduced_embeddings = title_vectors.toarray()
        except Exception as e:
            print(f"    Clustering failed: {e}. Using simple grouping.")
            return self._simple_role_grouping(job_titles)
        
        # Apply clustering
        if len(job_titles) > 5:
            try:
                clustering = DBSCAN(eps=0.7, min_samples=2)
                cluster_labels = clustering.fit_predict(reduced_embeddings)
            except:
                n_clusters = min(5, len(job_titles))
                clustering = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                cluster_labels = clustering.fit_predict(reduced_embeddings)
        else:
            cluster_labels = np.zeros(len(job_titles), dtype=int)
        
        # Analyze discovered clusters
        discovered_roles = {}
        for cluster_id in set(cluster_labels):
            if cluster_id != -1:
                cluster_titles = [job_titles[i] for i in range(len(job_titles)) if cluster_labels[i] == cluster_id]
                if cluster_titles and len(cluster_titles) > 1:
                    role_name = self._analyze_cluster_patterns(cluster_titles)
                    discovered_roles[cluster_id] = {
                        'role_name': role_name,
                        'titles': cluster_titles[:5],
                        'size': len(cluster_titles)
                    }
        
        self.discovered_roles = discovered_roles
        return discovered_roles
    
    def _simple_role_grouping(self, job_titles):
        """Fallback: Simple keyword-based role grouping"""
        role_groups = {}
        
        for title in job_titles:
            if not title:
                continue
                
            title_lower = str(title).lower()
            
            if 'data' in title_lower and 'scientist' in title_lower:
                group = 'data_scientist'
            elif 'machine learning' in title_lower or 'ml engineer' in title_lower or 'ai' in title_lower:
                group = 'ml_ai'
            elif 'frontend' in title_lower or 'front-end' in title_lower or 'react' in title_lower:
                group = 'frontend'
            elif 'backend' in title_lower or 'back-end' in title_lower:
                group = 'backend'
            elif 'devops' in title_lower or 'sre' in title_lower:
                group = 'devops'
            elif 'full stack' in title_lower or 'fullstack' in title_lower:
                group = 'fullstack'
            elif 'mobile' in title_lower or 'android' in title_lower or 'ios' in title_lower:
                group = 'mobile'
            elif 'data engineer' in title_lower:
                group = 'data_engineer'
            else:
                group = 'other'
                
            if group not in role_groups:
                role_groups[group] = []
            role_groups[group].append(title)
        
        discovered_roles = {}
        for i, (group, titles) in enumerate(role_groups.items()):
            if len(titles) > 1:
                discovered_roles[i] = {
                    'role_name': f"Discovered {group.replace('_', ' ').title()}",
                    'titles': titles[:5],
                    'size': len(titles)
                }
        
        return discovered_roles
    
    def _analyze_cluster_patterns(self, titles):
        """Analyze title patterns to name discovered roles"""
        common_patterns = {
            'Backend': ['backend', 'back-end', 'server', 'api', 'database'],
            'Frontend': ['frontend', 'front-end', 'ui', 'ux', 'react', 'angular', 'vue'],
            'Full Stack': ['full stack', 'full-stack', 'fullstack'],
            'DevOps': ['devops', 'sre', 'cloud', 'infrastructure', 'deployment'],
            'Data': ['data', 'analytics', 'analysis', 'bi', 'database'],
            'ML/AI': ['machine learning', 'ml', 'ai', 'artificial intelligence', 'deep learning'],
            'Mobile': ['mobile', 'android', 'ios', 'react native', 'flutter'],
            'QA': ['qa', 'test', 'quality', 'automation']
        }
        
        title_text = ' '.join([str(t) for t in titles]).lower()
        scores = {}
        
        for pattern, keywords in common_patterns.items():
            score = sum(1 for keyword in keywords if keyword in title_text)
            scores[pattern] = score
        
        if scores:
            best_pattern = max(scores, key=scores.get)
            if scores[best_pattern] > 0:
                return f"{best_pattern} Developer"
        
        return "Software Engineer"
    
    def classify_role(self, title):
        """Classify role using keywords and ML"""
        if not title or pd.isna(title):
            return 'Other'
            
        title_lower = str(title).lower()
        
        role_keywords = {
            'Backend Developer': ['backend', 'back-end', 'django', 'flask', 'api', 'node.js', 'nodejs', 'php', 'golang', 'java backend', 'spring boot', 'server side'],
            'Frontend Developer': ['frontend', 'front-end', 'ui', 'react', 'angular', 'vue', 'javascript', 'typescript', 'css', 'html', 'web developer'],
            'Full Stack Developer': ['full stack', 'full-stack', 'fullstack', 'mern', 'mean'],
            'Data Scientist': ['data scientist', 'analytics', 'analyst', 'data analysis', 'bi', 'business intelligence'],
            'ML/AI Engineer': ['ml', 'ai', 'machine learning', 'genai', 'nlp', 'artificial intelligence', 'deep learning', 'computer vision'],
            'DevOps Engineer': ['devops', 'sre', 'cloud', 'aws', 'azure', 'kubernetes', 'docker', 'ci/cd', 'infrastructure'],
            'Mobile Developer': ['mobile', 'android', 'ios', 'react native', 'flutter', 'swift', 'kotlin'],
            'Data Engineer': ['data engineer', 'etl', 'data pipeline', 'data warehouse', 'bigdata'],
            'QA Engineer': ['qa', 'quality', 'testing', 'test engineer', 'automation testing', 'sdet'],
            'Software Engineer': ['software engineer', 'swe', 'software developer', 'programmer', 'developer', 'engineer']
        }
        
        for role, keywords in role_keywords.items():
            if any(keyword in title_lower for keyword in keywords):
                return role
        
        # Check discovered roles
        for cluster_info in self.discovered_roles.values():
            role_name = cluster_info['role_name'].lower()
            if any(keyword in title_lower for keyword in role_name.split()):
                return cluster_info['role_name']
        
        return 'Other'


def enhanced_job_type_classification(df):
    """ML classification for job types"""
    print("  → Running ML job type classification...")
    
    # Use existing Job Type if available, otherwise classify from title
    if 'Job Type' not in df.columns or df['Job Type'].isna().sum() > len(df) * 0.5:
        
        # Training data for job type classification
        train_data = [
            ("remote work from home", "Remote"),
            ("work from anywhere distributed", "Remote"),
            ("remote position telecommute", "Remote"),
            ("contract basis temporary", "Contract"),
            ("freelance project consulting", "Contract"),
            ("contract to hire c2c", "Contract"),
            ("intern internship trainee", "Internship"),
            ("trainee graduate program", "Internship"),
            ("full time permanent staff", "Full-time"),
            ("permanent position fte", "Full-time"),
        ]
        
        train_texts = [data[0] for data in train_data]
        train_labels = [data[1] for data in train_data]
        
        # Build ML model
        model = make_pipeline(
            TfidfVectorizer(ngram_range=(1, 3), max_features=500),
            RandomForestClassifier(n_estimators=100, random_state=42)
        )
        
        model.fit(train_texts, train_labels)
        
        # Predict on actual data
        job_titles = df['Job Title'].fillna('').astype(str)
        predictions = model.predict(job_titles)
        probabilities = model.predict_proba(job_titles)
        
        # Add predictions
        df['Job Type'] = predictions
        df['Type_Confidence'] = np.max(probabilities, axis=1)
        
        print(f"    High confidence (>0.8): {len(df[df['Type_Confidence'] > 0.8])}")
        print(f"    Medium confidence (0.5-0.8): {len(df[(df['Type_Confidence'] >= 0.5) & (df['Type_Confidence'] <= 0.8)])}")
        print(f"    Low confidence (<0.5): {len(df[df['Type_Confidence'] < 0.5])}")
    else:
        print(f"    Using existing Job Type classification")
        df['Type_Confidence'] = 1.0
    
    return df


def advanced_role_standardization(df):
    """Enhanced role standardization with ML clustering"""
    print("  → Running advanced role standardization...")
    
    role_expander = DynamicRoleExpander()
    
    # Get all job titles
    job_titles = df['Job Title'].fillna('').astype(str).tolist()
    
    # Discover role patterns
    discovered_roles = role_expander.expand_roles_with_ml(job_titles)
    
    # Classify each job
    df['Role'] = df['Job Title'].fillna('').astype(str).apply(role_expander.classify_role)
    
    if discovered_roles:
        print(f"    Discovered {len(discovered_roles)} role patterns:")
        for role_id, role_info in list(discovered_roles.items())[:5]:  # Show first 5
            print(f"      • {role_info['role_name']}: {role_info['size']} jobs")
    
    return df


def extract_experience_from_titles(df):
    """Extract experience requirements from job titles"""
    print("  → Extracting experience requirements...")
    
    exp_extractor = ExperienceExtractor()
    
    df['Experience_Years'] = df['Job Title'].apply(exp_extractor.extract_experience)
    
    print(f"    Jobs with experience info: {len(df[df['Experience_Years'] > 0])}")
    print(f"    Average experience: {df['Experience_Years'].mean():.1f} years")
    print(f"    Max experience: {df['Experience_Years'].max()} years")
    
    return df


def extract_skills_from_title(df):
    """Extract technical skills from job titles"""
    print("  → Extracting skills from job titles...")
    
    # Comprehensive skills database
    SKILLS_DB = {
        # Programming Languages
        'python', 'java', 'c++', 'c#', 'golang', 'go', 'php', 'javascript', 'typescript',
        'ruby', 'scala', 'kotlin', 'swift', 'rust', 'r', 'perl',
        
        # Frontend
        'react', 'vue', 'angular', 'next.js', 'nextjs', 'svelte', 'html', 'css', 'sass',
        'redux', 'webpack', 'tailwind',
        
        # Backend
        'django', 'flask', 'fastapi', 'node.js', 'nodejs', 'express', 'spring', 'laravel',
        'asp.net', 'rails', 'nest.js',
        
        # Databases
        'sql', 'nosql', 'mongodb', 'postgresql', 'mysql', 'redis', 'elasticsearch',
        'cassandra', 'dynamodb', 'oracle',
        
        # Cloud & DevOps
        'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'jenkins', 'terraform',
        'ansible', 'chef', 'puppet', 'ci/cd', 'gitlab', 'github actions',
        
        # Mobile
        'android', 'ios', 'react native', 'flutter', 'xamarin', 'ionic',
        
        # Data & ML
        'machine learning', 'ml', 'ai', 'data science', 'pandas', 'tensorflow', 'pytorch',
        'scikit-learn', 'keras', 'spark', 'hadoop', 'airflow', 'nlp', 'computer vision',
        
        # Other
        'api', 'rest', 'graphql', 'microservices', 'kafka', 'rabbitmq', 'grpc',
        'agile', 'scrum', 'git'
    }
    
    all_skills = []
    
    for title in df['Job Title'].fillna('').astype(str).str.lower():
        found_skills = set()
        
        # Direct keyword matching
        for skill in SKILLS_DB:
            if skill in title:
                found_skills.add(skill)
        
        # Bigram matching for two-word skills
        words = title.split()
        bigrams = [' '.join(words[i:i+2]) for i in range(len(words)-1)]
        for bigram in bigrams:
            if bigram in SKILLS_DB:
                found_skills.add(bigram)
        
        all_skills.append(', '.join(sorted(found_skills)) if found_skills else 'Not specified')
    
    df['Skills'] = all_skills
    df['Skills_Count'] = df['Skills'].apply(lambda x: len(x.split(', ')) if x != 'Not specified' else 0)
    
    print(f"    Jobs with skills: {len(df[df['Skills_Count'] > 0])}")
    print(f"    Average skills per job: {df['Skills_Count'].mean():.2f}")
    
    return df


def run_ml_pipeline():
    """Main ML processing pipeline"""
    print("=" * 80)
    print("ADVANCED ML PROCESSING PIPELINE")
    print("=" * 80)
    
    print(f"\n[1/5] Loading data from: {INPUT_FILE}")
    
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"    ✓ Loaded {len(df)} job listings")
        print(f"    Columns: {list(df.columns)}")
    except FileNotFoundError:
        print(f"    ✗ Error: File not found at {INPUT_FILE}")
        print("    Please run scraper.py first to generate raw data")
        return
    except Exception as e:
        print(f"    ✗ Error loading data: {e}")
        return

    print(f"\n[2/5] Data Cleaning & Deduplication")
    print(f"    Original listings: {len(df)}")
    
    # Remove rows with missing critical fields
    df.dropna(subset=['Job Title', 'Job Link'], inplace=True)
    print(f"    After removing incomplete records: {len(df)}")
    
    # Remove duplicates
    initial_count = len(df)
    df.drop_duplicates(subset=['Job Link'], keep='first', inplace=True)
    duplicates_removed = initial_count - len(df)
    print(f"    Duplicates removed: {duplicates_removed}")
    print(f"    ✓ Clean dataset: {len(df)} jobs")

    print(f"\n[3/5] ML Processing & Feature Engineering")
    
    # Apply ML transformations
    df = enhanced_job_type_classification(df)
    df = advanced_role_standardization(df)
    df = extract_experience_from_titles(df)
    df = extract_skills_from_title(df)
    
    print(f"\n[4/5] Final Data Preparation")
    
    # Reorder columns for better readability
    column_order = [
        'Job Title', 'Company', 'Location', 'Job Type', 'Role',
        'Experience_Years', 'Skills', 'Posted Date', 'Job Link'
    ]
    
    # Only include columns that exist
    final_columns = [col for col in column_order if col in df.columns]
    
    # Add any remaining columns
    remaining_cols = [col for col in df.columns if col not in final_columns and col not in ['Type_Confidence', 'Skills_Count']]
    final_columns.extend(remaining_cols)
    
    df = df[final_columns]
    
    print(f"\n[5/5] Summary Statistics")
    print(f"    Final dataset: {len(df)} jobs")
    
    print(f"\n    ✓ Role Distribution:")
    role_counts = df['Role'].value_counts()
    for role, count in role_counts.head(10).items():
        print(f"      {role}: {count}")
    
    print(f"\n    ✓ Job Type Distribution:")
    type_counts = df['Job Type'].value_counts()
    for job_type, count in type_counts.items():
        print(f"      {job_type}: {count}")
    
    if 'Experience_Years' in df.columns:
        print(f"\n    ✓ Experience Range:")
        print(f"      Min: {df['Experience_Years'].min()} years")
        print(f"      Max: {df['Experience_Years'].max()} years")
        print(f"      Average: {df['Experience_Years'].mean():.1f} years")
    
    print(f"\n    ✓ Skills Analysis:")
    jobs_with_skills = len(df[df['Skills'] != 'Not specified'])
    print(f"      Jobs with extracted skills: {jobs_with_skills}")
    
    # Top skills
    all_skills_flat = []
    for skills_str in df['Skills']:
        if skills_str != 'Not specified':
            all_skills_flat.extend(skills_str.split(', '))
    
    if all_skills_flat:
        skill_counter = Counter(all_skills_flat)
        print(f"      Top 10 skills:")
        for skill, count in skill_counter.most_common(10):
            print(f"        • {skill}: {count}")
    
    print(f"\n    ✓ Saving processed data...")
    
    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE")
    print("=" * 80)
    print(f"✓ Output saved: {OUTPUT_FILE}")
    print(f"✓ Total columns: {len(df.columns)}")
    print(f"✓ Columns: {', '.join(df.columns)}")
    print("\nDataset is ready for analysis!")
    print("=" * 80)


if __name__ == "__main__":
    run_ml_pipeline()