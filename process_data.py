import pandas as pd
import spacy
import numpy as np
import os
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import DBSCAN, KMeans
from sklearn.pipeline import make_pipeline
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.decomposition import PCA
import re

# Define paths using os.path.join
BASE_DIR = r'C:\Sathvik-py\Talrn\job_scraper'
DATA_DIR = os.path.join(BASE_DIR, 'Data')
INPUT_FILE = os.path.join(DATA_DIR, 'jobs_raw_data.csv')
OUTPUT_FILE = os.path.join(DATA_DIR, 'jobs_advanced.csv')

# Create Data directory if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)

class DynamicRoleExpander:
    """ML-powered dynamic role title expansion and discovery"""
    
    def __init__(self):
        self.role_embeddings = {}
        self.discovered_roles = {}
        
    def expand_roles_with_ml(self, job_titles):
        """Use clustering to discover new role patterns dynamically"""
        print("Discovering new role patterns with ML clustering...")
        
        if len(job_titles) < 2:
            return {}
            
        # Use TF-IDF for text representation
        vectorizer = TfidfVectorizer(max_features=100, stop_words='english')
        try:
            title_vectors = vectorizer.fit_transform(job_titles)
            
            # Reduce dimensionality for clustering
            if len(job_titles) > 10:
                pca = PCA(n_components=min(10, len(job_titles)-1))
                reduced_embeddings = pca.fit_transform(title_vectors.toarray())
            else:
                reduced_embeddings = title_vectors.toarray()
        except Exception as e:
            print(f"Clustering failed: {e}. Using simple grouping.")
            return self._simple_role_grouping(job_titles)
        
        # Cluster to discover new role patterns
        if len(job_titles) > 5:
            try:
                clustering = DBSCAN(eps=0.7, min_samples=2)
                cluster_labels = clustering.fit_predict(reduced_embeddings)
            except:
                # Fallback to KMeans if DBSCAN fails
                n_clusters = min(5, len(job_titles))
                clustering = KMeans(n_clusters=n_clusters, random_state=42)
                cluster_labels = clustering.fit_predict(reduced_embeddings)
        else:
            cluster_labels = np.zeros(len(job_titles))
        
        # Analyze clusters to discover new roles
        discovered_roles = {}
        for cluster_id in set(cluster_labels):
            if cluster_id != -1:  # Ignore noise
                cluster_titles = [job_titles[i] for i in range(len(job_titles)) if cluster_labels[i] == cluster_id]
                if cluster_titles and len(cluster_titles) > 1:
                    # Find common patterns in cluster
                    role_name = self._analyze_cluster_patterns(cluster_titles)
                    discovered_roles[cluster_id] = {
                        'role_name': role_name,
                        'titles': cluster_titles[:5],  # Sample titles
                        'size': len(cluster_titles)
                    }
        
        self.discovered_roles = discovered_roles
        return discovered_roles
    
    def _simple_role_grouping(self, job_titles):
        """Simple role grouping when clustering fails"""
        role_groups = {}
        
        for title in job_titles:
            title_lower = title.lower()
            if 'data' in title_lower and 'scientist' in title_lower:
                group = 'data_scientist'
            elif 'frontend' in title_lower or 'front-end' in title_lower:
                group = 'frontend'
            elif 'backend' in title_lower or 'back-end' in title_lower:
                group = 'backend'
            elif 'devops' in title_lower:
                group = 'devops'
            elif 'full stack' in title_lower or 'fullstack' in title_lower:
                group = 'fullstack'
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
        # Common tech role patterns
        common_patterns = {
            'backend': ['backend', 'back-end', 'server', 'api', 'database'],
            'frontend': ['frontend', 'front-end', 'ui', 'ux', 'react', 'angular', 'vue'],
            'fullstack': ['full stack', 'full-stack', 'fullstack'],
            'devops': ['devops', 'sre', 'cloud', 'infrastructure', 'deployment'],
            'data': ['data', 'analytics', 'analysis', 'bi', 'database'],
            'ml': ['machine learning', 'ml', 'ai', 'artificial intelligence', 'deep learning'],
            'mobile': ['mobile', 'android', 'ios', 'react native', 'flutter']
        }
        
        title_text = ' '.join(titles).lower()
        scores = {}
        
        for pattern, keywords in common_patterns.items():
            score = sum(1 for keyword in keywords if keyword in title_text)
            scores[pattern] = score
        
        if scores:
            best_pattern = max(scores, key=scores.get)
            if scores[best_pattern] > 0:
                return f"Discovered {best_pattern.title()} Role"
        
        return "Emerging Tech Role"
    
    def classify_role(self, title):
        """Single method to classify role with traditional + ML approaches"""
        title_lower = title.lower()
        
        # Traditional keyword-based role mapping
        role_keywords = {
            'Backend Developer': ['backend', 'back-end', 'django', 'flask', 'api', 'node.js', 'php', 'golang', 'server', 'database'],
            'Frontend Developer': ['frontend', 'front-end', 'ui', 'react', 'angular', 'vue', 'javascript', 'typescript', 'css'],
            'Full Stack Developer': ['full stack', 'full-stack', 'fullstack'],
            'Data Scientist': ['data scientist', 'analytics', 'analyst', 'data analysis', 'bi', 'business intelligence'],
            'ML/AI Engineer': ['ml', 'ai', 'machine learning', 'genai', 'nlp', 'artificial intelligence', 'deep learning', 'neural network'],
            'DevOps Engineer': ['devops', 'sre', 'cloud', 'aws', 'azure', 'gcp', 'infrastructure', 'deployment', 'ci/cd'],
            'Software Engineer': ['software engineer', 'developer', 'swe', 'programmer', 'software developer'],
            'Mobile Developer': ['mobile', 'android', 'ios', 'react native', 'flutter', 'mobile app'],
            'QA Engineer': ['qa', 'quality assurance', 'testing', 'test engineer', 'automation testing']
        }
        
        # Traditional keyword matching
        for role, keywords in role_keywords.items():
            if any(keyword in title_lower for keyword in keywords):
                return role
        
        # Use discovered roles for better matching
        for cluster_info in self.discovered_roles.values():
            role_name = cluster_info['role_name'].lower()
            if any(keyword in title_lower for keyword in role_name.split()):
                return cluster_info['role_name']
        
        return 'Other'

def enhanced_job_type_classification(df):
    """Advanced ML classification for job types with expanded training"""
    print("Running enhanced ML job type classification...")
    
    # Expanded training data with more patterns
    train_data = [
        # Remote work patterns
        ("remote work from home flexible location", "Remote"),
        ("work from anywhere distributed team", "Remote"),
        ("virtual office telecommute", "Remote"),
        ("remote position work from home", "Remote"),
        ("fully remote flexible location", "Remote"),
        
        # Contract patterns
        ("6 month contract temporary position", "Contract"),
        ("freelance project contract basis", "Contract"),
        ("contract to hire temporary role", "Contract"),
        ("contractor freelance consultant", "Contract"),
        ("fixed term contract project based", "Contract"),
        
        # Internship patterns  
        ("summer intern student position", "Internship"),
        ("campus hiring internship program", "Internship"),
        ("graduate intern trainee position", "Internship"),
        ("internship for students campus", "Internship"),
        ("student intern graduate trainee", "Internship"),
        
        # Full-time patterns
        ("permanent role full time employment", "Full-time"),
        ("full time with benefits permanent", "Full-time"),
        ("regular employment full time position", "Full-time"),
        ("full time staff employee", "Full-time"),
        ("full time permanent benefits", "Full-time"),
        
        # Mixed and complex patterns
        ("senior python developer remote usa", "Remote"),
        ("react developer contract london", "Contract"),
        ("data science internship summer 2024", "Internship"),
        ("software engineer full time bangalore", "Full-time"),
        ("devops engineer remote contract", "Contract"),
    ]
    
    train_texts = [data[0] for data in train_data]
    train_labels = [data[1] for data in train_data]
    
    # Enhanced ML pipeline with better features
    model = make_pipeline(
        TfidfVectorizer(ngram_range=(1, 3), max_features=1000),
        RandomForestClassifier(n_estimators=100, random_state=42)
    )
    
    # Train the model
    model.fit(train_texts, train_labels)
    
    # Predict with confidence scores
    job_titles = df['Job Title'].astype(str)
    predictions = model.predict(job_titles)
    probabilities = model.predict_proba(job_titles)
    
    # Add predictions and confidence scores
    df['Predicted Job Type'] = predictions
    df['Prediction Confidence'] = np.max(probabilities, axis=1)
    
    # Apply business rules for low-confidence predictions
    low_confidence_mask = df['Prediction Confidence'] < 0.6
    df.loc[low_confidence_mask, 'Predicted Job Type'] = 'Unknown'
    
    print(f"Job type classification complete. Confidence stats:")
    print(f"High confidence (>0.8): {len(df[df['Prediction Confidence'] > 0.8])}")
    print(f"Medium confidence (0.6-0.8): {len(df[(df['Prediction Confidence'] >= 0.6) & (df['Prediction Confidence'] <= 0.8)])}")
    print(f"Low confidence (<0.6): {len(df[df['Prediction Confidence'] < 0.6])}")
    
    return df

def advanced_role_standardization(df):
    """Enhanced role standardization with ML-powered expansion - Single column output"""
    print("Running advanced role standardization...")
    
    # Initialize dynamic role expander
    role_expander = DynamicRoleExpander()
    
    # Use ML to discover new role patterns
    job_titles = df['Job Title'].astype(str).tolist()
    discovered_roles = role_expander.expand_roles_with_ml(job_titles)
    
    # Apply unified role classification (traditional + ML)
    df['Role'] = df['Job Title'].astype(str).apply(role_expander.classify_role)
    
    # Print discovery statistics
    if discovered_roles:
        print(f"Discovered {len(discovered_roles)} new role patterns:")
        for role_id, role_info in discovered_roles.items():
            print(f"  - {role_info['role_name']}: {role_info['size']} jobs")
    
    return df

def extract_skills_with_advanced_nlp(df, nlp_model):
    """Enhanced skill extraction with advanced NLP techniques"""
    print("Extracting skills with advanced NLP...")
    
    # Expanded skills database
    SKILLS_DB = [
        # Programming Languages
        'python', 'java', 'c++', 'golang', 'php', 'javascript', 'typescript',
        'r', 'scala', 'kotlin', 'swift', 'objective-c', 'ruby',
        
        # Cloud & DevOps
        'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform', 'ansible',
        'jenkins', 'gitlab', 'github actions', 'ci/cd',
        
        # Databases
        'sql', 'nosql', 'mongodb', 'postgresql', 'redis', 'mysql', 'cassandra',
        'dynamodb', 'elasticsearch',
        
        # Frontend Technologies
        'react', 'vue', 'angular', 'next.js', 'nuxt.js', 'svelte', 'html', 'css',
        'sass', 'less', 'bootstrap', 'tailwind',
        
        # Backend Frameworks
        'django', 'flask', 'fastapi', 'node.js', 'express', 'laravel', 'spring',
        'rails', 'asp.net',
        
        # Data Science & ML
        'pandas', 'numpy', 'tensorflow', 'pytorch', 'scikit-learn', 'spacy', 'nltk',
        'opencv', 'matplotlib', 'seaborn', 'plotly',
        
        # Tools & Methodologies
        'rest', 'api', 'graphql', 'git', 'linux', 'agile', 'scrum', 'jira',
        'microservices', 'serverless'
    ]
    
    all_skills = []
    skill_confidence = []
    
    # Process job titles for skill extraction
    for title in df['Job Title'].astype(str).str.lower():
        found_skills = set()
        confidence_score = 0
        
        # Method 1: Direct keyword matching
        for skill in SKILLS_DB:
            if skill in title:
                found_skills.add(skill)
                confidence_score += 0.3  # Higher confidence for exact matches
        
        # Method 2: NLP-based extraction
        doc = nlp_model(title)
        for token in doc:
            if token.text in SKILLS_DB and token.text not in found_skills:
                found_skills.add(token.text)
                confidence_score += 0.2  # Lower confidence for NLP matches
        
        # Method 3: Bigram matching for compound skills
        words = title.split()
        bigrams = [' '.join(words[i:i+2]) for i in range(len(words)-1)]
        for bigram in bigrams:
            if bigram in SKILLS_DB and bigram not in found_skills:
                found_skills.add(bigram)
                confidence_score += 0.4  # Highest confidence for compound matches
        
        all_skills.append(list(found_skills))
        skill_confidence.append(min(1.0, confidence_score))  # Cap at 1.0
    
    df['Extracted Skills'] = all_skills
    df['Skill Extraction Confidence'] = skill_confidence
    df['Number of Skills'] = df['Extracted Skills'].apply(len)
    
    print(f"Skill extraction complete. Average skills per job: {df['Number of Skills'].mean():.2f}")
    print(f"Average extraction confidence: {df['Skill Extraction Confidence'].mean():.2f}")
    
    return df

def dynamic_keyword_refinement(df):
    """ML-powered dynamic keyword refinement for role matching"""
    print("Running dynamic keyword refinement...")
    
    # Analyze successful matches to refine keywords
    successful_matches = df[df['Role'] != 'Other']
    
    if len(successful_matches) > 0:
        # Extract common patterns from successful matches
        vectorizer = CountVectorizer(ngram_range=(1, 2), max_features=50)
        title_vectors = vectorizer.fit_transform(successful_matches['Job Title'])
        feature_names = vectorizer.get_feature_names_out()
        
        print("Top role indicators found:")
        for i, feature in enumerate(feature_names[:10]):
            print(f"  - {feature}")
    
    return df

def run_ml_pipeline():
    """Main function to run the enhanced ML pipeline"""
    print(f"Loading data from: {INPUT_FILE}")
    
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"Successfully loaded {len(df)} job listings")
    except FileNotFoundError:
        print(f"Error: Input file not found at {INPUT_FILE}")
        print("Please run scraper.py first to collect data")
        return
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # --- Step 1: Clean and De-duplicate Data ---
    print(f"\n--- Step 1: Data Cleaning ---")
    print(f"Original number of listings: {len(df)}")
    
    # Remove rows with missing critical data
    df.dropna(subset=['Job Title', 'Job Link'], inplace=True)
    
    # De-duplicate based on Job Link only
    initial_count = len(df)
    df.drop_duplicates(subset=['Job Link'], keep='first', inplace=True)
    
    print(f"After cleaning and de-duplication: {len(df)}")
    print(f"Removed {initial_count - len(df)} duplicate/invalid entries")

    # --- Step 2: Load NLP Model ---
    print(f"\n--- Step 2: Loading NLP Model ---")
    try:
        nlp = spacy.load("en_core_web_sm")
        print("spaCy NLP model loaded successfully")
    except OSError:
        print("Downloading spaCy model...")
        os.system("python -m spacy download en_core_web_sm")
        nlp = spacy.load("en_core_web_sm")
    
    # --- Step 3: Run Enhanced ML and NLP Functions ---
    print(f"\n--- Step 3: Running ML Processing ---")
    
    df = enhanced_job_type_classification(df)
    df = advanced_role_standardization(df)
    df = extract_skills_with_advanced_nlp(df, nlp)
    df = dynamic_keyword_refinement(df)
    
    # --- Step 4: Generate Summary Statistics ---
    print(f"\n--- Step 4: Summary Statistics ---")
    print(f"Final dataset size: {len(df)} jobs")
    print(f"Role Distribution:")
    role_counts = df['Role'].value_counts()
    for role, count in role_counts.items():
        print(f"  - {role}: {count} jobs")
    
    print(f"Job Type Distribution:")
    type_counts = df['Predicted Job Type'].value_counts()
    for job_type, count in type_counts.items():
        print(f"  - {job_type}: {count} jobs")
    
    # --- Step 5: Save Final Enriched Output ---
    print(f"\n--- Step 5: Saving Results ---")
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"Enhanced ML processing complete!")
    print(f"Enriched data saved to: {OUTPUT_FILE}")
    print(f"Total features generated: {len(df.columns)}")
    print("Features:", list(df.columns))

if __name__ == "__main__":
    run_ml_pipeline()