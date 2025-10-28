from flask import Flask, render_template, request, jsonify, send_file
import os
import sys
import pandas as pd
import threading
import time
from datetime import datetime
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)
try:
    Base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    Base_dir = os.getcwd()

Data_dir = os.path.join(Base_dir, 'Data')
LINKEDIN_FILE = os.path.join(Data_dir, "linkedin_jobs_scraper.csv")
SHINE_FILE = os.path.join(Data_dir, "Shine_jobs_scraper.csv")

scraping_status = {
    "is_running": False,
    "current_platform": None,
    "progress": 0,
    "message": "",
    "results": None
}

class JobScraper:
    def __init__(self):
        self.ensure_data_directory()
    
    def ensure_data_directory(self):
        """Ensure Data directory exists"""
        if not os.path.exists(Data_dir):
            os.makedirs(Data_dir)
    
    def update_status(self, platform, progress, message, is_running=True, results=None):
        """Update scraping status"""
        global scraping_status
        scraping_status = {
            "is_running": is_running,
            "current_platform": platform,
            "progress": progress,
            "message": message,
            "results": results
        }
    
    def run_linkedin_scraper(self, job_titles):
        """Run LinkedIn scraper with user-defined job titles"""
        try:
            self.update_status("LinkedIn", 10, "Starting LinkedIn scraper...")
            
            # Import the LinkedIn module
            from linkedin import KEYWORDS as LINKEDIN_DEFAULT_KEYWORDS
            import linkedin as linkedin_module
            
            # Store original keywords
            original_keywords = linkedin_module.KEYWORDS
            
            try:
                # Replace with user-defined keywords
                linkedin_module.KEYWORDS = job_titles
                
                self.update_status("LinkedIn", 30, f"Searching for {len(job_titles)} job titles on LinkedIn...")
                
                # Execute the main scraping logic
                linkedin_module.driver.quit()  # Close any existing driver
                
                # Reinitialize driver
                options = linkedin_module.webdriver.ChromeOptions()
                options.add_argument("--start-maximized")
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_argument("--disable-notifications")
                options.add_argument(f"user-agent={linkedin_module.random_user_agent()}")
                
                linkedin_module.driver = linkedin_module.webdriver.Chrome(options=options)
                linkedin_module.wait = linkedin_module.WebDriverWait(linkedin_module.driver, 15)
                
                # Run the scraping process
                total_keywords = len(job_titles)
                for idx, keyword in enumerate(job_titles):
                    self.update_status("LinkedIn", 30 + (idx / total_keywords) * 60, 
                                     f"Searching: {keyword} ({idx+1}/{total_keywords})")
                    
                    print(f"Searching: {keyword}")
                    
                    for page in range(linkedin_module.MAX_PAGES):
                        start = page * linkedin_module.JOBS_PER_PAGE
                        url = linkedin_module.build_url(keyword, start)
                        print(f"Page {page + 1}, URL: {url}")
                        linkedin_module.driver.get(url)
                        time.sleep(linkedin_module.random.uniform(2.0, 4.0))

                        linkedin_module.handle_popups()
                        linkedin_module.close_login_popup()

                        try:
                            linkedin_module.wait.until(
                                linkedin_module.EC.presence_of_all_elements_located(
                                    (linkedin_module.By.CSS_SELECTOR, "ul.jobs-search__results-list li")
                                )
                            )
                        except:
                            print("No jobs found on this page.")
                            continue

                        cards = linkedin_module.driver.find_elements(
                            linkedin_module.By.CSS_SELECTOR, "ul.jobs-search__results-list li"
                        )
                        print(f"Found {len(cards)} cards")

                        for i, card in enumerate(cards):
                            job_data = linkedin_module.extract_job_data(card)
                            if job_data["jobUrl"] == "N/A" or job_data["jobUrl"] in linkedin_module.seen_links:
                                continue

                            linkedin_module.seen_links.add(job_data["jobUrl"])
                            print(f"[{i+1}] {job_data['job_title'][:60]}")

                            linkedin_module.writer.writerow([
                                job_data["job_title"],
                                job_data["company_name"],
                                job_data["jobUrl"],
                                job_data["salary"],
                                job_data["location"],
                                job_data["postedTime"],
                                job_data["experienceLevel"]
                            ])
                            linkedin_module.csv_file.flush()

                            linkedin_module.handle_popups()
                            linkedin_module.close_login_popup()
                            time.sleep(linkedin_module.random.uniform(1.0, 2.0))
                
                # Close resources
                linkedin_module.csv_file.close()
                linkedin_module.driver.quit()
                
                self.update_status("LinkedIn", 95, "Finalizing LinkedIn data...")
                
                # Check results
                if os.path.exists(LINKEDIN_FILE):
                    df = pd.read_csv(LINKEDIN_FILE)
                    job_count = len(df)
                    return True, f"LinkedIn scraping completed! Found {job_count} jobs."
                else:
                    return False, "LinkedIn scraping completed but no data file found."
                    
            except Exception as e:
                return False, f"LinkedIn scraping error: {str(e)}"
            finally:
                # Restore original keywords
                linkedin_module.KEYWORDS = original_keywords
                
        except Exception as e:
            return False, f"Failed to initialize LinkedIn scraper: {str(e)}"
    
    def run_shine_scraper(self, job_titles):
        """Run Shine.com scraper with user-defined job titles"""
        try:
            self.update_status("Shine", 10, "Starting Shine.com scraper...")
            
            # Import the Shine module
            from main import job_roles as SHINE_DEFAULT_ROLES
            import main as main_module
            
            # Store original job roles
            original_roles = main_module.job_roles
            
            try:
                # Replace with user-defined job titles
                main_module.job_roles = job_titles
                
                self.update_status("Shine", 20, f"Searching for {len(job_titles)} roles on Shine.com...")
                
                # Execute the main scraping function
                main_module.seen_job_links = set()
                
                # Run the scraping process
                shine_jobs = main_module.scrape_shine()
                
                self.update_status("Shine", 80, "Saving Shine.com data...")
                
                # Save results
                main_module.save_to_csv(shine_jobs, 'remote_contract_software_jobs.csv')
                
                # Check results
                if os.path.exists(SHINE_FILE):
                    job_count = len(shine_jobs)
                    return True, f"Shine.com scraping completed! Found {job_count} remote contract jobs."
                else:
                    return False, "Shine.com scraping completed but no data file found."
                    
            except Exception as e:
                return False, f"Shine.com scraping error: {str(e)}"
            finally:
                # Restore original job roles
                main_module.job_roles = original_roles
                
        except Exception as e:
            return False, f"Failed to initialize Shine.com scraper: {str(e)}"

# Initialize scraper
scraper = JobScraper()

@app.route('/')
def index():
    """Main page"""
    return render_template('index.html')

@app.route('/start_scraping', methods=['POST'])
def start_scraping():
    """Start the scraping process"""
    global scraping_status
    
    if scraping_status["is_running"]:
        return jsonify({"success": False, "message": "Scraping is already in progress"})
    
    data = request.json
    job_titles = data.get('job_titles', [])
    platforms = data.get('platforms', [])
    
    if not job_titles:
        return jsonify({"success": False, "message": "Please enter at least one job title"})
    
    if not platforms:
        return jsonify({"success": False, "message": "Please select at least one platform"})
    
    # Start scraping in a separate thread
    def scrape_jobs():
        global scraping_status
        results = {}
        
        for platform in platforms:
            if platform == "linkedin":
                scraper.update_status("LinkedIn", 5, "Initializing LinkedIn scraper...")
                success, message = scraper.run_linkedin_scraper(job_titles)
                results["linkedin"] = {"success": success, "message": message}
            elif platform == "shine":
                scraper.update_status("Shine", 5, "Initializing Shine.com scraper...")
                success, message = scraper.run_shine_scraper(job_titles)
                results["shine"] = {"success": success, "message": message}
            
            time.sleep(1)  # Brief pause between platforms
        
        scraping_status["is_running"] = False
        scraping_status["results"] = results
        scraping_status["message"] = "Scraping completed"
    
    scraping_status["is_running"] = True
    scraping_status["current_platform"] = None
    scraping_status["progress"] = 0
    scraping_status["message"] = "Starting scraping process..."
    scraping_status["results"] = None
    
    thread = threading.Thread(target=scrape_jobs)
    thread.daemon = True
    thread.start()
    
    return jsonify({"success": True, "message": "Scraping started successfully"})

@app.route('/scraping_status')
def get_scraping_status():
    """Get current scraping status"""
    return jsonify(scraping_status)

@app.route('/download/<platform>')
def download_file(platform):
    """Download the scraped data"""
    if platform == "linkedin":
        file_path = LINKEDIN_FILE
        filename = "linkedin_jobs.csv"
    elif platform == "shine":
        file_path = SHINE_FILE
        filename = "shine_jobs.csv"
    else:
        return "Invalid platform", 400
    
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True, download_name=filename)
    else:
        return "File not found", 404

@app.route('/preview_data')
def preview_data():
    """Preview the scraped data"""
    platform = request.args.get('platform')
    
    if platform == "linkedin":
        file_path = LINKEDIN_FILE
    elif platform == "shine":
        file_path = SHINE_FILE
    else:
        return jsonify({"error": "Invalid platform"})
    
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path)
            # Return first 10 rows as HTML table
            preview_html = df.head(10).to_html(classes='table table-striped', index=False)
            total_jobs = len(df)
            return jsonify({
                "success": True,
                "preview": preview_html,
                "total_jobs": total_jobs
            })
        except Exception as e:
            return jsonify({"error": f"Error reading file: {str(e)}"})
    else:
        return jsonify({"error": "File not found"})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)