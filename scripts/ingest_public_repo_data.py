import os
import sys
import requests
import random 

# --- Configuration ---
PORT_CLIENT_ID = os.getenv("PORT_CLIENT_ID")
PORT_CLIENT_SECRET = os.getenv("PORT_CLIENT_SECRET")
GH_TOKEN = os.getenv("GH_PAT")

TARGET_REPO = "microsoft/PowerToys"
REPO_ENTITY_IDENTIFIER = "PowerToys" 

PORT_API_URL = "https://api.getport.io/v1"
PR_BLUEPRINT = "githubPullRequest" 
ISSUE_BLUEPRINT = "githubIssue"   
VULNERABILITY_BLUEPRINT = "githubDependabotAlert" 

# --- WALMART DUMMY DATA POOLS ---
WALMART_LABELS = ["bug", "feature-request", "tech-debt", "security", "hotfix", "documentation"]
WALMART_TEAMS = ["Payments-Bentonville", "Mobile-Bangalore", "Platform-Austin", "SupplyChain-Dallas", "Ecomm-Reston"]
WALMART_DEVELOPERS = [
    "alex.chen", "brenda.smith", "carlos.garcia", "diana.jones", "ethan.williams",
    "fiona.davis", "greg.miller", "hannah.wilson", "ian.moore", "jenna.taylor"
]
WALMART_PROJECTS = ["Q3_Checkout_Redesign", "Holiday_Scale_Prep", "SupplyChain_API_V2", "Mobile_App_Refresh"]


# --- Helper Functions ---

def get_port_api_token():
    """Gets a fresh, short-lived Port API access token."""
    print("Requesting new Port API access token...")
    credentials = {'clientId': PORT_CLIENT_ID, 'clientSecret': PORT_CLIENT_SECRET}
    token_response = requests.post(f"{PORT_API_URL}/auth/access_token", json=credentials)
    token_response.raise_for_status()
    print("Successfully received new token.")
    return token_response.json()['accessToken']

def fetch_github_data(endpoint):
    # This function is unchanged
    all_data = []
    url = f"https://api.github.com/repos/{TARGET_REPO}/{endpoint}"
    headers = {'Authorization': f'token {GH_TOKEN}', 'Accept': 'application/vnd.github.v3+json'}
    params = {'state': 'all', 'per_page': 100}
    while url:
        print(f"Fetching from {url}...")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        all_data.extend(response.json())
        if 'next' in response.links: url = response.links['next']['url']
        else: url = None
        params = None
    return all_data

def upsert_entities_in_bulk(access_token, blueprint_id, entities):
    # This function is unchanged
    if not entities:
        print(f"No entities to upsert for blueprint {blueprint_id}.")
        return
    headers = {'Authorization': f'Bearer {access_token}'}
    payload = {"entities": entities}
    print(f"Upserting {len(entities)} entities to blueprint {blueprint_id} in a single request...")
    response = requests.post(
        f"{PORT_API_URL}/blueprints/{blueprint_id}/entities/bulk",
        json=payload,
        headers=headers
    )
    if not response.ok:
        print(f"Error during bulk upsert for {blueprint_id}: {response.status_code} {response.text}", file=sys.stderr)
    else:
        print(f"Successfully upserted batch for {blueprint_id}.")
    return response


def fetch_dependabot_alerts(repo_slug):
    """Fetches Dependabot alerts from a repository."""
    print(f"\n--- Fetching Dependabot Alerts for {repo_slug} ---")
    all_alerts = []
    # Note: Requires repo-level admin permissions or security_events scope for the PAT
    url = f"https://api.github.com/repos/{repo_slug}/dependabot/alerts"
    headers = {'Authorization': f'token {GH_TOKEN}', 'Accept': 'application/vnd.github.v3+json'}
    # ... (Add pagination logic similar to fetch_github_data) ...
    # This is a bit more complex, for the demo you can create dummy data
    print("For demo purposes, we will create dummy vulnerability data.")
    dummy_alerts = [
        {"number": 1, "security_vulnerability": {"package": {"name": "log4j-core"}, "severity": "critical"}, "html_url": "https://github.com/microsoft/PowerToys/security/dependabot/1"},
        {"number": 2, "security_vulnerability": {"package": {"name": "jackson-databind"}, "severity": "high"}, "html_url": "https://github.com/microsoft/PowerToys/security/dependabot/2"},
        {"number": 3, "security_vulnerability": {"package": {"name": "moment"}, "severity": "medium"}, "html_url": "https://github.com/microsoft/PowerToys/security/dependabot/3"}
    ]
    return dummy_alerts

# --- Main Logic ---

def main():
    if not all([PORT_CLIENT_ID, PORT_CLIENT_SECRET, GH_TOKEN]):
        print("Error: Missing required secrets.", file=sys.stderr)
        sys.exit(1)

    # --- Process Pull Requests ---
    print("\n--- Processing Pull Requests ---")
    github_prs = fetch_github_data("pulls")
    port_pr_entities = []
    print(f"Found {len(github_prs)} PRs. Now enriching with dummy data...")
    
    # ---- Randomly assign reviewers and an assignee ---
    pr_assignees = [random.choice(WALMART_DEVELOPERS)] if pr.get("assignees") else []
    pr_reviewers = random.sample(WALMART_DEVELOPERS, k=2) # Pick 2 unique random reviewers
    
    for i, pr in enumerate(github_prs):
    try:
        # Safer random assignment
        pr_assignees = [random.choice(WALMART_DEVELOPERS)] if pr.get("assignees") else []
        pr_reviewers = random.choices(WALMART_DEVELOPERS, k=2) # Using choices is safer than sample

        port_pr_entities.append({
            "identifier": str(pr["number"]), "title": pr["title"],
            "properties": { "url": pr["html_url"], "status": pr["state"], "creator": pr.get("user", {}).get("login"), "createdAt": pr["created_at"], "updatedAt": pr["updated_at"], "assignees": pr_assignees, "reviewers": pr_reviewers },
            "relations": { "repository": REPO_ENTITY_IDENTIFIER }
        })
         if (i + 1) % 500 == 0:
                print(f"Processed {i + 1}/{len(github_prs)} PRs...")
        except Exception as e:
            print(f"Error processing PR #{pr.get('number', 'N/A')}: {e}", file=sys.stderr)
            continue # Skip this PR and continue

    print("Finished enriching PRs. Now sending to Port...")
    
    # Get a fresh token right before we use it
    port_token_for_prs = get_port_api_token()
    upsert_entities_in_bulk(port_token_for_prs, PR_BLUEPRINT, port_pr_entities)

    # --- Process Issues ---
    print("\n--- Processing Issues ---")
    github_issues = fetch_github_data("issues")
    port_issue_entities = []
    print(f"Found {len(github_issues)} issues. Now enriching with dummy data...")

    # Start the process issue loop
    for i, issue in enumerate(github_issues):
        if 'pull_request' in issue: continue
        try:
           
        # This line creates the 'issue_labels' variable for the current issue
        # ---issue_labels = [label['name'] for label in issue.get('labels', [])]
        # ---primary_label_value = issue_labels[0] if issue_labels else "No Label"
        
        # Safer random assignment
        # Randomly assign labels, a primary label, assignee, and project
        num_labels = random.randint(1, 2)
        issue_labels = random.sample(WALMART_LABELS, k=num_labels)
        primary_label_value = issue_labels[0]
        issue_assignee = random.choice(WALMART_DEVELOPERS)
        issue_project = random.choice(WALMART_PROJECTS)
        
        port_issue_entities.append({
            "identifier": str(issue["number"]), "title": issue["title"],
            "properties": { "url": issue["html_url"], "status": issue["state"], "creator": issue.get("user", {}).get("login"), "labels": issue_labels, "primaryLabel": primary_label_value, "assignee": issue_assignee, "project": issue_project   }, 
            "relations": { "repository": REPO_ENTITY_IDENTIFIER }
        })

    if (i + 1) % 500 == 0:
                print(f"Processed {i + 1}/{len(github_issues)} issues...")
        except Exception as e:
            print(f"Error processing Issue #{issue.get('number', 'N/A')}: {e}", file=sys.stderr)
            continue # Skip this issue and continue

    print("Finished enriching issues. Now sending to Port...")

    # Get another fresh token right before we use it
    port_token_for_issues = get_port_api_token()
    upsert_entities_in_bulk(port_token_for_issues, ISSUE_BLUEPRINT, port_issue_entities)

    print("\nIngestion complete!")

if __name__ == "__main__":
    main()
