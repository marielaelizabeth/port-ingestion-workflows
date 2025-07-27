import os
import sys
import requests
import random
import uuid 
from datetime import datetime

# --- Configuration ---
PORT_CLIENT_ID = os.getenv("PORT_CLIENT_ID")
PORT_CLIENT_SECRET = os.getenv("PORT_CLIENT_SECRET")
GH_TOKEN = os.getenv("GH_PAT")

TARGET_REPO = "microsoft/PowerToys"
REPO_ENTITY_IDENTIFIER = "PowerToys"

PORT_API_URL = "https://api.getport.io/v1"
PR_BLUEPRINT = "githubPullRequest"
ISSUE_BLUEPRINT = "githubIssue"
# VULNERABILITY_BLUEPRINT = "githubDependabotAlert" # We are not using this in this script yet

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
    print("Requesting new Port API access token...")
    credentials = {'clientId': PORT_CLIENT_ID, 'clientSecret': PORT_CLIENT_SECRET}
    token_response = requests.post(f"{PORT_API_URL}/auth/access_token", json=credentials)
    token_response.raise_for_status()
    print("Successfully received new token.")
    return token_response.json()['accessToken']

def fetch_github_data(endpoint):
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
    """
    Creates or updates entities in Port using the Bulk API with a unique runId.
    """
    if not entities:
        print(f"No entities to upsert for blueprint {blueprint_id}.")
        return

    headers = {'Authorization': f'Bearer {access_token}'}
    payload = {"entities": entities}
    
    # Generate a new, unique runId for every single API call
    run_id = str(uuid.uuid4())
    
    print(f"Upserting {len(entities)} entities to blueprint {blueprint_id} with runId: {run_id}...")
    
    # Add the runId as a query parameter to the URL
    api_url = f"{PORT_API_URL}/blueprints/{blueprint_id}/entities/bulk?runId={run_id}"
    
    response = requests.post(
        api_url, # Use the new URL with the runId
        json=payload,
        headers=headers
    )
    
    if not response.ok:
        print(f"Error during bulk upsert for {blueprint_id}: {response.status_code} {response.text}", file=sys.stderr)
    else:
        print(f"Successfully started bulk upsert job for {blueprint_id}.")
    return response

def delete_all_entities_of_blueprint(access_token, blueprint_id):
    """
    Deletes all entities associated with a specific blueprint.
    This is a destructive operation used to ensure a clean slate.
    """
    print(f"--- DELETING ALL ENTITIES for blueprint: {blueprint_id} ---")
    headers = {'Authorization': f'Bearer {access_token}'}
    
    # We construct a query to delete all entities of the blueprint
    # The jq query 'true' selects every entity.
    delete_payload = {
        "query": "true"
    }
    
    response = requests.delete(
        f"{PORT_API_URL}/blueprints/{blueprint_id}/entities",
        json=delete_payload,
        headers=headers
    )
    
    if not response.ok:
        print(f"Error during entity deletion for {blueprint_id}: {response.status_code} {response.text}", file=sys.stderr)
    else:
        print(f"✅ Successfully deleted all existing entities for {blueprint_id}.")
    return response

# --- Main Logic ---

def main():
    if not all([PORT_CLIENT_ID, PORT_CLIENT_SECRET, GH_TOKEN]):
        print("Error: Missing required secrets. Please check repository configuration.", file=sys.stderr)
        sys.exit(1)

    # =========================================================================
    # 👇 START OF NEW "INITIAL CLEANUP" SECTION
    # =========================================================================
    print("Authenticating with Port for initial cleanup...")
    # --- We get a token once here just for the delete operations
    cleanup_token = get_port_api_token() 

    # --- Call the delete function for both blueprints before doing anything else
    delete_all_entities_of_blueprint(cleanup_token, PR_BLUEPRINT)
    delete_all_entities_of_blueprint(cleanup_token, ISSUE_BLUEPRINT)
    # =========================================================================
    # 👆 END OF NEW "INITIAL CLEANUP" SECTION
    # =========================================================================    

    # --- Capture a unique timestamp for this specific workflow run
    ingestion_timestamp = datetime.utcnow().isoformat() + "Z"
    print(f"Starting ingestion with unique timestamp: {ingestion_timestamp}")
    
    # --- Process Pull Requests ---
    print("\n--- Processing Pull Requests ---")
    github_prs = fetch_github_data("pulls")
    port_pr_entities = []
    print(f"Found {len(github_prs)} PRs. Now enriching with dummy data...")

    for i, pr in enumerate(github_prs):
        try:
            # Randomly assign reviewers and an assignee INSIDE the loop
            
            # pr_assignees = [random.choice(WALMART_DEVELOPERS)]
            # pr_reviewers = random.choices(WALMART_DEVELOPERS, k=random.randint(1, 2)) # Use safer 'choices'
            pr_assignees = "alex.chen"
            pr_reviewers = "carlos.garcia"            

            port_pr_entities.append({
                "identifier": str(pr["number"]),
                "title": pr["title"],
                "properties": {
                    "url": pr["html_url"], "status": pr["state"], "creator": pr.get("user", {}).get("login"),
                    "createdAt": pr["created_at"], "updatedAt": pr["updated_at"],
                    "assignees": pr_assignees, "reviewers": pr_reviewers
                },
                "relations": { "repository": REPO_ENTITY_IDENTIFIER }
            })
            if (i + 1) % 500 == 0:
                print(f"Processed {i + 1}/{len(github_prs)} PRs...")
        except Exception as e:
            print(f"Error processing PR #{pr.get('number', 'N/A')}: {e}", file=sys.stderr)
            continue # Skip this PR and continue

    print("Finished enriching PRs. Now sending to Port...")
    port_token_for_prs = get_port_api_token()
    upsert_entities_in_bulk(port_token_for_prs, PR_BLUEPRINT, port_pr_entities)

    # --- Process Issues ---
    print("\n--- Processing Issues ---")
    github_issues = fetch_github_data("issues")
    port_issue_entities = []
    print(f"Found {len(github_issues)} issues. Now enriching with dummy data...")

    for i, issue in enumerate(github_issues):
        if 'pull_request' in issue:
            continue
        try:
            # Randomly assign labels, a primary label, assignee, and project INSIDE the loop
            num_labels = random.randint(1, 2)
            issue_labels = random.choices(WALMART_LABELS, k=num_labels) # Use safer 'choices'
            primary_label_value = issue_labels[0]
            
            # issue_assignee = random.choice(WALMART_DEVELOPERS)
            # issue_project = random.choice(WALMART_PROJECTS)
            issue_assignee = "diana.jones"
            issue_project = "Q3_Checkout_Redesign"

            port_issue_entities.append({
                "identifier": str(issue["number"]),
                "title": issue["title"],
                "properties": {
                    "url": issue["html_url"], "status": issue["state"], "creator": issue.get("user", {}).get("login"),
                    "labels": issue_labels, "primaryLabel": primary_label_value,
                    "assignee": issue_assignee, "project": issue_project
                },
                "relations": { "repository": REPO_ENTITY_IDENTIFIER }
            })
            if (i + 1) % 500 == 0:
                print(f"Processed {i + 1}/{len(github_issues)} issues...")
        except Exception as e:
            print(f"Error processing Issue #{issue.get('number', 'N/A')}: {e}", file=sys.stderr)
            continue # Skip this issue and continue

    print("Finished enriching issues. Now sending to Port...")
    port_token_for_issues = get_port_api_token()
    upsert_entities_in_bulk(port_token_for_issues, ISSUE_BLUEPRINT, port_issue_entities)

    print("\nIngestion complete!")

if __name__ == "__main__":
    main()
