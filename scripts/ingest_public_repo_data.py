import os
import sys
import requests
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

# --- WALMART DUMMY DATA POOLS (DETERMINISTIC) ---
# Active projects are high priority
ACTIVE_PROJECTS = ["Q3_Checkout_Redesign", "Holiday_Scale_Prep", "Mobile_App_Refresh"]
ALL_PROJECTS = ["Q3_Checkout_Redesign", "Holiday_Scale_Prep", "SupplyChain_API_V2", "Mobile_App_Refresh", "Legacy_System_Deprecation", "Data_Warehouse_Migration"]

# Labels for open vs. closed issues
BUG_LABELS = ["bug, sev-2", "bug, performance", "bug, UI"]
FEATURE_LABELS = ["feature-request, Q3", "feature-request, mobile", "tech-debt"]

# Developers for assignment
WALMART_DEVELOPERS = [
    "alex.chen", "brenda.smith", "carlos.garcia", "diana.jones", "ethan.williams",
    "fiona.davis", "greg.miller", "hannah.wilson", "ian.moore", "jenna.taylor"
]

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
    if not entities:
        print(f"No entities to upsert for blueprint {blueprint_id}.")
        return
    headers = {'Authorization': f'Bearer {access_token}'}
    payload = {"entities": entities}
    run_id = str(uuid.uuid4())
    print(f"Upserting {len(entities)} entities to blueprint {blueprint_id} with runId: {run_id}...")
    api_url = f"{PORT_API_URL}/blueprints/{blueprint_id}/entities/bulk?runId={run_id}"
    response = requests.post(api_url, json=payload, headers=headers)
    if not response.ok:
        print(f"Error during bulk upsert for {blueprint_id}: {response.status_code} {response.text}", file=sys.stderr)
    else:
        print(f"Successfully started bulk upsert job for {blueprint_id}.")
    return response

def delete_all_entities_of_blueprint(access_token, blueprint_id):
    print(f"--- DELETING ALL ENTITIES for blueprint: {blueprint_id} ---")
    headers = {'Authorization': f'Bearer {access_token}'}
    delete_payload = {"query": "true"}
    response = requests.delete(f"{PORT_API_URL}/blueprints/{blueprint_id}/entities", json=delete_payload, headers=headers)
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

    print("Authenticating with Port for initial cleanup...")
    cleanup_token = get_port_api_token()
    delete_all_entities_of_blueprint(cleanup_token, PR_BLUEPRINT)
    delete_all_entities_of_blueprint(cleanup_token, ISSUE_BLUEPRINT)

    # --- Process Pull Requests ---
    print("\n--- Processing Pull Requests ---")
    github_prs = fetch_github_data("pulls")
    port_pr_entities = []
    print(f"Found {len(github_prs)} PRs. Now enriching with deterministic data...")

    for i, pr in enumerate(github_prs):
        try:
            assignee_index = i % len(WALMART_DEVELOPERS)
            reviewer_index = (i + 1) % len(WALMART_DEVELOPERS)
            
            pr_assignees = WALMART_DEVELOPERS[assignee_index]
            pr_reviewers = WALMART_DEVELOPERS[reviewer_index]

            port_pr_entities.append({
                "identifier": str(pr["number"]),
                "title": pr["title"],
                "properties": {
                    "url": pr.get("html_url"), "status": pr.get("state"), "creator": pr.get("user", {}).get("login"),
                    "createdAt": pr.get("created_at"), "updatedAt": pr.get("updated_at"),
                    # 👇 CRITICAL FIX: The keys here MUST match the blueprint identifiers
                    "assignTo": pr_assignees,
                    "reviewBy": pr_reviewers
                },
                "relations": { "repository": REPO_ENTITY_IDENTIFIER }
            })
        except Exception as e:
            print(f"Error processing PR #{pr.get('number', 'N/A')}: {e}", file=sys.stderr)
            continue

    print("Finished enriching PRs. Now sending to Port...")
    port_token_for_prs = get_port_api_token()
    upsert_entities_in_bulk(port_token_for_prs, PR_BLUEPRINT, port_pr_entities)

    # --- Process Issues ---
    print("\n--- Processing Issues ---")
    github_issues = fetch_github_data("issues")
    port_issue_entities = []
    print(f"Found {len(github_issues)} issues. Now enriching with deterministic data...")

    for i, issue in enumerate(github_issues):
        if 'pull_request' in issue: continue
        try:
            if issue['state'] == 'open':
                issue_project = ACTIVE_PROJECTS[i % len(ACTIVE_PROJECTS)]
                issue_labels = BUG_LABELS[i % len(BUG_LABELS)] if i % 2 == 0 else FEATURE_LABELS[i % len(FEATURE_LABELS)]
            else:
                issue_project = ALL_PROJECTS[i % len(ALL_PROJECTS)]
                issue_labels = "documentation, done"

            primary_label_value = issue_labels.split(',')[0]
            issue_assignee = WALMART_DEVELOPERS[i % len(WALMART_DEVELOPERS)]

            port_issue_entities.append({
                "identifier": str(issue["number"]),
                "title": issue["title"],
                "properties": {
                    "url": issue.get("html_url"), "status": issue.get("state"), "creator": issue.get("user", {}).get("login"),
                    # 👇 CRITICAL FIX: The keys here MUST match the blueprint identifiers
                    "labels": issue_labels, "primaryLabel": primary_label_value,
                    "assignee": issue_assignee, "project": issue_project
                },
                "relations": { "repository": REPO_ENTITY_IDENTIFIER }
            })
        except Exception as e:
            print(f"Error processing Issue #{issue.get('number', 'N/A')}: {e}", file=sys.stderr)
            continue

    print("Finished enriching issues. Now sending to Port...")
    port_token_for_issues = get_port_api_token()
    upsert_entities_in_bulk(port_token_for_issues, ISSUE_BLUEPRINT, port_issue_entities)

    print("\nIngestion complete!")

if __name__ == "__main__":
    main()
