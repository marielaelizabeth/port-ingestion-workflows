import os
import sys
import requests

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
    for pr in github_prs:
        port_pr_entities.append({
            "identifier": str(pr["number"]), "title": pr["title"],
            "properties": { "url": pr["html_url"], "status": pr["state"], "creator": pr.get("user", {}).get("login"), "createdAt": pr["created_at"], "updatedAt": pr["updated_at"] },
            "relations": { "repository": REPO_ENTITY_IDENTIFIER }
        })
    # Get a fresh token right before we use it
    port_token_for_prs = get_port_api_token()
    upsert_entities_in_bulk(port_token_for_prs, PR_BLUEPRINT, port_pr_entities)

    # --- Process Issues ---
    print("\n--- Processing Issues ---")
    github_issues = fetch_github_data("issues")
    port_issue_entities = []

    # Start the process issue loop
    for issue in github_issues:
        if 'pull_request' in issue: 
            continue # Skip PRs
            
        # This line creates the 'issue_labels' variable for the current issue
        issue_labels = [label['name'] for label in issue.get('labels', [])]
        primary_label_value = issue_labels[0] if issue_labels else "No Label"

        port_issue_entities.append({
            "identifier": str(issue["number"]), "title": issue["title"],
            "properties": { "url": issue["html_url"], "status": issue["state"], "creator": issue.get("user", {}).get("login"), "labels": issue_labels, "primaryLabel": primary_label_value  }, 
            "relations": { "repository": REPO_ENTITY_IDENTIFIER }
        })
    # Get another fresh token right before we use it
    port_token_for_issues = get_port_api_token()
    upsert_entities_in_bulk(port_token_for_issues, ISSUE_BLUEPRINT, port_issue_entities)

    print("\nIngestion complete!")

if __name__ == "__main__":
    main()
